package ratelimit_test

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/Rishabh-10/distributed-rate-limiter/internal/config"
	"github.com/Rishabh-10/distributed-rate-limiter/internal/httpapi"
	"github.com/Rishabh-10/distributed-rate-limiter/internal/limiter"
	"github.com/Rishabh-10/distributed-rate-limiter/internal/observability"
	"github.com/Rishabh-10/distributed-rate-limiter/internal/quota"
	"github.com/Rishabh-10/distributed-rate-limiter/pkg/ratelimit"
)

// startLimiter runs the real service over miniredis and returns its URL. These
// tests exercise the whole path — middleware, HTTP, handler, sorted set — so a
// mismatch anywhere between client and server shows up here.
func startLimiter(t *testing.T, quotaYAML string) string {
	t.Helper()

	cfg, err := config.Parse([]byte(quotaYAML))
	if err != nil {
		t.Fatalf("parse config: %v", err)
	}
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rdb.Close() })

	handler := httpapi.NewServer(httpapi.Deps{
		Quotas: quota.NewStore(quota.New(cfg.Limiter)),
		Limiter: limiter.NewDispatcher(map[string]limiter.Algorithm{
			limiter.AlgorithmSlidingWindow: limiter.NewSlidingWindow(rdb),
		}),
		Metrics:   observability.NewMetrics(),
		Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
		KeyPrefix: "rl",
		Health:    func(context.Context) error { return rdb.Ping(context.Background()).Err() },
	}).Handler()

	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	return srv.URL
}

const threePerMinute = `
limiter:
  default:
    algorithm: sliding_window
    limit: 3
    window: 1m
`

func okHandler() (http.Handler, *int) {
	calls := 0
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("upstream"))
	}), &calls
}

func get(t *testing.T, h http.Handler, path, clientID string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	if clientID != "" {
		req.Header.Set("X-Client-ID", clientID)
	}
	req.RemoteAddr = "192.0.2.10:1234"
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestMiddlewareLimitsAndReturns429(t *testing.T) {
	url := startLimiter(t, threePerMinute)
	upstream, calls := okHandler()
	h := ratelimit.New(url, ratelimit.WithTimeout(2*time.Second)).Middleware(upstream)

	for i := 1; i <= 3; i++ {
		rec := get(t, h, "/api/hello", "alice")
		if rec.Code != http.StatusOK {
			t.Fatalf("request %d: status = %d, want 200 (body %s)", i, rec.Code, rec.Body)
		}
		if got := rec.Header().Get("X-RateLimit-Remaining"); got == "" {
			t.Errorf("request %d: rate limit headers not forwarded", i)
		}
	}

	rec := get(t, h, "/api/hello", "alice")
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("fourth request: status = %d, want 429", rec.Code)
	}
	if got := rec.Header().Get("Retry-After"); got != "60" {
		t.Errorf("Retry-After = %q, want 60", got)
	}
	if got := rec.Header().Get("X-RateLimit-Limit"); got != "3" {
		t.Errorf("X-RateLimit-Limit = %q, want 3", got)
	}
	if *calls != 3 {
		t.Errorf("upstream was called %d times, want 3 — the rejected request must not reach it", *calls)
	}
}

// The quota is per client and per resource, and the middleware derives both
// from the request rather than the caller having to say so.
func TestMiddlewareDerivesClientAndResource(t *testing.T) {
	url := startLimiter(t, threePerMinute)
	upstream, _ := okHandler()
	h := ratelimit.New(url, ratelimit.WithTimeout(2*time.Second)).Middleware(upstream)

	for i := 0; i < 3; i++ {
		get(t, h, "/api/hello", "alice")
	}
	if rec := get(t, h, "/api/hello", "alice"); rec.Code != http.StatusTooManyRequests {
		t.Fatalf("alice should be exhausted, got %d", rec.Code)
	}
	if rec := get(t, h, "/api/hello", "bob"); rec.Code != http.StatusOK {
		t.Errorf("bob got %d, want 200 — alice's usage must not affect him", rec.Code)
	}
	if rec := get(t, h, "/api/other", "alice"); rec.Code != http.StatusOK {
		t.Errorf("alice on a different path got %d, want 200", rec.Code)
	}
}

// Identity falls back to the peer address so that anonymous traffic is still
// limited, and forwarded headers are not trusted by default.
func TestMiddlewareFallsBackToPeerAddress(t *testing.T) {
	url := startLimiter(t, threePerMinute)
	upstream, _ := okHandler()
	h := ratelimit.New(url, ratelimit.WithTimeout(2*time.Second)).Middleware(upstream)

	for i := 0; i < 3; i++ {
		if rec := get(t, h, "/api/hello", ""); rec.Code != http.StatusOK {
			t.Fatalf("anonymous request %d: status = %d, want 200", i, rec.Code)
		}
	}
	if rec := get(t, h, "/api/hello", ""); rec.Code != http.StatusTooManyRequests {
		t.Errorf("anonymous caller was not limited: status = %d, want 429", rec.Code)
	}
}

// An unreachable limiter must not take the protected service down with it.
func TestMiddlewareFailsOpenWhenLimiterIsUnreachable(t *testing.T) {
	upstream, calls := okHandler()
	var errs []error
	// Port 1 is reserved; nothing is listening.
	h := ratelimit.New("http://127.0.0.1:1",
		ratelimit.WithTimeout(100*time.Millisecond),
		ratelimit.WithErrorHandler(func(_ *http.Request, err error) { errs = append(errs, err) }),
	).Middleware(upstream)

	rec := get(t, h, "/api/hello", "alice")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: an unreachable limiter must fail open", rec.Code)
	}
	if *calls != 1 {
		t.Errorf("upstream called %d times, want 1", *calls)
	}
	if len(errs) != 1 {
		t.Errorf("error handler called %d times, want 1 — failures must stay visible", len(errs))
	}
}

func TestMiddlewareFailsClosedWhenConfigured(t *testing.T) {
	upstream, calls := okHandler()
	h := ratelimit.New("http://127.0.0.1:1",
		ratelimit.WithTimeout(100*time.Millisecond),
		ratelimit.WithFailClosed(),
	).Middleware(upstream)

	rec := get(t, h, "/api/hello", "alice")
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", rec.Code)
	}
	if *calls != 0 {
		t.Errorf("upstream called %d times, want 0", *calls)
	}
}

// A slow limiter is treated as an absent one rather than being waited on.
func TestMiddlewareTimesOutSlowLimiter(t *testing.T) {
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(2 * time.Second):
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(slow.Close)

	upstream, calls := okHandler()
	h := ratelimit.New(slow.URL, ratelimit.WithTimeout(50*time.Millisecond)).Middleware(upstream)

	start := time.Now()
	rec := get(t, h, "/api/hello", "alice")
	elapsed := time.Since(start)

	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200 (fail open)", rec.Code)
	}
	if *calls != 1 {
		t.Errorf("upstream called %d times, want 1", *calls)
	}
	if elapsed > time.Second {
		t.Errorf("request waited %s on a slow limiter, want the timeout to bound it", elapsed)
	}
}

func TestMiddlewareSkipFunc(t *testing.T) {
	url := startLimiter(t, "limiter:\n  default:\n    limit: 1\n    window: 1m\n")
	upstream, calls := okHandler()
	h := ratelimit.New(url,
		ratelimit.WithTimeout(2*time.Second),
		ratelimit.WithSkipFunc(func(r *http.Request) bool { return r.URL.Path == "/healthz" }),
	).Middleware(upstream)

	for i := 0; i < 5; i++ {
		if rec := get(t, h, "/healthz", "alice"); rec.Code != http.StatusOK {
			t.Fatalf("health check %d was limited: status = %d", i, rec.Code)
		}
	}
	if *calls != 5 {
		t.Errorf("upstream called %d times, want 5", *calls)
	}
}

func TestMiddlewareCustomDenyHandler(t *testing.T) {
	url := startLimiter(t, "limiter:\n  default:\n    limit: 1\n    window: 1m\n")
	upstream, _ := okHandler()
	deny := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte("slow down"))
	})
	h := ratelimit.New(url,
		ratelimit.WithTimeout(2*time.Second),
		ratelimit.WithDenyHandler(deny),
	).Middleware(upstream)

	get(t, h, "/api", "alice")
	rec := get(t, h, "/api", "alice")
	if rec.Code != http.StatusServiceUnavailable || rec.Body.String() != "slow down" {
		t.Errorf("deny response = %d %q, want the custom handler's", rec.Code, rec.Body)
	}
	if rec.Header().Get("X-RateLimit-Limit") == "" {
		t.Error("rate limit headers should already be set when the deny handler runs")
	}
}

func TestMiddlewareCustomIdentityAndCost(t *testing.T) {
	url := startLimiter(t, "limiter:\n  default:\n    limit: 4\n    window: 1m\n")
	upstream, _ := okHandler()
	h := ratelimit.New(url,
		ratelimit.WithTimeout(2*time.Second),
		ratelimit.WithClientIDFunc(func(r *http.Request) string { return r.Header.Get("X-Api-Key") }),
		ratelimit.WithResourceFunc(func(*http.Request) string { return "shared" }),
		ratelimit.WithCostFunc(func(*http.Request) int64 { return 2 }),
	).Middleware(upstream)

	req := func(key, path string) int {
		r := httptest.NewRequest(http.MethodGet, path, nil)
		r.Header.Set("X-Api-Key", key)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, r)
		return rec.Code
	}

	// Two requests at cost 2 exhaust a limit of 4, and the resource function
	// collapses distinct paths onto one bucket.
	if code := req("k1", "/a"); code != http.StatusOK {
		t.Fatalf("first request: %d", code)
	}
	if code := req("k1", "/b"); code != http.StatusOK {
		t.Fatalf("second request: %d", code)
	}
	if code := req("k1", "/c"); code != http.StatusTooManyRequests {
		t.Errorf("third request: %d, want 429", code)
	}
	if code := req("k2", "/a"); code != http.StatusOK {
		t.Errorf("a different key got %d, want 200", code)
	}
}

func TestCheckRequiresClientID(t *testing.T) {
	c := ratelimit.New("http://127.0.0.1:1")
	if _, err := c.Check(context.Background(), ratelimit.Request{}); err == nil {
		t.Error("Check with no ClientID succeeded, want an error")
	}
}

func TestCheckReportsServerErrors(t *testing.T) {
	broken := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "nope", http.StatusInternalServerError)
	}))
	t.Cleanup(broken.Close)

	c := ratelimit.New(broken.URL, ratelimit.WithTimeout(time.Second))
	if _, err := c.Check(context.Background(), ratelimit.Request{ClientID: "alice"}); err == nil {
		t.Error("Check succeeded against a broken limiter, want an error")
	}
}

// A trailing slash on the base URL is an easy mistake and must not produce
// //v1/check.
func TestNewTrimsTrailingSlash(t *testing.T) {
	url := startLimiter(t, threePerMinute)
	c := ratelimit.New(url+"/", ratelimit.WithTimeout(2*time.Second))
	if _, err := c.Check(context.Background(), ratelimit.Request{ClientID: "alice"}); err != nil {
		t.Errorf("Check: %v", err)
	}
}
