//go:build integration

package integration

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Rishabh-10/distributed-rate-limiter/internal/breaker"
	"github.com/Rishabh-10/distributed-rate-limiter/internal/config"
	"github.com/Rishabh-10/distributed-rate-limiter/internal/httpapi"
	"github.com/Rishabh-10/distributed-rate-limiter/internal/limiter"
	"github.com/Rishabh-10/distributed-rate-limiter/internal/observability"
	"github.com/Rishabh-10/distributed-rate-limiter/internal/quota"
	"github.com/Rishabh-10/distributed-rate-limiter/internal/redisx"
	"github.com/Rishabh-10/distributed-rate-limiter/pkg/ratelimit"
)

// startService runs the real HTTP service against real Redis and returns its
// URL. Everything from here down is the deployed path: client, HTTP, handler,
// guard, sorted set.
func startService(t *testing.T, addr, quotaYAML string) string {
	t.Helper()

	cfg, err := config.Parse([]byte(quotaYAML))
	if err != nil {
		t.Fatalf("parse config: %v", err)
	}
	rdb := redisx.New(config.Redis{
		Addr:           addr,
		CommandTimeout: config.Duration(500 * time.Millisecond),
		DialTimeout:    config.Duration(500 * time.Millisecond),
		PoolSize:       50,
		KeyPrefix:      "rl",
	})
	t.Cleanup(func() { rdb.Close() })

	cb := breaker.New(5, time.Second)
	guarded := limiter.NewGuard(
		limiter.NewDispatcher(map[string]limiter.Algorithm{
			limiter.AlgorithmSlidingWindow: limiter.NewSlidingWindow(rdb),
		}),
		cfg.Limiter.FailOpen,
		limiter.WithBreaker(cb),
		limiter.WithTimeout(cfg.Limiter.DecisionTimeout.Std()),
		limiter.WithUnavailableFunc(redisx.IsUnavailable),
	)

	srv := httptest.NewServer(httpapi.NewServer(httpapi.Deps{
		Quotas:       quota.NewStore(quota.New(cfg.Limiter)),
		Limiter:      guarded,
		Metrics:      observability.NewMetrics(),
		Logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
		KeyPrefix:    "rl-itest-" + t.Name(),
		Health:       func(ctx context.Context) error { return rdb.Ping(ctx).Err() },
		BreakerState: func() string { return string(cb.State()) },
	}).Handler())
	t.Cleanup(srv.Close)
	return srv.URL
}

// The end-to-end path a real adopter gets: wrap a handler, receive 429s.
func TestEndToEndThroughMiddleware(t *testing.T) {
	dialRedis(t, redisAddr())
	url := startService(t, redisAddr(), `
limiter:
  default:
    algorithm: sliding_window
    limit: 5
    window: 1m
`)

	var served atomic.Int64
	upstream := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		served.Add(1)
		w.Write([]byte("ok"))
	})
	handler := ratelimit.New(url, ratelimit.WithTimeout(2*time.Second)).Middleware(upstream)

	// A fresh identity per run: the window outlives the test, so reusing a
	// fixed client ID would leave the next run starting from an exhausted
	// quota.
	clientID := uniqueKey(t)

	app := httptest.NewServer(handler)
	defer app.Close()

	client := &http.Client{Timeout: 5 * time.Second}
	var codes []int
	for i := 0; i < 8; i++ {
		req, _ := http.NewRequest(http.MethodGet, app.URL+"/api/hello", nil)
		req.Header.Set("X-Client-ID", clientID)
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		codes = append(codes, resp.StatusCode)

		if resp.StatusCode == http.StatusTooManyRequests && resp.Header.Get("Retry-After") == "" {
			t.Errorf("request %d: 429 without a Retry-After header", i)
		}
	}

	for i, code := range codes {
		want := http.StatusOK
		if i >= 5 {
			want = http.StatusTooManyRequests
		}
		if code != want {
			t.Errorf("request %d: status = %d, want %d (all: %v)", i, code, want, codes)
		}
	}
	if got := served.Load(); got != 5 {
		t.Errorf("upstream served %d requests, want 5", got)
	}
}

// Many callers hitting the HTTP service at once must still see the exact
// quota — the transaction's guarantee has to survive the whole stack, not just
// the algorithm in isolation.
func TestServiceIsExactUnderConcurrentHTTPLoad(t *testing.T) {
	dialRedis(t, redisAddr())
	url := startService(t, redisAddr(), `
limiter:
  default:
    algorithm: sliding_window
    limit: 50
    window: 1m
`)

	client := ratelimit.New(url,
		ratelimit.WithHTTPClient(&http.Client{Timeout: 10 * time.Second}))

	clientID := uniqueKey(t)
	const requests = 200
	var allowed, denied, failed atomic.Int64
	start := make(chan struct{})
	var wg sync.WaitGroup

	for i := 0; i < requests; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			d, err := client.Check(context.Background(), ratelimit.Request{
				ClientID: clientID,
				Resource: "/api",
			})
			switch {
			case err != nil:
				failed.Add(1)
			case d.Allowed:
				allowed.Add(1)
			default:
				denied.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()

	if got := failed.Load(); got != 0 {
		t.Fatalf("%d checks failed, want none", got)
	}
	if got := allowed.Load(); got != 50 {
		t.Errorf("allowed %d of %d concurrent requests, want exactly 50", got, requests)
	}
	if got := denied.Load(); got != requests-50 {
		t.Errorf("denied %d, want %d", got, requests-50)
	}
}

// Readiness tells the truth about Redis while the data path keeps serving:
// the two endpoints answer different questions on purpose.
func TestReadinessReportsRedisWhileCheckKeepsServing(t *testing.T) {
	dialRedis(t, redisAddr())
	p := newProxy(t, redisAddr())
	url := startService(t, p.Addr(), `
limiter:
  fail_open: true
  default:
    limit: 5
    window: 1m
`)

	client := &http.Client{Timeout: 5 * time.Second}

	resp, err := client.Get(url + "/readyz")
	if err != nil {
		t.Fatalf("readyz: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("readyz while healthy = %d, want 200", resp.StatusCode)
	}

	p.Close()

	resp, err = client.Get(url + "/readyz")
	if err != nil {
		t.Fatalf("readyz: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("readyz during outage = %d, want 503", resp.StatusCode)
	}

	// Meanwhile the data path still answers, degraded.
	body := strings.NewReader(`{"client_id":"someone","resource":"/api"}`)
	resp, err = client.Post(url+"/v1/check", "application/json", body)
	if err != nil {
		t.Fatalf("check during outage: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("check during outage = %d, want 200", resp.StatusCode)
	}
	if got := resp.Header.Get("X-RateLimit-Degraded"); got != "true" {
		t.Errorf("X-RateLimit-Degraded = %q, want true", got)
	}

	// Liveness must stay green throughout: the process is fine, its dependency
	// is not.
	resp, err = client.Get(url + "/healthz")
	if err != nil {
		t.Fatalf("healthz: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("healthz during outage = %d, want 200", resp.StatusCode)
	}
}
