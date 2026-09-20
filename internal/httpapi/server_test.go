package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/Rishabh-10/distributed-rate-limiter/internal/config"
	"github.com/Rishabh-10/distributed-rate-limiter/internal/limiter"
	"github.com/Rishabh-10/distributed-rate-limiter/internal/observability"
	"github.com/Rishabh-10/distributed-rate-limiter/internal/quota"
)

const testConfig = `
limiter:
  default:
    algorithm: sliding_window
    limit: 3
    window: 1m
  clients:
    acme:
      limit: 100
  resources:
    "/search":
      limit: 1
      window: 1s
  overrides:
    - client: acme
      resource: "/search"
      limit: 50
      window: 1s
`

// stubLimiter returns a canned decision, for the paths that have nothing to do
// with Redis.
type stubLimiter struct {
	decision limiter.Decision
	err      error
	panics   bool
}

func (s stubLimiter) Allow(context.Context, string, limiter.Rule, int64) (limiter.Decision, error) {
	if s.panics {
		panic("boom")
	}
	return s.decision, s.err
}

func newTestServer(t *testing.T, algo limiter.Algorithm, deps ...func(*Deps)) http.Handler {
	t.Helper()
	cfg, err := config.Parse([]byte(testConfig))
	if err != nil {
		t.Fatalf("parse test config: %v", err)
	}
	d := Deps{
		Quotas:    quota.NewStore(quota.New(cfg.Limiter)),
		Limiter:   algo,
		Metrics:   observability.NewMetrics(),
		Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
		KeyPrefix: "rl",
	}
	for _, fn := range deps {
		fn(&d)
	}
	return NewServer(d).Handler()
}

// realLimiter wires the actual sliding window over miniredis, so the handler
// is tested against the algorithm it really uses.
func realLimiter(t *testing.T) limiter.Algorithm {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rdb.Close() })
	return limiter.NewDispatcher(map[string]limiter.Algorithm{
		limiter.AlgorithmSlidingWindow: limiter.NewSlidingWindow(rdb),
	})
}

func postCheck(t *testing.T, h http.Handler, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/v1/check", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func decodeCheck(t *testing.T, rec *httptest.ResponseRecorder) CheckResponse {
	t.Helper()
	var resp CheckResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response %q: %v", rec.Body.String(), err)
	}
	return resp
}

func TestCheckEnforcesLimitAndReportsHeaders(t *testing.T) {
	h := newTestServer(t, realLimiter(t))

	for i := 1; i <= 3; i++ {
		rec := postCheck(t, h, `{"client_id":"alice","resource":"/api"}`)
		if rec.Code != http.StatusOK {
			t.Fatalf("request %d: status = %d, want 200", i, rec.Code)
		}
		resp := decodeCheck(t, rec)
		if !resp.Allowed {
			t.Fatalf("request %d denied, want allowed", i)
		}
		if resp.Limit != 3 {
			t.Errorf("request %d: limit = %d, want 3", i, resp.Limit)
		}
		if want := int64(3 - i); resp.Remaining != want {
			t.Errorf("request %d: remaining = %d, want %d", i, resp.Remaining, want)
		}
		if resp.QuotaSource != string(quota.SourceDefault) {
			t.Errorf("request %d: quota_source = %q, want default", i, resp.QuotaSource)
		}
		wantRemaining := strconv.FormatInt(int64(3-i), 10)
		if got := rec.Header().Get("X-RateLimit-Remaining"); got != wantRemaining {
			t.Errorf("request %d: X-RateLimit-Remaining = %q, want %q", i, got, wantRemaining)
		}
		if got := rec.Header().Get("Retry-After"); got != "" {
			t.Errorf("request %d: Retry-After = %q on an allowed request, want none", i, got)
		}
	}

	// The decision API answers 200 even when denying: turning the denial into
	// a 429 is the caller's job, since the caller owns the limited request.
	rec := postCheck(t, h, `{"client_id":"alice","resource":"/api"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("denied request: status = %d, want 200", rec.Code)
	}
	resp := decodeCheck(t, rec)
	if resp.Allowed {
		t.Error("fourth request allowed, want denied")
	}
	if resp.RetryAfterMs <= 0 {
		t.Errorf("retry_after_ms = %d, want a positive hint", resp.RetryAfterMs)
	}
	if got := rec.Header().Get("Retry-After"); got != "60" {
		t.Errorf("Retry-After = %q, want 60", got)
	}
	if got := rec.Header().Get("X-RateLimit-Remaining"); got != "0" {
		t.Errorf("X-RateLimit-Remaining = %q, want 0", got)
	}
}

// Quotas are per client and per resource: one client exhausting one route must
// not affect anyone else.
func TestCheckIsolatesClientsAndResources(t *testing.T) {
	h := newTestServer(t, realLimiter(t))

	for i := 0; i < 3; i++ {
		postCheck(t, h, `{"client_id":"alice","resource":"/api"}`)
	}
	if resp := decodeCheck(t, postCheck(t, h, `{"client_id":"alice","resource":"/api"}`)); resp.Allowed {
		t.Fatal("alice should be exhausted on /api")
	}
	if resp := decodeCheck(t, postCheck(t, h, `{"client_id":"bob","resource":"/api"}`)); !resp.Allowed {
		t.Error("bob was denied by alice's usage")
	}
	if resp := decodeCheck(t, postCheck(t, h, `{"client_id":"alice","resource":"/other"}`)); !resp.Allowed {
		t.Error("alice was denied on a resource she had not used")
	}
}

func TestCheckAppliesQuotaPrecedence(t *testing.T) {
	h := newTestServer(t, realLimiter(t))

	tests := []struct {
		body       string
		wantLimit  int64
		wantSource string
	}{
		{`{"client_id":"acme","resource":"/search"}`, 50, "override"},
		{`{"client_id":"acme","resource":"/other"}`, 100, "client"},
		{`{"client_id":"stranger","resource":"/search"}`, 1, "resource"},
		{`{"client_id":"stranger","resource":"/other"}`, 3, "default"},
	}
	for _, tt := range tests {
		resp := decodeCheck(t, postCheck(t, h, tt.body))
		if resp.Limit != tt.wantLimit || resp.QuotaSource != tt.wantSource {
			t.Errorf("%s -> limit=%d source=%s, want limit=%d source=%s",
				tt.body, resp.Limit, resp.QuotaSource, tt.wantLimit, tt.wantSource)
		}
	}
}

func TestCheckCost(t *testing.T) {
	h := newTestServer(t, realLimiter(t))

	resp := decodeCheck(t, postCheck(t, h, `{"client_id":"alice","resource":"/api","cost":3}`))
	if !resp.Allowed || resp.Remaining != 0 {
		t.Fatalf("cost-3 request: allowed=%v remaining=%d, want true/0", resp.Allowed, resp.Remaining)
	}
	if resp := decodeCheck(t, postCheck(t, h, `{"client_id":"alice","resource":"/api"}`)); resp.Allowed {
		t.Error("request after a full-cost charge was allowed")
	}
}

func TestCheckRejectsBadRequests(t *testing.T) {
	h := newTestServer(t, stubLimiter{decision: limiter.Decision{Allowed: true}})

	tests := []struct {
		name     string
		body     string
		wantCode int
		wantErr  string
	}{
		{"missing client_id", `{"resource":"/api"}`, http.StatusBadRequest, CodeBadRequest},
		{"empty body", ``, http.StatusBadRequest, CodeBadRequest},
		{"malformed json", `{"client_id":`, http.StatusBadRequest, CodeBadRequest},
		{"unknown field", `{"client_id":"a","clint_id":"typo"}`, http.StatusBadRequest, CodeBadRequest},
		{"negative cost", `{"client_id":"a","cost":-1}`, http.StatusBadRequest, CodeBadRequest},
		{"oversized body", `{"client_id":"` + strings.Repeat("x", maxBodyBytes) + `"}`,
			http.StatusRequestEntityTooLarge, CodePayloadLarge},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := postCheck(t, h, tt.body)
			if rec.Code != tt.wantCode {
				t.Fatalf("status = %d, want %d (body %s)", rec.Code, tt.wantCode, rec.Body)
			}
			var resp ErrorResponse
			if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
				t.Fatalf("decode error body %q: %v", rec.Body, err)
			}
			if resp.Error.Code != tt.wantErr {
				t.Errorf("error code = %q, want %q", resp.Error.Code, tt.wantErr)
			}
		})
	}
}

func TestCheckRejectsWrongMethod(t *testing.T) {
	h := newTestServer(t, stubLimiter{})
	req := httptest.NewRequest(http.MethodGet, "/v1/check", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want 405", rec.Code)
	}
}

// A degraded decision must be visibly degraded: an allow that was never
// enforced should not look like an ordinary allow.
func TestCheckSurfacesDegradedDecisions(t *testing.T) {
	h := newTestServer(t, stubLimiter{decision: limiter.Decision{
		Allowed: true, Limit: 3, Remaining: 3, Degraded: true,
	}})

	rec := postCheck(t, h, `{"client_id":"alice"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if got := rec.Header().Get("X-RateLimit-Degraded"); got != "true" {
		t.Errorf("X-RateLimit-Degraded = %q, want true", got)
	}
	if resp := decodeCheck(t, rec); !resp.Degraded || !resp.Allowed {
		t.Errorf("degraded=%v allowed=%v, want both true", resp.Degraded, resp.Allowed)
	}
}

// An error that reaches the handler is a real failure, not an outage that was
// already handled by fail-open. It must not be reported as an allow.
func TestCheckReportsLimiterFailure(t *testing.T) {
	h := newTestServer(t, stubLimiter{err: errors.New("no implementation registered")})

	rec := postCheck(t, h, `{"client_id":"alice"}`)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
	var resp ErrorResponse
	json.Unmarshal(rec.Body.Bytes(), &resp)
	if resp.Error.Code != CodeUnavailable {
		t.Errorf("error code = %q, want %q", resp.Error.Code, CodeUnavailable)
	}
}

func TestPanicIsRecovered(t *testing.T) {
	h := newTestServer(t, stubLimiter{panics: true})

	rec := postCheck(t, h, `{"client_id":"alice"}`)
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", rec.Code)
	}
}

func TestQuotaEndpoint(t *testing.T) {
	h := newTestServer(t, stubLimiter{})

	req := httptest.NewRequest(http.MethodGet, "/v1/quotas/acme?resource=/search", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var resp QuotaResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Limit != 50 || resp.Source != "override" || resp.WindowMs != 1000 {
		t.Errorf("quota = %+v, want limit 50 from an override over a 1s window", resp)
	}
}

// A reload swaps the quota table under a running server.
func TestQuotaEndpointReflectsReload(t *testing.T) {
	cfg, err := config.Parse([]byte(testConfig))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	store := quota.NewStore(quota.New(cfg.Limiter))
	h := newTestServer(t, stubLimiter{}, func(d *Deps) { d.Quotas = store })

	next, err := config.Parse([]byte("limiter:\n  clients:\n    acme:\n      limit: 7\n"))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	store.Set(quota.New(next.Limiter))

	req := httptest.NewRequest(http.MethodGet, "/v1/quotas/acme", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	var resp QuotaResponse
	json.Unmarshal(rec.Body.Bytes(), &resp)
	if resp.Limit != 7 {
		t.Errorf("limit after reload = %d, want 7", resp.Limit)
	}
}

// Liveness must not depend on Redis: restarting a healthy process because its
// datastore is down turns one outage into two.
func TestHealthzIgnoresRedis(t *testing.T) {
	h := newTestServer(t, stubLimiter{}, func(d *Deps) {
		d.Health = func(context.Context) error { return errors.New("connection refused") }
	})

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200 even with Redis down", rec.Code)
	}
}

func TestReadyzReportsRedis(t *testing.T) {
	t.Run("healthy", func(t *testing.T) {
		h := newTestServer(t, stubLimiter{}, func(d *Deps) {
			d.Health = func(context.Context) error { return nil }
			d.BreakerState = func() string { return "closed" }
		})
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", rec.Code)
		}
		var resp HealthResponse
		json.Unmarshal(rec.Body.Bytes(), &resp)
		if resp.Status != "ok" || resp.Breaker != "closed" {
			t.Errorf("body = %+v, want ok/closed", resp)
		}
	})

	t.Run("redis down", func(t *testing.T) {
		h := newTestServer(t, stubLimiter{}, func(d *Deps) {
			d.Health = func(context.Context) error { return errors.New("connection refused") }
			d.BreakerState = func() string { return "open" }
		})
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want 503", rec.Code)
		}
		var resp HealthResponse
		json.Unmarshal(rec.Body.Bytes(), &resp)
		if resp.Status != "degraded" || resp.Breaker != "open" {
			t.Errorf("body = %+v, want degraded/open", resp)
		}
	})

	t.Run("slow redis does not hang the probe", func(t *testing.T) {
		h := newTestServer(t, stubLimiter{}, func(d *Deps) {
			d.HealthTimeout = 20 * time.Millisecond
			d.Health = func(ctx context.Context) error {
				<-ctx.Done()
				return ctx.Err()
			}
		})
		start := time.Now()
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
		if elapsed := time.Since(start); elapsed > time.Second {
			t.Errorf("probe took %s, want it bounded by HealthTimeout", elapsed)
		}
		if rec.Code != http.StatusServiceUnavailable {
			t.Errorf("status = %d, want 503", rec.Code)
		}
	})
}

func TestMetricsEndpoint(t *testing.T) {
	h := newTestServer(t, realLimiter(t))
	postCheck(t, h, `{"client_id":"alice","resource":"/api"}`)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	for _, want := range []string{
		"ratelimiter_decisions_total",
		"ratelimiter_http_requests_total",
		`route="/v1/check"`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics output is missing %q", want)
		}
	}
	// Client IDs must never reach a metric label: they are unbounded and
	// attacker-controlled.
	if strings.Contains(body, "alice") {
		t.Error("client ID leaked into a metric label")
	}
}
