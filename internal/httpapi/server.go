// Package httpapi exposes the limiter over REST.
//
// The central endpoint, POST /v1/check, is a *decision* API: it answers with
// 200 and a body saying whether the request should be allowed. It does not
// return 429 itself. The caller — usually pkg/ratelimit's middleware — is the
// one holding the request being limited, so it is the one that turns
// `allowed: false` into a 429 for its own client.
package httpapi

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/Rishabh-10/distributed-rate-limiter/internal/limiter"
	"github.com/Rishabh-10/distributed-rate-limiter/internal/observability"
	"github.com/Rishabh-10/distributed-rate-limiter/internal/quota"
)

// Deps are the collaborators a Server needs.
type Deps struct {
	// Quotas is read per request, so a config reload takes effect immediately.
	Quotas *quota.Store
	// Limiter is the guarded dispatcher: it never returns an availability
	// error, it returns a degraded decision.
	Limiter limiter.Algorithm
	Metrics *observability.Metrics
	Logger  *slog.Logger
	// KeyPrefix namespaces Redis keys. It is fixed at startup rather than read
	// from the hot-reloaded config: changing it at runtime would orphan every
	// in-flight counter and silently reset everyone's usage.
	KeyPrefix string
	// Health reports whether Redis is reachable, for /readyz.
	Health func(context.Context) error
	// BreakerState reports the circuit breaker's position, for /readyz.
	BreakerState func() string
	// HealthTimeout bounds the readiness probe.
	HealthTimeout time.Duration
}

// Server holds the HTTP handlers.
type Server struct {
	deps Deps
	log  *slog.Logger
}

// NewServer returns a Server. Callers get its routes from Handler.
func NewServer(deps Deps) *Server {
	if deps.Logger == nil {
		deps.Logger = slog.Default()
	}
	if deps.Metrics == nil {
		// The handlers record unconditionally, so an absent collector set is
		// filled in rather than nil-checked at every call site.
		deps.Metrics = observability.NewMetrics()
	}
	if deps.HealthTimeout <= 0 {
		deps.HealthTimeout = time.Second
	}
	return &Server{deps: deps, log: deps.Logger}
}

// Handler builds the router with the middleware chain applied.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	// Routes are instrumented individually with their registered pattern, so
	// the metric label is always a bounded route name and never a raw path.
	mux.Handle("POST /v1/check", s.instrument("/v1/check", http.HandlerFunc(s.handleCheck)))
	mux.Handle("GET /v1/quotas/{client}", s.instrument("/v1/quotas/{client}", http.HandlerFunc(s.handleQuota)))
	mux.Handle("GET /healthz", s.instrument("/healthz", http.HandlerFunc(s.handleHealthz)))
	mux.Handle("GET /readyz", s.instrument("/readyz", http.HandlerFunc(s.handleReadyz)))
	mux.Handle("GET /metrics", s.deps.Metrics.Handler())

	return recoverPanics(s.log)(mux)
}
