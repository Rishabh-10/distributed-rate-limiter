package observability

import (
	"net/http"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/Rishabh-10/distributed-rate-limiter/internal/limiter"
)

// Metrics holds the service's Prometheus collectors.
//
// Note what is deliberately absent: no metric is labelled by client ID.
// Client IDs are unbounded and attacker-controlled, and one label per client
// would turn a traffic spike into a Prometheus outage. Per-client visibility
// belongs in logs and in the /v1/quotas endpoint, not in a time series.
type Metrics struct {
	Registry *prometheus.Registry

	decisions     *prometheus.CounterVec
	degraded      *prometheus.CounterVec
	limiterCalls  *prometheus.HistogramVec
	httpRequests  *prometheus.CounterVec
	httpDurations *prometheus.HistogramVec
}

// NewMetrics builds the collectors on their own registry, so tests can create
// an isolated instance and the process exposes only what it declares.
func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()
	m := &Metrics{
		Registry: reg,
		decisions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "ratelimiter_decisions_total",
			Help: "Rate limiting decisions, by outcome.",
		}, []string{"algorithm", "allowed", "degraded"}),
		degraded: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "ratelimiter_degraded_total",
			Help: "Decisions made without consulting Redis, by reason.",
		}, []string{"reason"}),
		limiterCalls: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "ratelimiter_backend_duration_seconds",
			Help: "Time spent in the limiter backend, including Redis.",
			// The interesting range is sub-millisecond to the command timeout;
			// the default buckets waste most of their resolution above 1s.
			Buckets: []float64{.0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5, 1},
		}, []string{"algorithm", "outcome"}),
		httpRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "ratelimiter_http_requests_total",
			Help: "HTTP requests served, by route and status.",
		}, []string{"route", "method", "status"}),
		httpDurations: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "ratelimiter_http_request_duration_seconds",
			Help:    "HTTP request latency, by route.",
			Buckets: []float64{.0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5},
		}, []string{"route", "method"}),
	}
	reg.MustRegister(
		m.decisions, m.degraded, m.limiterCalls, m.httpRequests, m.httpDurations,
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return m
}

// ObserveDecision records the outcome of one limiting decision.
func (m *Metrics) ObserveDecision(algorithm string, d limiter.Decision, seconds float64) {
	m.decisions.WithLabelValues(algorithm, strconv.FormatBool(d.Allowed), strconv.FormatBool(d.Degraded)).Inc()
	outcome := "allowed"
	if !d.Allowed {
		outcome = "denied"
	}
	if d.Degraded {
		outcome = "degraded"
	}
	m.limiterCalls.WithLabelValues(algorithm, outcome).Observe(seconds)
}

// ObserveError records a decision that failed outright (neither allowed nor
// denied — the limiter itself broke).
func (m *Metrics) ObserveError(algorithm string, seconds float64) {
	m.limiterCalls.WithLabelValues(algorithm, "error").Observe(seconds)
}

// ObserveDegrade records that Redis was bypassed.
func (m *Metrics) ObserveDegrade(reason limiter.DegradeReason) {
	m.degraded.WithLabelValues(string(reason)).Inc()
}

// ObserveHTTP records one served HTTP request. route is the registered
// pattern, never the raw path, so path parameters cannot inflate cardinality.
func (m *Metrics) ObserveHTTP(route, method string, status int, seconds float64) {
	m.httpRequests.WithLabelValues(route, method, strconv.Itoa(status)).Inc()
	m.httpDurations.WithLabelValues(route, method).Observe(seconds)
}

// RegisterBreakerState exposes the circuit breaker's position as a gauge:
// 0 closed, 1 half-open, 2 open.
func (m *Metrics) RegisterBreakerState(state func() string) {
	m.Registry.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "ratelimiter_breaker_state",
		Help: "Circuit breaker state guarding Redis: 0 closed, 1 half-open, 2 open.",
	}, func() float64 {
		switch state() {
		case "half_open":
			return 1
		case "open":
			return 2
		default:
			return 0
		}
	}))
}

// Handler serves the metrics endpoint.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{Registry: m.Registry})
}
