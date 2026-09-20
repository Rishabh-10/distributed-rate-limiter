package limiter

import (
	"context"
	"time"
)

// CircuitBreaker is the subset of a breaker the guard needs.
//
// It is declared here rather than imported so that this package stays free of
// any Redis dependency: the algorithms talk to Redis, the policy around them
// does not have to.
type CircuitBreaker interface {
	Allow() bool
	Success()
	Failure()
}

// DegradeReason explains why a decision was not enforced.
type DegradeReason string

// Reasons a request bypassed Redis.
const (
	ReasonBreakerOpen DegradeReason = "breaker_open"
	ReasonUnavailable DegradeReason = "redis_unavailable"
)

// Guard wraps an Algorithm with the availability policy: a per-call timeout, a
// circuit breaker, and fail-open behaviour.
//
// The design rule it enforces is that a rate limiter must not be able to take
// down the service it protects. When Redis cannot answer, the request is
// allowed and the decision is flagged Degraded — enforcement is given up
// deliberately and visibly, rather than the request failing.
//
// The breaker matters more than it first appears. Without it, every request
// during an outage waits out the full command timeout before failing open, so
// a dead Redis silently adds that latency to every call. With it, a handful of
// failures trip the breaker and later requests skip Redis entirely.
type Guard struct {
	inner   Algorithm
	breaker CircuitBreaker

	failOpen bool
	timeout  time.Duration
	// unavailable decides whether an error means "Redis could not answer".
	// Only those errors are eligible for fail-open; a programming error must
	// not be laundered into an allow.
	unavailable func(error) bool
	onDegrade   func(DegradeReason, error)
}

// GuardOption configures a Guard.
type GuardOption func(*Guard)

// WithBreaker attaches a circuit breaker.
func WithBreaker(b CircuitBreaker) GuardOption {
	return func(g *Guard) { g.breaker = b }
}

// WithTimeout bounds each call to the underlying algorithm. Zero leaves the
// caller's own deadline in charge.
func WithTimeout(d time.Duration) GuardOption {
	return func(g *Guard) { g.timeout = d }
}

// WithUnavailableFunc sets the error classifier.
func WithUnavailableFunc(f func(error) bool) GuardOption {
	return func(g *Guard) { g.unavailable = f }
}

// WithDegradeHook registers a callback invoked on every unenforced decision.
// It is where metrics and sampled logging hang.
func WithDegradeHook(f func(DegradeReason, error)) GuardOption {
	return func(g *Guard) { g.onDegrade = f }
}

// NewGuard wraps inner. failOpen decides what happens when Redis is
// unreachable: allow the request (true) or refuse it (false).
func NewGuard(inner Algorithm, failOpen bool, opts ...GuardOption) *Guard {
	g := &Guard{
		inner:       inner,
		failOpen:    failOpen,
		unavailable: func(error) bool { return true },
	}
	for _, opt := range opts {
		opt(g)
	}
	return g
}

// Allow implements Algorithm.
func (g *Guard) Allow(ctx context.Context, key string, rule Rule, cost int64) (Decision, error) {
	if g.breaker != nil && !g.breaker.Allow() {
		return g.degrade(rule, ReasonBreakerOpen, nil), nil
	}

	callCtx := ctx
	if g.timeout > 0 {
		var cancel context.CancelFunc
		callCtx, cancel = context.WithTimeout(ctx, g.timeout)
		defer cancel()
	}

	d, err := g.inner.Allow(callCtx, key, rule, cost)
	if err != nil {
		if !g.unavailable(err) {
			// Not an availability problem — a bug, a misconfiguration, a
			// WRONGTYPE. Surface it instead of quietly allowing traffic.
			return Decision{}, err
		}
		if g.breaker != nil {
			g.breaker.Failure()
		}
		return g.degrade(rule, ReasonUnavailable, err), nil
	}

	if g.breaker != nil {
		g.breaker.Success()
	}
	return d, nil
}

// degrade builds the decision used when Redis was not consulted.
func (g *Guard) degrade(rule Rule, reason DegradeReason, err error) Decision {
	if g.onDegrade != nil {
		g.onDegrade(reason, err)
	}
	d := Decision{
		Allowed:  g.failOpen,
		Limit:    rule.Limit,
		Degraded: true,
	}
	if g.failOpen {
		// No usage data, so no meaningful remaining count. Reporting the full
		// quota is the honest reading of "nothing was counted against you".
		d.Remaining = rule.Limit
	} else {
		// Failing closed: tell the caller to come back after the breaker has
		// had a chance to probe, rather than immediately.
		d.RetryAfter = rule.Window
	}
	return d
}
