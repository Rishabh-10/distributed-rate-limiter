// Package limiter defines the rate-limiting contract and its algorithm
// implementations. Everything here is transport-agnostic: the HTTP layer maps
// requests onto a Rule and renders the resulting Decision, but knows nothing
// about how the decision was reached.
package limiter

import (
	"context"
	"time"
)

// Algorithm names understood by the config loader and the algorithm registry.
const (
	AlgorithmSlidingWindow = "sliding_window"
	AlgorithmTokenBucket   = "token_bucket"
)

// Rule is the quota that applies to one (client, resource) pair.
type Rule struct {
	Algorithm string        `json:"algorithm"`
	Limit     int64         `json:"limit"`
	Window    time.Duration `json:"window"`
	// Burst is the bucket capacity for token_bucket. It is ignored by
	// sliding_window, where Limit already is the ceiling.
	Burst int64 `json:"burst,omitempty"`
}

// Decision is the answer to "may this request proceed right now?".
type Decision struct {
	Allowed   bool  `json:"allowed"`
	Limit     int64 `json:"limit"`
	Remaining int64 `json:"remaining"`
	// ResetAfter is how long until the window is fully clear, i.e. until the
	// client is back to a full quota.
	ResetAfter time.Duration `json:"-"`
	// RetryAfter is how long until at least one slot frees up. Zero when the
	// request was allowed.
	RetryAfter time.Duration `json:"-"`
	// Degraded reports that Redis could not be consulted and the service fell
	// open. The decision is still Allowed, but it was not enforced.
	Degraded bool `json:"degraded"`
}

// Algorithm decides whether a request against key is permitted under rule.
//
// cost is the number of slots the request consumes; it is normally 1, but a
// caller may charge an expensive operation more.
//
// Implementations return an error only for infrastructure failures (Redis
// unreachable, timeout). A request that is simply over quota is a successful
// call returning Allowed=false — callers rely on that distinction to decide
// whether to fail open.
type Algorithm interface {
	Allow(ctx context.Context, key string, rule Rule, cost int64) (Decision, error)
}

// Clock is the time source used to score entries. It exists so tests can
// advance time without sleeping.
type Clock interface {
	Now() time.Time
}

// SystemClock reads the host clock.
type SystemClock struct{}

// Now implements Clock.
func (SystemClock) Now() time.Time { return time.Now() }
