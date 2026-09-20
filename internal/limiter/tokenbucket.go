package limiter

import (
	"context"
	_ "embed"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

//go:embed tokenbucket.lua
var tokenBucketScript string

// TokenBucket refills a per-client allowance at a steady rate and lets a
// client spend it in bursts up to the bucket's capacity.
//
// # How it differs from the sliding window
//
// The sliding window answers "how many requests in the last N seconds?"
// exactly, by keeping one sorted-set entry per request. That is precise, and
// it costs memory proportional to traffic: a client doing 10k requests a
// minute keeps 10k members alive.
//
// A token bucket keeps two numbers per client — tokens and a timestamp —
// regardless of traffic, and it expresses a different intent: a sustained rate
// with an explicit burst allowance, rather than a hard ceiling over a trailing
// window. Pick it for high-volume clients, or when bursts are something to
// accommodate rather than to stop.
//
// # Why this one uses a script
//
// Refilling requires reading the stored timestamp, computing elapsed time and
// writing the result back. MULTI/EXEC cannot do that: commands inside a
// transaction are queued and cannot branch on each other's results. So the
// arithmetic runs inside Redis as a Lua script, which is atomic for the same
// reason the transaction is — Redis runs it to completion without interleaving
// anyone else's commands.
type TokenBucket struct {
	rdb    redis.Cmdable
	clock  Clock
	script *redis.Script
	// expiryGrace pads the TTL past the time a bucket needs to refill fully,
	// so a key cannot vanish while it still holds a deficit.
	expiryGrace time.Duration
}

// TokenBucketOption configures a TokenBucket.
type TokenBucketOption func(*TokenBucket)

// WithBucketClock sets the time source, for tests.
func WithBucketClock(c Clock) TokenBucketOption {
	return func(t *TokenBucket) { t.clock = c }
}

// NewTokenBucket returns a TokenBucket backed by rdb.
func NewTokenBucket(rdb redis.Cmdable, opts ...TokenBucketOption) *TokenBucket {
	t := &TokenBucket{
		rdb:         rdb,
		clock:       SystemClock{},
		script:      redis.NewScript(tokenBucketScript),
		expiryGrace: time.Second,
	}
	for _, opt := range opts {
		opt(t)
	}
	return t
}

// Allow implements Algorithm.
func (t *TokenBucket) Allow(ctx context.Context, key string, rule Rule, cost int64) (Decision, error) {
	if cost <= 0 {
		cost = 1
	}

	// Burst defaults to the limit, which makes an unconfigured bucket behave
	// like a plain rate cap rather than silently refusing everything.
	capacity := rule.Burst
	if capacity <= 0 {
		capacity = rule.Limit
	}

	// A bucket that starts full needs one full window to refill from empty, so
	// the TTL is sized to the refill time of its whole capacity.
	refill := time.Duration(float64(capacity) / float64(rule.Limit) * float64(rule.Window))
	ttl := refill + t.expiryGrace

	// Run prefers EVALSHA and falls back to EVAL when the script is not yet
	// cached, so a restarted or failed-over Redis recovers by itself.
	raw, err := t.script.Run(ctx, t.rdb, []string{key},
		t.clock.Now().UnixMicro(),
		rule.Limit,
		rule.Window.Microseconds(),
		capacity,
		cost,
		ttl.Milliseconds(),
	).Int64Slice()
	if err != nil {
		return Decision{}, fmt.Errorf("token bucket script: %w", err)
	}
	if len(raw) != 4 {
		return Decision{}, fmt.Errorf("token bucket script returned %d values, want 4", len(raw))
	}

	return Decision{
		Allowed:    raw[0] == 1,
		Limit:      capacity,
		Remaining:  raw[1],
		RetryAfter: time.Duration(raw[2]) * time.Microsecond,
		ResetAfter: time.Duration(raw[3]) * time.Microsecond,
	}, nil
}
