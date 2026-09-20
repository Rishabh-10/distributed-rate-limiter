package limiter

import (
	"context"
	"fmt"
	"math/rand/v2"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// SlidingWindow counts requests with a Redis sorted set holding one member per
// request, scored by arrival time.
//
// # Why a transaction
//
// The naive implementation reads the count, decides, then writes. Two limiter
// instances serving the same client can both read "99 of 100 used" and both
// admit a request, overshooting the quota. There is no way to close that
// window from the client side.
//
// So the whole operation is one MULTI/EXEC block, pipelined into a single
// round trip:
//
//	ZREMRANGEBYSCORE key -inf (now-window)   drop everything older than the window
//	ZADD             key now  <member>       optimistically record this request
//	ZCARD            key                     how many are in the window now
//	ZRANGE           key 0 0 WITHSCORES      oldest survivor, for Retry-After
//	PEXPIRE          key window              idle clients clean themselves up
//
// Redis executes the block atomically, so concurrent callers are serialised:
// one gets ZCARD 100, the next gets 101. The decision falls out of a value
// nobody else can have seen, which is what makes the count correct across
// instances. There is no read-then-decide gap left to lose.
//
// The cost of deciding *after* the write is that a rejected request has
// already been recorded. Unless CountRejected is set, a compensating ZREM
// hands the slot back, so a client that keeps hammering a closed door does not
// extend its own lockout.
type SlidingWindow struct {
	rdb redis.Cmdable

	clock Clock
	// countRejected keeps rejected requests in the window instead of removing
	// them again.
	countRejected bool
	// expiryGrace pads the key TTL so a key cannot expire a hair before the
	// last entry inside it logically should.
	expiryGrace time.Duration
}

// SlidingWindowOption configures a SlidingWindow.
type SlidingWindowOption func(*SlidingWindow)

// WithClock sets the time source. Tests use this to move time without sleeping.
func WithClock(c Clock) SlidingWindowOption {
	return func(s *SlidingWindow) { s.clock = c }
}

// WithCountRejected makes rejected requests occupy a slot in the window, so
// sustained abuse extends the lockout rather than merely being refused.
func WithCountRejected(count bool) SlidingWindowOption {
	return func(s *SlidingWindow) { s.countRejected = count }
}

// NewSlidingWindow returns a SlidingWindow backed by rdb.
func NewSlidingWindow(rdb redis.Cmdable, opts ...SlidingWindowOption) *SlidingWindow {
	s := &SlidingWindow{
		rdb:         rdb,
		clock:       SystemClock{},
		expiryGrace: time.Second,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Allow implements Algorithm.
func (s *SlidingWindow) Allow(ctx context.Context, key string, rule Rule, cost int64) (Decision, error) {
	if cost <= 0 {
		cost = 1
	}

	now := s.clock.Now()
	// Scores are microseconds, not nanoseconds. Redis stores sorted-set scores
	// as IEEE doubles, whose 53-bit mantissa cannot hold a nanosecond Unix
	// timestamp exactly; microseconds stay exact for another two centuries and
	// are far finer than any window worth configuring.
	nowMicro := now.UnixMicro()
	cutoff := nowMicro - rule.Window.Microseconds()

	members := make([]redis.Z, cost)
	ids := make([]any, cost)
	for i := range members {
		// The member must be unique per request. Using the bare timestamp
		// would collapse same-instant requests into one ZSET member and
		// silently undercount them.
		id := fmt.Sprintf("%d-%s", nowMicro, strconv.FormatUint(rand.Uint64(), 36))
		members[i] = redis.Z{Score: float64(nowMicro), Member: id}
		ids[i] = id
	}

	pipe := s.rdb.TxPipeline()
	// Inclusive upper bound: an entry that is exactly one window old has left
	// the trailing window (now-window, now] and must go.
	pipe.ZRemRangeByScore(ctx, key, "-inf", strconv.FormatInt(cutoff, 10))
	pipe.ZAdd(ctx, key, members...)
	card := pipe.ZCard(ctx, key)
	oldest := pipe.ZRangeWithScores(ctx, key, 0, 0)
	pipe.PExpire(ctx, key, rule.Window+s.expiryGrace)

	if _, err := pipe.Exec(ctx); err != nil {
		return Decision{}, fmt.Errorf("sliding window transaction: %w", err)
	}

	count := card.Val()
	allowed := count <= rule.Limit

	// effective is how many entries remain attributable to this window once we
	// have honoured the CountRejected policy.
	effective := count
	if !allowed && !s.countRejected {
		// Best effort: if this ZREM fails the entries expire with the key
		// anyway, so the worst case is a briefly stricter limit. Detached from
		// ctx so a client that hangs up mid-request still gets cleaned up.
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
		if err := s.rdb.ZRem(cleanupCtx, key, ids...).Err(); err == nil {
			effective = count - cost
		}
		cancel()
	}

	d := Decision{
		Allowed:   allowed,
		Limit:     rule.Limit,
		Remaining: max(0, rule.Limit-effective),
	}
	if effective > 0 {
		// Upper bound: the newest entry in the window expires at most one full
		// window from now, and nothing outlives it.
		d.ResetAfter = rule.Window
	}
	if !allowed {
		d.RetryAfter = retryAfter(oldest.Val(), now, rule.Window)
	}
	return d, nil
}

// retryAfter reports how long until the oldest entry leaves the window, which
// is when the next slot frees up.
//
// For cost > 1 this is a lower bound: freeing several slots at once may take
// longer than the single oldest entry's expiry. Callers should treat it as
// "not before this", which is exactly what Retry-After means.
func retryAfter(oldest []redis.Z, now time.Time, window time.Duration) time.Duration {
	if len(oldest) == 0 {
		return 0
	}
	expiresAt := time.UnixMicro(int64(oldest[0].Score)).Add(window)
	wait := expiresAt.Sub(now)
	if wait < 0 {
		return 0
	}
	return wait
}
