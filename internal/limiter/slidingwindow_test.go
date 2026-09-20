package limiter

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// fakeClock lets a test move time without sleeping, so window-boundary
// behaviour is asserted exactly rather than approximately.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock {
	// An arbitrary fixed instant; only deltas matter.
	return &fakeClock{now: time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func newTestLimiter(t *testing.T, opts ...SlidingWindowOption) (*SlidingWindow, *redis.Client, *fakeClock) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rdb.Close() })

	clock := newFakeClock()
	sw := NewSlidingWindow(rdb, append([]SlidingWindowOption{WithClock(clock)}, opts...)...)
	return sw, rdb, clock
}

func rule(limit int64, window time.Duration) Rule {
	return Rule{Algorithm: AlgorithmSlidingWindow, Limit: limit, Window: window}
}

func TestSlidingWindowAllowsUpToLimit(t *testing.T) {
	sw, _, _ := newTestLimiter(t)
	ctx := context.Background()
	r := rule(3, time.Second)

	for i := int64(1); i <= 3; i++ {
		d, err := sw.Allow(ctx, "k", r, 1)
		if err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
		if !d.Allowed {
			t.Fatalf("request %d was denied, want allowed", i)
		}
		if want := 3 - i; d.Remaining != want {
			t.Errorf("request %d: remaining = %d, want %d", i, d.Remaining, want)
		}
		if d.RetryAfter != 0 {
			t.Errorf("request %d: retry-after = %s on an allowed request, want 0", i, d.RetryAfter)
		}
	}

	d, err := sw.Allow(ctx, "k", r, 1)
	if err != nil {
		t.Fatalf("fourth request: %v", err)
	}
	if d.Allowed {
		t.Error("fourth request was allowed, want denied")
	}
	if d.Remaining != 0 {
		t.Errorf("remaining = %d, want 0", d.Remaining)
	}
	// The oldest entry landed at t0 and no time has passed, so a slot frees up
	// one full window from now.
	if d.RetryAfter != time.Second {
		t.Errorf("retry-after = %s, want 1s", d.RetryAfter)
	}
}

func TestSlidingWindowRecoversAsEntriesAge(t *testing.T) {
	sw, _, clock := newTestLimiter(t)
	ctx := context.Background()
	r := rule(2, time.Second)

	mustAllow(t, sw, ctx, r, "first")
	clock.Advance(400 * time.Millisecond)
	mustAllow(t, sw, ctx, r, "second")

	mustDeny(t, sw, ctx, r, "third, window full")

	// Past the first entry's expiry but not the second's: exactly one slot
	// should have opened up.
	clock.Advance(700 * time.Millisecond)
	mustAllow(t, sw, ctx, r, "fourth, first entry aged out")
	mustDeny(t, sw, ctx, r, "fifth, window full again")
}

// The window is trailing and half-open: an entry exactly one window old has
// left it.
func TestSlidingWindowBoundaryIsExclusive(t *testing.T) {
	sw, _, clock := newTestLimiter(t)
	ctx := context.Background()
	r := rule(1, time.Second)

	mustAllow(t, sw, ctx, r, "first")

	clock.Advance(time.Second - time.Microsecond)
	mustDeny(t, sw, ctx, r, "one microsecond inside the window")

	clock.Advance(time.Microsecond)
	mustAllow(t, sw, ctx, r, "exactly one window later")
}

func TestRejectedRequestDoesNotConsumeSlot(t *testing.T) {
	sw, rdb, _ := newTestLimiter(t)
	ctx := context.Background()
	r := rule(2, time.Second)

	mustAllow(t, sw, ctx, r, "first")
	mustAllow(t, sw, ctx, r, "second")
	for i := 0; i < 5; i++ {
		mustDeny(t, sw, ctx, r, "over limit")
	}

	// The five rejected attempts were compensated away: the window still holds
	// exactly the two requests that were actually served.
	if got := rdb.ZCard(ctx, "k").Val(); got != 2 {
		t.Errorf("window holds %d entries after 5 rejections, want 2", got)
	}
}

func TestRejectedRequestConsumesSlotWhenConfigured(t *testing.T) {
	sw, rdb, _ := newTestLimiter(t, WithCountRejected(true))
	ctx := context.Background()
	r := rule(2, time.Second)

	mustAllow(t, sw, ctx, r, "first")
	mustAllow(t, sw, ctx, r, "second")
	for i := 0; i < 5; i++ {
		mustDeny(t, sw, ctx, r, "over limit")
	}

	if got := rdb.ZCard(ctx, "k").Val(); got != 7 {
		t.Errorf("window holds %d entries, want 7 (rejections counted)", got)
	}
}

func TestSlidingWindowCost(t *testing.T) {
	sw, rdb, _ := newTestLimiter(t)
	ctx := context.Background()
	r := rule(5, time.Second)

	d, err := sw.Allow(ctx, "k", r, 3)
	if err != nil {
		t.Fatalf("Allow: %v", err)
	}
	if !d.Allowed || d.Remaining != 2 {
		t.Fatalf("cost-3 request: allowed=%v remaining=%d, want true/2", d.Allowed, d.Remaining)
	}

	// 3 + 3 exceeds 5, so this is refused whole: partial charging would let a
	// caller pay for less than it asked for.
	d, err = sw.Allow(ctx, "k", r, 3)
	if err != nil {
		t.Fatalf("Allow: %v", err)
	}
	if d.Allowed {
		t.Error("second cost-3 request was allowed, want denied")
	}
	if d.Remaining != 2 {
		t.Errorf("remaining = %d, want 2 (the refused cost was returned)", d.Remaining)
	}
	if got := rdb.ZCard(ctx, "k").Val(); got != 3 {
		t.Errorf("window holds %d entries, want 3", got)
	}

	// A cheaper request still fits in what is left.
	if d, err := sw.Allow(ctx, "k", r, 2); err != nil || !d.Allowed {
		t.Errorf("cost-2 request: allowed=%v err=%v, want allowed", d.Allowed, err)
	}
}

// Distinct clients must not interfere, and neither must distinct resources for
// the same client.
func TestSlidingWindowIsolatesKeys(t *testing.T) {
	sw, _, _ := newTestLimiter(t)
	ctx := context.Background()
	r := rule(1, time.Second)

	mustAllowKey(t, sw, ctx, "rl:sw:{alice}:/a", r, "alice on /a")
	mustDenyKey(t, sw, ctx, "rl:sw:{alice}:/a", r, "alice again on /a")
	mustAllowKey(t, sw, ctx, "rl:sw:{alice}:/b", r, "alice on /b")
	mustAllowKey(t, sw, ctx, "rl:sw:{bob}:/a", r, "bob on /a")
}

// Idle clients must not leak keys: the TTL is refreshed on every write, so a
// client that stops calling disappears one window later.
func TestSlidingWindowSetsTTL(t *testing.T) {
	sw, rdb, _ := newTestLimiter(t)
	ctx := context.Background()
	r := rule(5, time.Minute)

	mustAllow(t, sw, ctx, r, "first")

	ttl := rdb.TTL(ctx, "k").Val()
	if ttl <= 0 {
		t.Fatalf("TTL = %s, want a positive expiry", ttl)
	}
	if ttl > time.Minute+2*time.Second {
		t.Errorf("TTL = %s, want no more than the window plus a small grace", ttl)
	}
}

// Requests in the same microsecond must each be counted. Scoring by timestamp
// alone would collapse them into one sorted-set member.
func TestSlidingWindowCountsSameInstantRequests(t *testing.T) {
	sw, rdb, _ := newTestLimiter(t) // fake clock: every call shares one instant
	ctx := context.Background()
	r := rule(100, time.Second)

	for i := 0; i < 10; i++ {
		mustAllow(t, sw, ctx, r, "same-instant request")
	}
	if got := rdb.ZCard(ctx, "k").Val(); got != 10 {
		t.Errorf("window holds %d entries, want 10", got)
	}
}

func mustAllow(t *testing.T, sw *SlidingWindow, ctx context.Context, r Rule, what string) {
	t.Helper()
	mustAllowKey(t, sw, ctx, "k", r, what)
}

func mustDeny(t *testing.T, sw *SlidingWindow, ctx context.Context, r Rule, what string) {
	t.Helper()
	mustDenyKey(t, sw, ctx, "k", r, what)
}

func mustAllowKey(t *testing.T, sw *SlidingWindow, ctx context.Context, key string, r Rule, what string) {
	t.Helper()
	d, err := sw.Allow(ctx, key, r, 1)
	if err != nil {
		t.Fatalf("%s: %v", what, err)
	}
	if !d.Allowed {
		t.Fatalf("%s: denied, want allowed", what)
	}
}

func mustDenyKey(t *testing.T, sw *SlidingWindow, ctx context.Context, key string, r Rule, what string) {
	t.Helper()
	d, err := sw.Allow(ctx, key, r, 1)
	if err != nil {
		t.Fatalf("%s: %v", what, err)
	}
	if d.Allowed {
		t.Fatalf("%s: allowed, want denied", what)
	}
}
