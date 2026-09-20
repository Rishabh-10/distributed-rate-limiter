package limiter

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newTestBucket(t *testing.T) (*TokenBucket, *redis.Client, *fakeClock) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rdb.Close() })

	clock := newFakeClock()
	return NewTokenBucket(rdb, WithBucketClock(clock)), rdb, clock
}

func bucketRule(limit, burst int64, window time.Duration) Rule {
	return Rule{Algorithm: AlgorithmTokenBucket, Limit: limit, Window: window, Burst: burst}
}

// A fresh client starts with a full bucket and can spend it all at once. That
// burst tolerance is the whole reason to choose this algorithm.
func TestTokenBucketAllowsABurstThenRefills(t *testing.T) {
	tb, _, clock := newTestBucket(t)
	ctx := context.Background()
	r := bucketRule(10, 10, time.Second) // 10 per second, burst 10

	for i := 1; i <= 10; i++ {
		d, err := tb.Allow(ctx, "k", r, 1)
		if err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
		if !d.Allowed {
			t.Fatalf("request %d denied, want the full burst to be allowed", i)
		}
		if want := int64(10 - i); d.Remaining != want {
			t.Errorf("request %d: remaining = %d, want %d", i, d.Remaining, want)
		}
	}

	d, err := tb.Allow(ctx, "k", r, 1)
	if err != nil {
		t.Fatalf("Allow: %v", err)
	}
	if d.Allowed {
		t.Error("request past an empty bucket was allowed")
	}
	if d.RetryAfter <= 0 {
		t.Error("retry-after = 0 on a denial, want the time to earn one token")
	}

	// A tenth of a second earns exactly one token back.
	clock.Advance(100 * time.Millisecond)
	if d, err := tb.Allow(ctx, "k", r, 1); err != nil || !d.Allowed {
		t.Errorf("after refill: allowed=%v err=%v, want allowed", d.Allowed, err)
	}
	if d, err := tb.Allow(ctx, "k", r, 1); err != nil || d.Allowed {
		t.Errorf("second request after one token refilled: allowed=%v err=%v, want denied", d.Allowed, err)
	}
}

// Idle time must not accumulate credit beyond the bucket's capacity, or a
// client that goes quiet for an hour could return with an hour of traffic.
func TestTokenBucketCapsAtCapacity(t *testing.T) {
	tb, _, clock := newTestBucket(t)
	ctx := context.Background()
	r := bucketRule(10, 10, time.Second)

	for i := 0; i < 10; i++ {
		tb.Allow(ctx, "k", r, 1)
	}
	clock.Advance(time.Hour)

	allowed := 0
	for i := 0; i < 20; i++ {
		if d, err := tb.Allow(ctx, "k", r, 1); err == nil && d.Allowed {
			allowed++
		}
	}
	if allowed != 10 {
		t.Errorf("an hour of idling bought %d requests, want the capacity of 10", allowed)
	}
}

// Burst can exceed the steady rate: 5/second sustained, 20 in hand.
func TestTokenBucketBurstExceedsRate(t *testing.T) {
	tb, _, clock := newTestBucket(t)
	ctx := context.Background()
	r := bucketRule(5, 20, time.Second)

	allowed := 0
	for i := 0; i < 25; i++ {
		if d, err := tb.Allow(ctx, "k", r, 1); err == nil && d.Allowed {
			allowed++
		}
	}
	if allowed != 20 {
		t.Errorf("initial burst allowed %d, want 20", allowed)
	}

	// One second of refill earns the steady rate, not the capacity.
	clock.Advance(time.Second)
	allowed = 0
	for i := 0; i < 25; i++ {
		if d, err := tb.Allow(ctx, "k", r, 1); err == nil && d.Allowed {
			allowed++
		}
	}
	if allowed != 5 {
		t.Errorf("after one second of refill %d requests were allowed, want 5", allowed)
	}
}

// An unset burst behaves like a plain rate cap rather than refusing everything.
func TestTokenBucketDefaultsBurstToLimit(t *testing.T) {
	tb, _, _ := newTestBucket(t)
	ctx := context.Background()
	r := bucketRule(3, 0, time.Second)

	allowed := 0
	for i := 0; i < 5; i++ {
		if d, err := tb.Allow(ctx, "k", r, 1); err == nil && d.Allowed {
			allowed++
		}
	}
	if allowed != 3 {
		t.Errorf("allowed %d with no burst configured, want the limit of 3", allowed)
	}
}

func TestTokenBucketCost(t *testing.T) {
	tb, _, _ := newTestBucket(t)
	ctx := context.Background()
	r := bucketRule(10, 10, time.Second)

	d, err := tb.Allow(ctx, "k", r, 7)
	if err != nil || !d.Allowed || d.Remaining != 3 {
		t.Fatalf("cost-7 request: allowed=%v remaining=%d err=%v", d.Allowed, d.Remaining, err)
	}
	// 4 > 3 remaining, so this is refused whole and the tokens stay put.
	if d, err := tb.Allow(ctx, "k", r, 4); err != nil || d.Allowed {
		t.Errorf("cost-4 request: allowed=%v err=%v, want denied", d.Allowed, err)
	}
	if d, err := tb.Allow(ctx, "k", r, 3); err != nil || !d.Allowed {
		t.Errorf("cost-3 request: allowed=%v err=%v, want allowed", d.Allowed, err)
	}
}

func TestTokenBucketIsolatesKeys(t *testing.T) {
	tb, _, _ := newTestBucket(t)
	ctx := context.Background()
	r := bucketRule(1, 1, time.Second)

	if d, _ := tb.Allow(ctx, "alice", r, 1); !d.Allowed {
		t.Fatal("alice's first request was denied")
	}
	if d, _ := tb.Allow(ctx, "alice", r, 1); d.Allowed {
		t.Fatal("alice's second request was allowed")
	}
	if d, _ := tb.Allow(ctx, "bob", r, 1); !d.Allowed {
		t.Error("bob was denied by alice's usage")
	}
}

func TestTokenBucketSetsTTL(t *testing.T) {
	tb, rdb, _ := newTestBucket(t)
	ctx := context.Background()

	if _, err := tb.Allow(ctx, "k", bucketRule(10, 10, time.Minute), 1); err != nil {
		t.Fatalf("Allow: %v", err)
	}
	ttl := rdb.TTL(ctx, "k").Val()
	if ttl <= 0 || ttl > time.Minute+2*time.Second {
		t.Errorf("TTL = %s, want roughly the refill time of a full bucket", ttl)
	}
}

// The dispatcher is what makes the algorithm a per-rule choice.
func TestDispatcherRoutesByAlgorithm(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rdb.Close() })

	d := NewDispatcher(map[string]Algorithm{
		AlgorithmSlidingWindow: NewSlidingWindow(rdb),
		AlgorithmTokenBucket:   NewTokenBucket(rdb),
	})
	ctx := context.Background()

	if _, err := d.Allow(ctx, "sw", rule(5, time.Second), 1); err != nil {
		t.Errorf("sliding window: %v", err)
	}
	if _, err := d.Allow(ctx, "tb", bucketRule(5, 5, time.Second), 1); err != nil {
		t.Errorf("token bucket: %v", err)
	}

	// An algorithm with no implementation is a wiring bug and must surface as
	// an error rather than being treated as an outage and failed open.
	_, err := d.Allow(ctx, "x", Rule{Algorithm: "leaky_bucket", Limit: 1, Window: time.Second}, 1)
	if err == nil {
		t.Error("unknown algorithm returned no error")
	}
}
