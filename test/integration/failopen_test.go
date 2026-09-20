//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/Rishabh-10/distributed-rate-limiter/internal/breaker"
	"github.com/Rishabh-10/distributed-rate-limiter/internal/config"
	"github.com/Rishabh-10/distributed-rate-limiter/internal/limiter"
	"github.com/Rishabh-10/distributed-rate-limiter/internal/redisx"
)

// An outage must degrade, not fail. The proxy in front of Redis is severed
// mid-test, which produces the same dial errors and connection resets a real
// outage does — the path that unit tests with a stub error can only imitate.
func TestFailsOpenWhenRedisDisappears(t *testing.T) {
	dialRedis(t, redisAddr())
	p := newProxy(t, redisAddr())

	cb := breaker.New(3, 5*time.Second)
	guarded := guardedLimiter(t, p.Addr(), true, cb)

	key := uniqueKey(t)
	rule := limiter.Rule{Algorithm: limiter.AlgorithmSlidingWindow, Limit: 2, Window: time.Minute}
	ctx := context.Background()

	// Healthy: the limit is enforced.
	for i := 0; i < 2; i++ {
		d, err := guarded.Allow(ctx, key, rule, 1)
		if err != nil || !d.Allowed || d.Degraded {
			t.Fatalf("healthy request %d: allowed=%v degraded=%v err=%v", i, d.Allowed, d.Degraded, err)
		}
	}
	if d, _ := guarded.Allow(ctx, key, rule, 1); d.Allowed {
		t.Fatal("third request was allowed while Redis was healthy, want it denied")
	}

	// Redis goes away.
	p.Close()

	d, err := guarded.Allow(ctx, key, rule, 1)
	if err != nil {
		t.Fatalf("request during outage returned an error, want a degraded allow: %v", err)
	}
	if !d.Allowed {
		t.Error("request during outage was denied, want fail-open")
	}
	if !d.Degraded {
		t.Error("decision during outage was not marked degraded")
	}
}

// Once the breaker trips, an outage must stop costing latency: the point of
// the breaker is that a dead Redis is skipped rather than waited on.
func TestBreakerStopsPayingForDeadRedis(t *testing.T) {
	dialRedis(t, redisAddr())
	p := newProxy(t, redisAddr())

	cb := breaker.New(3, 30*time.Second)
	guarded := guardedLimiter(t, p.Addr(), true, cb)

	key := uniqueKey(t)
	rule := limiter.Rule{Algorithm: limiter.AlgorithmSlidingWindow, Limit: 100, Window: time.Minute}
	ctx := context.Background()

	guarded.Allow(ctx, key, rule, 1)
	p.Close()

	// Trip it: each of these pays a dial timeout.
	for i := 0; i < 5; i++ {
		guarded.Allow(ctx, key, rule, 1)
	}
	if got := cb.State(); got != breaker.StateOpen {
		t.Fatalf("breaker state = %s after repeated failures, want open", got)
	}

	start := time.Now()
	for i := 0; i < 100; i++ {
		d, err := guarded.Allow(ctx, key, rule, 1)
		if err != nil || !d.Allowed || !d.Degraded {
			t.Fatalf("request %d: allowed=%v degraded=%v err=%v", i, d.Allowed, d.Degraded, err)
		}
	}
	elapsed := time.Since(start)

	// 100 requests against a dead Redis, short-circuited, should be
	// sub-millisecond each. Anything near the timeout means the breaker is not
	// actually cutting the calls off.
	if elapsed > 100*time.Millisecond {
		t.Errorf("100 short-circuited requests took %s, want them to skip Redis entirely", elapsed)
	}
}

// Failing closed is the opposite trade: enforcement is preserved and
// availability is not.
func TestFailsClosedWhenConfigured(t *testing.T) {
	dialRedis(t, redisAddr())
	p := newProxy(t, redisAddr())
	p.Close()

	guarded := guardedLimiter(t, p.Addr(), false, breaker.New(3, time.Second))

	d, err := guarded.Allow(context.Background(), uniqueKey(t),
		limiter.Rule{Algorithm: limiter.AlgorithmSlidingWindow, Limit: 10, Window: time.Minute}, 1)
	if err != nil {
		t.Fatalf("Allow: %v", err)
	}
	if d.Allowed {
		t.Error("request was allowed with Redis down and fail_open disabled")
	}
	if !d.Degraded {
		t.Error("decision was not marked degraded")
	}
}

// Enforcement must resume by itself once Redis returns — an operator should
// not have to restart anything.
func TestRecoversWhenRedisReturns(t *testing.T) {
	dialRedis(t, redisAddr())

	// A proxy that can be taken down and brought back on the same address.
	p := newProxy(t, redisAddr())
	addr := p.Addr()

	cb := breaker.New(2, 200*time.Millisecond)
	guarded := guardedLimiter(t, addr, true, cb)

	key := uniqueKey(t)
	rule := limiter.Rule{Algorithm: limiter.AlgorithmSlidingWindow, Limit: 1, Window: time.Minute}
	ctx := context.Background()

	if d, err := guarded.Allow(ctx, key, rule, 1); err != nil || !d.Allowed {
		t.Fatalf("first request: allowed=%v err=%v", d.Allowed, err)
	}

	p.Close()
	for i := 0; i < 3; i++ {
		guarded.Allow(ctx, key, rule, 1)
	}
	if cb.State() != breaker.StateOpen {
		t.Fatal("breaker did not open during the outage")
	}

	// Redis comes back at the same address.
	restarted := reopenProxy(t, addr, redisAddr())
	defer restarted.Close()

	// Wait out the cool-off, then the next call probes and should find Redis
	// healthy again — and the quota from before the outage is still there.
	time.Sleep(300 * time.Millisecond)

	deadline := time.Now().Add(5 * time.Second)
	for {
		d, err := guarded.Allow(ctx, key, rule, 1)
		if err == nil && !d.Degraded {
			if d.Allowed {
				t.Error("enforcement resumed but the over-quota request was allowed")
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("enforcement did not resume within 5s: degraded=%v err=%v", d.Degraded, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// Idle clients must not accumulate keys forever: the window's TTL is what
// keeps Redis memory proportional to active clients rather than to every
// client that has ever called.
func TestKeysExpireWhenClientsGoIdle(t *testing.T) {
	addr := redisAddr()
	rdb := dialRedis(t, addr)

	sw := limiter.NewSlidingWindow(newClient(t, addr))
	key := uniqueKey(t)
	rule := limiter.Rule{Algorithm: limiter.AlgorithmSlidingWindow, Limit: 5, Window: time.Second}

	if _, err := sw.Allow(context.Background(), key, rule, 1); err != nil {
		t.Fatalf("Allow: %v", err)
	}

	ttl, err := rdb.TTL(context.Background(), key).Result()
	if err != nil {
		t.Fatalf("TTL: %v", err)
	}
	if ttl <= 0 || ttl > 3*time.Second {
		t.Fatalf("TTL = %s, want a positive expiry close to the window", ttl)
	}

	// Window plus grace plus slack.
	time.Sleep(2500 * time.Millisecond)

	n, err := rdb.Exists(context.Background(), key).Result()
	if err != nil {
		t.Fatalf("EXISTS: %v", err)
	}
	if n != 0 {
		t.Errorf("key still present after the window elapsed, want it expired")
	}
}

// guardedLimiter builds the production wiring — sliding window, guard,
// breaker, real error classification — against the given address.
func guardedLimiter(t *testing.T, addr string, failOpen bool, cb *breaker.Breaker) limiter.Algorithm {
	t.Helper()
	rdb := redisx.New(config.Redis{
		Addr:           addr,
		CommandTimeout: config.Duration(100 * time.Millisecond),
		DialTimeout:    config.Duration(100 * time.Millisecond),
		PoolSize:       20,
		KeyPrefix:      "rl",
	})
	t.Cleanup(func() { rdb.Close() })

	return limiter.NewGuard(
		limiter.NewDispatcher(map[string]limiter.Algorithm{
			limiter.AlgorithmSlidingWindow: limiter.NewSlidingWindow(rdb),
		}),
		failOpen,
		limiter.WithBreaker(cb),
		limiter.WithTimeout(300*time.Millisecond),
		limiter.WithUnavailableFunc(redisx.IsUnavailable),
	)
}
