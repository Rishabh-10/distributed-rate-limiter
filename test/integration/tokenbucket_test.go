//go:build integration

package integration

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Rishabh-10/distributed-rate-limiter/internal/limiter"
)

// miniredis runs Lua through a Go interpreter; real Redis runs it through its
// own. Number handling and reply conversion differ between them, so the script
// has to be exercised against the real server before it can be trusted.
func TestTokenBucketAgainstRealRedis(t *testing.T) {
	addr := redisAddr()
	dialRedis(t, addr)

	tb := limiter.NewTokenBucket(newClient(t, addr))
	key := uniqueKey(t)
	rule := limiter.Rule{
		Algorithm: limiter.AlgorithmTokenBucket,
		Limit:     20,          // 20 tokens per second
		Window:    time.Second, //
		Burst:     10,          // but only 10 in hand at once
	}
	ctx := context.Background()

	allowed := 0
	for i := 0; i < 15; i++ {
		d, err := tb.Allow(ctx, key, rule, 1)
		if err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
		if d.Allowed {
			allowed++
		}
	}
	if allowed != 10 {
		t.Errorf("initial burst allowed %d, want the capacity of 10", allowed)
	}

	// 20 tokens/second means 250ms buys 5 more.
	time.Sleep(250 * time.Millisecond)

	allowed = 0
	for i := 0; i < 15; i++ {
		if d, err := tb.Allow(ctx, key, rule, 1); err == nil && d.Allowed {
			allowed++
		}
	}
	// Timing is real here, so allow a token either side rather than pinning an
	// exact count to the scheduler's mood.
	if allowed < 4 || allowed > 7 {
		t.Errorf("after 250ms of refill %d requests were allowed, want about 5", allowed)
	}
}

// The script must be atomic under contention for the same reason the
// transaction is: a read-modify-write that interleaves hands out free tokens.
func TestTokenBucketIsExactUnderConcurrency(t *testing.T) {
	addr := redisAddr()
	dialRedis(t, addr)

	const capacity = 50
	tb := limiter.NewTokenBucket(newClient(t, addr))
	key := uniqueKey(t)
	// A one-hour window makes refill during the test negligible, so any excess
	// is a concurrency bug rather than tokens legitimately earned.
	rule := limiter.Rule{
		Algorithm: limiter.AlgorithmTokenBucket,
		Limit:     capacity,
		Window:    time.Hour,
		Burst:     capacity,
	}

	var allowed, failed atomic.Int64
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 400; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			d, err := tb.Allow(context.Background(), key, rule, 1)
			if err != nil {
				failed.Add(1)
				return
			}
			if d.Allowed {
				allowed.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()

	if got := failed.Load(); got != 0 {
		t.Fatalf("%d requests errored, want none", got)
	}
	if got := allowed.Load(); got != capacity {
		t.Errorf("allowed %d of 400 concurrent requests, want exactly %d", got, capacity)
	}
}

// EVALSHA misses must recover on their own: a restarted or failed-over Redis
// has an empty script cache, and the limiter cannot require a redeploy to
// start working again.
func TestTokenBucketRecoversFromFlushedScriptCache(t *testing.T) {
	addr := redisAddr()
	rdb := dialRedis(t, addr)

	tb := limiter.NewTokenBucket(newClient(t, addr))
	key := uniqueKey(t)
	rule := limiter.Rule{
		Algorithm: limiter.AlgorithmTokenBucket,
		Limit:     10, Window: time.Minute, Burst: 10,
	}
	ctx := context.Background()

	if _, err := tb.Allow(ctx, key, rule, 1); err != nil {
		t.Fatalf("first request: %v", err)
	}
	if err := rdb.ScriptFlush(ctx).Err(); err != nil {
		t.Fatalf("SCRIPT FLUSH: %v", err)
	}
	if _, err := tb.Allow(ctx, key, rule, 1); err != nil {
		t.Errorf("request after the script cache was flushed: %v", err)
	}
}
