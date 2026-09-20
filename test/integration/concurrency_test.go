//go:build integration

package integration

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Rishabh-10/distributed-rate-limiter/internal/limiter"
)

// This is the test the whole design exists to pass.
//
// Two limiter instances — separate clients, separate connection pools, as two
// pods would be — serve one client's quota through a shared Redis while 500
// requests arrive at once. Exactly 100 may be admitted.
//
// A read-then-decide implementation passes this only by luck: both instances
// read "99 used" and both admit, overshooting the quota. Putting the trim, the
// add and the count inside one MULTI/EXEC is what makes the number exact, and
// this test is what proves it. If someone later "optimises" the transaction
// into separate calls, this fails.
func TestConcurrentRequestsNeverExceedTheLimit(t *testing.T) {
	addr := redisAddr()
	dialRedis(t, addr) // fail fast with a helpful message if Redis is absent

	const (
		limit    = 100
		requests = 500
		window   = time.Minute
	)

	instanceA := limiter.NewSlidingWindow(newClient(t, addr))
	instanceB := limiter.NewSlidingWindow(newClient(t, addr))

	key := uniqueKey(t)
	rule := limiter.Rule{Algorithm: limiter.AlgorithmSlidingWindow, Limit: limit, Window: window}

	var allowed, denied, failed atomic.Int64
	start := make(chan struct{})
	var wg sync.WaitGroup

	for i := 0; i < requests; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// Every goroutine blocks here and is released together, so the
			// requests genuinely contend instead of trickling in.
			<-start

			instance := instanceA
			if i%2 == 1 {
				instance = instanceB
			}
			d, err := instance.Allow(context.Background(), key, rule, 1)
			switch {
			case err != nil:
				failed.Add(1)
			case d.Allowed:
				allowed.Add(1)
			default:
				denied.Add(1)
			}
		}(i)
	}

	close(start)
	wg.Wait()

	if got := failed.Load(); got != 0 {
		t.Fatalf("%d requests errored, want none", got)
	}
	if got := allowed.Load(); got != limit {
		t.Errorf("allowed %d of %d requests, want exactly %d", got, requests, limit)
	}
	if got := denied.Load(); got != requests-limit {
		t.Errorf("denied %d requests, want %d", got, requests-limit)
	}
}

// The compensating ZREM must not lose slots under contention either: after a
// storm of rejections the window should hold exactly the requests that were
// actually served, so the next window is not silently smaller.
func TestRejectedRequestsDoNotLeakSlots(t *testing.T) {
	addr := redisAddr()
	rdb := dialRedis(t, addr)

	const limit = 20
	sw := limiter.NewSlidingWindow(newClient(t, addr))
	key := uniqueKey(t)
	rule := limiter.Rule{Algorithm: limiter.AlgorithmSlidingWindow, Limit: limit, Window: time.Minute}

	var wg sync.WaitGroup
	for i := 0; i < 300; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sw.Allow(context.Background(), key, rule, 1)
		}()
	}
	wg.Wait()

	count, err := rdb.ZCard(context.Background(), key).Result()
	if err != nil {
		t.Fatalf("ZCARD: %v", err)
	}
	if count != limit {
		t.Errorf("window holds %d entries after 300 concurrent requests, want %d", count, limit)
	}
}

// Repeated rounds catch an overshoot that a single round might hide: each
// round must admit exactly the limit again once the previous one has aged out.
func TestLimitHoldsAcrossSuccessiveWindows(t *testing.T) {
	addr := redisAddr()
	dialRedis(t, addr)

	const (
		limit  = 25
		window = 300 * time.Millisecond
		rounds = 3
	)
	sw := limiter.NewSlidingWindow(newClient(t, addr))
	key := uniqueKey(t)
	rule := limiter.Rule{Algorithm: limiter.AlgorithmSlidingWindow, Limit: limit, Window: window}

	for round := 1; round <= rounds; round++ {
		var allowed atomic.Int64
		var wg sync.WaitGroup
		for i := 0; i < limit*4; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if d, err := sw.Allow(context.Background(), key, rule, 1); err == nil && d.Allowed {
					allowed.Add(1)
				}
			}()
		}
		wg.Wait()

		if got := allowed.Load(); got != limit {
			t.Fatalf("round %d: allowed %d, want %d", round, got, limit)
		}
		// Let the whole window age out before the next round.
		time.Sleep(window + 100*time.Millisecond)
	}
}

// Different clients must not contend with each other, however hot the traffic.
func TestConcurrentClientsAreIsolated(t *testing.T) {
	addr := redisAddr()
	dialRedis(t, addr)

	const (
		clients = 20
		limit   = 5
	)
	sw := limiter.NewSlidingWindow(newClient(t, addr))
	prefix := uniqueKey(t)
	rule := limiter.Rule{Algorithm: limiter.AlgorithmSlidingWindow, Limit: limit, Window: time.Minute}

	allowedPer := make([]atomic.Int64, clients)
	var wg sync.WaitGroup
	for c := 0; c < clients; c++ {
		for i := 0; i < limit*3; i++ {
			wg.Add(1)
			go func(c int) {
				defer wg.Done()
				key := prefix + ":" + string(rune('a'+c))
				if d, err := sw.Allow(context.Background(), key, rule, 1); err == nil && d.Allowed {
					allowedPer[c].Add(1)
				}
			}(c)
		}
	}
	wg.Wait()

	for c := 0; c < clients; c++ {
		if got := allowedPer[c].Load(); got != limit {
			t.Errorf("client %d was allowed %d requests, want %d", c, got, limit)
		}
	}
}

func newClient(t *testing.T, addr string) *redis.Client {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{
		Addr:         addr,
		DialTimeout:  2 * time.Second,
		ReadTimeout:  2 * time.Second,
		WriteTimeout: 2 * time.Second,
		PoolSize:     100,
	})
	t.Cleanup(func() { rdb.Close() })
	return rdb
}
