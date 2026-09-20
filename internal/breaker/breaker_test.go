package breaker

import (
	"sync"
	"testing"
	"time"
)

// withFixedClock replaces the breaker's clock so cool-off behaviour can be
// asserted without sleeping.
func withFixedClock(b *Breaker) func(time.Duration) {
	var mu sync.Mutex
	now := time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)
	b.now = func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return now
	}
	return func(d time.Duration) {
		mu.Lock()
		defer mu.Unlock()
		now = now.Add(d)
	}
}

func TestBreakerOpensAfterThreshold(t *testing.T) {
	b := New(3, time.Minute)
	withFixedClock(b)

	for i := 0; i < 2; i++ {
		b.Failure()
		if !b.Allow() {
			t.Fatalf("breaker opened after %d failures, want it to hold until 3", i+1)
		}
	}

	b.Failure()
	if b.Allow() {
		t.Error("breaker still closed after 3 failures")
	}
	if got := b.State(); got != StateOpen {
		t.Errorf("state = %s, want open", got)
	}
}

// The threshold counts *consecutive* failures: an intervening success means
// Redis is answering, so occasional blips must not accumulate into a trip.
func TestBreakerResetsOnSuccess(t *testing.T) {
	b := New(3, time.Minute)
	withFixedClock(b)

	b.Failure()
	b.Failure()
	b.Success()
	b.Failure()
	b.Failure()

	if !b.Allow() {
		t.Error("breaker opened on non-consecutive failures")
	}
}

func TestBreakerHalfOpensAfterCooloff(t *testing.T) {
	b := New(1, time.Minute)
	advance := withFixedClock(b)

	b.Failure()
	if b.Allow() {
		t.Fatal("breaker did not open")
	}

	advance(30 * time.Second)
	if b.Allow() {
		t.Error("breaker probed before its cool-off elapsed")
	}

	advance(31 * time.Second)
	if !b.Allow() {
		t.Fatal("breaker did not half-open after its cool-off")
	}
	if got := b.State(); got != StateHalfOpen {
		t.Errorf("state = %s, want half_open", got)
	}

	// Only one probe at a time: the rest keep being short-circuited rather
	// than piling onto a Redis that may still be down.
	if b.Allow() {
		t.Error("breaker admitted a second concurrent probe")
	}

	b.Success()
	if got := b.State(); got != StateClosed {
		t.Errorf("state after successful probe = %s, want closed", got)
	}
	if !b.Allow() {
		t.Error("breaker still blocking after a successful probe")
	}
}

// A failed probe means Redis is still unwell: reopen immediately rather than
// waiting for the full threshold again.
func TestBreakerReopensOnFailedProbe(t *testing.T) {
	b := New(2, time.Minute)
	advance := withFixedClock(b)

	b.Failure()
	b.Failure()
	advance(time.Minute)

	if !b.Allow() {
		t.Fatal("breaker did not half-open")
	}
	b.Failure()

	if b.Allow() {
		t.Error("breaker allowed traffic after a failed probe")
	}
	if got := b.State(); got != StateOpen {
		t.Errorf("state = %s, want open", got)
	}

	advance(time.Minute)
	if !b.Allow() {
		t.Error("breaker did not half-open again after a second cool-off")
	}
}

// A zero threshold means "no breaker": every call goes through, and the
// per-command timeout is the only protection.
func TestBreakerDisabled(t *testing.T) {
	b := New(0, time.Minute)
	for i := 0; i < 100; i++ {
		b.Failure()
	}
	if !b.Allow() {
		t.Error("disabled breaker blocked a call")
	}
	if got := b.State(); got != StateClosed {
		t.Errorf("state = %s, want closed", got)
	}
}

func TestBreakerIsConcurrencySafe(t *testing.T) {
	b := New(5, time.Millisecond)

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				b.Allow()
				if (i+j)%3 == 0 {
					b.Failure()
				} else {
					b.Success()
				}
				b.State()
			}
		}(i)
	}
	wg.Wait()
}
