package limiter

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Rishabh-10/distributed-rate-limiter/internal/breaker"
)

// stubAlgorithm returns a canned decision or error and counts its calls.
type stubAlgorithm struct {
	calls    int
	decision Decision
	err      error
	delay    time.Duration
}

func (s *stubAlgorithm) Allow(ctx context.Context, _ string, _ Rule, _ int64) (Decision, error) {
	s.calls++
	if s.delay > 0 {
		select {
		case <-time.After(s.delay):
		case <-ctx.Done():
			return Decision{}, ctx.Err()
		}
	}
	return s.decision, s.err
}

var errDown = errors.New("dial tcp 127.0.0.1:6379: connect: connection refused")

func TestGuardFailsOpenWhenRedisIsDown(t *testing.T) {
	stub := &stubAlgorithm{err: errDown}
	g := NewGuard(stub, true)

	d, err := g.Allow(context.Background(), "k", rule(10, time.Second), 1)
	if err != nil {
		t.Fatalf("Allow returned an error, want a degraded decision: %v", err)
	}
	if !d.Allowed {
		t.Error("request denied while Redis is down, want fail-open allow")
	}
	if !d.Degraded {
		t.Error("decision not marked degraded")
	}
	if d.Remaining != 10 {
		t.Errorf("remaining = %d, want the full quota when nothing was counted", d.Remaining)
	}
}

func TestGuardFailsClosedWhenConfigured(t *testing.T) {
	g := NewGuard(&stubAlgorithm{err: errDown}, false)

	d, err := g.Allow(context.Background(), "k", rule(10, time.Second), 1)
	if err != nil {
		t.Fatalf("Allow: %v", err)
	}
	if d.Allowed {
		t.Error("request allowed, want fail-closed denial")
	}
	if !d.Degraded {
		t.Error("decision not marked degraded")
	}
	if d.RetryAfter == 0 {
		t.Error("retry-after = 0 on a fail-closed denial, want a positive hint")
	}
}

// A bug is not an outage: errors that do not mean "Redis is unavailable" must
// surface rather than being laundered into an allow.
func TestGuardPropagatesNonAvailabilityErrors(t *testing.T) {
	bug := errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
	g := NewGuard(&stubAlgorithm{err: bug}, true,
		WithUnavailableFunc(func(err error) bool { return errors.Is(err, errDown) }))

	if _, err := g.Allow(context.Background(), "k", rule(10, time.Second), 1); !errors.Is(err, bug) {
		t.Fatalf("error = %v, want it to propagate", err)
	}
}

func TestGuardTimesOutSlowRedis(t *testing.T) {
	stub := &stubAlgorithm{delay: time.Second}
	g := NewGuard(stub, true, WithTimeout(20*time.Millisecond))

	start := time.Now()
	d, err := g.Allow(context.Background(), "k", rule(10, time.Second), 1)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("Allow: %v", err)
	}
	if !d.Allowed || !d.Degraded {
		t.Errorf("allowed=%v degraded=%v, want a degraded allow", d.Allowed, d.Degraded)
	}
	if elapsed > 500*time.Millisecond {
		t.Errorf("waited %s for a slow Redis, want the call bounded by the timeout", elapsed)
	}
}

// Once the breaker is open, a dead Redis must cost nothing at all: the
// underlying call is skipped entirely.
func TestGuardBreakerStopsCallingDeadRedis(t *testing.T) {
	stub := &stubAlgorithm{err: errDown}
	cb := breaker.New(3, time.Minute)
	g := NewGuard(stub, true, WithBreaker(cb))

	r := rule(10, time.Second)
	for i := 0; i < 3; i++ {
		if _, err := g.Allow(context.Background(), "k", r, 1); err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
	}
	if stub.calls != 3 {
		t.Fatalf("underlying calls = %d, want 3 before the breaker trips", stub.calls)
	}

	for i := 0; i < 10; i++ {
		d, err := g.Allow(context.Background(), "k", r, 1)
		if err != nil {
			t.Fatalf("post-trip call %d: %v", i, err)
		}
		if !d.Allowed || !d.Degraded {
			t.Fatalf("post-trip call %d: allowed=%v degraded=%v", i, d.Allowed, d.Degraded)
		}
	}
	if stub.calls != 3 {
		t.Errorf("underlying calls = %d, want the breaker to have skipped Redis entirely", stub.calls)
	}
}

func TestGuardRecordsDegradeReasons(t *testing.T) {
	var reasons []DegradeReason
	stub := &stubAlgorithm{err: errDown}
	cb := breaker.New(1, time.Minute)
	g := NewGuard(stub, true,
		WithBreaker(cb),
		WithDegradeHook(func(r DegradeReason, _ error) { reasons = append(reasons, r) }))

	r := rule(10, time.Second)
	g.Allow(context.Background(), "k", r, 1) // trips the breaker
	g.Allow(context.Background(), "k", r, 1) // skipped by the open breaker

	want := []DegradeReason{ReasonUnavailable, ReasonBreakerOpen}
	if len(reasons) != len(want) {
		t.Fatalf("reasons = %v, want %v", reasons, want)
	}
	for i := range want {
		if reasons[i] != want[i] {
			t.Errorf("reason[%d] = %s, want %s", i, reasons[i], want[i])
		}
	}
}

// A recovered Redis must be picked back up: the breaker half-opens, the probe
// succeeds, and enforcement resumes.
func TestGuardResumesEnforcementAfterRecovery(t *testing.T) {
	stub := &stubAlgorithm{err: errDown}
	cb := breaker.New(2, 50*time.Millisecond)
	g := NewGuard(stub, true, WithBreaker(cb))

	r := rule(10, time.Second)
	g.Allow(context.Background(), "k", r, 1)
	g.Allow(context.Background(), "k", r, 1)

	stub.err = nil
	stub.decision = Decision{Allowed: false, Limit: 10}

	// Still open: the cool-off has not elapsed.
	if d, _ := g.Allow(context.Background(), "k", r, 1); !d.Degraded {
		t.Error("breaker closed before its cool-off elapsed")
	}

	time.Sleep(60 * time.Millisecond)

	d, err := g.Allow(context.Background(), "k", r, 1)
	if err != nil {
		t.Fatalf("Allow after recovery: %v", err)
	}
	if d.Degraded {
		t.Error("decision still degraded after Redis recovered")
	}
	if d.Allowed {
		t.Error("enforcement did not resume: an over-quota request was allowed")
	}
}
