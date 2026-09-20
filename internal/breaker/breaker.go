// Package breaker implements the circuit breaker that guards calls to Redis.
//
// It lives in its own dependency-free package so that both the limiter (which
// consumes a breaker) and the Redis layer (which supplies the failures) can
// use it without either importing the other.
package breaker

import (
	"sync"
	"time"
)

// State is the breaker's current position.
type State string

// Breaker states.
const (
	StateClosed   State = "closed"    // traffic flows to Redis
	StateOpen     State = "open"      // Redis is skipped entirely
	StateHalfOpen State = "half_open" // one probe is allowed through
)

// Breaker is a small circuit breaker guarding Redis calls.
//
// Without it, a dead Redis costs every single request a full command timeout
// before the limiter gives up and fails open. With it, the first few failures
// trip the breaker and subsequent requests skip Redis entirely, so an outage
// costs microseconds instead of tens of milliseconds per request.
type Breaker struct {
	threshold int
	openFor   time.Duration
	now       func() time.Time

	mu       sync.Mutex
	state    State
	failures int
	openedAt time.Time
	// probing guards the half-open state so exactly one request at a time is
	// used to test whether Redis has come back.
	probing bool
}

// New returns a breaker that opens after threshold consecutive failures and
// stays open for openFor before probing again.
//
// A threshold of zero disables the breaker: every call is allowed through and
// the per-command timeout is the only protection.
func New(threshold int, openFor time.Duration) *Breaker {
	return &Breaker{
		threshold: threshold,
		openFor:   openFor,
		now:       time.Now,
		state:     StateClosed,
	}
}

// Allow reports whether a call to Redis should be attempted.
func (b *Breaker) Allow() bool {
	if b == nil || b.threshold <= 0 {
		return true
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	switch b.state {
	case StateClosed:
		return true
	case StateOpen:
		if b.now().Sub(b.openedAt) < b.openFor {
			return false
		}
		// Cool-off elapsed: promote to half-open and let this caller probe.
		b.state = StateHalfOpen
		b.probing = true
		return true
	default: // StateHalfOpen
		if b.probing {
			return false
		}
		b.probing = true
		return true
	}
}

// Success records a successful call, closing the breaker.
func (b *Breaker) Success() {
	if b == nil || b.threshold <= 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.state = StateClosed
	b.failures = 0
	b.probing = false
}

// Failure records a failed call. A failure while probing reopens the breaker
// immediately — one bad probe is enough to know Redis is still unwell.
func (b *Breaker) Failure() {
	if b == nil || b.threshold <= 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.state == StateHalfOpen {
		b.state = StateOpen
		b.openedAt = b.now()
		b.probing = false
		return
	}
	b.failures++
	if b.failures >= b.threshold {
		b.state = StateOpen
		b.openedAt = b.now()
	}
}

// State returns the breaker's current state, for metrics and /readyz.
func (b *Breaker) State() State {
	if b == nil || b.threshold <= 0 {
		return StateClosed
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}
