package observability

import (
	"sync"
	"time"
)

// Sampler rate-limits logging.
//
// A Redis outage produces one failure per request, and logging each one turns
// a Redis incident into a disk-space incident. The first event gets through
// immediately — an operator should see the problem at once — and after that at
// most one per interval.
type Sampler struct {
	interval time.Duration
	now      func() time.Time

	mu      sync.Mutex
	last    time.Time
	skipped int
}

// NewSampler returns a Sampler admitting at most one event per interval.
func NewSampler(interval time.Duration) *Sampler {
	return &Sampler{interval: interval, now: time.Now}
}

// Allow reports whether to log this event, and how many were suppressed since
// the last one that was logged. Including that count keeps the logs honest
// about the true rate.
func (s *Sampler) Allow() (allow bool, skipped int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now()
	if !s.last.IsZero() && now.Sub(s.last) < s.interval {
		s.skipped++
		return false, 0
	}
	skipped = s.skipped
	s.skipped = 0
	s.last = now
	return true, skipped
}
