package limiter

import (
	"context"
	"fmt"
)

// Dispatcher routes each request to the algorithm named by its rule, so the
// choice of algorithm is per-rule config rather than a process-wide decision.
type Dispatcher struct {
	algorithms map[string]Algorithm
}

// NewDispatcher returns a Dispatcher over the given algorithms, keyed by the
// names used in config (sliding_window, token_bucket).
func NewDispatcher(algorithms map[string]Algorithm) *Dispatcher {
	return &Dispatcher{algorithms: algorithms}
}

// Allow implements Algorithm.
func (d *Dispatcher) Allow(ctx context.Context, key string, rule Rule, cost int64) (Decision, error) {
	algo, ok := d.algorithms[rule.Algorithm]
	if !ok {
		// Config validation rejects unknown algorithm names, so reaching here
		// means the process was built without the implementation the config
		// asks for. That is a wiring bug, not a Redis failure: it must not be
		// laundered into a fail-open allow.
		return Decision{}, fmt.Errorf("no implementation registered for algorithm %q", rule.Algorithm)
	}
	return algo.Allow(ctx, key, rule, cost)
}
