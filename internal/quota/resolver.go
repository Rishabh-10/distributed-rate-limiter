// Package quota turns a (client, resource) pair into the rule that governs it.
//
// The resolver is immutable once built. Hot reloads construct a fresh resolver
// and swap it into a Store atomically, so in-flight requests keep using a
// consistent snapshot of the quota table rather than observing a half-applied
// edit.
package quota

import (
	"sync/atomic"

	"github.com/Rishabh-10/distributed-rate-limiter/internal/config"
	"github.com/Rishabh-10/distributed-rate-limiter/internal/limiter"
)

// Source says which config entry produced a rule. It is reported by the
// /v1/quotas endpoint, which exists to answer "why is this client getting
// this limit?".
type Source string

// The four places a rule can come from, most specific first.
const (
	SourceOverride Source = "override"
	SourceClient   Source = "client"
	SourceResource Source = "resource"
	SourceDefault  Source = "default"
)

// Resolution is a resolved rule plus the reason it was chosen.
type Resolution struct {
	Rule   limiter.Rule `json:"rule"`
	Source Source       `json:"source"`
}

// Resolver answers quota lookups from an immutable snapshot of the config.
type Resolver struct {
	def       limiter.Rule
	clients   map[string]limiter.Rule
	resources map[string]limiter.Rule
	overrides map[overrideKey]limiter.Rule
}

type overrideKey struct {
	client   string
	resource string
}

// New builds a Resolver from the limiter section of a config.
//
// The config package has already pushed defaults into every rule, so each map
// entry here is complete and lookups never need to merge anything.
func New(cfg config.Limiter) *Resolver {
	r := &Resolver{
		def:       cfg.Default.Limiter(),
		clients:   make(map[string]limiter.Rule, len(cfg.Clients)),
		resources: make(map[string]limiter.Rule, len(cfg.Resources)),
		overrides: make(map[overrideKey]limiter.Rule, len(cfg.Overrides)),
	}
	for name, rule := range cfg.Clients {
		r.clients[name] = rule.Limiter()
	}
	for name, rule := range cfg.Resources {
		r.resources[name] = rule.Limiter()
	}
	for _, o := range cfg.Overrides {
		r.overrides[overrideKey{o.Client, o.Resource}] = o.Rule.Limiter()
	}
	return r
}

// Resolve returns the rule governing client on resource.
//
// Precedence, most specific wins:
//
//	override (client + resource) > client > resource > default
//
// A client-level rule beats a resource-level one deliberately: an enterprise
// customer's negotiated quota should not be silently capped by a generic
// per-route limit. Use an override to combine the two.
func (r *Resolver) Resolve(client, resource string) Resolution {
	if rule, ok := r.overrides[overrideKey{client, resource}]; ok {
		return Resolution{Rule: rule, Source: SourceOverride}
	}
	if rule, ok := r.clients[client]; ok {
		return Resolution{Rule: rule, Source: SourceClient}
	}
	if rule, ok := r.resources[resource]; ok {
		return Resolution{Rule: rule, Source: SourceResource}
	}
	return Resolution{Rule: r.def, Source: SourceDefault}
}

// Store holds the current Resolver and allows lock-free reads.
type Store struct {
	current atomic.Pointer[Resolver]
}

// NewStore returns a Store holding r.
func NewStore(r *Resolver) *Store {
	s := &Store{}
	s.Set(r)
	return s
}

// Load returns the current Resolver.
func (s *Store) Load() *Resolver { return s.current.Load() }

// Set replaces the current Resolver. Requests already in flight finish against
// the snapshot they loaded.
func (s *Store) Set(r *Resolver) { s.current.Store(r) }
