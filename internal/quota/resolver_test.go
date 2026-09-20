package quota

import (
	"testing"
	"time"

	"github.com/Rishabh-10/distributed-rate-limiter/internal/config"
)

func testResolver(t *testing.T) *Resolver {
	t.Helper()
	cfg, err := config.Parse([]byte(`
limiter:
  default:
    algorithm: sliding_window
    limit: 100
    window: 1m
  clients:
    acme:
      limit: 1000
    free:
      limit: 10
  resources:
    "/v1/search":
      limit: 5
      window: 1s
  overrides:
    - client: acme
      resource: "/v1/search"
      limit: 50
      window: 1s
`))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	return New(cfg.Limiter)
}

func TestResolvePrecedence(t *testing.T) {
	r := testResolver(t)

	tests := []struct {
		name       string
		client     string
		resource   string
		wantLimit  int64
		wantWindow time.Duration
		wantSource Source
	}{
		{"override beats everything", "acme", "/v1/search", 50, time.Second, SourceOverride},
		{"client beats resource", "acme", "/v1/other", 1000, time.Minute, SourceClient},
		{"client beats resource even on a limited route", "free", "/v1/search", 10, time.Minute, SourceClient},
		{"resource when client is unknown", "stranger", "/v1/search", 5, time.Second, SourceResource},
		{"default when nothing matches", "stranger", "/v1/other", 100, time.Minute, SourceDefault},
		{"empty identifiers fall back to default", "", "", 100, time.Minute, SourceDefault},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := r.Resolve(tt.client, tt.resource)
			if got.Rule.Limit != tt.wantLimit {
				t.Errorf("limit = %d, want %d", got.Rule.Limit, tt.wantLimit)
			}
			if got.Rule.Window != tt.wantWindow {
				t.Errorf("window = %s, want %s", got.Rule.Window, tt.wantWindow)
			}
			if got.Source != tt.wantSource {
				t.Errorf("source = %s, want %s", got.Source, tt.wantSource)
			}
		})
	}
}

func TestStoreSwapsAtomically(t *testing.T) {
	first := testResolver(t)
	s := NewStore(first)

	if got := s.Load().Resolve("acme", "/x").Rule.Limit; got != 1000 {
		t.Fatalf("limit before swap = %d, want 1000", got)
	}

	cfg, err := config.Parse([]byte("limiter:\n  clients:\n    acme:\n      limit: 7\n"))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	s.Set(New(cfg.Limiter))

	if got := s.Load().Resolve("acme", "/x").Rule.Limit; got != 7 {
		t.Errorf("limit after swap = %d, want 7", got)
	}
	// The old snapshot is untouched, which is what in-flight requests keep using.
	if got := first.Resolve("acme", "/x").Rule.Limit; got != 1000 {
		t.Errorf("old resolver mutated: limit = %d, want 1000", got)
	}
}
