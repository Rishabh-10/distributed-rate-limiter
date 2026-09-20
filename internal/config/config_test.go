package config

import (
	"strings"
	"testing"
	"time"

	"github.com/Rishabh-10/distributed-rate-limiter/internal/limiter"
)

func TestParseAppliesDefaults(t *testing.T) {
	cfg, err := Parse([]byte("limiter:\n  default:\n    limit: 5\n    window: 1s\n"))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if cfg.Server.Addr != ":8080" {
		t.Errorf("server.addr = %q, want :8080", cfg.Server.Addr)
	}
	if got := cfg.Redis.CommandTimeout.Std(); got != 50*time.Millisecond {
		t.Errorf("redis.command_timeout = %s, want 50ms", got)
	}
	if !cfg.Limiter.FailOpen {
		t.Error("limiter.fail_open should default to true")
	}
	if cfg.Limiter.CountRejected {
		t.Error("limiter.count_rejected should default to false")
	}
	if cfg.Limiter.Default.Algorithm != limiter.AlgorithmSlidingWindow {
		t.Errorf("default algorithm = %q, want sliding_window", cfg.Limiter.Default.Algorithm)
	}
}

func TestParseInheritsUnsetRuleFields(t *testing.T) {
	cfg, err := Parse([]byte(`
limiter:
  default:
    algorithm: sliding_window
    limit: 100
    window: 1m
  clients:
    acme:
      limit: 1000
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

	acme := cfg.Limiter.Clients["acme"]
	if acme.Limit != 1000 {
		t.Errorf("acme limit = %d, want 1000", acme.Limit)
	}
	if acme.Window.Std() != time.Minute {
		t.Errorf("acme window = %s, want 1m (inherited)", acme.Window)
	}
	if acme.Algorithm != limiter.AlgorithmSlidingWindow {
		t.Errorf("acme algorithm = %q, want inherited sliding_window", acme.Algorithm)
	}

	search := cfg.Limiter.Resources["/v1/search"]
	if search.Limit != 5 || search.Window.Std() != time.Second {
		t.Errorf("search rule = %d/%s, want 5/1s", search.Limit, search.Window)
	}

	if n := len(cfg.Limiter.Overrides); n != 1 {
		t.Fatalf("got %d overrides, want 1", n)
	}
	ov := cfg.Limiter.Overrides[0]
	if ov.Client != "acme" || ov.Resource != "/v1/search" {
		t.Errorf("override scope = %s/%s", ov.Client, ov.Resource)
	}
	if ov.Limit != 50 || ov.Window.Std() != time.Second {
		t.Errorf("override rule = %d/%s, want 50/1s", ov.Limit, ov.Window)
	}
}

func TestParseRejectsInvalidConfigs(t *testing.T) {
	tests := []struct {
		name string
		yaml string
		want string
	}{
		{
			name: "zero limit",
			yaml: "limiter:\n  default:\n    limit: 0\n    window: 1s\n",
			want: "limit must be positive",
		},
		{
			name: "unknown algorithm",
			yaml: "limiter:\n  default:\n    algorithm: leaky_bucket\n    limit: 5\n    window: 1s\n",
			want: "algorithm",
		},
		{
			name: "override missing resource",
			yaml: "limiter:\n  overrides:\n    - client: acme\n      limit: 5\n",
			want: "client and resource are both required",
		},
		{
			name: "bad duration",
			yaml: "limiter:\n  default:\n    window: 1 fortnight\n",
			want: "invalid duration",
		},
		{
			name: "not yaml",
			yaml: "limiter: [this is: not, a mapping",
			want: "parse config",
		},
		{
			name: "bad log level",
			yaml: "log:\n  level: chatty\n",
			want: "log.level",
		},
		{
			name: "non-positive command timeout",
			yaml: "redis:\n  command_timeout: 0s\n",
			want: "command_timeout must be positive",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse([]byte(tt.yaml))
			if err == nil {
				t.Fatalf("Parse(%q) succeeded, want error containing %q", tt.yaml, tt.want)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("error = %q, want it to contain %q", err, tt.want)
			}
		})
	}
}

// A zero window on a client rule must inherit rather than fail validation:
// `acme: {limit: 1000}` is the shape most of the config file uses.
func TestParseAcceptsPartialClientRule(t *testing.T) {
	cfg, err := Parse([]byte("limiter:\n  clients:\n    acme:\n      limit: 1000\n"))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if got := cfg.Limiter.Clients["acme"].Window.Std(); got != time.Minute {
		t.Errorf("inherited window = %s, want 1m", got)
	}
}
