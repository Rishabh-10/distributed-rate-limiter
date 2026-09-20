// Package config loads, validates and hot-reloads the service configuration.
//
// Quotas live entirely in this YAML file; there is no admin API and no quota
// state in Redis. Editing the file is the supported way to change limits, and
// the running service picks the change up without a restart (see watch.go).
package config

import (
	"errors"
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/Rishabh-10/distributed-rate-limiter/internal/limiter"
)

// Duration is a time.Duration that unmarshals from a YAML string such as
// "1m" or "50ms". yaml.v3 has no native support for time.Duration.
type Duration time.Duration

// UnmarshalYAML implements yaml.Unmarshaler.
func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	var s string
	if err := value.Decode(&s); err != nil {
		return fmt.Errorf("duration must be a string like \"1m\" or \"50ms\": %w", err)
	}
	parsed, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", s, err)
	}
	*d = Duration(parsed)
	return nil
}

// MarshalYAML implements yaml.Marshaler.
func (d Duration) MarshalYAML() (any, error) { return d.String(), nil }

// String renders the duration in Go's duration syntax.
func (d Duration) String() string { return time.Duration(d).String() }

// Std converts back to a time.Duration.
func (d Duration) Std() time.Duration { return time.Duration(d) }

// Config is the whole configuration file.
type Config struct {
	Server  Server  `yaml:"server"`
	Redis   Redis   `yaml:"redis"`
	Limiter Limiter `yaml:"limiter"`
	Log     Log     `yaml:"log"`
}

// Server holds HTTP server settings.
type Server struct {
	Addr          string   `yaml:"addr"`
	ReadTimeout   Duration `yaml:"read_timeout"`
	WriteTimeout  Duration `yaml:"write_timeout"`
	IdleTimeout   Duration `yaml:"idle_timeout"`
	ShutdownGrace Duration `yaml:"shutdown_grace"`
}

// Redis holds connection settings for the shared Redis instance.
type Redis struct {
	Addr     string `yaml:"addr"`
	Password string `yaml:"password"`
	DB       int    `yaml:"db"`
	// CommandTimeout bounds every Redis round trip. It is deliberately small:
	// a slow Redis must never become a slow API.
	CommandTimeout Duration `yaml:"command_timeout"`
	DialTimeout    Duration `yaml:"dial_timeout"`
	PoolSize       int      `yaml:"pool_size"`
	// KeyPrefix namespaces every key this service writes.
	KeyPrefix string `yaml:"key_prefix"`
}

// Log holds logging settings.
type Log struct {
	Level  string `yaml:"level"`  // debug | info | warn | error
	Format string `yaml:"format"` // json | text
}

// Limiter holds limiting behaviour and the quota table.
type Limiter struct {
	// FailOpen allows requests through when Redis cannot be reached. Turning
	// this off makes the limiter fail closed, which trades availability for
	// strict enforcement.
	FailOpen bool `yaml:"fail_open"`
	// CountRejected keeps rejected requests in the window, so sustained abuse
	// extends the lockout. When false (the default) a rejected request is
	// removed again and does not consume a slot.
	CountRejected bool    `yaml:"count_rejected"`
	Breaker       Breaker `yaml:"breaker"`
	// DecisionTimeout bounds a whole decision, which may take more than one
	// Redis round trip (the transaction, then a compensating ZREM). It is
	// therefore larger than redis.command_timeout, which bounds a single one.
	DecisionTimeout Duration `yaml:"decision_timeout"`

	// Default applies when no more specific rule matches. It must be complete:
	// every other rule inherits its unset fields from here.
	Default Rule `yaml:"default"`
	// Clients keys rules by client ID.
	Clients map[string]Rule `yaml:"clients"`
	// Resources keys rules by resource (typically a route).
	Resources map[string]Rule `yaml:"resources"`
	// Overrides are the most specific rules: one client on one resource.
	Overrides []Override `yaml:"overrides"`
}

// Breaker configures the circuit breaker guarding Redis.
type Breaker struct {
	// FailureThreshold is the number of consecutive failures that trips the
	// breaker open. Zero disables the breaker entirely.
	FailureThreshold int `yaml:"failure_threshold"`
	// OpenFor is how long the breaker stays open before probing Redis again.
	OpenFor Duration `yaml:"open_for"`
}

// Rule is a quota as written in the config file. Unset fields are inherited
// from Limiter.Default, so a per-client entry can say only `limit: 1000`.
type Rule struct {
	Algorithm string   `yaml:"algorithm"`
	Limit     int64    `yaml:"limit"`
	Window    Duration `yaml:"window"`
	Burst     int64    `yaml:"burst"`
}

// Override is a Rule scoped to one client on one resource.
type Override struct {
	Client   string `yaml:"client"`
	Resource string `yaml:"resource"`
	Rule     `yaml:",inline"`
}

// Limiter converts a config Rule into the limiter package's Rule.
func (r Rule) Limiter() limiter.Rule {
	return limiter.Rule{
		Algorithm: r.Algorithm,
		Limit:     r.Limit,
		Window:    r.Window.Std(),
		Burst:     r.Burst,
	}
}

// inherit returns r with every unset field filled in from parent.
func (r Rule) inherit(parent Rule) Rule {
	if r.Algorithm == "" {
		r.Algorithm = parent.Algorithm
	}
	if r.Limit == 0 {
		r.Limit = parent.Limit
	}
	if r.Window == 0 {
		r.Window = parent.Window
	}
	if r.Burst == 0 {
		r.Burst = parent.Burst
	}
	return r
}

// Default returns a Config with every optional field populated. Load starts
// from this, so a minimal config file only needs to state its quotas.
func Default() Config {
	return Config{
		Server: Server{
			Addr:          ":8080",
			ReadTimeout:   Duration(5 * time.Second),
			WriteTimeout:  Duration(10 * time.Second),
			IdleTimeout:   Duration(60 * time.Second),
			ShutdownGrace: Duration(10 * time.Second),
		},
		Redis: Redis{
			Addr:           "localhost:6379",
			DB:             0,
			CommandTimeout: Duration(50 * time.Millisecond),
			DialTimeout:    Duration(200 * time.Millisecond),
			PoolSize:       50,
			KeyPrefix:      "rl",
		},
		Log: Log{Level: "info", Format: "json"},
		Limiter: Limiter{
			FailOpen:        true,
			CountRejected:   false,
			DecisionTimeout: Duration(150 * time.Millisecond),
			Breaker: Breaker{
				FailureThreshold: 5,
				OpenFor:          Duration(5 * time.Second),
			},
			Default: Rule{
				Algorithm: limiter.AlgorithmSlidingWindow,
				Limit:     100,
				Window:    Duration(time.Minute),
			},
		},
	}
}

// Load reads, normalises and validates the config file at path.
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	return Parse(raw)
}

// Parse normalises and validates config bytes. It is separated from Load so
// tests (and the hot-reloader) can work on in-memory documents.
func Parse(raw []byte) (*Config, error) {
	cfg := Default()
	dec := yaml.Unmarshal
	if err := dec(raw, &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	cfg.normalise()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// normalise pushes Default's values down into every other rule, so callers
// never have to reason about inheritance again.
func (c *Config) normalise() {
	def := c.Limiter.Default
	for k, r := range c.Limiter.Clients {
		c.Limiter.Clients[k] = r.inherit(def)
	}
	for k, r := range c.Limiter.Resources {
		c.Limiter.Resources[k] = r.inherit(def)
	}
	for i, o := range c.Limiter.Overrides {
		// An override inherits from the client rule when one exists, so
		// `overrides: [{client: acme, resource: /search, limit: 50}]` picks up
		// acme's algorithm rather than the global default's.
		parent := def
		if cr, ok := c.Limiter.Clients[o.Client]; ok {
			parent = cr
		}
		c.Limiter.Overrides[i].Rule = o.Rule.inherit(parent)
	}
}

// Validate reports the first problem that would make the config unusable.
func (c *Config) Validate() error {
	if c.Server.Addr == "" {
		return errors.New("server.addr must not be empty")
	}
	if c.Redis.Addr == "" {
		return errors.New("redis.addr must not be empty")
	}
	if c.Redis.CommandTimeout <= 0 {
		return errors.New("redis.command_timeout must be positive")
	}
	if c.Redis.PoolSize <= 0 {
		return errors.New("redis.pool_size must be positive")
	}
	if c.Limiter.DecisionTimeout <= 0 {
		return errors.New("limiter.decision_timeout must be positive")
	}
	if c.Limiter.Breaker.FailureThreshold > 0 && c.Limiter.Breaker.OpenFor <= 0 {
		return errors.New("limiter.breaker.open_for must be positive when the breaker is enabled")
	}
	if err := validateRule("limiter.default", c.Limiter.Default); err != nil {
		return err
	}
	for name, r := range c.Limiter.Clients {
		if err := validateRule("limiter.clients."+name, r); err != nil {
			return err
		}
	}
	for name, r := range c.Limiter.Resources {
		if err := validateRule("limiter.resources."+name, r); err != nil {
			return err
		}
	}
	for i, o := range c.Limiter.Overrides {
		where := fmt.Sprintf("limiter.overrides[%d]", i)
		if o.Client == "" || o.Resource == "" {
			return fmt.Errorf("%s: client and resource are both required", where)
		}
		if err := validateRule(where, o.Rule); err != nil {
			return err
		}
	}
	switch c.Log.Level {
	case "debug", "info", "warn", "error":
	default:
		return fmt.Errorf("log.level %q must be one of debug, info, warn, error", c.Log.Level)
	}
	switch c.Log.Format {
	case "json", "text":
	default:
		return fmt.Errorf("log.format %q must be json or text", c.Log.Format)
	}
	return nil
}

func validateRule(where string, r Rule) error {
	switch r.Algorithm {
	case limiter.AlgorithmSlidingWindow, limiter.AlgorithmTokenBucket:
	default:
		return fmt.Errorf("%s: algorithm %q must be %q or %q",
			where, r.Algorithm, limiter.AlgorithmSlidingWindow, limiter.AlgorithmTokenBucket)
	}
	if r.Limit <= 0 {
		return fmt.Errorf("%s: limit must be positive, got %d", where, r.Limit)
	}
	if r.Window <= 0 {
		return fmt.Errorf("%s: window must be positive, got %s", where, r.Window)
	}
	if r.Burst < 0 {
		return fmt.Errorf("%s: burst must not be negative, got %d", where, r.Burst)
	}
	return nil
}
