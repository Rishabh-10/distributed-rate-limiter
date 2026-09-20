package config

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestWatchReloadsOnChange(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	write(t, path, "limiter:\n  default:\n    limit: 5\n    window: 1s\n")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	loaded := make(chan *Config, 4)
	w, err := Watch(ctx, path, discardLogger(), func(c *Config) { loaded <- c })
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	defer w.Close()

	write(t, path, "limiter:\n  default:\n    limit: 42\n    window: 1s\n")

	select {
	case cfg := <-loaded:
		if cfg.Limiter.Default.Limit != 42 {
			t.Errorf("reloaded limit = %d, want 42", cfg.Limiter.Default.Limit)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for config reload")
	}
}

// A broken edit must not be applied: the service keeps serving the last good
// config rather than falling over because someone fat-fingered the quota table.
func TestWatchIgnoresInvalidConfig(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	write(t, path, "limiter:\n  default:\n    limit: 5\n    window: 1s\n")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	loaded := make(chan *Config, 4)
	w, err := Watch(ctx, path, discardLogger(), func(c *Config) { loaded <- c })
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	defer w.Close()

	write(t, path, "limiter:\n  default:\n    limit: -1\n    window: 1s\n")

	select {
	case cfg := <-loaded:
		t.Fatalf("invalid config was applied: limit=%d", cfg.Limiter.Default.Limit)
	case <-time.After(time.Second):
		// No callback within the debounce window plus slack: correct.
	}

	// A subsequent good edit must still be picked up — one bad write should not
	// wedge the watcher.
	write(t, path, "limiter:\n  default:\n    limit: 7\n    window: 1s\n")
	select {
	case cfg := <-loaded:
		if cfg.Limiter.Default.Limit != 7 {
			t.Errorf("reloaded limit = %d, want 7", cfg.Limiter.Default.Limit)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("watcher stopped reloading after an invalid config")
	}
}

func write(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
