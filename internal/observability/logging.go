// Package observability wires up logging and metrics.
package observability

import (
	"io"
	"log/slog"
	"strings"

	"github.com/Rishabh-10/distributed-rate-limiter/internal/config"
)

// NewLogger builds a structured logger from config.
func NewLogger(cfg config.Log, w io.Writer) *slog.Logger {
	opts := &slog.HandlerOptions{Level: parseLevel(cfg.Level)}
	if strings.EqualFold(cfg.Format, "text") {
		return slog.New(slog.NewTextHandler(w, opts))
	}
	return slog.New(slog.NewJSONHandler(w, opts))
}

func parseLevel(level string) slog.Level {
	switch strings.ToLower(level) {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
