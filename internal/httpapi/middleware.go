package httpapi

import (
	"log/slog"
	"net/http"
	"runtime/debug"
	"time"
)

// statusRecorder captures the status code for logging and metrics.
type statusRecorder struct {
	http.ResponseWriter
	status int
	wrote  bool
}

func (r *statusRecorder) WriteHeader(status int) {
	if !r.wrote {
		r.status = status
		r.wrote = true
	}
	r.ResponseWriter.WriteHeader(status)
}

func (r *statusRecorder) Write(b []byte) (int, error) {
	if !r.wrote {
		r.status = http.StatusOK
		r.wrote = true
	}
	return r.ResponseWriter.Write(b)
}

// instrument records metrics and an access log line for one route.
//
// The route argument is the registered pattern rather than r.URL.Path: paths
// carry client IDs, and a metric label per client is how a monitoring system
// gets taken down by its own instrumentation.
func (s *Server) instrument(route string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}

		next.ServeHTTP(rec, r)

		elapsed := time.Since(start)
		s.deps.Metrics.ObserveHTTP(route, r.Method, rec.status, elapsed.Seconds())
		// Health checks are high-volume and uninteresting; logging them at
		// info level buries everything else.
		level := slog.LevelInfo
		if route == "/healthz" || route == "/readyz" {
			level = slog.LevelDebug
		}
		s.log.Log(r.Context(), level, "request",
			"route", route,
			"method", r.Method,
			"status", rec.status,
			"duration_ms", elapsed.Milliseconds(),
		)
	})
}

// recoverPanics keeps one bad request from taking the process down. A limiter
// that crashes is worse than a limiter that fails open.
func recoverPanics(log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if v := recover(); v != nil {
					log.Error("panic serving request",
						"path", r.URL.Path,
						"method", r.Method,
						"panic", v,
						"stack", string(debug.Stack()),
					)
					writeError(w, log, http.StatusInternalServerError, CodeInternal,
						"internal error")
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}
