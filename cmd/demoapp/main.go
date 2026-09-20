// Command demoapp is a small service that sits behind the rate limiter, so the
// whole system can be exercised end to end with `docker compose up`.
//
// It is the worked example of the integration story: a service adds three
// lines and its handlers are limited, with 429s, Retry-After and fail-open
// behaviour handled for it.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/Rishabh-10/distributed-rate-limiter/pkg/ratelimit"
)

func main() {
	addr := flag.String("addr", envOr("DEMO_ADDR", ":8081"), "address to listen on")
	limiterURL := flag.String("limiter", envOr("LIMITER_URL", "http://localhost:8080"),
		"base URL of the rate limiter service")
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	limiter := ratelimit.New(*limiterURL,
		ratelimit.WithTimeout(50*time.Millisecond),
		// Health checks must never be throttled: an orchestrator reading a 429
		// as "unhealthy" would restart a perfectly good process.
		ratelimit.WithSkipFunc(func(r *http.Request) bool { return r.URL.Path == "/healthz" }),
		ratelimit.WithErrorHandler(func(r *http.Request, err error) {
			log.Warn("rate limiter unreachable, allowing request",
				"path", r.URL.Path, "error", err)
		}),
	)

	mux := http.NewServeMux()
	mux.Handle("/api/", limiter.Middleware(http.HandlerFunc(hello)))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte(`{"status":"ok"}`))
	})

	log.Info("demoapp listening", "addr", *addr, "limiter", *limiterURL)
	srv := &http.Server{
		Addr:              *addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	if err := srv.ListenAndServe(); err != nil {
		fmt.Fprintf(os.Stderr, "demoapp: %v\n", err)
		os.Exit(1)
	}
}

func hello(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	json.NewEncoder(w).Encode(map[string]string{
		"message": "hello",
		"path":    r.URL.Path,
		"client":  r.Header.Get("X-Client-ID"),
	})
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
