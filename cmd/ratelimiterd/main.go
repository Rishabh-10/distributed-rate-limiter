// Command ratelimiterd serves the distributed rate limiter over HTTP.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Rishabh-10/distributed-rate-limiter/internal/breaker"
	"github.com/Rishabh-10/distributed-rate-limiter/internal/config"
	"github.com/Rishabh-10/distributed-rate-limiter/internal/httpapi"
	"github.com/Rishabh-10/distributed-rate-limiter/internal/limiter"
	"github.com/Rishabh-10/distributed-rate-limiter/internal/observability"
	"github.com/Rishabh-10/distributed-rate-limiter/internal/quota"
	"github.com/Rishabh-10/distributed-rate-limiter/internal/redisx"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "ratelimiterd: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	configPath := flag.String("config", "configs/config.yaml", "path to the configuration file")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		return err
	}
	applyEnvOverrides(cfg)

	log := observability.NewLogger(cfg.Log, os.Stdout)
	slog.SetDefault(log)

	metrics := observability.NewMetrics()
	rdb := redisx.New(cfg.Redis)
	defer rdb.Close()

	cb := breaker.New(cfg.Limiter.Breaker.FailureThreshold, cfg.Limiter.Breaker.OpenFor.Std())
	metrics.RegisterBreakerState(func() string { return string(cb.State()) })

	// Degradation is the one event worth shouting about, but shouting once per
	// request during an outage is how logs become the next incident.
	degradeSampler := observability.NewSampler(5 * time.Second)

	// Both algorithms are always registered; which one a request gets is a
	// per-rule config choice, not a process-wide one.
	algorithms := map[string]limiter.Algorithm{
		limiter.AlgorithmSlidingWindow: limiter.NewSlidingWindow(rdb,
			limiter.WithCountRejected(cfg.Limiter.CountRejected)),
		limiter.AlgorithmTokenBucket: limiter.NewTokenBucket(rdb),
	}
	guarded := limiter.NewGuard(
		limiter.NewDispatcher(algorithms),
		cfg.Limiter.FailOpen,
		limiter.WithBreaker(cb),
		limiter.WithTimeout(cfg.Limiter.DecisionTimeout.Std()),
		limiter.WithUnavailableFunc(redisx.IsUnavailable),
		limiter.WithDegradeHook(func(reason limiter.DegradeReason, err error) {
			metrics.ObserveDegrade(reason)
			if ok, skipped := degradeSampler.Allow(); ok {
				log.Warn("rate limiting degraded, not enforcing quotas",
					"reason", reason,
					"fail_open", cfg.Limiter.FailOpen,
					"suppressed_since_last", skipped,
					"error", err)
			}
		}),
	)

	quotas := quota.NewStore(quota.New(cfg.Limiter))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Quota edits take effect without a restart. Only the quota table is
	// reloaded: changing listen addresses or Redis endpoints under a running
	// server is a redeploy, not a config edit.
	watcher, err := config.Watch(ctx, *configPath, log, func(next *config.Config) {
		quotas.Set(quota.New(next.Limiter))
		log.Info("quota table reloaded",
			"clients", len(next.Limiter.Clients),
			"resources", len(next.Limiter.Resources),
			"overrides", len(next.Limiter.Overrides))
	})
	if err != nil {
		return fmt.Errorf("watch config: %w", err)
	}
	defer watcher.Close()

	srv := &http.Server{
		Addr: cfg.Server.Addr,
		Handler: httpapi.NewServer(httpapi.Deps{
			Quotas:       quotas,
			Limiter:      guarded,
			Metrics:      metrics,
			Logger:       log,
			KeyPrefix:    cfg.Redis.KeyPrefix,
			Health:       func(ctx context.Context) error { return rdb.Ping(ctx).Err() },
			BreakerState: func() string { return string(cb.State()) },
		}).Handler(),
		ReadTimeout:  cfg.Server.ReadTimeout.Std(),
		WriteTimeout: cfg.Server.WriteTimeout.Std(),
		IdleTimeout:  cfg.Server.IdleTimeout.Std(),
	}

	errc := make(chan error, 1)
	go func() {
		log.Info("ratelimiterd listening",
			"addr", cfg.Server.Addr,
			"redis", cfg.Redis.Addr,
			"fail_open", cfg.Limiter.FailOpen,
			"default_limit", cfg.Limiter.Default.Limit,
			"default_window", cfg.Limiter.Default.Window.String())
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errc <- err
		}
	}()

	// Startup does not block on Redis. The limiter is useful (failing open)
	// before Redis is reachable, and refusing to start would make the limiter
	// a hard dependency of every deploy.
	go func() {
		pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		if err := rdb.Ping(pingCtx).Err(); err != nil {
			log.Warn("redis unreachable at startup, serving degraded until it returns",
				"redis", cfg.Redis.Addr, "error", err)
			return
		}
		log.Info("redis reachable", "addr", cfg.Redis.Addr)
	}()

	select {
	case err := <-errc:
		return fmt.Errorf("http server: %w", err)
	case <-ctx.Done():
		stop() // restore default signal handling: a second Ctrl-C kills us
	}

	log.Info("shutting down", "grace", cfg.Server.ShutdownGrace.String())
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.Server.ShutdownGrace.Std())
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("graceful shutdown: %w", err)
	}
	log.Info("stopped")
	return nil
}

// applyEnvOverrides lets a container override the two settings that differ
// between environments without templating the config file.
func applyEnvOverrides(cfg *config.Config) {
	if addr := os.Getenv("RATELIMITER_ADDR"); addr != "" {
		cfg.Server.Addr = addr
	}
	if addr := os.Getenv("REDIS_ADDR"); addr != "" {
		cfg.Redis.Addr = addr
	}
	if pw := os.Getenv("REDIS_PASSWORD"); pw != "" {
		cfg.Redis.Password = pw
	}
}
