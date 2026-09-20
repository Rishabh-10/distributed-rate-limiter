// Package redisx wraps the Redis client with the two things the limiter needs
// from its datastore: hard latency bounds, and an honest answer to "is Redis
// simply unavailable?".
//
// That second question drives fail-open. Conflating "Redis is down" with "this
// client is over quota" would either lock every client out during an outage or
// let every client through during normal operation, so the classification
// below is deliberately explicit about which errors mean what.
package redisx

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"

	"github.com/redis/go-redis/v9"

	"github.com/Rishabh-10/distributed-rate-limiter/internal/config"
)

// New builds a Redis client from config. The timeouts are small by design: a
// rate limiter that blocks on a struggling Redis has turned itself into the
// outage it was meant to prevent.
func New(cfg config.Redis) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:         cfg.Addr,
		Password:     cfg.Password,
		DB:           cfg.DB,
		DialTimeout:  cfg.DialTimeout.Std(),
		ReadTimeout:  cfg.CommandTimeout.Std(),
		WriteTimeout: cfg.CommandTimeout.Std(),
		PoolSize:     cfg.PoolSize,
		// Wait no longer for a free connection than for the command itself.
		PoolTimeout: cfg.CommandTimeout.Std(),
		// One retry absorbs a single dropped connection; more than that and we
		// are better off failing fast and letting the breaker take over.
		MaxRetries: 1,
	})
}

// IsUnavailable reports whether err means Redis could not answer — as opposed
// to answering something we did not like.
//
// Only errors matching this are eligible for fail-open. Anything else (a
// malformed command, a WRONGTYPE, an OOM rejection) is a bug or a genuine
// server-side refusal, and is surfaced rather than silently allowed.
func IsUnavailable(err error) bool {
	if err == nil {
		return false
	}
	// redis.Nil means "no such key", which is a perfectly good answer.
	if errors.Is(err, redis.Nil) {
		return false
	}
	// Our own deadline fired, or the caller went away: Redis did not answer in
	// time, which for our purposes is the same as being down.
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return true
	}
	if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		return true
	}
	if errors.Is(err, redis.ErrClosed) || errors.Is(err, redis.ErrPoolExhausted) {
		return true
	}
	// Server-side conditions that mean "cannot serve you right now". These
	// arrive as plain string replies, so matching on the error prefix is the
	// only option go-redis gives us.
	msg := err.Error()
	for _, prefix := range []string{
		"LOADING",     // restarting and still reading the dataset from disk
		"MASTERDOWN",  // replica cut off from its primary
		"CLUSTERDOWN", // cluster has no coverage for this slot
		"TRYAGAIN",    // slot migration in progress
		"READONLY",    // failed over to a replica mid-request
		"BUSY",        // a long script is blocking the server
		"NOREPLICAS",  // write rejected for lack of replicas
		"connection refused",
		"connection reset",
		"no such host",
		"i/o timeout",
	} {
		if strings.Contains(msg, prefix) {
			return true
		}
	}
	return false
}
