//go:build integration

// Package integration exercises the limiter against a real Redis.
//
// These tests are behind the `integration` build tag because they need a
// server: `make integration` starts one in Docker and runs them. They are
// where the claims that miniredis cannot verify get checked — real
// concurrency, real network failures, real key expiry.
package integration

import (
	"context"
	"io"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// redisAddr returns the Redis to test against.
func redisAddr() string {
	if addr := os.Getenv("REDIS_ADDR"); addr != "" {
		return addr
	}
	return "localhost:6379"
}

// dialRedis returns a client, failing loudly rather than skipping: these tests
// were asked for explicitly by the build tag, so a missing Redis is a setup
// error, not a reason to report success.
func dialRedis(t *testing.T, addr string) *redis.Client {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{
		Addr:         addr,
		DialTimeout:  2 * time.Second,
		ReadTimeout:  2 * time.Second,
		WriteTimeout: 2 * time.Second,
		PoolSize:     100,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		rdb.Close()
		t.Fatalf("cannot reach Redis at %s: %v\nStart one with: make redis-up", addr, err)
	}
	t.Cleanup(func() { rdb.Close() })
	return rdb
}

// uniqueKey keeps parallel tests and repeated runs from sharing state.
func uniqueKey(t *testing.T) string {
	t.Helper()
	return "test:" + t.Name() + ":" + time.Now().Format("150405.000000")
}

// proxy is a TCP relay in front of Redis. Closing it severs every connection,
// which is how these tests produce a genuine outage — real dial errors, real
// resets — without needing control of the Redis container.
type proxy struct {
	listener net.Listener
	target   string

	mu     sync.Mutex
	conns  []net.Conn
	closed bool
}

func newProxy(t *testing.T, target string) *proxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	p := &proxy{listener: ln, target: target}
	go p.serve()
	t.Cleanup(p.Close)
	return p
}

func (p *proxy) Addr() string { return p.listener.Addr().String() }

// reopenProxy brings a severed proxy back on the same address, standing in for
// a Redis that has restarted where its clients expect to find it.
func reopenProxy(t *testing.T, addr, target string) *proxy {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		ln, err := net.Listen("tcp", addr)
		if err == nil {
			p := &proxy{listener: ln, target: target}
			go p.serve()
			t.Cleanup(p.Close)
			return p
		}
		if time.Now().After(deadline) {
			t.Fatalf("cannot rebind %s: %v", addr, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (p *proxy) serve() {
	for {
		client, err := p.listener.Accept()
		if err != nil {
			return
		}
		upstream, err := net.DialTimeout("tcp", p.target, 2*time.Second)
		if err != nil {
			client.Close()
			continue
		}
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			client.Close()
			upstream.Close()
			return
		}
		p.conns = append(p.conns, client, upstream)
		p.mu.Unlock()

		go func() { io.Copy(upstream, client); upstream.Close() }()
		go func() { io.Copy(client, upstream); client.Close() }()
	}
}

// Close takes Redis away: the listener stops accepting and every live
// connection is severed mid-flight.
func (p *proxy) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return
	}
	p.closed = true
	p.listener.Close()
	for _, c := range p.conns {
		c.Close()
	}
	p.conns = nil
}
