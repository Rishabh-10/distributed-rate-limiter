package redisx

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"

	"github.com/redis/go-redis/v9"
)

// The classification below is what decides whether a failure fails open, so
// each case is spelled out rather than trusted to a catch-all.
func TestIsUnavailable(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},

		// Redis answered; we just did not like the answer.
		{"key missing", redis.Nil, false},
		{"wrong type", errors.New("WRONGTYPE Operation against a key holding the wrong kind of value"), false},
		{"bad command", errors.New("ERR unknown command 'ZFOO'"), false},
		{"out of memory", errors.New("OOM command not allowed when used memory > 'maxmemory'"), false},

		// Redis did not answer.
		{"deadline exceeded", context.DeadlineExceeded, true},
		{"cancelled", context.Canceled, true},
		{"eof", io.EOF, true},
		{"closed client", redis.ErrClosed, true},
		{"net error", &net.OpError{Op: "dial", Err: errors.New("connect: connection refused")}, true},
		{"loading dataset", errors.New("LOADING Redis is loading the dataset in memory"), true},
		{"replica cut off", errors.New("MASTERDOWN Link with MASTER is down"), true},
		{"failed over to replica", errors.New("READONLY You can't write against a read only replica"), true},
		{"cluster down", errors.New("CLUSTERDOWN Hash slot not served"), true},
		{"busy script", errors.New("BUSY Redis is busy running a script"), true},
		{"connection refused", errors.New("dial tcp 127.0.0.1:6379: connect: connection refused"), true},
		{"i/o timeout", errors.New("read tcp 127.0.0.1:6379: i/o timeout"), true},

		// Classification must survive the wrapping the limiter does on the way up.
		{"wrapped timeout", fmt.Errorf("sliding window transaction: %w", context.DeadlineExceeded), true},
		{"wrapped wrongtype", fmt.Errorf("sliding window transaction: %w", errors.New("WRONGTYPE")), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsUnavailable(tt.err); got != tt.want {
				t.Errorf("IsUnavailable(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

// A client pointed at nothing must report unavailability, not hang.
func TestNewClientFailsFastWhenRedisIsAbsent(t *testing.T) {
	// Port 1 is reserved and nothing listens there.
	rdb := New(testRedisConfig("127.0.0.1:1"))
	defer rdb.Close()

	err := rdb.Ping(context.Background()).Err()
	if err == nil {
		t.Fatal("ping succeeded against a closed port")
	}
	if !IsUnavailable(err) {
		t.Errorf("IsUnavailable(%v) = false, want true", err)
	}
}
