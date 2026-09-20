package redisx

import (
	"time"

	"github.com/Rishabh-10/distributed-rate-limiter/internal/config"
)

// testRedisConfig returns a Redis config pointed at addr with timeouts short
// enough to keep tests quick.
func testRedisConfig(addr string) config.Redis {
	return config.Redis{
		Addr:           addr,
		CommandTimeout: config.Duration(100 * time.Millisecond),
		DialTimeout:    config.Duration(100 * time.Millisecond),
		PoolSize:       4,
		KeyPrefix:      "rl",
	}
}
