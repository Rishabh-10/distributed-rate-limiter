# Distributed Rate Limiter

A rate-limiting service in Go, backed by Redis, that stays correct when many
instances serve the same client at once — and stays available when Redis does
not.

```
                  ┌──────────────┐
  client ───────▶ │   your app   │  wraps its handler in ratelimit.Middleware
                  └──────┬───────┘
                         │ POST /v1/check   (50ms timeout, fails open)
                  ┌──────▼───────┐
                  │ ratelimiterd │  quota resolver → algorithm → Redis
                  └──────┬───────┘
                         │ MULTI ▸ ZREMRANGEBYSCORE ▸ ZADD ▸ ZCARD ▸ ZRANGE ▸ PEXPIRE ▸ EXEC
                  ┌──────▼───────┐
                  │    Redis     │  rl:sw:{client}:/resource
                  └──────────────┘
```

## Quick start

```bash
docker compose -f deploy/docker-compose.yml up --build
```

That runs Redis, the limiter, and a demo app sitting behind it. `free-tier` is
capped at 10 requests a minute in [configs/config.yaml](configs/config.yaml):

```bash
for i in $(seq 1 15); do
  curl -s -o /dev/null -w "%{http_code} " localhost:8081/api/hello \
    -H "X-Client-ID: free-tier"
done
# 200 200 200 200 200 200 200 200 200 200 429 429 429 429 429
```

The 429s carry `Retry-After` and the usual `X-RateLimit-*` headers.

## The problem this solves

The obvious implementation reads a counter, decides, then writes it back:

```
count = GET key          # 99
if count < limit:        # yes
    INCR key             # 100
```

Two limiter instances can both read 99 and both admit a request, so a limit of
100 lets 101 through. Under real concurrency the overshoot grows with the
number of instances and the request rate. Nothing on the client side can close
that gap, because the gap is *between* the read and the write.

So the whole decision happens inside one `MULTI`/`EXEC` block, pipelined into a
single round trip:

```
MULTI
  ZREMRANGEBYSCORE key -inf (now-window)   -- drop entries older than the window
  ZADD             key now  <member>       -- optimistically record this request
  ZCARD            key                     -- how many are in the window now
  ZRANGE           key 0 0 WITHSCORES      -- oldest survivor, for Retry-After
  PEXPIRE          key window              -- idle clients clean themselves up
EXEC
```

Redis executes that atomically, so concurrent callers are serialised: one sees
`ZCARD` 100, the next sees 101. The decision comes from a number nobody else
can have observed, which is what makes the count exact across instances.

Two details that are easy to get wrong:

- **Member uniqueness.** Each entry is `<micros>-<random>`. Scoring by bare
  timestamp collapses same-instant requests into one sorted-set member and
  silently undercounts them.
- **Score precision.** Scores are microseconds, not nanoseconds. Redis stores
  sorted-set scores as IEEE doubles, whose 53-bit mantissa cannot hold a
  nanosecond Unix timestamp exactly.

Deciding *after* the write means a rejected request has already been recorded.
A compensating `ZREM` hands the slot back, so a client hammering a closed door
does not extend its own lockout. Set `count_rejected: true` to keep them.

[`TestConcurrentRequestsNeverExceedTheLimit`](test/integration/concurrency_test.go)
is the proof: two limiter instances, one Redis, 500 simultaneous requests
against a limit of 100, and exactly 100 are admitted.

## Failing open

A rate limiter must not become the outage it exists to prevent. When Redis
cannot be reached, requests are **allowed** and the decision is flagged
`degraded` — enforcement is given up deliberately and visibly.

Three layers, each tested:

1. **Timeouts.** Every Redis command is bounded by `redis.command_timeout`
   (50ms), and each whole decision by `limiter.decision_timeout` (150ms).
2. **Error classification.** Only errors that mean "Redis could not answer"
   qualify — dial failures, timeouts, `LOADING`, `CLUSTERDOWN`, `READONLY`. A
   `WRONGTYPE` or a missing algorithm is a bug, and is surfaced as a 503 rather
   than laundered into an allow. See
   [internal/redisx/client.go](internal/redisx/client.go).
3. **A circuit breaker.** Without one, every request during an outage waits out
   the full timeout first — a dead Redis would silently add 50ms to every call.
   After five consecutive failures the breaker opens and Redis is skipped
   entirely, then half-opens to probe for recovery.

Readiness and the data path answer different questions on purpose: `/readyz`
reports 503 while Redis is down, and `/v1/check` keeps serving. Conflating them
would make a Redis outage cascade into every service behind the limiter.

Set `fail_open: false` to invert the trade — enforcement preserved,
availability sacrificed. Right for a paid metering boundary, wrong for
protecting a backend from overload.

## Algorithms

Chosen per rule, in config.

| | `sliding_window` (default) | `token_bucket` |
|---|---|---|
| Semantics | Exactly N requests in the trailing window; no boundary bursts | Steady refill rate plus an explicit burst allowance |
| State per client | One sorted-set entry per request | Two numbers: tokens and a timestamp |
| Atomicity | `MULTI`/`EXEC` — no read-then-decide step | A Lua script |

The token bucket needs a script rather than a transaction because refilling is
a read-modify-write: how many tokens to add depends on the stored timestamp,
and queued commands inside `MULTI` cannot branch on values they have not read
yet. The script is atomic for the same reason the transaction is. See
[internal/limiter/tokenbucket.lua](internal/limiter/tokenbucket.lua).

Use the sliding window when a hard ceiling matters. Use the token bucket for
bursty or very high-volume clients, where per-request state costs too much.

## Using it from Go

```go
rl := ratelimit.New("http://limiter:8080",
    ratelimit.WithTimeout(50*time.Millisecond),
    ratelimit.WithSkipFunc(func(r *http.Request) bool { return r.URL.Path == "/healthz" }),
    ratelimit.WithErrorHandler(func(r *http.Request, err error) {
        log.Warn("limiter unreachable, allowing", "error", err)
    }),
)

mux.Handle("/api/", rl.Middleware(apiHandler))
```

The middleware identifies the caller from `X-Client-ID`, falling back to the
peer address. Forwarded headers (`X-Forwarded-For` and friends) are **not**
trusted by default — anyone can set them, and a limiter keyed on a spoofable
identifier limits nobody. Behind a proxy you control, pass `WithClientIDFunc`.

`Check` is available directly for callers that are not HTTP handlers.

## API

| Endpoint | Behaviour |
|---|---|
| `POST /v1/check` | The decision. Always answers **200** with a body — turning a denial into a 429 is the caller's job, since the caller owns the request being limited. Sets `X-RateLimit-*` and `Retry-After`. |
| `GET /v1/quotas/{client}?resource=` | The effective rule and which config entry produced it. Answers "why is this client limited to N?" without replaying precedence by hand. |
| `GET /healthz` | Liveness. Never checks Redis. |
| `GET /readyz` | Readiness. 503 when Redis is unreachable. |
| `GET /metrics` | Prometheus. No metric is labelled by client ID: client IDs are unbounded and attacker-controlled, and a label per client is how a monitoring system gets taken down by its own instrumentation. |

```bash
curl -s -X POST localhost:8080/v1/check \
  -H 'Content-Type: application/json' \
  -d '{"client_id":"acme-corp","resource":"/v1/search","cost":1}'
```

```json
{
  "allowed": true,
  "limit": 50,
  "remaining": 49,
  "reset_after_ms": 1000,
  "retry_after_ms": 0,
  "algorithm": "sliding_window",
  "window_ms": 1000,
  "degraded": false,
  "quota_source": "override"
}
```

## Configuration

Quotas live in one YAML file and are **hot-reloaded**: edit them while the
service runs and the change takes effect within a fraction of a second. A file
that fails to parse or validate is logged and ignored, leaving the previous
quotas in force — a typo in the quota table must not take the limiter down.

```yaml
limiter:
  default:
    algorithm: sliding_window
    limit: 100
    window: 1m
  clients:
    acme-corp: { limit: 1000, window: 1m }
  resources:
    "/v1/search": { limit: 5, window: 1s }
  overrides:
    - { client: acme-corp, resource: "/v1/search", limit: 50, window: 1s }
```

Precedence, most specific first: **override → client → resource → default**.
A client rule beats a resource rule deliberately — an enterprise customer's
negotiated quota should not be silently capped by a generic per-route limit.
Use an override to combine the two. Unset fields are inherited, so a per-client
entry can say only `limit: 1000`.

Only the quota table reloads. Listen addresses and Redis endpoints are a
redeploy, not a config edit. `REDIS_ADDR`, `REDIS_PASSWORD` and
`RATELIMITER_ADDR` override the file for containers.

> On Docker Desktop, file-change events do not always propagate across bind
> mounts. Hot reload is reliable on Linux and when running the binary directly;
> in Docker Desktop you may need `docker compose restart limiter`.

## Development

```bash
make test          # unit tests; Redis is faked with miniredis, no Docker needed
make race          # the same under the race detector (needs a C toolchain)
make integration   # starts Redis in Docker, runs the real-Redis suite
make up            # the whole stack: Redis, limiter, demo app
make help          # everything else
```

Integration tests use their own Redis on port 6380, so a Redis you already run
locally is left untouched. Override with `make integration REDIS_PORT=6379`.

That suite covers what miniredis cannot: exactness under real concurrency, key
expiry in real time, the Lua script on a real interpreter, and genuine network
failure — a TCP proxy in front of Redis is severed mid-test to produce real
dial errors and resets rather than a stubbed error value.

## Known trade-offs

- **Clock skew.** Entries are scored with the limiter process's clock, because
  a value read from Redis `TIME` cannot be fed back into the same `MULTI`.
  Instances therefore need NTP; skew shifts window edges by the amount of the
  skew.
- **Memory.** The sliding window stores one entry per request, so a client
  doing 10k requests a minute keeps 10k entries alive. `PEXPIRE` bounds this to
  active clients; the token bucket is the O(1)-per-client answer.
- **One Redis is the failure domain.** Fail-open is the availability answer, and
  it explicitly gives up enforcement during an outage.
- **The check API is one network hop.** The limiter answers in single-digit
  milliseconds, but it is still a hop. A caller that cannot afford one can use
  the `limiter` package directly against Redis instead of the REST service.

## Layout

```
cmd/ratelimiterd     the service
cmd/demoapp          an app behind the middleware, for the compose demo
internal/limiter     algorithms, the fail-open guard, key construction
internal/redisx      Redis client and error classification
internal/breaker     circuit breaker
internal/config      config loading, validation, hot reload
internal/quota       (client, resource) → rule, with precedence
internal/httpapi     REST handlers
pkg/ratelimit        importable client and http.Handler middleware
test/integration     real-Redis tests (build tag: integration)
```
