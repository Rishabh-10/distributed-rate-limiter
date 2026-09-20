-- Token bucket, evaluated atomically inside Redis.
--
-- KEYS[1]  bucket key
-- ARGV[1]  now, in microseconds
-- ARGV[2]  limit: tokens refilled per window
-- ARGV[3]  window, in microseconds
-- ARGV[4]  capacity: the burst a client may accumulate
-- ARGV[5]  cost of this request
-- ARGV[6]  key TTL, in milliseconds
--
-- Returns {allowed, tokens_remaining, retry_after_micros, reset_after_micros}.
--
-- This is a script rather than a MULTI/EXEC block because the refill is a
-- read-modify-write: how many tokens to add depends on the stored timestamp,
-- and a transaction cannot branch on a value it has not read yet. Queued
-- commands in MULTI see no intermediate results, so the arithmetic has to
-- happen server-side. A script gives the same atomicity the sliding window
-- gets from its transaction.

local now      = tonumber(ARGV[1])
local limit    = tonumber(ARGV[2])
local window   = tonumber(ARGV[3])
local capacity = tonumber(ARGV[4])
local cost     = tonumber(ARGV[5])
local ttl      = tonumber(ARGV[6])

local rate = limit / window  -- tokens per microsecond

local state  = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(state[1])
local ts     = tonumber(state[2])

if tokens == nil or ts == nil then
  -- An unseen client starts with a full bucket.
  tokens = capacity
  ts = now
end

-- Clamp elapsed at zero: clocks on separate limiter instances can disagree,
-- and a negative delta would hand out tokens that were never earned.
local elapsed = now - ts
if elapsed < 0 then elapsed = 0 end

tokens = math.min(capacity, tokens + elapsed * rate)

local allowed = 0
if tokens >= cost then
  tokens = tokens - cost
  allowed = 1
end

redis.call('HSET', KEYS[1], 'tokens', tokens, 'ts', now)
redis.call('PEXPIRE', KEYS[1], ttl)

local retry = 0
if allowed == 0 then
  retry = math.ceil((cost - tokens) / rate)
end

local reset = math.ceil((capacity - tokens) / rate)

-- Redis truncates floats returned from Lua, so every value is made an integer
-- deliberately rather than by accident.
return {allowed, math.floor(tokens), retry, reset}
