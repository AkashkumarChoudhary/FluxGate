// Atomic token bucket: read-refill-decrement in one Redis call (spec §9 —
// eliminates TOCTOU). Returns {allowed (0|1), retryAfterSeconds}.
// KEYS[1] bucket hash key
// ARGV[1] capacity, ARGV[2] refill tokens/sec, ARGV[3] now-ms
export const TOKEN_BUCKET_LUA = `
local capacity = tonumber(ARGV[1])
local rate = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local state = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(state[1])
local ts = tonumber(state[2])
if tokens == nil then
  tokens = capacity
  ts = now
end
local elapsed = math.max(0, now - ts) / 1000.0
tokens = math.min(capacity, tokens + elapsed * rate)
local allowed = 0
if tokens >= 1 then
  tokens = tokens - 1
  allowed = 1
end
redis.call('HSET', KEYS[1], 'tokens', tokens, 'ts', now)
redis.call('PEXPIRE', KEYS[1], math.ceil(capacity / rate * 2000))
local retryAfter = 0
if allowed == 0 then
  retryAfter = math.ceil((1 - tokens) / rate)
end
return {allowed, retryAfter}
`;

export interface BucketParams {
  capacity: number;
  refillPerSec: number;
}
