// Atomic compare-and-set requeue (spec §8): only requeue if the member's score
// is still <= now. Score update (not ZREM+ZADD) means the member never leaves
// the set — no crash window where the schedule is lost. Returns 1 if THIS
// caller performed the requeue, 0 if another poller already did.
export const CLAIM_REQUEUE_LUA = `
local score = redis.call('ZSCORE', KEYS[1], ARGV[1])
if score and tonumber(score) <= tonumber(ARGV[2]) then
  redis.call('ZADD', KEYS[1], ARGV[3], ARGV[1])
  return 1
end
return 0
`;
