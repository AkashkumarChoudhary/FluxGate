import Redis from 'ioredis';
import { TOKEN_BUCKET_LUA } from '@fluxgate/shared';
import { ratelimitRejectedTotal } from '../metrics/registry';

const CAPACITY = Number(process.env.DISPATCH_BUCKET_CAPACITY ?? 10);
const REFILL_PER_SEC = Number(process.env.DISPATCH_BUCKET_REFILL_PER_SEC ?? 5);
const POLL_MS = 200;

type RedisWithBucket = Redis & {
  tokenBucket(key: string, capacity: number, rate: number, nowMs: number): Promise<[number, number]>;
};

let redis: RedisWithBucket | undefined;
function getRedis(): RedisWithBucket {
  if (!redis) {
    redis = new Redis(process.env.REDIS_URL ?? 'redis://localhost:6379') as RedisWithBucket;
    redis.defineCommand('tokenBucket', { numberOfKeys: 1, lua: TOKEN_BUCKET_LUA });
  }
  return redis;
}

// Layer 2 (spec §9): wait in-process until a token is available. Unbounded by
// design — events are never dropped or reordered; kafkajs heartbeat() on each
// iteration keeps the consumer in the group. Fails open if Redis is down.
export async function waitForDispatchToken(
  tenantId: string,
  heartbeat: () => Promise<void>,
): Promise<void> {
  let waited = false;
  for (;;) {
    let allowed: number;
    try {
      [allowed] = await getRedis().tokenBucket(
        `ratelimit:dispatch:${tenantId}`,
        CAPACITY,
        REFILL_PER_SEC,
        Date.now(),
      );
    } catch (err) {
      console.warn('dispatch limiter unavailable, failing open', err);
      return;
    }
    if (allowed === 1) return;
    if (!waited) {
      waited = true;
      ratelimitRejectedTotal.inc({ tenantId, layer: 'dispatch' });
    }
    await heartbeat();
    await new Promise((r) => setTimeout(r, POLL_MS));
  }
}
