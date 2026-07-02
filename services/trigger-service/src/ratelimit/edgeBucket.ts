import type { Request, Response, NextFunction } from 'express';
import { TOKEN_BUCKET_LUA, type BucketParams } from '@fluxgate/shared';
import { getRedis } from '../cron/schedule';
import { ratelimitRejectedTotal } from '../metrics/registry';
import { tenantIdOf } from '../api/auth';

const DEFAULTS: BucketParams = {
  capacity: Number(process.env.EDGE_BUCKET_CAPACITY ?? 20),
  refillPerSec: Number(process.env.EDGE_BUCKET_REFILL_PER_SEC ?? 10),
};

let defined = false;
function redisWithBucket() {
  const redis = getRedis() as ReturnType<typeof getRedis> & {
    tokenBucket(key: string, capacity: number, rate: number, nowMs: number): Promise<[number, number]>;
  };
  if (!defined) {
    redis.defineCommand('tokenBucket', { numberOfKeys: 1, lua: TOKEN_BUCKET_LUA });
    defined = true;
  }
  return redis;
}

// Layer 1: empty bucket -> 429 + Retry-After. Fails open if Redis is down (spec §12).
export async function edgeRateLimit(req: Request, res: Response, next: NextFunction) {
  const tenantId = tenantIdOf(req);
  try {
    const [allowed, retryAfter] = await redisWithBucket().tokenBucket(
      `ratelimit:edge:${tenantId}`,
      DEFAULTS.capacity,
      DEFAULTS.refillPerSec,
      Date.now(),
    );
    if (allowed === 1) return next();
    ratelimitRejectedTotal.inc({ tenantId, layer: 'edge' });
    res.set('Retry-After', String(Math.max(1, retryAfter)));
    return res.status(429).json({ error: 'rate limit exceeded' });
  } catch (err) {
    console.warn('rate limiter unavailable, failing open', err);
    return next();
  }
}
