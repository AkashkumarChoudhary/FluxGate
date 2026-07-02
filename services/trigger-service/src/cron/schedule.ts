import Redis from 'ioredis';
import cronParser from 'cron-parser';

export const CRON_ZSET = 'cron:triggers';

let redis: Redis | undefined;
export function getRedis(): Redis {
  if (!redis) redis = new Redis(process.env.REDIS_URL ?? 'redis://localhost:6379');
  return redis;
}

export function nextFireMs(expression: string, from: Date = new Date()): number {
  return cronParser.parseExpression(expression, { currentDate: from }).next().getTime();
}

export async function syncCronSchedule(triggerId: string, expression: string): Promise<void> {
  await getRedis().zadd(CRON_ZSET, nextFireMs(expression), triggerId);
}

export async function removeCronSchedule(triggerId: string): Promise<void> {
  await getRedis().zrem(CRON_ZSET, triggerId);
}
