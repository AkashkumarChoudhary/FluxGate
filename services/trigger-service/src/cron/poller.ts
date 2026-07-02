import { getPrisma } from '@fluxgate/db';
import { cronEventId, type TriggerEvent } from '@fluxgate/shared';
import { produceTriggerEvent } from '../kafka/producer';
import { getRedis, nextFireMs, CRON_ZSET, removeCronSchedule } from './schedule';
import { CLAIM_REQUEUE_LUA } from './claim.lua';

const POLL_INTERVAL_MS = Number(process.env.CRON_POLL_INTERVAL_MS ?? 500);

export function startCronPoller(): NodeJS.Timeout {
  const redis = getRedis();
  redis.defineCommand('claimRequeue', { numberOfKeys: 1, lua: CLAIM_REQUEUE_LUA });

  const tick = async () => {
    const now = Date.now();
    const due = await redis.zrangebyscore(CRON_ZSET, 0, now, 'WITHSCORES');
    for (let i = 0; i < due.length; i += 2) {
      const triggerId = due[i];
      const scheduledFireMs = Number(due[i + 1]);
      try {
        await fireCronTrigger(triggerId, scheduledFireMs, now);
      } catch (err) {
        // Produce failure: do NOT requeue — trigger stays due, next tick retries (spec §12).
        console.error(`cron fire failed for ${triggerId}, will retry next poll`, err);
      }
    }
  };
  return setInterval(() => void tick(), POLL_INTERVAL_MS);
}

async function fireCronTrigger(triggerId: string, scheduledFireMs: number, now: number): Promise<void> {
  const redis = getRedis() as ReturnType<typeof getRedis> & {
    claimRequeue(key: string, member: string, now: number, nextMs: number): Promise<number>;
  };
  const trigger = await getPrisma().trigger.findUnique({ where: { id: triggerId } });
  if (!trigger || trigger.status !== 'ACTIVE' || trigger.type !== 'CRON') {
    await removeCronSchedule(triggerId); // stale member; DB is source of truth
    return;
  }
  const expression = (trigger.config as { expression: string }).expression;

  // 1. Produce FIRST. eventId embeds scheduledFireMs, so a crash between produce
  //    and requeue re-produces the same eventId and Temporal dedup swallows it (spec §8).
  const event: TriggerEvent = {
    eventId: cronEventId(triggerId, scheduledFireMs),
    tenantId: trigger.tenantId,
    triggerId,
    type: 'CRON',
    payload: {},
    version: 1,
    firedAt: new Date(now).toISOString(),
    scheduledFor: new Date(scheduledFireMs).toISOString(),
  };
  await produceTriggerEvent(event);

  // 2. Atomic claim/requeue: only one concurrent poller advances the schedule.
  await redis.claimRequeue(CRON_ZSET, triggerId, now, nextFireMs(expression, new Date(now)));
}
