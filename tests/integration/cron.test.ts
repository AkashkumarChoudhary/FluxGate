import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import Redis from 'ioredis';
import { api, createTenant, waitFor, sleep } from './helpers';
import { startMockDestination, type MockDestination } from './mockDestination';
import { prisma } from './db';

let dest: MockDestination;
const redis = new Redis(process.env.REDIS_URL ?? 'redis://localhost:6379');
beforeAll(async () => {
  dest = await startMockDestination(9997);
});
afterAll(async () => {
  await dest.close();
  redis.disconnect();
});

describe('cron scheduler', () => {
  it('due trigger fires, requeues to next occurrence, execution has scheduledFor', async () => {
    const tenant = await createTenant('cron-t');
    const trig = await api(
      '/triggers',
      { method: 'POST', body: JSON.stringify({ name: 'every-min', type: 'CRON', config: { expression: '* * * * *' } }) },
      tenant.apiKey,
    );
    expect(trig.status).toBe(201);
    await api(
      `/triggers/${trig.body.id}/actions`,
      { method: 'POST', body: JSON.stringify({ type: 'HTTP', config: { url: 'http://localhost:9997/' } }) },
      tenant.apiKey,
    );

    const scoreBefore = await redis.zscore('cron:triggers', trig.body.id);
    expect(scoreBefore).not.toBeNull();

    // Force it due NOW instead of waiting up to a minute
    await redis.zadd('cron:triggers', Date.now() - 1000, trig.body.id);

    const execution = await waitFor(async () => {
      const e = await prisma.execution.findFirst({
        where: { triggerId: trig.body.id, status: 'COMPLETED' },
      });
      return e ?? null;
    });
    expect(execution.scheduledFor).not.toBeNull();
    expect(execution.dedupeKey).toMatch(new RegExp(`^${trig.body.id}:\\d+$`));

    // Requeued to the future
    const scoreAfter = Number(await redis.zscore('cron:triggers', trig.body.id));
    expect(scoreAfter).toBeGreaterThan(Date.now());

    // Exactly one execution for that scheduled fire
    await sleep(2000);
    const count = await prisma.execution.count({ where: { triggerId: trig.body.id } });
    expect(count).toBe(1);

    // pause -> removed from set
    await api(`/triggers/${trig.body.id}`, { method: 'PATCH', body: JSON.stringify({ status: 'PAUSED' }) }, tenant.apiKey);
    expect(await redis.zscore('cron:triggers', trig.body.id)).toBeNull();
  });
});
