import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { api, createTenant, waitFor } from './helpers';
import { startMockDestination, type MockDestination } from './mockDestination';
import { prisma } from './db';

let dest: MockDestination;
beforeAll(async () => {
  dest = await startMockDestination(9995);
});
afterAll(async () => {
  await dest.close();
});

async function fireWebhook(name: string) {
  const tenant = await createTenant(name);
  const trig = await api(
    '/triggers',
    { method: 'POST', body: JSON.stringify({ name: 'wh', type: 'WEBHOOK', config: {} }) },
    tenant.apiKey,
  );
  await api(
    `/triggers/${trig.body.id}/actions`,
    { method: 'POST', body: JSON.stringify({ type: 'HTTP', config: { url: 'http://localhost:9995/' } }) },
    tenant.apiKey,
  );
  const fire = await api(`/webhooks/${trig.body.id}`, { method: 'POST', body: '{}' }, tenant.apiKey);
  return fire.body.eventId as string;
}

describe('failure paths', () => {
  it('destination 4xx -> FAILED immediately, exactly one attempt, no retries', async () => {
    dest.failNext(422, 100);
    const eventId = await fireWebhook('fail-4xx');
    const execution = await waitFor(async () => {
      const e = await prisma.execution.findUnique({ where: { dedupeKey: eventId }, include: { steps: true } });
      return e?.status === 'FAILED' ? e : null;
    });
    expect(execution.steps).toHaveLength(1);
    expect(execution.failureReason).toContain('422');
    dest.failNext(200, 0);
  });

  it('destination 5xx twice then 200 -> retries with backoff, COMPLETED, full attempt history', async () => {
    dest.failNext(503, 2);
    const eventId = await fireWebhook('fail-5xx');
    const execution = await waitFor(async () => {
      const e = await prisma.execution.findUnique({ where: { dedupeKey: eventId }, include: { steps: true } });
      return e?.status === 'COMPLETED' ? e : null;
    }, 60_000);
    expect(execution.steps.map((s) => s.attemptNumber).sort()).toEqual([1, 2, 3]);
    expect(execution.steps.filter((s) => s.status === 'FAILED')).toHaveLength(2);
  });
});
