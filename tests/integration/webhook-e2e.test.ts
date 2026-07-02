import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { api, createTenant, waitFor } from './helpers';
import { startMockDestination, type MockDestination } from './mockDestination';
import { prisma } from './db';

let dest: MockDestination;
beforeAll(async () => {
  dest = await startMockDestination(9999);
});
afterAll(async () => {
  await dest.close();
});

describe('webhook e2e', () => {
  it('POST webhook -> execution COMPLETED, step recorded, destination called', async () => {
    const tenant = await createTenant('e2e-webhook');
    const trig = await api(
      '/triggers',
      { method: 'POST', body: JSON.stringify({ name: 'wh', type: 'WEBHOOK', config: {} }) },
      tenant.apiKey,
    );
    await api(
      `/triggers/${trig.body.id}/actions`,
      {
        method: 'POST',
        body: JSON.stringify({
          type: 'HTTP',
          config: { url: 'http://localhost:9999/hook', bodyTemplate: { greeting: 'hi {{payload.name}}' } },
        }),
      },
      tenant.apiKey,
    );

    const fire = await api(`/webhooks/${trig.body.id}`, { method: 'POST', body: JSON.stringify({ name: 'akash' }) }, tenant.apiKey);
    expect(fire.status).toBe(202);

    const execution = await waitFor(async () => {
      const e = await prisma.execution.findUnique({
        where: { dedupeKey: fire.body.eventId },
        include: { steps: true },
      });
      return e?.status === 'COMPLETED' ? e : null;
    });

    expect(execution.steps).toHaveLength(1);
    expect(execution.steps[0].attemptNumber).toBe(1);
    expect(execution.temporalWorkflowId).toBe(`exec-${fire.body.eventId}`);
    const call = dest.received.find((c) => c.url === '/hook' && c.body.includes('hi akash'));
    expect(call).toBeDefined();

    // paused trigger -> 409, nothing enqueued
    await api(`/triggers/${trig.body.id}`, { method: 'PATCH', body: JSON.stringify({ status: 'PAUSED' }) }, tenant.apiKey);
    expect((await api(`/webhooks/${trig.body.id}`, { method: 'POST', body: '{}' }, tenant.apiKey)).status).toBe(409);

    // unknown trigger -> 404
    expect((await api('/webhooks/00000000-0000-0000-0000-000000000000', { method: 'POST', body: '{}' }, tenant.apiKey)).status).toBe(404);
  });
});
