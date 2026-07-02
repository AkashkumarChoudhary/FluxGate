import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { api, createTenant, waitFor } from './helpers';
import { startMockDestination, type MockDestination } from './mockDestination';
import { prisma } from './db';

let dest: MockDestination;
beforeAll(async () => {
  dest = await startMockDestination(9996);
});
afterAll(async () => {
  await dest.close();
});

async function setupTenantWithWebhook(name: string) {
  const tenant = await createTenant(name);
  const trig = await api(
    '/triggers',
    { method: 'POST', body: JSON.stringify({ name: 'wh', type: 'WEBHOOK', config: {} }) },
    tenant.apiKey,
  );
  await api(
    `/triggers/${trig.body.id}/actions`,
    { method: 'POST', body: JSON.stringify({ type: 'HTTP', config: { url: 'http://localhost:9996/' } }) },
    tenant.apiKey,
  );
  return { tenant, triggerId: trig.body.id as string };
}

describe('two-layer rate limiting', () => {
  it('edge: flooding tenant gets 429 + Retry-After; other tenant unaffected', async () => {
    const hot = await setupTenantWithWebhook('rl-hot');
    const cold = await setupTenantWithWebhook('rl-cold');

    const results = await Promise.all(
      Array.from({ length: 60 }, () =>
        api(`/webhooks/${hot.triggerId}`, { method: 'POST', body: '{}' }, hot.tenant.apiKey),
      ),
    );
    const accepted = results.filter((r) => r.status === 202);
    const rejected = results.filter((r) => r.status === 429);
    expect(rejected.length).toBeGreaterThan(0);
    expect(accepted.length).toBeGreaterThan(0);
    expect(rejected[0].headers.get('retry-after')).toMatch(/^\d+$/);

    const coldRes = await api(`/webhooks/${cold.triggerId}`, { method: 'POST', body: '{}' }, cold.tenant.apiKey);
    expect(coldRes.status).toBe(202);

    // Dispatch layer: accepted events all complete eventually (delayed, never dropped).
    await waitFor(async () => {
      const done = await prisma.execution.count({
        where: { tenantId: hot.tenant.id, status: 'COMPLETED' },
      });
      return done === accepted.length ? done : null;
    }, 120_000);

    const coldDone = await waitFor(async () => {
      const e = await prisma.execution.findFirst({ where: { tenantId: cold.tenant.id, status: 'COMPLETED' } });
      return e ?? null;
    });
    expect(coldDone).toBeDefined();
  });
});
