import { describe, it, expect } from 'vitest';
import { api, createTenant } from './helpers';

describe('tenant/trigger/action CRUD', () => {
  it('full lifecycle with tenant isolation', async () => {
    const tenantA = await createTenant('crud-a');
    const tenantB = await createTenant('crud-b');

    expect((await api('/triggers', { method: 'POST', body: '{}' })).status).toBe(401);

    const trig = await api(
      '/triggers',
      { method: 'POST', body: JSON.stringify({ name: 'wh', type: 'WEBHOOK', config: {} }) },
      tenantA.apiKey,
    );
    expect(trig.status).toBe(201);

    const badCron = await api(
      '/triggers',
      { method: 'POST', body: JSON.stringify({ name: 'bad', type: 'CRON', config: { expression: 'nope' } }) },
      tenantA.apiKey,
    );
    expect(badCron.status).toBe(400);

    const action = await api(
      `/triggers/${trig.body.id}/actions`,
      { method: 'POST', body: JSON.stringify({ type: 'HTTP', config: { url: 'https://example.com/x' } }) },
      tenantA.apiKey,
    );
    expect(action.status).toBe(201);
    expect(action.body.config.method).toBe('POST');

    expect((await api(`/triggers/${trig.body.id}`, {}, tenantB.apiKey)).status).toBe(404);

    const paused = await api(
      `/triggers/${trig.body.id}`,
      { method: 'PATCH', body: JSON.stringify({ status: 'PAUSED' }) },
      tenantA.apiKey,
    );
    expect(paused.body.status).toBe('PAUSED');
  });
});
