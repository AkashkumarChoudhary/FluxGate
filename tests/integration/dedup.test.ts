import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { Kafka } from 'kafkajs';
import { api, createTenant, waitFor, sleep } from './helpers';
import { startMockDestination, type MockDestination } from './mockDestination';
import { prisma } from './db';

let dest: MockDestination;
beforeAll(async () => {
  dest = await startMockDestination(9998);
});
afterAll(async () => {
  await dest.close();
});

describe('idempotency', () => {
  it('same eventId produced twice -> exactly one execution', async () => {
    const tenant = await createTenant('dedup');
    const trig = await api(
      '/triggers',
      { method: 'POST', body: JSON.stringify({ name: 'wh', type: 'WEBHOOK', config: {} }) },
      tenant.apiKey,
    );
    await api(
      `/triggers/${trig.body.id}/actions`,
      { method: 'POST', body: JSON.stringify({ type: 'HTTP', config: { url: 'http://localhost:9998/' } }) },
      tenant.apiKey,
    );

    // Bypass the API: produce the SAME event twice directly to Kafka,
    // simulating consumer redelivery (crash after dispatch, before offset commit).
    const eventId = `dup-test-${Math.random().toString(36).slice(2)}`;
    const event = {
      eventId,
      tenantId: tenant.id,
      triggerId: trig.body.id,
      type: 'WEBHOOK',
      payload: {},
      version: 1,
      firedAt: new Date().toISOString(),
    };
    const producer = new Kafka({ clientId: 'test', brokers: ['localhost:9092'] }).producer();
    await producer.connect();
    await producer.send({ topic: 'trigger-events', messages: [{ key: tenant.id, value: JSON.stringify(event) }] });
    await producer.send({ topic: 'trigger-events', messages: [{ key: tenant.id, value: JSON.stringify(event) }] });
    await producer.disconnect();

    await waitFor(async () => {
      const e = await prisma.execution.findUnique({ where: { dedupeKey: eventId } });
      return e?.status === 'COMPLETED' ? e : null;
    });
    await sleep(3000); // give a hypothetical duplicate time to appear

    const count = await prisma.execution.count({ where: { dedupeKey: eventId } });
    expect(count).toBe(1);
    expect(dest.received.length).toBe(1);
  });
});
