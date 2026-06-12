import { describe, it, expect } from 'vitest';
import { cronTriggerConfigSchema, httpActionConfigSchema, triggerEventSchema } from './schemas';

describe('cronTriggerConfigSchema', () => {
  it('accepts a valid 5-field cron expression', () => {
    expect(cronTriggerConfigSchema.parse({ expression: '*/5 * * * *' })).toEqual({ expression: '*/5 * * * *' });
  });
  it('rejects an invalid expression', () => {
    expect(() => cronTriggerConfigSchema.parse({ expression: 'not a cron' })).toThrow();
  });
});

describe('httpActionConfigSchema', () => {
  it('applies defaults', () => {
    const parsed = httpActionConfigSchema.parse({ url: 'https://example.com/hook' });
    expect(parsed.method).toBe('POST');
    expect(parsed.headers).toEqual({});
    expect(parsed.timeoutMs).toBe(10000);
  });
  it('rejects a non-url', () => {
    expect(() => httpActionConfigSchema.parse({ url: 'nope' })).toThrow();
  });
});

describe('triggerEventSchema', () => {
  it('round-trips a valid event', () => {
    const event = {
      eventId: 'e1', tenantId: 't1', triggerId: 'tr1', type: 'WEBHOOK',
      payload: { a: 1 }, version: 1, firedAt: new Date().toISOString(),
    };
    expect(triggerEventSchema.parse(event)).toEqual(event);
  });
});
