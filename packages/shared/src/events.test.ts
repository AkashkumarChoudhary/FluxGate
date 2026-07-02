import { describe, it, expect } from 'vitest';
import { cronEventId, workflowIdFor, TOPIC_TRIGGER_EVENTS } from './events';

describe('eventId helpers', () => {
  it('cron eventId embeds triggerId and scheduled fire ms deterministically', () => {
    expect(cronEventId('trig-1', 1718200000000)).toBe('trig-1:1718200000000');
    expect(cronEventId('trig-1', 1718200000000)).toBe(cronEventId('trig-1', 1718200000000));
  });
  it('workflowIdFor prefixes exec-', () => {
    expect(workflowIdFor('abc')).toBe('exec-abc');
  });
  it('topic constant', () => {
    expect(TOPIC_TRIGGER_EVENTS).toBe('trigger-events');
  });
});
