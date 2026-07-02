import { randomUUID } from 'node:crypto';

export const TOPIC_TRIGGER_EVENTS = 'trigger-events';

export interface TriggerEvent {
  eventId: string;
  tenantId: string;
  triggerId: string;
  type: 'WEBHOOK' | 'CRON';
  payload: Record<string, unknown>;
  version: 1;
  firedAt: string; // ISO
  scheduledFor?: string; // ISO, cron only
}

export function webhookEventId(): string {
  return randomUUID();
}

export function cronEventId(triggerId: string, scheduledFireMs: number): string {
  return `${triggerId}:${scheduledFireMs}`;
}

export function workflowIdFor(eventId: string): string {
  return `exec-${eventId}`;
}
