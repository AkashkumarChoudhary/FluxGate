import { z } from 'zod';
import cronParser from 'cron-parser';

export const cronTriggerConfigSchema = z.object({
  expression: z.string().refine(
    (expr) => {
      try {
        cronParser.parseExpression(expr);
        return true;
      } catch {
        return false;
      }
    },
    { message: 'invalid cron expression' },
  ),
});

export const webhookTriggerConfigSchema = z.object({}).passthrough();

export const httpActionConfigSchema = z.object({
  url: z.string().url(),
  method: z.enum(['GET', 'POST', 'PUT', 'PATCH', 'DELETE']).default('POST'),
  headers: z.record(z.string()).default({}),
  bodyTemplate: z.record(z.unknown()).optional(),
  timeoutMs: z.number().int().positive().default(10000),
});

export const triggerEventSchema = z.object({
  eventId: z.string().min(1),
  tenantId: z.string().min(1),
  triggerId: z.string().min(1),
  type: z.enum(['WEBHOOK', 'CRON']),
  payload: z.record(z.unknown()),
  version: z.literal(1),
  firedAt: z.string().datetime(),
  scheduledFor: z.string().datetime().optional(),
});

export type CronTriggerConfig = z.infer<typeof cronTriggerConfigSchema>;
export type HttpActionConfig = z.infer<typeof httpActionConfigSchema>;
