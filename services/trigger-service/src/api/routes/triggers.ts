import { Router, type IRouter } from 'express';
import { z } from 'zod';
import { getPrisma } from '@fluxgate/db';
import { cronTriggerConfigSchema, webhookTriggerConfigSchema } from '@fluxgate/shared';
import { apiKeyAuth, tenantIdOf } from '../auth';
import { syncCronSchedule, removeCronSchedule } from '../../cron/schedule';

export const triggersRouter: IRouter = Router();
triggersRouter.use(apiKeyAuth);

const createTriggerSchema = z.discriminatedUnion('type', [
  z.object({ name: z.string().min(1), type: z.literal('WEBHOOK'), config: webhookTriggerConfigSchema }),
  z.object({ name: z.string().min(1), type: z.literal('CRON'), config: cronTriggerConfigSchema }),
]);

triggersRouter.post('/', async (req, res) => {
  const parsed = createTriggerSchema.safeParse(req.body);
  if (!parsed.success) return res.status(400).json({ error: parsed.error.flatten() });
  const trigger = await getPrisma().trigger.create({
    data: { tenantId: tenantIdOf(req), ...parsed.data },
  });
  if (trigger.type === 'CRON') await syncCronSchedule(trigger.id, (trigger.config as { expression: string }).expression);
  res.status(201).json(trigger);
});

triggersRouter.get('/', async (req, res) => {
  res.json(await getPrisma().trigger.findMany({ where: { tenantId: tenantIdOf(req) } }));
});

triggersRouter.get('/:id', async (req, res) => {
  const trigger = await getPrisma().trigger.findFirst({
    where: { id: req.params.id, tenantId: tenantIdOf(req) },
    include: { actions: { orderBy: { order: 'asc' } } },
  });
  if (!trigger) return res.status(404).json({ error: 'not found' });
  res.json(trigger);
});

const patchTriggerSchema = z.object({
  name: z.string().min(1).optional(),
  status: z.enum(['ACTIVE', 'PAUSED']).optional(),
});

triggersRouter.patch('/:id', async (req, res) => {
  const parsed = patchTriggerSchema.safeParse(req.body);
  if (!parsed.success) return res.status(400).json({ error: parsed.error.flatten() });
  const existing = await getPrisma().trigger.findFirst({ where: { id: req.params.id, tenantId: tenantIdOf(req) } });
  if (!existing) return res.status(404).json({ error: 'not found' });
  const trigger = await getPrisma().trigger.update({ where: { id: existing.id }, data: parsed.data });
  if (trigger.type === 'CRON') {
    if (trigger.status === 'PAUSED') await removeCronSchedule(trigger.id);
    else await syncCronSchedule(trigger.id, (trigger.config as { expression: string }).expression);
  }
  res.json(trigger);
});

triggersRouter.delete('/:id', async (req, res) => {
  const existing = await getPrisma().trigger.findFirst({ where: { id: req.params.id, tenantId: tenantIdOf(req) } });
  if (!existing) return res.status(404).json({ error: 'not found' });
  await getPrisma().trigger.delete({ where: { id: existing.id } }); // actions cascade via FK
  if (existing.type === 'CRON') await removeCronSchedule(existing.id);
  res.status(204).end();
});
