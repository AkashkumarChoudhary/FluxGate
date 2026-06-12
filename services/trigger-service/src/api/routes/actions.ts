import { Router } from 'express';
import { z } from 'zod';
import { getPrisma } from '@fluxgate/db';
import { httpActionConfigSchema } from '@fluxgate/shared';
import { apiKeyAuth, tenantIdOf } from '../auth';

// Mounted at /triggers/:triggerId/actions
export const actionsRouter = Router({ mergeParams: true });
actionsRouter.use(apiKeyAuth);

const createActionSchema = z.object({
  type: z.literal('HTTP'),
  config: httpActionConfigSchema,
  order: z.number().int().min(0).default(0),
});

actionsRouter.post('/', async (req, res) => {
  const trigger = await getPrisma().trigger.findFirst({
    where: { id: req.params.triggerId, tenantId: tenantIdOf(req) },
  });
  if (!trigger) return res.status(404).json({ error: 'trigger not found' });
  const parsed = createActionSchema.safeParse(req.body);
  if (!parsed.success) return res.status(400).json({ error: parsed.error.flatten() });
  const action = await getPrisma().action.create({
    data: { triggerId: trigger.id, tenantId: trigger.tenantId, ...parsed.data },
  });
  res.status(201).json(action);
});

actionsRouter.delete('/:actionId', async (req, res) => {
  const action = await getPrisma().action.findFirst({
    where: { id: req.params.actionId, triggerId: req.params.triggerId, tenantId: tenantIdOf(req) },
  });
  if (!action) return res.status(404).json({ error: 'not found' });
  await getPrisma().action.delete({ where: { id: action.id } });
  res.status(204).end();
});
