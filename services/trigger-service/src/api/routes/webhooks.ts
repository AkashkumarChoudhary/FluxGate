import { Router, type IRouter } from 'express';
import { getPrisma } from '@fluxgate/db';
import { webhookEventId, type TriggerEvent } from '@fluxgate/shared';
import { apiKeyAuth, tenantIdOf } from '../auth';
import { produceTriggerEvent } from '../../kafka/producer';

export const webhooksRouter: IRouter = Router();
webhooksRouter.use(apiKeyAuth);

// POST /webhooks/:triggerId — body is the event payload, forwarded verbatim.
webhooksRouter.post('/:triggerId', async (req, res) => {
  const trigger = await getPrisma().trigger.findFirst({
    where: { id: req.params.triggerId, tenantId: tenantIdOf(req) },
  });
  if (!trigger || trigger.type !== 'WEBHOOK') return res.status(404).json({ error: 'webhook trigger not found' });
  if (trigger.status === 'PAUSED') return res.status(409).json({ error: 'trigger is paused' });

  const event: TriggerEvent = {
    eventId: webhookEventId(),
    tenantId: trigger.tenantId,
    triggerId: trigger.id,
    type: 'WEBHOOK',
    payload: req.body ?? {},
    version: 1,
    firedAt: new Date().toISOString(),
  };

  try {
    await produceTriggerEvent(event);
  } catch (err) {
    // Spec §12: produce failure -> 503, nothing half-recorded; caller retries.
    console.error('kafka produce failed', err);
    return res.status(503).json({ error: 'event bus unavailable, retry' });
  }
  res.status(202).json({ eventId: event.eventId });
});
