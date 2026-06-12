import express, { type Express } from 'express';
import { tenantsRouter } from './routes/tenants';
import { triggersRouter } from './routes/triggers';
import { actionsRouter } from './routes/actions';
import { webhooksRouter } from './routes/webhooks';

export function createApp(): Express {
  const app = express();
  app.use(express.json({ limit: '256kb' }));
  app.get('/healthz', (_req, res) => res.json({ ok: true }));
  app.use('/tenants', tenantsRouter);
  app.use('/triggers', triggersRouter);
  app.use('/triggers/:triggerId/actions', actionsRouter);
  app.use('/webhooks', webhooksRouter);
  return app;
}
