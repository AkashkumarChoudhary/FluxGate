import express from 'express';
import { tenantsRouter } from './routes/tenants';
import { triggersRouter } from './routes/triggers';
import { actionsRouter } from './routes/actions';

export function createApp() {
  const app = express();
  app.use(express.json({ limit: '256kb' }));
  app.get('/healthz', (_req, res) => res.json({ ok: true }));
  app.use('/tenants', tenantsRouter);
  app.use('/triggers', triggersRouter);
  app.use('/triggers/:triggerId/actions', actionsRouter);
  return app;
}
