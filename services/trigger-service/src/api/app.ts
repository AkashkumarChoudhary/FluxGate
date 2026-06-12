import express from 'express';
import { tenantsRouter } from './routes/tenants';

export function createApp() {
  const app = express();
  app.use(express.json({ limit: '256kb' }));
  app.get('/healthz', (_req, res) => res.json({ ok: true }));
  app.use('/tenants', tenantsRouter);
  return app;
}
