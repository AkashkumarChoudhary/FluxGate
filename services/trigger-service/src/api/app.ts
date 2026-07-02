import express, { type Express, type Request, type Response, type NextFunction } from 'express';
import { tenantsRouter } from './routes/tenants';
import { triggersRouter } from './routes/triggers';
import { actionsRouter } from './routes/actions';
import { webhooksRouter } from './routes/webhooks';
import { registry } from '../metrics/registry';

export function createApp(): Express {
  const app = express();
  app.use(express.json({ limit: '256kb' }));
  app.get('/healthz', (_req, res) => res.json({ ok: true }));
  app.get('/metrics', async (_req, res) => {
    res.set('content-type', registry.contentType);
    res.end(await registry.metrics());
  });
  app.use('/tenants', tenantsRouter);
  app.use('/triggers', triggersRouter);
  app.use('/triggers/:triggerId/actions', actionsRouter);
  app.use('/webhooks', webhooksRouter);
  // Catch unhandled async errors from routes (Express 4 requires explicit error forwarding).
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  app.use((err: unknown, _req: Request, res: Response, _next: NextFunction) => {
    console.error('unhandled route error', err);
    res.status(500).json({ error: 'internal server error' });
  });
  return app;
}
