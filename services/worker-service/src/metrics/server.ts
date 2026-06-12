import express from 'express';
import { registry } from './registry';

export function startMetricsServer(port: number): void {
  const app = express();
  app.get('/metrics', async (_req, res) => {
    res.set('content-type', registry.contentType);
    res.end(await registry.metrics());
  });
  app.get('/healthz', (_req, res) => res.json({ ok: true }));
  app.listen(port, () => console.log(`worker metrics on :${port}`));
}
