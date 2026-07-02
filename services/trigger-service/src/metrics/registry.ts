import client from 'prom-client';

export const registry = new client.Registry();
client.collectDefaultMetrics({ register: registry });

export const ratelimitRejectedTotal = new client.Counter({
  name: 'fluxgate_ratelimit_rejected_total',
  help: 'Rate limit rejections/waits by layer',
  labelNames: ['tenantId', 'layer'] as const,
  registers: [registry],
});
