import client from 'prom-client';

export const registry = new client.Registry();
client.collectDefaultMetrics({ register: registry });

export const actionSuccessTotal = new client.Counter({
  name: 'fluxgate_action_success_total',
  help: 'Successful action executions',
  labelNames: ['tenantId', 'actionType'] as const,
  registers: [registry],
});

export const actionFailureTotal = new client.Counter({
  name: 'fluxgate_action_failure_total',
  help: 'Failed action execution attempts',
  labelNames: ['tenantId', 'actionType'] as const,
  registers: [registry],
});

export const workflowDuration = new client.Histogram({
  name: 'fluxgate_temporal_workflow_duration_seconds',
  help: 'Workflow start-to-complete duration',
  labelNames: ['workflowType'] as const,
  buckets: [0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60],
  registers: [registry],
});

export const consumerLag = new client.Gauge({
  name: 'fluxgate_kafka_consumer_lag',
  help: 'Consumer lag per partition',
  labelNames: ['topic', 'partition'] as const,
  registers: [registry],
});

export const ratelimitRejectedTotal = new client.Counter({
  name: 'fluxgate_ratelimit_rejected_total',
  help: 'Rate limit rejections/waits by layer',
  labelNames: ['tenantId', 'layer'] as const,
  registers: [registry],
});
