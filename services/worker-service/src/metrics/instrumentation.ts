import client from 'prom-client';
import { Kafka } from 'kafkajs';
import { registry, consumerLag } from './registry';

export const triggerLatency = new client.Histogram({
  name: 'fluxgate_trigger_latency_seconds',
  help: 'Latency from trigger fire to workflow dispatch',
  labelNames: ['tenantId', 'triggerType'] as const,
  buckets: [0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10],
  registers: [registry],
});

// Track consumed offsets per partition; a 15s admin poll computes lag against high watermarks.
const lastOffsets = new Map<string, number>();
export function observeOffset(topic: string, partition: number, offset: string): void {
  lastOffsets.set(`${topic}:${partition}`, Number(offset));
}

export async function startLagPoller(brokers: string[]): Promise<void> {
  const admin = new Kafka({ clientId: 'lag-poller', brokers }).admin();
  await admin.connect();
  setInterval(async () => {
    try {
      const topicOffsets = await admin.fetchTopicOffsets('trigger-events');
      for (const { partition, offset } of topicOffsets) {
        const consumed = lastOffsets.get(`trigger-events:${partition}`) ?? Number(offset) - 1;
        consumerLag.set({ topic: 'trigger-events', partition: String(partition) }, Math.max(0, Number(offset) - 1 - consumed));
      }
    } catch (err) {
      console.error('lag poll failed', err);
    }
  }, 15_000).unref();
}
