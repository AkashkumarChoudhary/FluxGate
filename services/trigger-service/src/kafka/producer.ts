import { Kafka, Producer } from 'kafkajs';
import { TOPIC_TRIGGER_EVENTS, type TriggerEvent } from '@fluxgate/shared';

let producer: Producer | undefined;

export async function getProducer(): Promise<Producer> {
  if (!producer) {
    const kafka = new Kafka({
      clientId: 'trigger-service',
      brokers: (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(','),
    });
    producer = kafka.producer({ idempotent: true });
    await producer.connect();
  }
  return producer;
}

// key = tenantId -> per-tenant ordering within a partition (spec §6)
export async function produceTriggerEvent(event: TriggerEvent): Promise<void> {
  const p = await getProducer();
  await p.send({
    topic: TOPIC_TRIGGER_EVENTS,
    messages: [{ key: event.tenantId, value: JSON.stringify(event) }],
  });
}

export async function disconnectProducer(): Promise<void> {
  await producer?.disconnect();
  producer = undefined;
}
