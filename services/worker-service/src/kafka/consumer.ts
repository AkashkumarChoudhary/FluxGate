import { Kafka } from 'kafkajs';
import { Client, Connection, WorkflowExecutionAlreadyStartedError } from '@temporalio/client';
import { WorkflowIdReusePolicy } from '@temporalio/common';
import { TOPIC_TRIGGER_EVENTS, triggerEventSchema, workflowIdFor, type TriggerEvent } from '@fluxgate/shared';
import { TASK_QUEUE } from '../temporal/worker';
import { waitForDispatchToken } from '../ratelimit/dispatchBucket';
import { triggerLatency, observeOffset } from '../metrics/instrumentation';

export async function runConsumer(): Promise<void> {
  const kafka = new Kafka({
    clientId: 'worker-service',
    brokers: (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(','),
  });
  const consumer = kafka.consumer({ groupId: 'fluxgate-workers' });
  const temporal = new Client({
    connection: await Connection.connect({ address: process.env.TEMPORAL_ADDRESS ?? 'localhost:7233' }),
  });

  await consumer.connect();
  await consumer.subscribe({ topic: TOPIC_TRIGGER_EVENTS, fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ topic, partition, message, heartbeat }) => {
      observeOffset(topic, partition, message.offset);
      const parsed = triggerEventSchema.safeParse(JSON.parse(message.value!.toString()));
      if (!parsed.success) {
        console.error('malformed TriggerEvent, skipping', parsed.error.flatten());
        return; // commit past it; nothing actionable
      }
      const event = parsed.data as TriggerEvent;

      // Layer-2 rate limit: in-process wait, heartbeat keeps us in the group (spec §9).
      await waitForDispatchToken(event.tenantId, heartbeat);

      try {
        await temporal.workflow.start('actionWorkflow', {
          taskQueue: TASK_QUEUE,
          workflowId: workflowIdFor(event.eventId),
          workflowIdReusePolicy: WorkflowIdReusePolicy.REJECT_DUPLICATE,
          args: [event],
        });
        triggerLatency.observe(
          { tenantId: event.tenantId, triggerType: event.type },
          (Date.now() - new Date(event.firedAt).getTime()) / 1000,
        );
      } catch (err) {
        if (err instanceof WorkflowExecutionAlreadyStartedError) {
          return; // duplicate delivery — dedup did its job (spec §7)
        }
        throw err; // kafkajs retries the batch; offset not committed
      }
    },
  });
}
