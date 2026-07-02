import { runTemporalWorker } from './temporal/worker';
import { runConsumer } from './kafka/consumer';
import { startMetricsServer } from './metrics/server';
import { startLagPoller } from './metrics/instrumentation';

async function main() {
  startMetricsServer(Number(process.env.WORKER_METRICS_PORT ?? 3001));
  await runTemporalWorker();
  await startLagPoller((process.env.KAFKA_BROKERS ?? 'localhost:9092').split(','));
  await runConsumer();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
