import { Worker, NativeConnection } from '@temporalio/worker';
import * as activities from './activities';

export const TASK_QUEUE = 'fluxgate-actions';

export async function runTemporalWorker(): Promise<Worker> {
  const connection = await NativeConnection.connect({
    address: process.env.TEMPORAL_ADDRESS ?? 'localhost:7233',
  });
  const worker = await Worker.create({
    connection,
    namespace: 'default',
    taskQueue: TASK_QUEUE,
    workflowsPath: require.resolve('./workflows/actionWorkflow'),
    activities,
  });
  void worker.run();
  return worker;
}
