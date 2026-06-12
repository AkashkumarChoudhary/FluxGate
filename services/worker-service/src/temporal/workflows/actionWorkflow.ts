import { proxyActivities } from '@temporalio/workflow';
import type { TriggerEvent } from '@fluxgate/shared';
import type * as activities from '../activities';

const { recordExecution, finalizeExecution } = proxyActivities<typeof activities>({
  startToCloseTimeout: '30 seconds',
  retry: { maximumAttempts: 5 },
});

// Retry policy is per-activity (spec §10): 4xx is nonRetryable via NonRetryableHttpError;
// timeout/5xx retries with exponential backoff.
const { executeHttpAction } = proxyActivities<typeof activities>({
  startToCloseTimeout: '60 seconds',
  retry: {
    initialInterval: '1 second',
    backoffCoefficient: 2,
    maximumInterval: '30 seconds',
    maximumAttempts: 5,
    nonRetryableErrorTypes: ['NonRetryableHttpError', 'NoActionConfigured'],
  },
});

export async function actionWorkflow(event: TriggerEvent): Promise<void> {
  const executionId = await recordExecution(event);
  try {
    await executeHttpAction(executionId, event);
    await finalizeExecution(executionId, 'COMPLETED');
  } catch (err) {
    const reason = err instanceof Error ? err.message : String(err);
    await finalizeExecution(executionId, 'FAILED', reason);
  }
}
