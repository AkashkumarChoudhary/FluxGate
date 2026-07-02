import { Context } from '@temporalio/activity';
import { ApplicationFailure } from '@temporalio/common';
import { getPrisma, Prisma } from '@fluxgate/db';
import { httpActionConfigSchema, type TriggerEvent } from '@fluxgate/shared';
import { renderBody } from './template';
import { actionSuccessTotal, actionFailureTotal } from '../../metrics/registry';

// 1. INSERT executions row. Idempotent: dedupeKey unique + skipDuplicates, then fetch.
export async function recordExecution(event: TriggerEvent): Promise<string> {
  const prisma = getPrisma();
  await prisma.execution.createMany({
    data: {
      triggerId: event.triggerId,
      tenantId: event.tenantId,
      dedupeKey: event.eventId,
      temporalWorkflowId: `exec-${event.eventId}`,
      status: 'RUNNING',
      triggeredAt: new Date(event.firedAt),
      scheduledFor: event.scheduledFor ? new Date(event.scheduledFor) : null,
    },
    skipDuplicates: true, // ON CONFLICT (dedupe_key) DO NOTHING
  });
  const execution = await prisma.execution.findUniqueOrThrow({ where: { dedupeKey: event.eventId } });
  return execution.id;
}

// 2. Load action config, make the HTTP call, append a step row per attempt.
//    4xx -> nonRetryable; timeout/5xx -> throw retryable (Temporal applies backoff).
export async function executeHttpAction(executionId: string, event: TriggerEvent): Promise<void> {
  const prisma = getPrisma();
  const action = await prisma.action.findFirst({
    where: { triggerId: event.triggerId },
    orderBy: { order: 'asc' },
  });
  if (!action) {
    throw ApplicationFailure.nonRetryable('trigger has no action configured', 'NoActionConfigured');
  }
  const config = httpActionConfigSchema.parse(action.config);
  const attemptNumber = Context.current().info.attempt;
  const body = config.method === 'GET' ? undefined : JSON.stringify(renderBody(config.bodyTemplate, event.payload));
  const request = { url: config.url, method: config.method, headers: config.headers, body: body ?? null };

  const start = Date.now();
  let response: Response | undefined;
  let error: unknown;
  try {
    response = await fetch(config.url, {
      method: config.method,
      headers: { 'content-type': 'application/json', ...config.headers },
      body,
      signal: AbortSignal.timeout(config.timeoutMs),
    });
  } catch (err) {
    error = err;
  }
  const durationMs = Date.now() - start;
  const responseJson = response
    ? { status: response.status, body: (await response.text()).slice(0, 4096) }
    : { error: String(error) };

  // Append-only step row keyed by attemptNumber; skipDuplicates makes Temporal replay safe.
  await prisma.executionStep.createMany({
    data: {
      executionId,
      actionId: action.id,
      order: action.order,
      status: response?.ok ? 'COMPLETED' : 'FAILED',
      request: request as Prisma.InputJsonValue,
      response: responseJson as Prisma.InputJsonValue,
      durationMs,
      attemptNumber,
    },
    skipDuplicates: true,
  });

  if (response?.ok) {
    actionSuccessTotal.inc({ tenantId: event.tenantId, actionType: 'HTTP' });
    return;
  }
  actionFailureTotal.inc({ tenantId: event.tenantId, actionType: 'HTTP' });
  if (response && response.status >= 400 && response.status < 500) {
    throw ApplicationFailure.nonRetryable(`destination returned ${response.status}`, 'NonRetryableHttpError');
  }
  throw new Error(response ? `destination returned ${response.status}` : `request failed: ${String(error)}`);
}

// 3. Final status. Idempotent: plain UPDATE to a terminal value.
export async function finalizeExecution(
  executionId: string,
  status: 'COMPLETED' | 'FAILED',
  failureReason?: string,
): Promise<void> {
  await getPrisma().execution.update({
    where: { id: executionId },
    data: { status, completedAt: new Date(), failureReason: failureReason ?? null },
  });
}
