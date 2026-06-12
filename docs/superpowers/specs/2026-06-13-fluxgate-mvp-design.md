# Fluxgate MVP — Design Spec

**Date:** 2026-06-13
**Status:** Approved for implementation planning
**Scope:** MVP (Weeks 1–3 of the project plan). Extended scope (multi-step pipelines, gRPC, DLQ replay) is out of scope and gets its own spec later.

## 1. What Fluxgate is

A production-grade, multi-tenant event automation engine. Tenants register **triggers** (webhook received, cron schedule) connected to **actions** (HTTP call in MVP). When a trigger fires, the system reliably executes the action — with retries, per-tenant ordering, per-tenant rate limiting, and a complete execution audit log.

Pitch: every trigger-to-action execution is a durable Temporal workflow; Kafka is the event bus partitioned by tenant; Redis sorted sets drive the cron scheduler; everything is observable via Prometheus and Grafana, shipped in one Docker Compose.

This is a portfolio project: every architecture decision must be interview-defensible, and the MVP includes the measurement harness that produces real numbers for resume bullets.

## 2. MVP scope

**In:**
- Docker Compose stack: Postgres 16, Redis 7, Kafka (KRaft mode, no ZooKeeper), Temporal + Temporal UI, Prometheus, Grafana (provisioned dashboard), both services.
- trigger-service: REST API (tenant registration, trigger CRUD, action CRUD), webhook ingestion endpoint, Kafka producer, cron poller.
- worker-service: Kafka consumer, Temporal worker, ActionWorkflow with a single HTTP action.
- Trigger types: `WEBHOOK`, `CRON`. Action type: `HTTP`.
- Two-layer per-tenant rate limiting (edge 429 + consumer dispatch bucket).
- Prometheus metrics + provisioned Grafana dashboard.
- Vitest integration tests against the real compose stack.
- k6 load harness producing the resume numbers.

**Out (deliberately):**
- `EVENT` trigger type — no defined emitter exists; an undefined trigger type is an interview liability. The enum stays extensible.
- Email/Slack actions — pipeline mechanics are identical to HTTP; they're config additions later.
- Multi-step pipelines and compensation — compensation is not meaningful with a single action; it enters with multi-step pipelines (extended scope).
- DLQ topic and replay API.
- gRPC. **Important note for extended scope:** the original plan (synchronous gRPC hop trigger-service → worker-service → Kafka) weakens the architecture by putting worker-service in the hot ingest path in front of an async bus. When gRPC is added, use it for an internal admin/query API (execution status lookups, replay commands) — not the ingest path.

## 3. Repo layout

pnpm workspaces monorepo:

```
fluxgate/
├── services/
│   ├── trigger-service/
│   │   └── src/
│   │       ├── api/            # express server, routes: tenants, triggers, actions, webhooks
│   │       ├── kafka/          # producer
│   │       ├── cron/           # sorted-set poller + Lua claim script
│   │       └── ratelimit/      # edge token bucket (Lua)
│   └── worker-service/
│       └── src/
│           ├── kafka/          # consumer
│           ├── temporal/       # workflows/, activities/, worker.ts
│           ├── ratelimit/      # dispatch token bucket (Lua)
│           └── metrics/        # prom-client registry + /metrics endpoint
├── packages/
│   ├── db/                     # prisma/schema.prisma + generated client (shared)
│   └── shared/                 # TriggerEvent type, zod schemas, constants, eventId helpers
├── load/                       # k6 scripts
├── docker-compose.yml
├── prometheus.yml
└── grafana/provisioning/       # datasource + dashboard JSON
```

Rationale: Prisma schema and client live in `packages/db` because both services need them — duplicating the schema across services is a drift trap. No `proto/` directories until gRPC exists.

## 4. Tech stack

Node 20, TypeScript 5.4, pnpm workspaces. Dependencies as in the bootstrap doc (`@temporalio/*` ^1.10, `kafkajs` ^2.2.4, `ioredis` ^5.4, `express` ^4.19, `prom-client` ^15, `zod` ^3.23, Prisma ^5.14) **plus `cron-parser`** for next-occurrence computation, **minus** gRPC packages and nodemailer (not in MVP). Test runner: Vitest. Load: k6.

## 5. Data model (Postgres via Prisma)

```
tenants          id, name, apiKeyHash, status, createdAt
triggers         id, tenantId, name, type (WEBHOOK|CRON), config Json,
                 status (ACTIVE|PAUSED), createdAt
actions          id, triggerId, tenantId, type (HTTP), config Json,
                 order Int, createdAt
executions       id, triggerId, tenantId, dedupeKey (UNIQUE), temporalWorkflowId,
                 temporalRunId, status (PENDING|RUNNING|COMPLETED|FAILED),
                 triggeredAt, scheduledFor (nullable), completedAt, failureReason
execution_steps  id, executionId, actionId, order, status, request Json,
                 response Json, durationMs, attemptNumber, createdAt
                 UNIQUE (executionId, actionId, attemptNumber)
```

Design decisions:
- `config` is Json on triggers and actions — type-specific config (cron expression; HTTP url/method/headers/body template) without a table per type. Validated by zod schemas in `packages/shared` at write time.
- `executions.dedupeKey` is the idempotency anchor (section 7); unique index enforces one execution per logical event.
- `executions.scheduledFor` is set for cron fires so on-time percentage is measurable directly from this table.
- `execution_steps` is append-only — never UPDATE a step row; retries INSERT with incremented `attemptNumber`. The unique constraint makes this structural, not conventional. Full retry history preserved.
- `COMPENSATED` status is deferred to extended scope along with compensation itself.
- Auth: tenants authenticate API/webhook calls with an API key (random 32-byte token, stored hashed); middleware resolves key → tenantId.

## 6. Event bus (Kafka)

- Topic `trigger-events`, message **key = tenantId** → per-tenant ordering within a partition. Partition count is a config knob; default 8 (≥ expected concurrent hot tenants — this also mitigates the rate-limit co-partitioning trade-off in section 9).
- Message value: `TriggerEvent { eventId, tenantId, triggerId, type, payload, version, firedAt, scheduledFor? }`. `version` supports schema evolution for downstream consumers.
- KRaft mode (no ZooKeeper) — one less container, ZooKeeper is deprecated upstream.
- Why Kafka over Redis pub-sub: pub-sub is fire-and-forget with no persistence; a missed trigger means a user's automation silently didn't run. Kafka gives disk persistence, replay from offset, and consumer lag as an operational metric.

## 7. Event flow and idempotency (the spine)

```
webhook POST ─► edge token bucket (429+Retry-After) ─► eventId = UUID
                                                          │
cron poller ─► due triggers ─► eventId = {triggerId}:{scheduledFireMs}
                                                          │
                                                          ▼
                                Kafka trigger-events (key = tenantId)
                                                          │
                                                          ▼
                          consumer ─► dispatch bucket (bounded wait if empty)
                                                          │
                                                          ▼
              Temporal startWorkflow: workflowId = exec-{eventId},
              WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE
```

**Idempotency is delegated to Temporal's workflow-ID dedup.** Every workflow starts with `workflowId = exec-{eventId}`. A duplicate Kafka delivery (consumer crashed after dispatch but before offset commit) gets "workflow already started" and is skipped. A cron double-fire produces the same eventId (it embeds `scheduledFireMs`), so it dedupes identically. Dedup happens *before any work runs*, enforced by Temporal's own store — stronger than checking an executions row inside the workflow.

End-to-end delivery semantics: **at-least-once everywhere, effectively-once execution** via dedup at dispatch. The `executions.dedupeKey` unique index is a second, defense-in-depth guard.

## 8. Cron scheduler (Redis sorted set)

- Sorted set `cron:triggers`; **member = triggerId, score = next-fire epoch-ms**. The score IS the schedule — no separate index. O(log N) insert, O(log N + M) due-range query.
- Poller loop in trigger-service (interval ~500ms): `ZRANGEBYSCORE cron:triggers 0 {now}` → for each due trigger:
  1. **Produce the TriggerEvent to Kafka** (eventId embeds the scheduled fire time).
  2. **Lua claim/requeue script**: atomically verify the member's score is still ≤ now and set it to the next occurrence (computed via `cron-parser`); returns whether this poller performed the requeue.
- Crash-safety analysis (this ordering is deliberate):
  - Crash between ZRANGEBYSCORE and produce → trigger still due, next poll retries. Nothing lost.
  - Crash between produce and requeue → next poll re-produces the same eventId → Temporal workflowId dedup swallows it. Duplicate, not loss.
  - Because the member never leaves the set (score update, not ZREM+ZADD), there is no window where a crash loses the schedule permanently.
  - The Lua compare-and-set also makes **concurrent pollers** safe: both may produce (deduped), but only one requeues, and the schedule can't skip or double-advance.
- Trigger CRUD keeps the set in sync: create/resume CRON trigger → ZADD with next occurrence; pause/delete → ZREM.
- Target: ≥98% of fires within 1s of scheduled time, measured by the load harness from `executions.scheduledFor` vs `triggeredAt`.

## 9. Per-tenant rate limiting (two layers)

Both layers are Redis token buckets implemented as Lua scripts (atomic read-refill-decrement — the TOCTOU race is eliminated because check-and-decrement is one atomic Redis operation). Bucket params (capacity, refill rate) per tenant with sane defaults; stored config can override per tenant later.

- **Layer 1 — edge (trigger-service webhook endpoint):** bucket empty → HTTP 429 with `Retry-After` header. Natural HTTP semantics; protects Kafka and Postgres from a flooding tenant before anything is enqueued.
- **Layer 2 — dispatch (worker-service consumer):** before starting a workflow, check the tenant's dispatch bucket. Empty → **wait in-process until a token is available** (poll the bucket with short sleeps, calling kafkajs `heartbeat()` on each iteration so the consumer isn't evicted from the group), then dispatch. The wait is unbounded by design — events are never dropped or reordered; the bucket's refill rate is the bound. Consumer slowdown is Kafka-native backpressure; per-tenant ordering is preserved; no events are lost or reordered.
- **Owned trade-off:** a tenant co-located on the same partition as a rate-limited tenant also waits. Mitigation: partition count ≥ expected hot tenants. Rejected alternatives, for the record: a delayed-retry topic breaks per-tenant ordering; dispatch-with-Temporal-startDelay shifts the flood onto the system the limiter exists to protect.
- Rejections/waits are counted in `fluxgate_ratelimit_rejected_total` (labeled by tenant and layer).

## 10. Temporal workflow

`actionWorkflow(event: TriggerEvent)` in worker-service:

1. `recordExecution` activity — INSERT executions row (dedupeKey = eventId, status RUNNING). Idempotent: `ON CONFLICT (dedupeKey) DO NOTHING` + fetch.
2. `executeHttpAction` activity — loads the action config, makes the HTTP call, INSERTs an `execution_steps` row per attempt with request/response/durationMs.
3. `finalizeExecution` activity — sets status COMPLETED/FAILED, completedAt, failureReason.

Retry policy is **per-activity**, not per-workflow. In `executeHttpAction`:
- Destination 4xx → `ApplicationFailure.nonRetryable` (the request is wrong; retrying won't fix it).
- Network timeout / 5xx → retryable, exponential backoff, max attempts (default 5).
- Retries exhausted → workflow records FAILED with the final failureReason.

Worker crash mid-workflow: Temporal replays event history on restart and resumes from the last recorded point; activities are idempotent (append-only steps keyed by attemptNumber; execution insert is ON CONFLICT) so replay is safe.

Why Temporal over BullMQ/manual retry loops: durable execution removes hand-rolled saga state (status columns + retry counters); the workflow history is the source of truth, and crash-resume is free.

## 11. Observability

prom-client metrics, `/metrics` on both services, scraped by Prometheus:

| Metric | Type | Labels |
|---|---|---|
| `fluxgate_trigger_latency_seconds` | histogram | tenantId, triggerType |
| `fluxgate_kafka_consumer_lag` | gauge | topic, partition |
| `fluxgate_temporal_workflow_duration_seconds` | histogram | workflowType |
| `fluxgate_action_success_total` / `fluxgate_action_failure_total` | counter | tenantId, actionType |
| `fluxgate_ratelimit_rejected_total` | counter | tenantId, layer |

`fluxgate_dlq_depth` arrives with the DLQ in extended scope. **Known and owned:** `tenantId` as a label is a cardinality risk at real scale (label explosion); acceptable at portfolio scale, and the mitigation story (drop the label, use exemplars or per-tenant recording rules) is itself interview material.

Grafana dashboard provisioned in compose (datasource + dashboard JSON checked in): trigger latency p50/p99, consumer lag per partition, workflow duration histogram, per-tenant action success rate, rate-limit rejections.

## 12. Error handling summary

- Invalid webhook payload / unknown trigger / paused trigger → 4xx at the edge, nothing enqueued.
- Kafka produce failure at the edge → 503 to the webhook caller (caller retries; nothing half-recorded). Cron produce failure → trigger stays due, next poll retries.
- Action HTTP 4xx → non-retryable, execution FAILED immediately with reason.
- Action timeout/5xx → per-activity retries with backoff; exhausted → FAILED.
- Consumer crash → Kafka redelivers from last committed offset; Temporal workflowId dedup absorbs duplicates.
- Worker crash mid-workflow → Temporal resumes from history.
- Redis down → cron pauses (fires late, not lost — set is persistent given Redis persistence/AOF default in compose); rate limiter fails open with a logged warning (availability over strictness for a portfolio system — documented choice).

## 13. Testing and measurement

**Integration tests (Vitest), run against the real compose stack** — no mocked Kafka/Redis; exercising the real coordination is the point:
1. Webhook e2e: POST webhook → execution COMPLETED, step row recorded, destination (local mock HTTP server) received the call.
2. Dedup: same eventId delivered twice → exactly one execution.
3. Cron: due trigger fires, requeues to next occurrence; two concurrent pollers → exactly one execution per scheduled fire.
4. Rate limiting: flood one tenant → 429s with Retry-After at the edge; dispatch bucket delays without reordering; second tenant unaffected (different partition).
5. Failure paths: destination 4xx → FAILED, no retries; destination 5xx → retries then COMPLETED/FAILED per config.

**Load harness (k6, in `load/`):** fires webhooks across N synthetic tenants at increasing rates; reports sustained triggers/min, end-to-end p50/p99 latency (webhook accept → execution complete, from the executions table), cron on-time percentage (scheduledFor vs triggeredAt), and rate-limit fairness (hot tenant throttled, others unaffected). These are the numbers that fill the resume bullets.

## 14. Build order (3 weeks at 2–3 hrs/day)

1. **Week 1:** compose stack (Postgres, Redis, Kafka KRaft, Temporal, Temporal UI) → `packages/db` schema + migrations → trigger-service CRUD + API-key auth → webhook endpoint producing to Kafka.
2. **Week 2:** worker-service consumer → ActionWorkflow + three activities → e2e: curl webhook → workflow visible in Temporal UI → executions/steps rows. First integration tests.
3. **Week 3:** cron scheduler (poller + Lua claim) → two-layer rate limiting → Prometheus metrics + Grafana provisioning → remaining integration tests → k6 harness + first measured numbers.
