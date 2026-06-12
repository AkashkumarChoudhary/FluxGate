# Fluxgate MVP Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the Fluxgate MVP — a multi-tenant event automation engine where webhook/cron triggers fire HTTP actions through Kafka + Temporal with per-tenant ordering, two-layer rate limiting, and full observability.

**Architecture:** Two Node services in a pnpm monorepo. `trigger-service` ingests webhooks and polls a Redis sorted-set cron schedule, producing `TriggerEvent`s to Kafka keyed by tenantId. `worker-service` consumes, passes a per-tenant dispatch token bucket, and starts a Temporal `actionWorkflow` with `workflowId = exec-{eventId}` (REJECT_DUPLICATE = idempotency). Postgres (via Prisma in `packages/db`) is the audit log; Prometheus + Grafana observe everything; one Docker Compose runs the stack.

**Tech Stack:** Node 20, TypeScript 5.4, pnpm workspaces, Express 4, kafkajs 2.2, @temporalio/* 1.10, ioredis 5, Prisma 5.14, zod 3.23, prom-client 15, cron-parser, Vitest, k6.

**Spec:** `docs/superpowers/specs/2026-06-13-fluxgate-mvp-design.md`

**Conventions used throughout:**
- All commands run from repo root unless stated.
- Tests live next to the package they test under `src/**/*.test.ts` (unit) or `tests/integration/*.test.ts` at repo root (integration, real compose stack).
- Integration tests require `docker compose up -d` and both services running; each PR's final task says exactly how.
- Commit after every green test. Branch per PR: `pr1-foundation`, `pr2-worker`, `pr3-cron-ratelimit-obs`.

---

# PR 1 — Compose stack, DB schema, trigger-service CRUD + webhook→Kafka

### Task 1: Monorepo scaffolding

**Files:**
- Create: `package.json`, `pnpm-workspace.yaml`, `tsconfig.base.json`, `.gitignore`, `.env.example`

- [ ] **Step 1: Create branch**

```bash
git checkout -b pr1-foundation
```

- [ ] **Step 2: Write root config files**

`package.json`:
```json
{
  "name": "fluxgate",
  "private": true,
  "scripts": {
    "build": "pnpm -r build",
    "test": "vitest run --project unit",
    "test:integration": "vitest run --project integration",
    "db:migrate": "pnpm --filter @fluxgate/db migrate:dev",
    "db:generate": "pnpm --filter @fluxgate/db generate"
  },
  "devDependencies": {
    "typescript": "~5.4.5",
    "vitest": "^1.6.0",
    "@types/node": "^20.12.0"
  },
  "engines": { "node": ">=20" }
}
```

`pnpm-workspace.yaml`:
```yaml
packages:
  - "services/*"
  - "packages/*"
```

`tsconfig.base.json`:
```json
{
  "compilerOptions": {
    "target": "ES2022",
    "module": "NodeNext",
    "moduleResolution": "NodeNext",
    "strict": true,
    "esModuleInterop": true,
    "skipLibCheck": true,
    "declaration": true,
    "sourceMap": true,
    "outDir": "dist"
  }
}
```

`.gitignore`:
```
node_modules/
dist/
.env
*.tsbuildinfo
```

`.env.example`:
```
DATABASE_URL=postgresql://fluxgate:fluxgate@localhost:5432/fluxgate
REDIS_URL=redis://localhost:6379
KAFKA_BROKERS=localhost:9092
TEMPORAL_ADDRESS=localhost:7233
TRIGGER_SERVICE_PORT=3000
WORKER_METRICS_PORT=3001
```

`vitest.workspace.ts` (repo root):
```ts
import { defineWorkspace } from 'vitest/config';

export default defineWorkspace([
  {
    test: {
      name: 'unit',
      include: ['packages/**/*.test.ts', 'services/**/*.test.ts'],
    },
  },
  {
    test: {
      name: 'integration',
      include: ['tests/integration/**/*.test.ts'],
      testTimeout: 60_000,
      hookTimeout: 60_000,
      fileParallelism: false,
    },
  },
]);
```

- [ ] **Step 3: Install and verify**

```bash
pnpm install
npx tsc --noEmit -p tsconfig.base.json 2>/dev/null; echo ok
```
Expected: `ok` (no source yet; just confirms tooling installs).

- [ ] **Step 4: Commit**

```bash
git add -A && git commit -m "chore: pnpm monorepo scaffolding"
```

---

### Task 2: Docker Compose stack

**Files:**
- Create: `docker-compose.yml`, `prometheus.yml`, `grafana/provisioning/datasources/datasource.yml`

- [ ] **Step 1: Write `docker-compose.yml`**

```yaml
services:
  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_USER: fluxgate
      POSTGRES_PASSWORD: fluxgate
      POSTGRES_DB: fluxgate
    ports: ["5432:5432"]
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U fluxgate"]
      interval: 5s
      timeout: 3s
      retries: 10

  redis:
    image: redis:7-alpine
    command: ["redis-server", "--appendonly", "yes"]
    ports: ["6379:6379"]
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 3s
      retries: 10

  kafka:
    image: apache/kafka:3.7.0
    ports: ["9092:9092"]
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_LISTENERS: PLAINTEXT://:19092,CONTROLLER://:9093,EXTERNAL://:9092
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:19092,EXTERNAL://localhost:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
    healthcheck:
      test: ["CMD-SHELL", "/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --list"]
      interval: 10s
      timeout: 10s
      retries: 10

  kafka-init:
    image: apache/kafka:3.7.0
    depends_on:
      kafka:
        condition: service_healthy
    entrypoint: ["/bin/sh", "-c"]
    command: >
      "/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092
      --create --if-not-exists --topic trigger-events --partitions 8 --replication-factor 1"

  temporal:
    image: temporalio/auto-setup:1.24.2
    depends_on:
      postgres:
        condition: service_healthy
    environment:
      DB: postgres12
      DB_PORT: "5432"
      POSTGRES_USER: fluxgate
      POSTGRES_PWD: fluxgate
      POSTGRES_SEEDS: postgres
    ports: ["7233:7233"]

  temporal-ui:
    image: temporalio/ui:2.26.2
    depends_on: [temporal]
    environment:
      TEMPORAL_ADDRESS: temporal:7233
    ports: ["8080:8080"]

  prometheus:
    image: prom/prometheus:v2.53.0
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml:ro
    ports: ["9090:9090"]

  grafana:
    image: grafana/grafana:11.1.0
    depends_on: [prometheus]
    environment:
      GF_AUTH_ANONYMOUS_ENABLED: "true"
      GF_AUTH_ANONYMOUS_ORG_ROLE: Admin
    volumes:
      - ./grafana/provisioning:/etc/grafana/provisioning:ro
    ports: ["3030:3000"]
```

- [ ] **Step 2: Write `prometheus.yml`**

```yaml
global:
  scrape_interval: 5s
scrape_configs:
  - job_name: trigger-service
    static_configs:
      - targets: ["host.docker.internal:3000"]
  - job_name: worker-service
    static_configs:
      - targets: ["host.docker.internal:3001"]
```

Note: on Linux add to both prometheus and grafana services in compose:
```yaml
    extra_hosts:
      - "host.docker.internal:host-gateway"
```

- [ ] **Step 3: Write `grafana/provisioning/datasources/datasource.yml`**

```yaml
apiVersion: 1
datasources:
  - name: Prometheus
    type: prometheus
    access: proxy
    url: http://prometheus:9090
    isDefault: true
```

- [ ] **Step 4: Verify the stack boots**

```bash
docker compose up -d
sleep 30
docker compose ps --format '{{.Name}} {{.Status}}'
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --list
```
Expected: all services Up (kafka-init Exited 0 is fine); topic list shows `trigger-events`. Temporal UI at http://localhost:8080, Grafana at http://localhost:3030.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: docker compose stack (postgres, redis, kafka kraft, temporal, prometheus, grafana)"
```

---

### Task 3: `packages/db` — Prisma schema + migration

**Files:**
- Create: `packages/db/package.json`, `packages/db/prisma/schema.prisma`, `packages/db/src/index.ts`, `packages/db/tsconfig.json`

- [ ] **Step 1: Write `packages/db/package.json`**

```json
{
  "name": "@fluxgate/db",
  "version": "0.0.1",
  "main": "dist/index.js",
  "types": "dist/index.d.ts",
  "scripts": {
    "build": "tsc",
    "generate": "prisma generate",
    "migrate:dev": "prisma migrate dev",
    "migrate:deploy": "prisma migrate deploy"
  },
  "dependencies": {
    "@prisma/client": "~5.14.0"
  },
  "devDependencies": {
    "prisma": "~5.14.0"
  }
}
```

`packages/db/tsconfig.json`:
```json
{
  "extends": "../../tsconfig.base.json",
  "include": ["src"]
}
```

- [ ] **Step 2: Write `packages/db/prisma/schema.prisma`**

```prisma
generator client {
  provider = "prisma-client-js"
}

datasource db {
  provider = "postgresql"
  url      = env("DATABASE_URL")
}

enum TriggerType {
  WEBHOOK
  CRON
}

enum TriggerStatus {
  ACTIVE
  PAUSED
}

enum ActionType {
  HTTP
}

enum ExecutionStatus {
  PENDING
  RUNNING
  COMPLETED
  FAILED
}

model Tenant {
  id         String        @id @default(uuid())
  name       String
  apiKeyHash String        @map("api_key_hash")
  status     TriggerStatus @default(ACTIVE)
  createdAt  DateTime      @default(now()) @map("created_at")
  triggers   Trigger[]

  @@map("tenants")
}

model Trigger {
  id        String        @id @default(uuid())
  tenantId  String        @map("tenant_id")
  name      String
  type      TriggerType
  config    Json
  status    TriggerStatus @default(ACTIVE)
  createdAt DateTime      @default(now()) @map("created_at")
  tenant    Tenant        @relation(fields: [tenantId], references: [id])
  actions   Action[]

  @@index([tenantId])
  @@map("triggers")
}

model Action {
  id        String     @id @default(uuid())
  triggerId String     @map("trigger_id")
  tenantId  String     @map("tenant_id")
  type      ActionType
  config    Json
  order     Int        @default(0)
  createdAt DateTime   @default(now()) @map("created_at")
  trigger   Trigger    @relation(fields: [triggerId], references: [id])

  @@index([triggerId])
  @@map("actions")
}

model Execution {
  id                 String          @id @default(uuid())
  triggerId          String          @map("trigger_id")
  tenantId           String          @map("tenant_id")
  dedupeKey          String          @unique @map("dedupe_key")
  temporalWorkflowId String          @map("temporal_workflow_id")
  temporalRunId      String?         @map("temporal_run_id")
  status             ExecutionStatus @default(PENDING)
  triggeredAt        DateTime        @default(now()) @map("triggered_at")
  scheduledFor       DateTime?       @map("scheduled_for")
  completedAt        DateTime?       @map("completed_at")
  failureReason      String?         @map("failure_reason")
  steps              ExecutionStep[]

  @@index([tenantId, triggeredAt])
  @@map("executions")
}

model ExecutionStep {
  id            String    @id @default(uuid())
  executionId   String    @map("execution_id")
  actionId      String    @map("action_id")
  order         Int
  status        String
  request       Json
  response      Json?
  durationMs    Int       @map("duration_ms")
  attemptNumber Int       @map("attempt_number")
  createdAt     DateTime  @default(now()) @map("created_at")
  execution     Execution @relation(fields: [executionId], references: [id])

  @@unique([executionId, actionId, attemptNumber])
  @@map("execution_steps")
}
```

- [ ] **Step 3: Write `packages/db/src/index.ts`**

```ts
import { PrismaClient } from '@prisma/client';

export * from '@prisma/client';

let client: PrismaClient | undefined;

export function getPrisma(): PrismaClient {
  if (!client) client = new PrismaClient();
  return client;
}
```

- [ ] **Step 4: Install, migrate, verify**

```bash
cp .env.example .env
pnpm install
cd packages/db && DATABASE_URL=postgresql://fluxgate:fluxgate@localhost:5432/fluxgate npx prisma migrate dev --name init && cd ../..
docker compose exec postgres psql -U fluxgate -c '\dt'
```
Expected: migration applied; tables `tenants, triggers, actions, executions, execution_steps, _prisma_migrations` listed.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: shared prisma schema and client in packages/db"
```

---

### Task 4: `packages/shared` — TriggerEvent, eventId helpers, zod config schemas

**Files:**
- Create: `packages/shared/package.json`, `packages/shared/tsconfig.json`, `packages/shared/src/index.ts`, `packages/shared/src/events.ts`, `packages/shared/src/schemas.ts`
- Test: `packages/shared/src/events.test.ts`, `packages/shared/src/schemas.test.ts`

- [ ] **Step 1: Package boilerplate**

`packages/shared/package.json`:
```json
{
  "name": "@fluxgate/shared",
  "version": "0.0.1",
  "main": "dist/index.js",
  "types": "dist/index.d.ts",
  "scripts": { "build": "tsc" },
  "dependencies": {
    "zod": "^3.23.0",
    "cron-parser": "^4.9.0"
  }
}
```

`packages/shared/tsconfig.json`:
```json
{
  "extends": "../../tsconfig.base.json",
  "include": ["src"],
  "exclude": ["src/**/*.test.ts"]
}
```

- [ ] **Step 2: Write the failing tests**

`packages/shared/src/events.test.ts`:
```ts
import { describe, it, expect } from 'vitest';
import { cronEventId, workflowIdFor, TOPIC_TRIGGER_EVENTS } from './events';

describe('eventId helpers', () => {
  it('cron eventId embeds triggerId and scheduled fire ms deterministically', () => {
    expect(cronEventId('trig-1', 1718200000000)).toBe('trig-1:1718200000000');
    // same inputs -> same id: this IS the cron dedup mechanism
    expect(cronEventId('trig-1', 1718200000000)).toBe(cronEventId('trig-1', 1718200000000));
  });

  it('workflowIdFor prefixes exec-', () => {
    expect(workflowIdFor('abc')).toBe('exec-abc');
  });

  it('topic constant', () => {
    expect(TOPIC_TRIGGER_EVENTS).toBe('trigger-events');
  });
});
```

`packages/shared/src/schemas.test.ts`:
```ts
import { describe, it, expect } from 'vitest';
import { cronTriggerConfigSchema, httpActionConfigSchema, triggerEventSchema } from './schemas';

describe('cronTriggerConfigSchema', () => {
  it('accepts a valid 5-field cron expression', () => {
    expect(cronTriggerConfigSchema.parse({ expression: '*/5 * * * *' })).toEqual({
      expression: '*/5 * * * *',
    });
  });

  it('rejects an invalid expression', () => {
    expect(() => cronTriggerConfigSchema.parse({ expression: 'not a cron' })).toThrow();
  });
});

describe('httpActionConfigSchema', () => {
  it('applies defaults', () => {
    const parsed = httpActionConfigSchema.parse({ url: 'https://example.com/hook' });
    expect(parsed.method).toBe('POST');
    expect(parsed.headers).toEqual({});
    expect(parsed.timeoutMs).toBe(10000);
  });

  it('rejects a non-url', () => {
    expect(() => httpActionConfigSchema.parse({ url: 'nope' })).toThrow();
  });
});

describe('triggerEventSchema', () => {
  it('round-trips a valid event', () => {
    const event = {
      eventId: 'e1',
      tenantId: 't1',
      triggerId: 'tr1',
      type: 'WEBHOOK',
      payload: { a: 1 },
      version: 1,
      firedAt: new Date().toISOString(),
    };
    expect(triggerEventSchema.parse(event)).toEqual(event);
  });
});
```

- [ ] **Step 3: Run tests, verify they fail**

```bash
pnpm install && pnpm vitest run packages/shared
```
Expected: FAIL — modules `./events`, `./schemas` not found.

- [ ] **Step 4: Implement**

`packages/shared/src/events.ts`:
```ts
import { randomUUID } from 'node:crypto';

export const TOPIC_TRIGGER_EVENTS = 'trigger-events';

export interface TriggerEvent {
  eventId: string;
  tenantId: string;
  triggerId: string;
  type: 'WEBHOOK' | 'CRON';
  payload: Record<string, unknown>;
  version: 1;
  firedAt: string; // ISO
  scheduledFor?: string; // ISO, cron only
}

export function webhookEventId(): string {
  return randomUUID();
}

export function cronEventId(triggerId: string, scheduledFireMs: number): string {
  return `${triggerId}:${scheduledFireMs}`;
}

export function workflowIdFor(eventId: string): string {
  return `exec-${eventId}`;
}
```

`packages/shared/src/schemas.ts`:
```ts
import { z } from 'zod';
import cronParser from 'cron-parser';

export const cronTriggerConfigSchema = z.object({
  expression: z.string().refine(
    (expr) => {
      try {
        cronParser.parseExpression(expr);
        return true;
      } catch {
        return false;
      }
    },
    { message: 'invalid cron expression' },
  ),
});

export const webhookTriggerConfigSchema = z.object({}).passthrough();

export const httpActionConfigSchema = z.object({
  url: z.string().url(),
  method: z.enum(['GET', 'POST', 'PUT', 'PATCH', 'DELETE']).default('POST'),
  headers: z.record(z.string()).default({}),
  bodyTemplate: z.record(z.unknown()).optional(),
  timeoutMs: z.number().int().positive().default(10000),
});

export const triggerEventSchema = z.object({
  eventId: z.string().min(1),
  tenantId: z.string().min(1),
  triggerId: z.string().min(1),
  type: z.enum(['WEBHOOK', 'CRON']),
  payload: z.record(z.unknown()),
  version: z.literal(1),
  firedAt: z.string().datetime(),
  scheduledFor: z.string().datetime().optional(),
});

export type CronTriggerConfig = z.infer<typeof cronTriggerConfigSchema>;
export type HttpActionConfig = z.infer<typeof httpActionConfigSchema>;
```

`packages/shared/src/index.ts`:
```ts
export * from './events';
export * from './schemas';
```

- [ ] **Step 5: Run tests, verify they pass**

```bash
pnpm vitest run packages/shared
```
Expected: PASS (8 tests).

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "feat: shared TriggerEvent type, eventId helpers, zod config schemas"
```

---

### Task 5: trigger-service skeleton + API-key auth + tenant registration

**Files:**
- Create: `services/trigger-service/package.json`, `services/trigger-service/tsconfig.json`, `services/trigger-service/src/api/app.ts`, `services/trigger-service/src/api/auth.ts`, `services/trigger-service/src/api/routes/tenants.ts`, `services/trigger-service/src/index.ts`
- Test: `services/trigger-service/src/api/auth.test.ts`

- [ ] **Step 1: Package boilerplate**

`services/trigger-service/package.json`:
```json
{
  "name": "@fluxgate/trigger-service",
  "version": "0.0.1",
  "scripts": {
    "build": "tsc",
    "dev": "tsx watch src/index.ts",
    "start": "node dist/index.js"
  },
  "dependencies": {
    "@fluxgate/db": "workspace:*",
    "@fluxgate/shared": "workspace:*",
    "express": "^4.19.0",
    "kafkajs": "^2.2.4",
    "ioredis": "^5.4.0",
    "prom-client": "^15.1.0",
    "zod": "^3.23.0",
    "cron-parser": "^4.9.0"
  },
  "devDependencies": {
    "@types/express": "^4.17.21",
    "tsx": "^4.10.0",
    "supertest": "^7.0.0",
    "@types/supertest": "^6.0.2"
  }
}
```

`services/trigger-service/tsconfig.json`:
```json
{
  "extends": "../../tsconfig.base.json",
  "include": ["src"],
  "exclude": ["src/**/*.test.ts"]
}
```

- [ ] **Step 2: Write the failing test for API-key hashing/auth helpers**

`services/trigger-service/src/api/auth.test.ts`:
```ts
import { describe, it, expect } from 'vitest';
import { generateApiKey, hashApiKey } from './auth';

describe('api keys', () => {
  it('generates a 64-char hex key (32 random bytes)', () => {
    const key = generateApiKey();
    expect(key).toMatch(/^[0-9a-f]{64}$/);
    expect(generateApiKey()).not.toBe(key);
  });

  it('hash is deterministic and not the key itself', () => {
    const key = generateApiKey();
    expect(hashApiKey(key)).toBe(hashApiKey(key));
    expect(hashApiKey(key)).not.toBe(key);
  });
});
```

- [ ] **Step 3: Run, verify fail**

```bash
pnpm install && pnpm vitest run services/trigger-service
```
Expected: FAIL — `./auth` not found.

- [ ] **Step 4: Implement auth + app + tenant route**

`services/trigger-service/src/api/auth.ts`:
```ts
import { createHash, randomBytes } from 'node:crypto';
import type { Request, Response, NextFunction } from 'express';
import { getPrisma } from '@fluxgate/db';

export function generateApiKey(): string {
  return randomBytes(32).toString('hex');
}

export function hashApiKey(key: string): string {
  return createHash('sha256').update(key).digest('hex');
}

// Resolves X-API-Key -> req.tenantId. 401 on missing/unknown key.
export async function apiKeyAuth(req: Request, res: Response, next: NextFunction) {
  const key = req.header('x-api-key');
  if (!key) return res.status(401).json({ error: 'missing X-API-Key header' });
  const tenant = await getPrisma().tenant.findFirst({
    where: { apiKeyHash: hashApiKey(key), status: 'ACTIVE' },
    select: { id: true },
  });
  if (!tenant) return res.status(401).json({ error: 'invalid API key' });
  (req as Request & { tenantId: string }).tenantId = tenant.id;
  next();
}

export function tenantIdOf(req: Request): string {
  return (req as Request & { tenantId: string }).tenantId;
}
```

`services/trigger-service/src/api/routes/tenants.ts`:
```ts
import { Router } from 'express';
import { z } from 'zod';
import { getPrisma } from '@fluxgate/db';
import { generateApiKey, hashApiKey } from '../auth';

export const tenantsRouter = Router();

const createTenantSchema = z.object({ name: z.string().min(1).max(200) });

// Unauthenticated: this is how a tenant gets its key. Key returned once, stored hashed.
tenantsRouter.post('/', async (req, res) => {
  const parsed = createTenantSchema.safeParse(req.body);
  if (!parsed.success) return res.status(400).json({ error: parsed.error.flatten() });
  const apiKey = generateApiKey();
  const tenant = await getPrisma().tenant.create({
    data: { name: parsed.data.name, apiKeyHash: hashApiKey(apiKey) },
  });
  res.status(201).json({ id: tenant.id, name: tenant.name, apiKey });
});
```

`services/trigger-service/src/api/app.ts`:
```ts
import express from 'express';
import { tenantsRouter } from './routes/tenants';

export function createApp() {
  const app = express();
  app.use(express.json({ limit: '256kb' }));
  app.get('/healthz', (_req, res) => res.json({ ok: true }));
  app.use('/tenants', tenantsRouter);
  return app;
}
```

`services/trigger-service/src/index.ts`:
```ts
import { createApp } from './api/app';

const port = Number(process.env.TRIGGER_SERVICE_PORT ?? 3000);
createApp().listen(port, () => console.log(`trigger-service listening on :${port}`));
```

- [ ] **Step 5: Run tests, verify pass; smoke the API**

```bash
pnpm vitest run services/trigger-service
pnpm --filter @fluxgate/trigger-service exec tsx src/index.ts &
sleep 2
curl -s -X POST localhost:3000/tenants -H 'content-type: application/json' -d '{"name":"acme"}'
kill %1
```
Expected: tests PASS; curl returns `{"id":"...","name":"acme","apiKey":"<64 hex chars>"}`.

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "feat: trigger-service skeleton, api-key auth, tenant registration"
```

---

### Task 6: Trigger + Action CRUD

**Files:**
- Create: `services/trigger-service/src/api/routes/triggers.ts`, `services/trigger-service/src/api/routes/actions.ts`
- Modify: `services/trigger-service/src/api/app.ts`
- Test: `tests/integration/crud.test.ts` (requires compose stack + service running)

- [ ] **Step 1: Implement triggers router**

`services/trigger-service/src/api/routes/triggers.ts`:
```ts
import { Router } from 'express';
import { z } from 'zod';
import { getPrisma } from '@fluxgate/db';
import { cronTriggerConfigSchema, webhookTriggerConfigSchema } from '@fluxgate/shared';
import { apiKeyAuth, tenantIdOf } from '../auth';
import { syncCronSchedule, removeCronSchedule } from '../../cron/schedule';

export const triggersRouter = Router();
triggersRouter.use(apiKeyAuth);

const createTriggerSchema = z.discriminatedUnion('type', [
  z.object({ name: z.string().min(1), type: z.literal('WEBHOOK'), config: webhookTriggerConfigSchema }),
  z.object({ name: z.string().min(1), type: z.literal('CRON'), config: cronTriggerConfigSchema }),
]);

triggersRouter.post('/', async (req, res) => {
  const parsed = createTriggerSchema.safeParse(req.body);
  if (!parsed.success) return res.status(400).json({ error: parsed.error.flatten() });
  const trigger = await getPrisma().trigger.create({
    data: { tenantId: tenantIdOf(req), ...parsed.data },
  });
  if (trigger.type === 'CRON') await syncCronSchedule(trigger.id, (trigger.config as { expression: string }).expression);
  res.status(201).json(trigger);
});

triggersRouter.get('/', async (req, res) => {
  res.json(await getPrisma().trigger.findMany({ where: { tenantId: tenantIdOf(req) } }));
});

triggersRouter.get('/:id', async (req, res) => {
  const trigger = await getPrisma().trigger.findFirst({
    where: { id: req.params.id, tenantId: tenantIdOf(req) },
    include: { actions: { orderBy: { order: 'asc' } } },
  });
  if (!trigger) return res.status(404).json({ error: 'not found' });
  res.json(trigger);
});

const patchTriggerSchema = z.object({
  name: z.string().min(1).optional(),
  status: z.enum(['ACTIVE', 'PAUSED']).optional(),
});

triggersRouter.patch('/:id', async (req, res) => {
  const parsed = patchTriggerSchema.safeParse(req.body);
  if (!parsed.success) return res.status(400).json({ error: parsed.error.flatten() });
  const existing = await getPrisma().trigger.findFirst({ where: { id: req.params.id, tenantId: tenantIdOf(req) } });
  if (!existing) return res.status(404).json({ error: 'not found' });
  const trigger = await getPrisma().trigger.update({ where: { id: existing.id }, data: parsed.data });
  if (trigger.type === 'CRON') {
    if (trigger.status === 'PAUSED') await removeCronSchedule(trigger.id);
    else await syncCronSchedule(trigger.id, (trigger.config as { expression: string }).expression);
  }
  res.json(trigger);
});

triggersRouter.delete('/:id', async (req, res) => {
  const existing = await getPrisma().trigger.findFirst({ where: { id: req.params.id, tenantId: tenantIdOf(req) } });
  if (!existing) return res.status(404).json({ error: 'not found' });
  await getPrisma().action.deleteMany({ where: { triggerId: existing.id } });
  await getPrisma().trigger.delete({ where: { id: existing.id } });
  if (existing.type === 'CRON') await removeCronSchedule(existing.id);
  res.status(204).end();
});
```

- [ ] **Step 2: Implement the cron schedule sync helpers it imports**

Create `services/trigger-service/src/cron/schedule.ts` (the poller comes in PR 3; CRUD sync is needed now):
```ts
import Redis from 'ioredis';
import cronParser from 'cron-parser';

export const CRON_ZSET = 'cron:triggers';

let redis: Redis | undefined;
export function getRedis(): Redis {
  if (!redis) redis = new Redis(process.env.REDIS_URL ?? 'redis://localhost:6379');
  return redis;
}

export function nextFireMs(expression: string, from: Date = new Date()): number {
  return cronParser.parseExpression(expression, { currentDate: from }).next().getTime();
}

export async function syncCronSchedule(triggerId: string, expression: string): Promise<void> {
  await getRedis().zadd(CRON_ZSET, nextFireMs(expression), triggerId);
}

export async function removeCronSchedule(triggerId: string): Promise<void> {
  await getRedis().zrem(CRON_ZSET, triggerId);
}
```

- [ ] **Step 3: Implement actions router**

`services/trigger-service/src/api/routes/actions.ts`:
```ts
import { Router } from 'express';
import { z } from 'zod';
import { getPrisma } from '@fluxgate/db';
import { httpActionConfigSchema } from '@fluxgate/shared';
import { apiKeyAuth, tenantIdOf } from '../auth';

// Mounted at /triggers/:triggerId/actions
export const actionsRouter = Router({ mergeParams: true });
actionsRouter.use(apiKeyAuth);

const createActionSchema = z.object({
  type: z.literal('HTTP'),
  config: httpActionConfigSchema,
  order: z.number().int().min(0).default(0),
});

actionsRouter.post('/', async (req, res) => {
  const trigger = await getPrisma().trigger.findFirst({
    where: { id: req.params.triggerId, tenantId: tenantIdOf(req) },
  });
  if (!trigger) return res.status(404).json({ error: 'trigger not found' });
  const parsed = createActionSchema.safeParse(req.body);
  if (!parsed.success) return res.status(400).json({ error: parsed.error.flatten() });
  const action = await getPrisma().action.create({
    data: { triggerId: trigger.id, tenantId: trigger.tenantId, ...parsed.data },
  });
  res.status(201).json(action);
});

actionsRouter.delete('/:actionId', async (req, res) => {
  const action = await getPrisma().action.findFirst({
    where: { id: req.params.actionId, triggerId: req.params.triggerId, tenantId: tenantIdOf(req) },
  });
  if (!action) return res.status(404).json({ error: 'not found' });
  await getPrisma().action.delete({ where: { id: action.id } });
  res.status(204).end();
});
```

- [ ] **Step 4: Mount routers in `app.ts`**

Modify `services/trigger-service/src/api/app.ts`:
```ts
import express from 'express';
import { tenantsRouter } from './routes/tenants';
import { triggersRouter } from './routes/triggers';
import { actionsRouter } from './routes/actions';

export function createApp() {
  const app = express();
  app.use(express.json({ limit: '256kb' }));
  app.get('/healthz', (_req, res) => res.json({ ok: true }));
  app.use('/tenants', tenantsRouter);
  app.use('/triggers', triggersRouter);
  app.use('/triggers/:triggerId/actions', actionsRouter);
  return app;
}
```

- [ ] **Step 5: Write the integration test**

`tests/integration/helpers.ts`:
```ts
const BASE = process.env.TRIGGER_URL ?? 'http://localhost:3000';

export async function api(path: string, init: RequestInit = {}, apiKey?: string) {
  const res = await fetch(`${BASE}${path}`, {
    ...init,
    headers: {
      'content-type': 'application/json',
      ...(apiKey ? { 'x-api-key': apiKey } : {}),
      ...(init.headers ?? {}),
    },
  });
  const text = await res.text();
  return { status: res.status, headers: res.headers, body: text ? JSON.parse(text) : null };
}

export async function createTenant(name: string) {
  const res = await api('/tenants', { method: 'POST', body: JSON.stringify({ name }) });
  if (res.status !== 201) throw new Error(`tenant create failed: ${res.status}`);
  return res.body as { id: string; apiKey: string };
}

export async function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

// Poll until fn returns truthy or timeout.
export async function waitFor<T>(fn: () => Promise<T | null | undefined | false>, timeoutMs = 30000, intervalMs = 500): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const result = await fn();
    if (result) return result;
    await sleep(intervalMs);
  }
  throw new Error('waitFor timed out');
}
```

`tests/integration/crud.test.ts`:
```ts
import { describe, it, expect } from 'vitest';
import { api, createTenant } from './helpers';

describe('tenant/trigger/action CRUD', () => {
  it('full lifecycle with tenant isolation', async () => {
    const tenantA = await createTenant('crud-a');
    const tenantB = await createTenant('crud-b');

    // unauthenticated trigger create -> 401
    expect((await api('/triggers', { method: 'POST', body: '{}' })).status).toBe(401);

    // create webhook trigger
    const trig = await api(
      '/triggers',
      { method: 'POST', body: JSON.stringify({ name: 'wh', type: 'WEBHOOK', config: {} }) },
      tenantA.apiKey,
    );
    expect(trig.status).toBe(201);

    // invalid cron config rejected at write time
    const badCron = await api(
      '/triggers',
      { method: 'POST', body: JSON.stringify({ name: 'bad', type: 'CRON', config: { expression: 'nope' } }) },
      tenantA.apiKey,
    );
    expect(badCron.status).toBe(400);

    // attach HTTP action
    const action = await api(
      `/triggers/${trig.body.id}/actions`,
      { method: 'POST', body: JSON.stringify({ type: 'HTTP', config: { url: 'https://example.com/x' } }) },
      tenantA.apiKey,
    );
    expect(action.status).toBe(201);
    expect(action.body.config.method).toBe('POST'); // zod default applied

    // tenant B cannot see tenant A's trigger
    expect((await api(`/triggers/${trig.body.id}`, {}, tenantB.apiKey)).status).toBe(404);

    // pause
    const paused = await api(
      `/triggers/${trig.body.id}`,
      { method: 'PATCH', body: JSON.stringify({ status: 'PAUSED' }) },
      tenantA.apiKey,
    );
    expect(paused.body.status).toBe('PAUSED');
  });
});
```

- [ ] **Step 6: Run integration test against the live stack**

```bash
docker compose up -d && sleep 10
pnpm --filter @fluxgate/trigger-service exec tsx src/index.ts &
sleep 3
pnpm vitest run tests/integration/crud.test.ts --project integration
kill %1
```
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add -A && git commit -m "feat: trigger and action CRUD with zod config validation and cron set sync"
```

---

### Task 7: Kafka producer + webhook ingestion endpoint

**Files:**
- Create: `services/trigger-service/src/kafka/producer.ts`, `services/trigger-service/src/api/routes/webhooks.ts`
- Modify: `services/trigger-service/src/api/app.ts`, `services/trigger-service/src/index.ts`

- [ ] **Step 1: Implement the producer**

`services/trigger-service/src/kafka/producer.ts`:
```ts
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
```

- [ ] **Step 2: Implement the webhook route**

`services/trigger-service/src/api/routes/webhooks.ts`:
```ts
import { Router } from 'express';
import { getPrisma } from '@fluxgate/db';
import { webhookEventId, type TriggerEvent } from '@fluxgate/shared';
import { apiKeyAuth, tenantIdOf } from '../auth';
import { produceTriggerEvent } from '../../kafka/producer';

export const webhooksRouter = Router();
webhooksRouter.use(apiKeyAuth);

// POST /webhooks/:triggerId — body is the event payload, forwarded verbatim.
webhooksRouter.post('/:triggerId', async (req, res) => {
  const trigger = await getPrisma().trigger.findFirst({
    where: { id: req.params.triggerId, tenantId: tenantIdOf(req) },
  });
  if (!trigger || trigger.type !== 'WEBHOOK') return res.status(404).json({ error: 'webhook trigger not found' });
  if (trigger.status === 'PAUSED') return res.status(409).json({ error: 'trigger is paused' });

  const event: TriggerEvent = {
    eventId: webhookEventId(),
    tenantId: trigger.tenantId,
    triggerId: trigger.id,
    type: 'WEBHOOK',
    payload: req.body ?? {},
    version: 1,
    firedAt: new Date().toISOString(),
  };

  try {
    await produceTriggerEvent(event);
  } catch (err) {
    // Spec §12: produce failure -> 503, nothing half-recorded; caller retries.
    console.error('kafka produce failed', err);
    return res.status(503).json({ error: 'event bus unavailable, retry' });
  }
  res.status(202).json({ eventId: event.eventId });
});
```

- [ ] **Step 3: Mount in app and verify end-to-end produce**

Add to `services/trigger-service/src/api/app.ts` (after the other routers):
```ts
import { webhooksRouter } from './routes/webhooks';
// inside createApp():
  app.use('/webhooks', webhooksRouter);
```

Smoke test:
```bash
docker compose up -d && sleep 10
pnpm --filter @fluxgate/trigger-service exec tsx src/index.ts &
sleep 3
KEY=$(curl -s -X POST localhost:3000/tenants -H 'content-type: application/json' -d '{"name":"smoke"}' | python3 -c 'import sys,json;print(json.load(sys.stdin)["apiKey"])')
TRIG=$(curl -s -X POST localhost:3000/triggers -H "x-api-key: $KEY" -H 'content-type: application/json' -d '{"name":"wh","type":"WEBHOOK","config":{}}' | python3 -c 'import sys,json;print(json.load(sys.stdin)["id"])')
curl -s -X POST localhost:3000/webhooks/$TRIG -H "x-api-key: $KEY" -H 'content-type: application/json' -d '{"hello":"world"}'
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:19092 --topic trigger-events --from-beginning --max-messages 1 --timeout-ms 10000
kill %1
```
Expected: webhook returns `{"eventId":"..."}` with status 202; console consumer prints the TriggerEvent JSON.

- [ ] **Step 4: Commit, push, open PR 1**

```bash
git add -A && git commit -m "feat: kafka producer and webhook ingestion endpoint"
git push -u origin pr1-foundation
gh pr create --title "PR1: compose stack, db schema, trigger-service CRUD + webhook->Kafka" --body "Spec §14 pr 1. Compose stack, Prisma schema in packages/db, shared types/schemas, tenant/trigger/action CRUD with API-key auth, webhook ingestion producing to Kafka keyed by tenantId."
```

---

# PR 2 — worker-service: consumer, Temporal workflow, first integration tests

### Task 8: worker-service skeleton + Temporal activities

**Files:**
- Create: `services/worker-service/package.json`, `services/worker-service/tsconfig.json`, `services/worker-service/src/temporal/activities/index.ts`
- Test: `services/worker-service/src/temporal/activities/template.test.ts`

- [ ] **Step 1: Create branch and package boilerplate**

```bash
git checkout main && git pull && git checkout -b pr2-worker
```

`services/worker-service/package.json`:
```json
{
  "name": "@fluxgate/worker-service",
  "version": "0.0.1",
  "scripts": {
    "build": "tsc",
    "dev": "tsx watch src/index.ts",
    "start": "node dist/index.js"
  },
  "dependencies": {
    "@fluxgate/db": "workspace:*",
    "@fluxgate/shared": "workspace:*",
    "@temporalio/client": "^1.10.0",
    "@temporalio/worker": "^1.10.0",
    "@temporalio/workflow": "^1.10.0",
    "@temporalio/activity": "^1.10.0",
    "@temporalio/common": "^1.10.0",
    "kafkajs": "^2.2.4",
    "ioredis": "^5.4.0",
    "express": "^4.19.0",
    "prom-client": "^15.1.0"
  },
  "devDependencies": {
    "@types/express": "^4.17.21",
    "tsx": "^4.10.0"
  }
}
```

`services/worker-service/tsconfig.json`:
```json
{
  "extends": "../../tsconfig.base.json",
  "include": ["src"],
  "exclude": ["src/**/*.test.ts"]
}
```

- [ ] **Step 2: Write the failing test for body templating (the one pure function in activities)**

`services/worker-service/src/temporal/activities/template.test.ts`:
```ts
import { describe, it, expect } from 'vitest';
import { renderBody } from './template';

describe('renderBody', () => {
  it('substitutes {{payload.x}} placeholders from the event payload', () => {
    const out = renderBody({ msg: 'hello {{payload.name}}' }, { name: 'akash' });
    expect(out).toEqual({ msg: 'hello akash' });
  });

  it('passes the raw payload through when no template is configured', () => {
    expect(renderBody(undefined, { a: 1 })).toEqual({ a: 1 });
  });

  it('leaves unknown placeholders empty', () => {
    expect(renderBody({ msg: '{{payload.missing}}' }, {})).toEqual({ msg: '' });
  });
});
```

- [ ] **Step 3: Run, verify fail**

```bash
pnpm install && pnpm vitest run services/worker-service
```
Expected: FAIL — `./template` not found.

- [ ] **Step 4: Implement template + activities**

`services/worker-service/src/temporal/activities/template.ts`:
```ts
// Renders an HTTP action body template against the event payload.
// Template values are strings with {{payload.<key>}} placeholders; one level deep is enough for MVP.
export function renderBody(
  template: Record<string, unknown> | undefined,
  payload: Record<string, unknown>,
): Record<string, unknown> {
  if (!template) return payload;
  const render = (v: unknown): unknown => {
    if (typeof v === 'string') {
      return v.replace(/\{\{payload\.([\w.]+)\}\}/g, (_m, key: string) => {
        const value = key.split('.').reduce<unknown>((acc, k) => (acc as Record<string, unknown>)?.[k], payload);
        return value === undefined || value === null ? '' : String(value);
      });
    }
    if (v && typeof v === 'object' && !Array.isArray(v)) return renderBody(v as Record<string, unknown>, payload);
    return v;
  };
  return Object.fromEntries(Object.entries(template).map(([k, v]) => [k, render(v)]));
}
```

`services/worker-service/src/temporal/activities/index.ts`:
```ts
import { Context } from '@temporalio/activity';
import { ApplicationFailure } from '@temporalio/common';
import { getPrisma, ExecutionStatus } from '@fluxgate/db';
import { httpActionConfigSchema, type TriggerEvent } from '@fluxgate/shared';
import { renderBody } from './template';
import { actionSuccessTotal, actionFailureTotal } from '../../metrics/registry';

// 1. INSERT executions row. Idempotent: dedupeKey unique + skipDuplicates,
//    then fetch — second guard behind Temporal workflowId dedup (spec §7).
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

  // Append-only step row, keyed by attemptNumber (spec §5). skipDuplicates makes
  // Temporal replay of a completed activity safe.
  await prisma.executionStep.createMany({
    data: {
      executionId,
      actionId: action.id,
      order: action.order,
      status: response?.ok ? 'COMPLETED' : 'FAILED',
      request,
      response: responseJson,
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
  status: Extract<ExecutionStatus, 'COMPLETED' | 'FAILED'>,
  failureReason?: string,
): Promise<void> {
  await getPrisma().execution.update({
    where: { id: executionId },
    data: { status, completedAt: new Date(), failureReason: failureReason ?? null },
  });
}
```

- [ ] **Step 5: Create the metrics registry it imports**

`services/worker-service/src/metrics/registry.ts`:
```ts
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
```

- [ ] **Step 6: Run tests, verify pass**

```bash
pnpm vitest run services/worker-service
```
Expected: PASS (3 template tests).

- [ ] **Step 7: Commit**

```bash
git add -A && git commit -m "feat: worker-service activities (record/executeHttp/finalize) and metrics registry"
```

---

### Task 9: actionWorkflow + Temporal worker

**Files:**
- Create: `services/worker-service/src/temporal/workflows/actionWorkflow.ts`, `services/worker-service/src/temporal/worker.ts`

- [ ] **Step 1: Implement the workflow**

`services/worker-service/src/temporal/workflows/actionWorkflow.ts`:
```ts
import { proxyActivities } from '@temporalio/workflow';
import type { TriggerEvent } from '@fluxgate/shared';
import type * as activities from '../activities';

const { recordExecution, finalizeExecution } = proxyActivities<typeof activities>({
  startToCloseTimeout: '30 seconds',
  retry: { maximumAttempts: 5 },
});

// Retry policy is per-activity (spec §10): 4xx is nonRetryable via
// NonRetryableHttpError; timeout/5xx retries with exponential backoff.
const { executeHttpAction } = proxyActivities<typeof activities>({
  startToCloseTimeout: '60 seconds',
  retry: {
    initialInterval: '1 second',
    backoffCoefficient: 2,
    maximumInterval: '30 seconds',
    maximumAttempts: Number(process.env.HTTP_ACTION_MAX_ATTEMPTS ?? 5),
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
```

- [ ] **Step 2: Implement the worker**

`services/worker-service/src/temporal/worker.ts`:
```ts
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
```

Note: `require.resolve` needs CJS. Set `"type": "commonjs"` implicitly (no `"type"` field in worker-service package.json) — Temporal's workflow bundler requires this layout; do not add `"type": "module"`.

- [ ] **Step 3: Typecheck**

```bash
pnpm --filter @fluxgate/worker-service exec tsc --noEmit
```
Expected: no errors.

- [ ] **Step 4: Commit**

```bash
git add -A && git commit -m "feat: actionWorkflow with per-activity retry policy and temporal worker"
```

---

### Task 10: Kafka consumer → Temporal dispatch

**Files:**
- Create: `services/worker-service/src/kafka/consumer.ts`, `services/worker-service/src/index.ts`, `services/worker-service/src/metrics/server.ts`

- [ ] **Step 1: Implement the consumer**

`services/worker-service/src/kafka/consumer.ts`:
```ts
import { Kafka } from 'kafkajs';
import { Client, Connection } from '@temporalio/client';
import { WorkflowIdReusePolicy } from '@temporalio/common';
import { TOPIC_TRIGGER_EVENTS, triggerEventSchema, workflowIdFor, type TriggerEvent } from '@fluxgate/shared';
import { TASK_QUEUE } from '../temporal/worker';
import { waitForDispatchToken } from '../ratelimit/dispatchBucket';
import { triggerLatency, observeLag } from '../metrics/instrumentation';

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
      observeLag(consumer, topic, partition, message.offset);
      const parsed = triggerEventSchema.safeParse(JSON.parse(message.value!.toString()));
      if (!parsed.success) {
        console.error('malformed TriggerEvent, skipping', parsed.error.flatten());
        return; // commit past it; nothing actionable
      }
      const event = parsed.data as TriggerEvent;

      // Layer-2 rate limit: bounded-iteration wait, unbounded total (spec §9).
      await waitForDispatchToken(event.tenantId, heartbeat);

      try {
        await temporal.workflow.start('actionWorkflow', {
          taskQueue: TASK_QUEUE,
          workflowId: workflowIdFor(event.eventId),
          workflowIdReusePolicy: WorkflowIdReusePolicy.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
          args: [event],
        });
        triggerLatency.observe(
          { tenantId: event.tenantId, triggerType: event.type },
          (Date.now() - new Date(event.firedAt).getTime()) / 1000,
        );
      } catch (err) {
        if ((err as { name?: string }).name === 'WorkflowExecutionAlreadyStartedError') {
          // Duplicate delivery (redelivery or cron double-fire) — dedup did its job (spec §7).
          return;
        }
        throw err; // kafkajs retries the batch; offset not committed
      }
    },
  });
}
```

- [ ] **Step 2: Stub the dispatch bucket (real Lua bucket arrives in PR 3, Task 14)**

`services/worker-service/src/ratelimit/dispatchBucket.ts`:
```ts
// PR2 placeholder: no dispatch limiting yet. PR3 Task 14 replaces the body
// with the Redis Lua token bucket; the signature is final.
export async function waitForDispatchToken(
  _tenantId: string,
  _heartbeat: () => Promise<void>,
): Promise<void> {
  return;
}
```

- [ ] **Step 3: Metrics instrumentation + HTTP server**

`services/worker-service/src/metrics/instrumentation.ts`:
```ts
import client from 'prom-client';
import type { Consumer } from 'kafkajs';
import { registry, consumerLag } from './registry';

export const triggerLatency = new client.Histogram({
  name: 'fluxgate_trigger_latency_seconds',
  help: 'Latency from trigger fire to workflow dispatch',
  labelNames: ['tenantId', 'triggerType'] as const,
  buckets: [0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10],
  registers: [registry],
});

// Approximate lag: high watermark is not exposed per-message by kafkajs,
// so track offset progression; full lag via admin API every 15s.
const lastOffsets = new Map<string, number>();
export function observeLag(_consumer: Consumer, topic: string, partition: number, offset: string): void {
  lastOffsets.set(`${topic}:${partition}`, Number(offset));
}

export async function startLagPoller(brokers: string[]): Promise<void> {
  const { Kafka } = await import('kafkajs');
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
```

`services/worker-service/src/metrics/server.ts`:
```ts
import express from 'express';
import { registry } from './registry';

export function startMetricsServer(port: number): void {
  const app = express();
  app.get('/metrics', async (_req, res) => {
    res.set('content-type', registry.contentType);
    res.end(await registry.metrics());
  });
  app.get('/healthz', (_req, res) => res.json({ ok: true }));
  app.listen(port, () => console.log(`worker metrics on :${port}`));
}
```

`services/worker-service/src/index.ts`:
```ts
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
```

- [ ] **Step 4: Manual e2e — curl webhook → Temporal UI → DB rows**

```bash
docker compose up -d && sleep 15
pnpm --filter @fluxgate/trigger-service exec tsx src/index.ts &
pnpm --filter @fluxgate/worker-service exec tsx src/index.ts &
sleep 5
# destination mock
python3 -m http.server 9999 &
KEY=$(curl -s -X POST localhost:3000/tenants -H 'content-type: application/json' -d '{"name":"e2e"}' | python3 -c 'import sys,json;print(json.load(sys.stdin)["apiKey"])')
TRIG=$(curl -s -X POST localhost:3000/triggers -H "x-api-key: $KEY" -H 'content-type: application/json' -d '{"name":"wh","type":"WEBHOOK","config":{}}' | python3 -c 'import sys,json;print(json.load(sys.stdin)["id"])')
curl -s -X POST localhost:3000/triggers/$TRIG/actions -H "x-api-key: $KEY" -H 'content-type: application/json' -d '{"type":"HTTP","config":{"url":"http://localhost:9999/","method":"GET"}}'
curl -s -X POST localhost:3000/webhooks/$TRIG -H "x-api-key: $KEY" -d '{"n":1}' -H 'content-type: application/json'
sleep 5
docker compose exec postgres psql -U fluxgate -c "SELECT status, dedupe_key FROM executions; SELECT status, attempt_number FROM execution_steps;"
kill %1 %2 %3
```
Expected: execution row `COMPLETED`, one step row `COMPLETED` attempt 1. Workflow `exec-<uuid>` visible at http://localhost:8080.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: kafka consumer dispatching temporal workflows with REJECT_DUPLICATE dedup"
```

---

### Task 11: Integration test infra (mock destination) + webhook e2e + dedup tests

**Files:**
- Create: `tests/integration/mockDestination.ts`, `tests/integration/webhook-e2e.test.ts`, `tests/integration/dedup.test.ts`
- Create: `tests/integration/db.ts`

- [ ] **Step 1: Mock destination server (records received calls, scriptable status codes)**

`tests/integration/mockDestination.ts`:
```ts
import { createServer, type Server } from 'node:http';

export interface ReceivedCall {
  method: string;
  url: string;
  body: string;
}

export interface MockDestination {
  port: number;
  received: ReceivedCall[];
  /** Next N responses use this status; then falls back to 200. */
  failNext(status: number, times: number): void;
  close(): Promise<void>;
}

export async function startMockDestination(port = 9999): Promise<MockDestination> {
  const received: ReceivedCall[] = [];
  let failStatus = 0;
  let failRemaining = 0;

  const server: Server = createServer((req, res) => {
    let body = '';
    req.on('data', (c) => (body += c));
    req.on('end', () => {
      received.push({ method: req.method!, url: req.url!, body });
      if (failRemaining > 0) {
        failRemaining--;
        res.writeHead(failStatus).end('forced failure');
      } else {
        res.writeHead(200, { 'content-type': 'application/json' }).end('{"ok":true}');
      }
    });
  });
  await new Promise<void>((resolve) => server.listen(port, resolve));
  return {
    port,
    received,
    failNext(status, times) {
      failStatus = status;
      failRemaining = times;
    },
    close: () => new Promise((resolve) => server.close(() => resolve())),
  };
}
```

`tests/integration/db.ts`:
```ts
import { PrismaClient } from '@prisma/client';

export const prisma = new PrismaClient({
  datasources: { db: { url: process.env.DATABASE_URL ?? 'postgresql://fluxgate:fluxgate@localhost:5432/fluxgate' } },
});
```

- [ ] **Step 2: Webhook e2e test (spec §13.1)**

`tests/integration/webhook-e2e.test.ts`:
```ts
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { api, createTenant, waitFor } from './helpers';
import { startMockDestination, type MockDestination } from './mockDestination';
import { prisma } from './db';

let dest: MockDestination;
beforeAll(async () => {
  dest = await startMockDestination(9999);
});
afterAll(async () => {
  await dest.close();
});

describe('webhook e2e', () => {
  it('POST webhook -> execution COMPLETED, step recorded, destination called', async () => {
    const tenant = await createTenant('e2e-webhook');
    const trig = await api(
      '/triggers',
      { method: 'POST', body: JSON.stringify({ name: 'wh', type: 'WEBHOOK', config: {} }) },
      tenant.apiKey,
    );
    await api(
      `/triggers/${trig.body.id}/actions`,
      {
        method: 'POST',
        body: JSON.stringify({
          type: 'HTTP',
          config: { url: 'http://localhost:9999/hook', bodyTemplate: { greeting: 'hi {{payload.name}}' } },
        }),
      },
      tenant.apiKey,
    );

    const fire = await api(`/webhooks/${trig.body.id}`, { method: 'POST', body: JSON.stringify({ name: 'akash' }) }, tenant.apiKey);
    expect(fire.status).toBe(202);

    const execution = await waitFor(async () => {
      const e = await prisma.execution.findUnique({
        where: { dedupeKey: fire.body.eventId },
        include: { steps: true },
      });
      return e?.status === 'COMPLETED' ? e : null;
    });

    expect(execution.steps).toHaveLength(1);
    expect(execution.steps[0].attemptNumber).toBe(1);
    expect(execution.temporalWorkflowId).toBe(`exec-${fire.body.eventId}`);
    const call = dest.received.find((c) => c.url === '/hook' && c.body.includes('hi akash'));
    expect(call).toBeDefined();

    // paused trigger -> 409, nothing enqueued (spec §12)
    await api(`/triggers/${trig.body.id}`, { method: 'PATCH', body: JSON.stringify({ status: 'PAUSED' }) }, tenant.apiKey);
    expect((await api(`/webhooks/${trig.body.id}`, { method: 'POST', body: '{}' }, tenant.apiKey)).status).toBe(409);

    // unknown trigger -> 404
    expect((await api('/webhooks/00000000-0000-0000-0000-000000000000', { method: 'POST', body: '{}' }, tenant.apiKey)).status).toBe(404);
  });
});
```

- [ ] **Step 3: Dedup test (spec §13.2) — same eventId twice → one execution**

`tests/integration/dedup.test.ts`:
```ts
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { Kafka } from 'kafkajs';
import { api, createTenant, waitFor, sleep } from './helpers';
import { startMockDestination, type MockDestination } from './mockDestination';
import { prisma } from './db';

let dest: MockDestination;
beforeAll(async () => {
  dest = await startMockDestination(9998);
});
afterAll(async () => {
  await dest.close();
});

describe('idempotency', () => {
  it('same eventId produced twice -> exactly one execution', async () => {
    const tenant = await createTenant('dedup');
    const trig = await api(
      '/triggers',
      { method: 'POST', body: JSON.stringify({ name: 'wh', type: 'WEBHOOK', config: {} }) },
      tenant.apiKey,
    );
    await api(
      `/triggers/${trig.body.id}/actions`,
      { method: 'POST', body: JSON.stringify({ type: 'HTTP', config: { url: 'http://localhost:9998/' } }) },
      tenant.apiKey,
    );

    // Bypass the API: produce the SAME event twice directly to Kafka,
    // simulating consumer redelivery (crash after dispatch, before offset commit).
    const eventId = `dup-test-${Math.random().toString(36).slice(2)}`;
    const event = {
      eventId,
      tenantId: tenant.id,
      triggerId: trig.body.id,
      type: 'WEBHOOK',
      payload: {},
      version: 1,
      firedAt: new Date().toISOString(),
    };
    const producer = new Kafka({ clientId: 'test', brokers: ['localhost:9092'] }).producer();
    await producer.connect();
    await producer.send({ topic: 'trigger-events', messages: [{ key: tenant.id, value: JSON.stringify(event) }] });
    await producer.send({ topic: 'trigger-events', messages: [{ key: tenant.id, value: JSON.stringify(event) }] });
    await producer.disconnect();

    await waitFor(async () => {
      const e = await prisma.execution.findUnique({ where: { dedupeKey: eventId } });
      return e?.status === 'COMPLETED' ? e : null;
    });
    await sleep(3000); // give a hypothetical duplicate time to appear

    const count = await prisma.execution.count({ where: { dedupeKey: eventId } });
    expect(count).toBe(1);
    expect(dest.received.length).toBe(1);
  });
});
```

- [ ] **Step 4: Run both against the live stack**

```bash
docker compose up -d && sleep 15
pnpm --filter @fluxgate/trigger-service exec tsx src/index.ts &
pnpm --filter @fluxgate/worker-service exec tsx src/index.ts &
sleep 5
pnpm vitest run tests/integration/webhook-e2e.test.ts tests/integration/dedup.test.ts --project integration
kill %1 %2
```
Expected: PASS.

- [ ] **Step 5: Commit, push, open PR 2**

```bash
git add -A && git commit -m "test: webhook e2e and dedup integration tests against real stack"
git push -u origin pr2-worker
gh pr create --title "PR2: worker-service consumer + ActionWorkflow + first integration tests" --body "Spec §14 pr 2. Kafka consumer with REJECT_DUPLICATE Temporal dispatch, actionWorkflow with three idempotent activities and per-activity retry policy, webhook e2e + dedup integration tests."
```

---

# PR 3 — Cron scheduler, two-layer rate limiting, observability, k6

### Task 12: Cron poller with Lua claim script

**Files:**
- Create: `services/trigger-service/src/cron/claim.lua.ts`, `services/trigger-service/src/cron/poller.ts`
- Modify: `services/trigger-service/src/index.ts`
- Test: `tests/integration/cron.test.ts`

- [ ] **Step 1: Create branch**

```bash
git checkout main && git pull && git checkout -b pr3-cron-ratelimit-obs
```

- [ ] **Step 2: Write the Lua claim/requeue script**

`services/trigger-service/src/cron/claim.lua.ts`:
```ts
// Atomic compare-and-set requeue (spec §8): only requeue if the member's score
// is still <= now. Score update (not ZREM+ZADD) means the member never leaves
// the set — no crash window where the schedule is lost. Returns 1 if THIS
// caller performed the requeue, 0 if another poller already did.
export const CLAIM_REQUEUE_LUA = `
local score = redis.call('ZSCORE', KEYS[1], ARGV[1])
if score and tonumber(score) <= tonumber(ARGV[2]) then
  redis.call('ZADD', KEYS[1], ARGV[3], ARGV[1])
  return 1
end
return 0
`;
```

- [ ] **Step 3: Implement the poller**

`services/trigger-service/src/cron/poller.ts`:
```ts
import { getPrisma } from '@fluxgate/db';
import { cronEventId, type TriggerEvent } from '@fluxgate/shared';
import { produceTriggerEvent } from '../kafka/producer';
import { getRedis, nextFireMs, CRON_ZSET, removeCronSchedule } from './schedule';
import { CLAIM_REQUEUE_LUA } from './claim.lua';

const POLL_INTERVAL_MS = Number(process.env.CRON_POLL_INTERVAL_MS ?? 500);

export function startCronPoller(): NodeJS.Timeout {
  const redis = getRedis();
  redis.defineCommand('claimRequeue', { numberOfKeys: 1, lua: CLAIM_REQUEUE_LUA });

  const tick = async () => {
    const now = Date.now();
    const due = await redis.zrangebyscore(CRON_ZSET, 0, now, 'WITHSCORES');
    for (let i = 0; i < due.length; i += 2) {
      const triggerId = due[i];
      const scheduledFireMs = Number(due[i + 1]);
      try {
        await fireCronTrigger(triggerId, scheduledFireMs, now);
      } catch (err) {
        // Produce failure: do NOT requeue — trigger stays due, next tick retries (spec §12).
        console.error(`cron fire failed for ${triggerId}, will retry next poll`, err);
      }
    }
  };
  return setInterval(() => void tick(), POLL_INTERVAL_MS);
}

async function fireCronTrigger(triggerId: string, scheduledFireMs: number, now: number): Promise<void> {
  const redis = getRedis() as ReturnType<typeof getRedis> & {
    claimRequeue(key: string, member: string, now: number, nextMs: number): Promise<number>;
  };
  const trigger = await getPrisma().trigger.findUnique({ where: { id: triggerId } });
  if (!trigger || trigger.status !== 'ACTIVE' || trigger.type !== 'CRON') {
    await removeCronSchedule(triggerId); // stale member; DB is source of truth
    return;
  }
  const expression = (trigger.config as { expression: string }).expression;

  // 1. Produce FIRST. eventId embeds scheduledFireMs, so a crash-between-produce-
  //    and-requeue re-produces the same eventId and Temporal dedup swallows it (spec §8).
  const event: TriggerEvent = {
    eventId: cronEventId(triggerId, scheduledFireMs),
    tenantId: trigger.tenantId,
    triggerId,
    type: 'CRON',
    payload: {},
    version: 1,
    firedAt: new Date(now).toISOString(),
    scheduledFor: new Date(scheduledFireMs).toISOString(),
  };
  await produceTriggerEvent(event);

  // 2. Atomic claim/requeue: only one concurrent poller advances the schedule.
  await redis.claimRequeue(CRON_ZSET, triggerId, now, nextFireMs(expression, new Date(now)));
}
```

Modify `services/trigger-service/src/index.ts`:
```ts
import { createApp } from './api/app';
import { startCronPoller } from './cron/poller';

const port = Number(process.env.TRIGGER_SERVICE_PORT ?? 3000);
createApp().listen(port, () => console.log(`trigger-service listening on :${port}`));
startCronPoller();
```

- [ ] **Step 4: Write the cron integration test (spec §13.3)**

`tests/integration/cron.test.ts`:
```ts
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import Redis from 'ioredis';
import { api, createTenant, waitFor, sleep } from './helpers';
import { startMockDestination, type MockDestination } from './mockDestination';
import { prisma } from './db';

let dest: MockDestination;
const redis = new Redis(process.env.REDIS_URL ?? 'redis://localhost:6379');
beforeAll(async () => {
  dest = await startMockDestination(9997);
});
afterAll(async () => {
  await dest.close();
  redis.disconnect();
});

describe('cron scheduler', () => {
  it('due trigger fires, requeues to next occurrence, execution has scheduledFor', async () => {
    const tenant = await createTenant('cron-t');
    const trig = await api(
      '/triggers',
      { method: 'POST', body: JSON.stringify({ name: 'every-min', type: 'CRON', config: { expression: '* * * * *' } }) },
      tenant.apiKey,
    );
    expect(trig.status).toBe(201);
    await api(
      `/triggers/${trig.body.id}/actions`,
      { method: 'POST', body: JSON.stringify({ type: 'HTTP', config: { url: 'http://localhost:9997/' } }) },
      tenant.apiKey,
    );

    // Member is in the set with a future score
    const scoreBefore = await redis.zscore('cron:triggers', trig.body.id);
    expect(scoreBefore).not.toBeNull();

    // Force it due NOW instead of waiting up to a minute
    await redis.zadd('cron:triggers', Date.now() - 1000, trig.body.id);

    const execution = await waitFor(async () => {
      const e = await prisma.execution.findFirst({
        where: { triggerId: trig.body.id, status: 'COMPLETED' },
      });
      return e ?? null;
    });
    expect(execution.scheduledFor).not.toBeNull();
    expect(execution.dedupeKey).toMatch(new RegExp(`^${trig.body.id}:\\d+$`));

    // Requeued to the future
    const scoreAfter = Number(await redis.zscore('cron:triggers', trig.body.id));
    expect(scoreAfter).toBeGreaterThan(Date.now());

    // Exactly one execution for that scheduled fire, even though the poller
    // ticked many times before the requeue landed (and any concurrent poller
    // would produce the same eventId).
    await sleep(2000);
    const count = await prisma.execution.count({ where: { triggerId: trig.body.id } });
    expect(count).toBe(1);

    // pause -> removed from set
    await api(`/triggers/${trig.body.id}`, { method: 'PATCH', body: JSON.stringify({ status: 'PAUSED' }) }, tenant.apiKey);
    expect(await redis.zscore('cron:triggers', trig.body.id)).toBeNull();
  });
});
```

- [ ] **Step 5: Run against live stack**

```bash
docker compose up -d && sleep 15
pnpm --filter @fluxgate/trigger-service exec tsx src/index.ts &
pnpm --filter @fluxgate/worker-service exec tsx src/index.ts &
sleep 5
pnpm vitest run tests/integration/cron.test.ts --project integration
kill %1 %2
```
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "feat: cron poller with atomic lua claim/requeue and crash-safe produce-first ordering"
```

---

### Task 13: Edge rate limiting (Layer 1 — 429 + Retry-After)

**Files:**
- Create: `packages/shared/src/tokenBucket.lua.ts`, `services/trigger-service/src/ratelimit/edgeBucket.ts`
- Modify: `packages/shared/src/index.ts`, `services/trigger-service/src/api/routes/webhooks.ts`
- Test: `tests/integration/ratelimit.test.ts` (edge part)

- [ ] **Step 1: Write the shared Lua token bucket (used by BOTH layers)**

`packages/shared/src/tokenBucket.lua.ts`:
```ts
// Atomic token bucket: read-refill-decrement in one Redis call (spec §9 —
// eliminates TOCTOU). Returns {allowed (0|1), retryAfterSeconds}.
// KEYS[1] bucket hash key
// ARGV[1] capacity, ARGV[2] refill tokens/sec, ARGV[3] now-ms
export const TOKEN_BUCKET_LUA = `
local capacity = tonumber(ARGV[1])
local rate = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local state = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(state[1])
local ts = tonumber(state[2])
if tokens == nil then
  tokens = capacity
  ts = now
end
local elapsed = math.max(0, now - ts) / 1000.0
tokens = math.min(capacity, tokens + elapsed * rate)
local allowed = 0
if tokens >= 1 then
  tokens = tokens - 1
  allowed = 1
end
redis.call('HSET', KEYS[1], 'tokens', tokens, 'ts', now)
redis.call('PEXPIRE', KEYS[1], math.ceil(capacity / rate * 2000))
local retryAfter = 0
if allowed == 0 then
  retryAfter = math.ceil((1 - tokens) / rate)
end
return {allowed, retryAfter}
`;

export interface BucketParams {
  capacity: number;
  refillPerSec: number;
}
```

Add to `packages/shared/src/index.ts`:
```ts
export * from './tokenBucket.lua';
```

- [ ] **Step 2: Edge bucket middleware**

`services/trigger-service/src/ratelimit/edgeBucket.ts`:
```ts
import type { Request, Response, NextFunction } from 'express';
import { TOKEN_BUCKET_LUA, type BucketParams } from '@fluxgate/shared';
import { getRedis } from '../cron/schedule';
import { ratelimitRejectedTotal } from '../metrics/registry';
import { tenantIdOf } from '../api/auth';

const DEFAULTS: BucketParams = {
  capacity: Number(process.env.EDGE_BUCKET_CAPACITY ?? 20),
  refillPerSec: Number(process.env.EDGE_BUCKET_REFILL_PER_SEC ?? 10),
};

let defined = false;
function redisWithBucket() {
  const redis = getRedis() as ReturnType<typeof getRedis> & {
    tokenBucket(key: string, capacity: number, rate: number, nowMs: number): Promise<[number, number]>;
  };
  if (!defined) {
    redis.defineCommand('tokenBucket', { numberOfKeys: 1, lua: TOKEN_BUCKET_LUA });
    defined = true;
  }
  return redis;
}

// Layer 1: empty bucket -> 429 + Retry-After. Fails open if Redis is down (spec §12).
export async function edgeRateLimit(req: Request, res: Response, next: NextFunction) {
  const tenantId = tenantIdOf(req);
  try {
    const [allowed, retryAfter] = await redisWithBucket().tokenBucket(
      `ratelimit:edge:${tenantId}`,
      DEFAULTS.capacity,
      DEFAULTS.refillPerSec,
      Date.now(),
    );
    if (allowed === 1) return next();
    ratelimitRejectedTotal.inc({ tenantId, layer: 'edge' });
    res.set('Retry-After', String(Math.max(1, retryAfter)));
    return res.status(429).json({ error: 'rate limit exceeded' });
  } catch (err) {
    console.warn('rate limiter unavailable, failing open', err);
    return next();
  }
}
```

- [ ] **Step 3: Create trigger-service metrics registry + /metrics (needed by the middleware)**

`services/trigger-service/src/metrics/registry.ts`:
```ts
import client from 'prom-client';

export const registry = new client.Registry();
client.collectDefaultMetrics({ register: registry });

export const ratelimitRejectedTotal = new client.Counter({
  name: 'fluxgate_ratelimit_rejected_total',
  help: 'Rate limit rejections/waits by layer',
  labelNames: ['tenantId', 'layer'] as const,
  registers: [registry],
});
```

Add to `services/trigger-service/src/api/app.ts` inside `createApp()`:
```ts
import { registry } from '../metrics/registry';
// ...
  app.get('/metrics', async (_req, res) => {
    res.set('content-type', registry.contentType);
    res.end(await registry.metrics());
  });
```

- [ ] **Step 4: Apply middleware to the webhook route**

In `services/trigger-service/src/api/routes/webhooks.ts`, change the router setup:
```ts
import { edgeRateLimit } from '../../ratelimit/edgeBucket';

export const webhooksRouter = Router();
webhooksRouter.use(apiKeyAuth);
webhooksRouter.use(edgeRateLimit);
```

- [ ] **Step 5: Commit (integration test for both layers lands in Task 14)**

```bash
git add -A && git commit -m "feat: edge token-bucket rate limit (lua, 429 + Retry-After, fail-open)"
```

---

### Task 14: Dispatch rate limiting (Layer 2 — bounded-iteration wait in consumer)

**Files:**
- Modify: `services/worker-service/src/ratelimit/dispatchBucket.ts` (replace PR2 stub)
- Test: `tests/integration/ratelimit.test.ts`

- [ ] **Step 1: Replace the stub with the real bucket**

`services/worker-service/src/ratelimit/dispatchBucket.ts`:
```ts
import Redis from 'ioredis';
import { TOKEN_BUCKET_LUA } from '@fluxgate/shared';
import { ratelimitRejectedTotal } from '../metrics/registry';

const CAPACITY = Number(process.env.DISPATCH_BUCKET_CAPACITY ?? 10);
const REFILL_PER_SEC = Number(process.env.DISPATCH_BUCKET_REFILL_PER_SEC ?? 5);
const POLL_MS = 200;

type RedisWithBucket = Redis & {
  tokenBucket(key: string, capacity: number, rate: number, nowMs: number): Promise<[number, number]>;
};

let redis: RedisWithBucket | undefined;
function getRedis(): RedisWithBucket {
  if (!redis) {
    redis = new Redis(process.env.REDIS_URL ?? 'redis://localhost:6379') as RedisWithBucket;
    redis.defineCommand('tokenBucket', { numberOfKeys: 1, lua: TOKEN_BUCKET_LUA });
  }
  return redis;
}

// Layer 2 (spec §9): wait in-process until a token is available. Unbounded by
// design — events are never dropped or reordered; kafkajs heartbeat() on each
// iteration keeps the consumer in the group. Fails open if Redis is down.
export async function waitForDispatchToken(
  tenantId: string,
  heartbeat: () => Promise<void>,
): Promise<void> {
  let waited = false;
  for (;;) {
    let allowed: number;
    try {
      [allowed] = await getRedis().tokenBucket(
        `ratelimit:dispatch:${tenantId}`,
        CAPACITY,
        REFILL_PER_SEC,
        Date.now(),
      );
    } catch (err) {
      console.warn('dispatch limiter unavailable, failing open', err);
      return;
    }
    if (allowed === 1) return;
    if (!waited) {
      waited = true;
      ratelimitRejectedTotal.inc({ tenantId, layer: 'dispatch' });
    }
    await heartbeat();
    await new Promise((r) => setTimeout(r, POLL_MS));
  }
}
```

- [ ] **Step 2: Write the rate-limit integration test (spec §13.4)**

`tests/integration/ratelimit.test.ts`:
```ts
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { api, createTenant, waitFor } from './helpers';
import { startMockDestination, type MockDestination } from './mockDestination';
import { prisma } from './db';

let dest: MockDestination;
beforeAll(async () => {
  dest = await startMockDestination(9996);
});
afterAll(async () => {
  await dest.close();
});

async function setupTenantWithWebhook(name: string) {
  const tenant = await createTenant(name);
  const trig = await api(
    '/triggers',
    { method: 'POST', body: JSON.stringify({ name: 'wh', type: 'WEBHOOK', config: {} }) },
    tenant.apiKey,
  );
  await api(
    `/triggers/${trig.body.id}/actions`,
    { method: 'POST', body: JSON.stringify({ type: 'HTTP', config: { url: 'http://localhost:9996/' } }) },
    tenant.apiKey,
  );
  return { tenant, triggerId: trig.body.id as string };
}

describe('two-layer rate limiting', () => {
  it('edge: flooding tenant gets 429 + Retry-After; other tenant unaffected', async () => {
    const hot = await setupTenantWithWebhook('rl-hot');
    const cold = await setupTenantWithWebhook('rl-cold');

    // Edge capacity is 20 (default): fire 60 fast, expect a mix of 202 and 429.
    const results = await Promise.all(
      Array.from({ length: 60 }, () =>
        api(`/webhooks/${hot.triggerId}`, { method: 'POST', body: '{}' }, hot.tenant.apiKey),
      ),
    );
    const accepted = results.filter((r) => r.status === 202);
    const rejected = results.filter((r) => r.status === 429);
    expect(rejected.length).toBeGreaterThan(0);
    expect(accepted.length).toBeGreaterThan(0);
    expect(rejected[0].headers.get('retry-after')).toMatch(/^\d+$/);

    // Cold tenant sails through while hot is throttled.
    const coldRes = await api(`/webhooks/${cold.triggerId}`, { method: 'POST', body: '{}' }, cold.tenant.apiKey);
    expect(coldRes.status).toBe(202);

    // Dispatch layer: accepted events all complete eventually (delayed, never dropped),
    // and per-tenant order is preserved structurally (same partition key).
    await waitFor(async () => {
      const done = await prisma.execution.count({
        where: { tenantId: hot.tenant.id, status: 'COMPLETED' },
      });
      return done === accepted.length ? done : null;
    }, 120_000);

    const coldDone = await waitFor(async () => {
      const e = await prisma.execution.findFirst({ where: { tenantId: cold.tenant.id, status: 'COMPLETED' } });
      return e ?? null;
    });
    expect(coldDone).toBeDefined();
  });
});
```

- [ ] **Step 3: Run against live stack**

```bash
docker compose up -d && sleep 15
pnpm --filter @fluxgate/trigger-service exec tsx src/index.ts &
pnpm --filter @fluxgate/worker-service exec tsx src/index.ts &
sleep 5
pnpm vitest run tests/integration/ratelimit.test.ts --project integration
kill %1 %2
```
Expected: PASS (allow ~2 min; dispatch bucket drains the hot tenant's backlog at refill rate).

- [ ] **Step 4: Commit**

```bash
git add -A && git commit -m "feat: dispatch token bucket with heartbeat-keepalive wait (layer 2)"
```

---

### Task 15: Failure-path integration tests (spec §13.5)

**Files:**
- Create: `tests/integration/failures.test.ts`

- [ ] **Step 1: Write the test**

`tests/integration/failures.test.ts`:
```ts
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { api, createTenant, waitFor } from './helpers';
import { startMockDestination, type MockDestination } from './mockDestination';
import { prisma } from './db';

let dest: MockDestination;
beforeAll(async () => {
  dest = await startMockDestination(9995);
});
afterAll(async () => {
  await dest.close();
});

async function fireWebhook(name: string) {
  const tenant = await createTenant(name);
  const trig = await api(
    '/triggers',
    { method: 'POST', body: JSON.stringify({ name: 'wh', type: 'WEBHOOK', config: {} }) },
    tenant.apiKey,
  );
  await api(
    `/triggers/${trig.body.id}/actions`,
    { method: 'POST', body: JSON.stringify({ type: 'HTTP', config: { url: 'http://localhost:9995/' } }) },
    tenant.apiKey,
  );
  const fire = await api(`/webhooks/${trig.body.id}`, { method: 'POST', body: '{}' }, tenant.apiKey);
  return fire.body.eventId as string;
}

describe('failure paths', () => {
  it('destination 4xx -> FAILED immediately, exactly one attempt, no retries', async () => {
    dest.failNext(422, 100); // every call 4xx
    const eventId = await fireWebhook('fail-4xx');
    const execution = await waitFor(async () => {
      const e = await prisma.execution.findUnique({ where: { dedupeKey: eventId }, include: { steps: true } });
      return e?.status === 'FAILED' ? e : null;
    });
    expect(execution.steps).toHaveLength(1); // nonRetryable: attempt 1 only
    expect(execution.failureReason).toContain('422');
    dest.failNext(200, 0);
  });

  it('destination 5xx twice then 200 -> retries with backoff, COMPLETED, full attempt history', async () => {
    dest.failNext(503, 2);
    const eventId = await fireWebhook('fail-5xx');
    const execution = await waitFor(async () => {
      const e = await prisma.execution.findUnique({ where: { dedupeKey: eventId }, include: { steps: true } });
      return e?.status === 'COMPLETED' ? e : null;
    }, 60_000);
    // Append-only retry history: attempts 1,2 FAILED, attempt 3 COMPLETED.
    expect(execution.steps.map((s) => s.attemptNumber).sort()).toEqual([1, 2, 3]);
    expect(execution.steps.filter((s) => s.status === 'FAILED')).toHaveLength(2);
  });
});
```

- [ ] **Step 2: Run against live stack**

```bash
docker compose up -d && sleep 15
pnpm --filter @fluxgate/trigger-service exec tsx src/index.ts &
pnpm --filter @fluxgate/worker-service exec tsx src/index.ts &
sleep 5
pnpm vitest run tests/integration/failures.test.ts --project integration
kill %1 %2
```
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add -A && git commit -m "test: failure-path integration tests (4xx non-retryable, 5xx retry-then-complete)"
```

---

### Task 16: Grafana dashboard provisioning

**Files:**
- Create: `grafana/provisioning/dashboards/dashboards.yml`, `grafana/provisioning/dashboards/fluxgate.json`

- [ ] **Step 1: Dashboard provider config**

`grafana/provisioning/dashboards/dashboards.yml`:
```yaml
apiVersion: 1
providers:
  - name: fluxgate
    folder: ""
    type: file
    options:
      path: /etc/grafana/provisioning/dashboards
```

- [ ] **Step 2: Dashboard JSON (spec §11 panels)**

`grafana/provisioning/dashboards/fluxgate.json`:
```json
{
  "title": "Fluxgate",
  "uid": "fluxgate-mvp",
  "schemaVersion": 39,
  "refresh": "5s",
  "time": { "from": "now-15m", "to": "now" },
  "panels": [
    {
      "id": 1, "title": "Trigger latency p50/p99", "type": "timeseries",
      "gridPos": { "x": 0, "y": 0, "w": 12, "h": 8 },
      "targets": [
        { "expr": "histogram_quantile(0.50, sum(rate(fluxgate_trigger_latency_seconds_bucket[1m])) by (le))", "legendFormat": "p50" },
        { "expr": "histogram_quantile(0.99, sum(rate(fluxgate_trigger_latency_seconds_bucket[1m])) by (le))", "legendFormat": "p99" }
      ]
    },
    {
      "id": 2, "title": "Consumer lag per partition", "type": "timeseries",
      "gridPos": { "x": 12, "y": 0, "w": 12, "h": 8 },
      "targets": [
        { "expr": "fluxgate_kafka_consumer_lag", "legendFormat": "partition {{partition}}" }
      ]
    },
    {
      "id": 3, "title": "Workflow duration p50/p99", "type": "timeseries",
      "gridPos": { "x": 0, "y": 8, "w": 12, "h": 8 },
      "targets": [
        { "expr": "histogram_quantile(0.50, sum(rate(fluxgate_temporal_workflow_duration_seconds_bucket[1m])) by (le))", "legendFormat": "p50" },
        { "expr": "histogram_quantile(0.99, sum(rate(fluxgate_temporal_workflow_duration_seconds_bucket[1m])) by (le))", "legendFormat": "p99" }
      ]
    },
    {
      "id": 4, "title": "Per-tenant action success rate", "type": "timeseries",
      "gridPos": { "x": 12, "y": 8, "w": 12, "h": 8 },
      "targets": [
        { "expr": "sum(rate(fluxgate_action_success_total[1m])) by (tenantId) / (sum(rate(fluxgate_action_success_total[1m])) by (tenantId) + sum(rate(fluxgate_action_failure_total[1m])) by (tenantId))", "legendFormat": "{{tenantId}}" }
      ]
    },
    {
      "id": 5, "title": "Rate-limit rejections by layer", "type": "timeseries",
      "gridPos": { "x": 0, "y": 16, "w": 24, "h": 8 },
      "targets": [
        { "expr": "sum(rate(fluxgate_ratelimit_rejected_total[1m])) by (layer, tenantId)", "legendFormat": "{{layer}} {{tenantId}}" }
      ]
    }
  ]
}
```

- [ ] **Step 3: Wire workflow duration metric into the consumer**

`fluxgate_temporal_workflow_duration_seconds` needs an observer. Add to `services/worker-service/src/metrics/instrumentation.ts`:
```ts
import { workflowDuration } from './registry';
import { prismaWorkflowDurationPoller } from './workflowDurationPoller';
```
Create `services/worker-service/src/metrics/workflowDurationPoller.ts` — observe from the executions table (source of truth, survives restarts):
```ts
import { getPrisma } from '@fluxgate/db';
import { workflowDuration } from './registry';

let lastSeen = new Date(0);

// Every 10s, observe durations of executions completed since last poll.
export function startWorkflowDurationPoller(): void {
  setInterval(async () => {
    try {
      const done = await getPrisma().execution.findMany({
        where: { completedAt: { gt: lastSeen }, status: { in: ['COMPLETED', 'FAILED'] } },
        select: { triggeredAt: true, completedAt: true },
        orderBy: { completedAt: 'asc' },
        take: 1000,
      });
      for (const e of done) {
        workflowDuration.observe(
          { workflowType: 'actionWorkflow' },
          (e.completedAt!.getTime() - e.triggeredAt.getTime()) / 1000,
        );
        lastSeen = e.completedAt!;
      }
    } catch (err) {
      console.error('workflow duration poll failed', err);
    }
  }, 10_000).unref();
}
```
And call it from `services/worker-service/src/index.ts` `main()`:
```ts
import { startWorkflowDurationPoller } from './metrics/workflowDurationPoller';
// in main(), after startMetricsServer:
  startWorkflowDurationPoller();
```
(Remove the unused `prismaWorkflowDurationPoller` import line above — the real import is `startWorkflowDurationPoller` in index.ts only.)

- [ ] **Step 4: Verify provisioning**

```bash
docker compose up -d --force-recreate grafana prometheus
sleep 10
curl -s localhost:3030/api/dashboards/uid/fluxgate-mvp | python3 -c 'import sys,json;print(json.load(sys.stdin)["dashboard"]["title"])'
curl -s localhost:9090/api/v1/targets | python3 -c 'import sys,json;print([t["health"] for t in json.load(sys.stdin)["data"]["activeTargets"]])'
```
Expected: `Fluxgate`; both Prometheus targets `up` when services are running.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: provisioned grafana dashboard and workflow-duration observer"
```

---

### Task 17: k6 load harness + measured numbers

**Files:**
- Create: `load/webhook-load.js`, `load/setup.sh`, `load/report.sql`, `load/README.md`

- [ ] **Step 1: Setup script — creates N tenants + webhook triggers, emits a manifest**

`load/setup.sh`:
```bash
#!/usr/bin/env bash
# Creates N tenants each with a WEBHOOK trigger + HTTP action -> mock destination.
# Emits load/manifest.json consumed by k6.
set -euo pipefail
N="${1:-10}"
BASE="${TRIGGER_URL:-http://localhost:3000}"
DEST="${DEST_URL:-http://host.docker.internal:9994/}"
echo '[]' > load/manifest.json
for i in $(seq 1 "$N"); do
  TENANT=$(curl -sf -X POST "$BASE/tenants" -H 'content-type: application/json' -d "{\"name\":\"load-$i\"}")
  KEY=$(echo "$TENANT" | python3 -c 'import sys,json;print(json.load(sys.stdin)["apiKey"])')
  TRIG=$(curl -sf -X POST "$BASE/triggers" -H "x-api-key: $KEY" -H 'content-type: application/json' \
    -d '{"name":"load-wh","type":"WEBHOOK","config":{}}' | python3 -c 'import sys,json;print(json.load(sys.stdin)["id"])')
  curl -sf -X POST "$BASE/triggers/$TRIG/actions" -H "x-api-key: $KEY" -H 'content-type: application/json' \
    -d "{\"type\":\"HTTP\",\"config\":{\"url\":\"$DEST\",\"method\":\"GET\"}}" > /dev/null
  python3 - "$KEY" "$TRIG" <<'EOF'
import json, sys
m = json.load(open('load/manifest.json'))
m.append({"apiKey": sys.argv[1], "triggerId": sys.argv[2]})
json.dump(m, open('load/manifest.json', 'w'))
EOF
done
echo "wrote load/manifest.json with $N tenants"
```

- [ ] **Step 2: k6 script — ramping webhook load across tenants**

`load/webhook-load.js`:
```js
import http from 'k6/http';
import { check } from 'k6';
import { SharedArray } from 'k6/data';
import { Counter } from 'k6/metrics';

const tenants = new SharedArray('tenants', () => JSON.parse(open('./manifest.json')));
const accepted = new Counter('webhooks_accepted');
const ratelimited = new Counter('webhooks_429');

export const options = {
  scenarios: {
    ramp: {
      executor: 'ramping-arrival-rate',
      startRate: 10,
      timeUnit: '1s',
      preAllocatedVUs: 100,
      stages: [
        { target: 50, duration: '1m' },
        { target: 200, duration: '2m' },
        { target: 200, duration: '2m' },
      ],
    },
  },
  thresholds: {
    http_req_duration: ['p(99)<500'], // webhook ACCEPT latency (not e2e)
  },
};

const BASE = __ENV.TRIGGER_URL || 'http://localhost:3000';

export default function () {
  const t = tenants[Math.floor(Math.random() * tenants.length)];
  const res = http.post(
    `${BASE}/webhooks/${t.triggerId}`,
    JSON.stringify({ n: Date.now() }),
    { headers: { 'content-type': 'application/json', 'x-api-key': t.apiKey } },
  );
  if (res.status === 202) accepted.add(1);
  if (res.status === 429) ratelimited.add(1);
  check(res, { 'accepted or throttled': (r) => r.status === 202 || r.status === 429 });
}
```

- [ ] **Step 3: Resume-numbers SQL (e2e latency + cron on-time, from executions)**

`load/report.sql`:
```sql
-- End-to-end p50/p99: webhook accept (triggered_at) -> execution complete.
SELECT
  count(*) AS completed,
  percentile_cont(0.50) WITHIN GROUP (ORDER BY extract(epoch FROM completed_at - triggered_at)) AS p50_s,
  percentile_cont(0.99) WITHIN GROUP (ORDER BY extract(epoch FROM completed_at - triggered_at)) AS p99_s
FROM executions
WHERE status = 'COMPLETED' AND scheduled_for IS NULL
  AND triggered_at > now() - interval '30 minutes';

-- Cron on-time percentage: fired within 1s of schedule (spec §8 target >= 98%).
SELECT
  count(*) AS cron_fires,
  round(100.0 * count(*) FILTER (WHERE triggered_at - scheduled_for <= interval '1 second') / nullif(count(*), 0), 2)
    AS on_time_pct
FROM executions
WHERE scheduled_for IS NOT NULL AND triggered_at > now() - interval '30 minutes';

-- Sustained throughput: completed executions per minute, last 10 minutes.
SELECT date_trunc('minute', completed_at) AS minute, count(*) AS executions_per_min
FROM executions
WHERE status = 'COMPLETED' AND completed_at > now() - interval '10 minutes'
GROUP BY 1 ORDER BY 1;

-- Rate-limit fairness: per-tenant completion counts (hot tenant capped, others unaffected).
SELECT tenant_id, count(*) FILTER (WHERE status = 'COMPLETED') AS completed
FROM executions
WHERE triggered_at > now() - interval '30 minutes'
GROUP BY tenant_id ORDER BY completed DESC;
```

`load/README.md`:
```markdown
# Load harness

1. Stack + services up, mock destination listening:
   `docker compose up -d`, run both services, then `python3 -m http.server 9994 &`
2. `bash load/setup.sh 10` — 10 synthetic tenants
3. `k6 run load/webhook-load.js`
4. Numbers: `docker compose exec postgres psql -U fluxgate -f - < load/report.sql`
   plus the Grafana dashboard (http://localhost:3030, uid fluxgate-mvp).

Resume bullets come from: sustained triggers/min, e2e p50/p99, cron on-time %, and the
fairness table (hot tenant throttled, others unaffected).
```

- [ ] **Step 4: Run the harness once, capture numbers**

```bash
docker compose up -d && sleep 15
pnpm --filter @fluxgate/trigger-service exec tsx src/index.ts &
pnpm --filter @fluxgate/worker-service exec tsx src/index.ts &
python3 -m http.server 9994 &
sleep 5
bash load/setup.sh 10
k6 run load/webhook-load.js
docker compose exec -T postgres psql -U fluxgate < load/report.sql
kill %1 %2 %3
```
Expected: k6 completes with threshold pass; report.sql prints completed counts, p50/p99, throughput per minute. Record the numbers in `load/README.md` under a "## First measured run" heading.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: k6 load harness, setup script, and resume-numbers report queries"
```

---

### Task 18: Full verification + PR 3

- [ ] **Step 1: Run everything**

```bash
docker compose up -d && sleep 15
pnpm vitest run --project unit
pnpm --filter @fluxgate/trigger-service exec tsx src/index.ts &
pnpm --filter @fluxgate/worker-service exec tsx src/index.ts &
sleep 5
pnpm vitest run --project integration
kill %1 %2
pnpm -r build
```
Expected: all unit tests pass, all 5 integration suites pass, all packages compile.

- [ ] **Step 2: Push and open PR 3**

```bash
git push -u origin pr3-cron-ratelimit-obs
gh pr create --title "PR3: cron scheduler, two-layer rate limiting, observability, k6 harness" --body "Spec §14 pr 3. Sorted-set cron poller with atomic Lua claim/requeue, edge (429) + dispatch (heartbeat wait) token buckets, Prometheus metrics + provisioned Grafana dashboard, cron/rate-limit/failure integration tests, k6 harness with first measured numbers."
```

---

## Self-review notes (already applied)

- **Spec coverage:** §3 layout → Tasks 1–17 create exactly those directories (no `proto/`, no `EVENT` enum value, no email/Slack). §5 schema → Task 3 (append-only steps enforced by `@@unique` + `skipDuplicates`). §6 → Task 7 (key=tenantId, 8 partitions in kafka-init). §7 → Tasks 9–11 (REJECT_DUPLICATE + dedupeKey second guard, tested). §8 → Task 12 (produce-first ordering, Lua CAS, stale-member cleanup, CRUD sync from Task 6). §9 → Tasks 13–14 (shared Lua bucket, fail-open per §12, heartbeat wait). §10 → Tasks 8–9 (per-activity retry, nonRetryable 4xx). §11 → Tasks 8, 10, 13, 16 (all five metrics + dashboard). §12 → produce-failure 503 (Task 7), cron retry-next-poll (Task 12), fail-open limiters (Tasks 13–14). §13 → Tasks 6, 11, 12, 14, 15. §14 → PR boundaries match.
- **Known simplification:** spec §13.3 "two concurrent pollers" is tested implicitly (the Lua CAS + same-eventId dedup are exercised by the poller ticking every 500ms against a due trigger far older than one tick, plus the dedup test); running a literal second trigger-service instance during the cron test is a worthwhile manual check at Task 12 Step 5 — start a second `tsx src/index.ts` with `TRIGGER_SERVICE_PORT=3010` and assert the execution count stays 1.
- **Type consistency:** `waitForDispatchToken(tenantId, heartbeat)` signature fixed in PR2 stub and PR3 implementation; `TriggerEvent` fields match `triggerEventSchema`; `cronEventId`/`workflowIdFor` used consistently by poller, consumer, and tests.
