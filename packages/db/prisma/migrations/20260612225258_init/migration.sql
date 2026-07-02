-- CreateEnum
CREATE TYPE "TriggerType" AS ENUM ('WEBHOOK', 'CRON');

-- CreateEnum
CREATE TYPE "TriggerStatus" AS ENUM ('ACTIVE', 'PAUSED');

-- CreateEnum
CREATE TYPE "ActionType" AS ENUM ('HTTP');

-- CreateEnum
CREATE TYPE "ExecutionStatus" AS ENUM ('PENDING', 'RUNNING', 'COMPLETED', 'FAILED');

-- CreateTable
CREATE TABLE "tenants" (
    "id" TEXT NOT NULL,
    "name" TEXT NOT NULL,
    "api_key_hash" TEXT NOT NULL,
    "status" "TriggerStatus" NOT NULL DEFAULT 'ACTIVE',
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "tenants_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "triggers" (
    "id" TEXT NOT NULL,
    "tenant_id" TEXT NOT NULL,
    "name" TEXT NOT NULL,
    "type" "TriggerType" NOT NULL,
    "config" JSONB NOT NULL,
    "status" "TriggerStatus" NOT NULL DEFAULT 'ACTIVE',
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "triggers_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "actions" (
    "id" TEXT NOT NULL,
    "trigger_id" TEXT NOT NULL,
    "tenant_id" TEXT NOT NULL,
    "type" "ActionType" NOT NULL,
    "config" JSONB NOT NULL,
    "order" INTEGER NOT NULL DEFAULT 0,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "actions_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "executions" (
    "id" TEXT NOT NULL,
    "trigger_id" TEXT NOT NULL,
    "tenant_id" TEXT NOT NULL,
    "dedupe_key" TEXT NOT NULL,
    "temporal_workflow_id" TEXT NOT NULL,
    "temporal_run_id" TEXT,
    "status" "ExecutionStatus" NOT NULL DEFAULT 'PENDING',
    "triggered_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "scheduled_for" TIMESTAMP(3),
    "completed_at" TIMESTAMP(3),
    "failure_reason" TEXT,

    CONSTRAINT "executions_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "execution_steps" (
    "id" TEXT NOT NULL,
    "execution_id" TEXT NOT NULL,
    "action_id" TEXT NOT NULL,
    "order" INTEGER NOT NULL,
    "status" TEXT NOT NULL,
    "request" JSONB NOT NULL,
    "response" JSONB,
    "duration_ms" INTEGER NOT NULL,
    "attempt_number" INTEGER NOT NULL,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "execution_steps_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE INDEX "triggers_tenant_id_idx" ON "triggers"("tenant_id");

-- CreateIndex
CREATE INDEX "actions_trigger_id_idx" ON "actions"("trigger_id");

-- CreateIndex
CREATE UNIQUE INDEX "executions_dedupe_key_key" ON "executions"("dedupe_key");

-- CreateIndex
CREATE INDEX "executions_tenant_id_triggered_at_idx" ON "executions"("tenant_id", "triggered_at");

-- CreateIndex
CREATE UNIQUE INDEX "execution_steps_execution_id_action_id_attempt_number_key" ON "execution_steps"("execution_id", "action_id", "attempt_number");

-- AddForeignKey
ALTER TABLE "triggers" ADD CONSTRAINT "triggers_tenant_id_fkey" FOREIGN KEY ("tenant_id") REFERENCES "tenants"("id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "actions" ADD CONSTRAINT "actions_trigger_id_fkey" FOREIGN KEY ("trigger_id") REFERENCES "triggers"("id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "execution_steps" ADD CONSTRAINT "execution_steps_execution_id_fkey" FOREIGN KEY ("execution_id") REFERENCES "executions"("id") ON DELETE RESTRICT ON UPDATE CASCADE;
