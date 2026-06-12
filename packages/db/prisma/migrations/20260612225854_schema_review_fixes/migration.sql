/*
  Warnings:

  - The `status` column on the `tenants` table would be dropped and recreated. This will lead to data loss if there is data in the column.
  - Changed the type of `status` on the `execution_steps` table. No cast exists, the column would be dropped and recreated, which cannot be done if there is data, since the column is required.

*/
-- CreateEnum
CREATE TYPE "TenantStatus" AS ENUM ('ACTIVE', 'PAUSED');

-- CreateEnum
CREATE TYPE "StepStatus" AS ENUM ('COMPLETED', 'FAILED');

-- DropForeignKey
ALTER TABLE "actions" DROP CONSTRAINT "actions_trigger_id_fkey";

-- DropForeignKey
ALTER TABLE "execution_steps" DROP CONSTRAINT "execution_steps_execution_id_fkey";

-- AlterTable
ALTER TABLE "execution_steps" DROP COLUMN "status",
ADD COLUMN     "status" "StepStatus" NOT NULL;

-- AlterTable
ALTER TABLE "tenants" DROP COLUMN "status",
ADD COLUMN     "status" "TenantStatus" NOT NULL DEFAULT 'ACTIVE';

-- AddForeignKey
ALTER TABLE "actions" ADD CONSTRAINT "actions_trigger_id_fkey" FOREIGN KEY ("trigger_id") REFERENCES "triggers"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "execution_steps" ADD CONSTRAINT "execution_steps_execution_id_fkey" FOREIGN KEY ("execution_id") REFERENCES "executions"("id") ON DELETE CASCADE ON UPDATE CASCADE;
