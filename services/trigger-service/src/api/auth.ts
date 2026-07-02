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
