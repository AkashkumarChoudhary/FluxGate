import { Router, type IRouter } from 'express';
import { z } from 'zod';
import { getPrisma } from '@fluxgate/db';
import { generateApiKey, hashApiKey } from '../auth';

export const tenantsRouter: IRouter = Router();

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
