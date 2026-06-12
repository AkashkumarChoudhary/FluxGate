const BASE = process.env.TRIGGER_URL ?? 'http://localhost:3100';

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
