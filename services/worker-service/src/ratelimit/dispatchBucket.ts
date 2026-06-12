// PR2 placeholder: no dispatch limiting yet. PR3 replaces the body with the
// Redis Lua token bucket; the signature is final.
export async function waitForDispatchToken(
  _tenantId: string,
  _heartbeat: () => Promise<void>,
): Promise<void> {
  return;
}
