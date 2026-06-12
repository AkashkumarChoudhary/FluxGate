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
