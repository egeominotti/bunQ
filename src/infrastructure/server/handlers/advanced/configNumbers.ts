export function toFiniteNumber(value: unknown): number | undefined {
  if (value === undefined || value === null) return undefined;
  const number = typeof value === 'number' ? value : Number(value);
  return Number.isFinite(number) ? number : undefined;
}

export function sanitizeConfigNumbers<T extends Record<string, unknown>>(
  config: T,
  numericKeys: readonly string[]
): T {
  if (!config || typeof config !== 'object') return config;
  const numeric = new Set<string>(numericKeys);
  const output: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(config)) {
    if (!numeric.has(key)) {
      output[key] = value;
      continue;
    }
    if (value === null) {
      output[key] = null;
      continue;
    }
    const number = toFiniteNumber(value);
    if (number !== undefined) output[key] = number;
  }
  return output as T;
}
