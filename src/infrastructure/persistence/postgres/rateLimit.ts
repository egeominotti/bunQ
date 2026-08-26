export interface NormalizedPostgresRateLimit {
  readonly durationMs: number | null;
  readonly ttlMs: number | null;
}

function positiveFinite(value: number | null | undefined): number | null {
  return Number.isFinite(value) && (value as number) > 0 ? (value as number) : null;
}

/** Match the in-memory limiter's handling of optional duration and TTL windows. */
export function normalizePostgresRateLimit(
  durationMs: number | null | undefined,
  ttlMs: number | null | undefined
): NormalizedPostgresRateLimit {
  return {
    durationMs: positiveFinite(durationMs),
    ttlMs: positiveFinite(ttlMs),
  };
}
