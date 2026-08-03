const DEFAULT_JOB_NAME = 'default';
const MAX_JOB_NAME_LENGTH = 256;

export interface JobPayloadSource {
  readonly name?: unknown;
  readonly data: unknown;
}

export interface NormalizedJobPayload {
  readonly name: string;
  readonly data: unknown;
  readonly legacy: boolean;
}

function legacyEnvelope(data: unknown): { name: string; data: unknown } | null {
  if (typeof data !== 'object' || data === null || Array.isArray(data)) return null;
  const record = data as Record<string, unknown>;
  if (typeof record.name !== 'string') return null;
  const { name, ...userData } = record;
  return { name, data: userData };
}

/** Return the stable public validation error for an invalid job name. */
export function validateJobName(name: unknown): string | null {
  if (typeof name !== 'string' || name.length === 0 || name.length > MAX_JOB_NAME_LENGTH) {
    return 'Job name must be a non-empty string of at most 256 characters';
  }
  return null;
}

function checkedName(name: unknown): string {
  const error = validateJobName(name);
  if (error) throw new Error(error);
  return name as string;
}

/**
 * Resolve a modern payload. A missing explicit name defaults without inspecting
 * `data`, so direct QueueManager callers can use `data.name` as an ordinary key.
 */
export function normalizeJobPayload(
  source: JobPayloadSource,
  fallback = DEFAULT_JOB_NAME
): NormalizedJobPayload {
  if (source.name !== undefined) {
    return { name: checkedName(source.name), data: source.data, legacy: false };
  }
  return { name: checkedName(fallback), data: source.data, legacy: false };
}

/** Decode a pre-2.8.56 wire or persistence envelope at a known legacy boundary. */
export function normalizeLegacyJobPayload(
  source: JobPayloadSource,
  fallback = DEFAULT_JOB_NAME
): NormalizedJobPayload {
  if (source.name !== undefined) return normalizeJobPayload(source, fallback);
  const legacy = legacyEnvelope(source.data);
  if (legacy) return { name: checkedName(legacy.name), data: legacy.data, legacy: true };
  return { name: checkedName(fallback), data: source.data, legacy: false };
}
