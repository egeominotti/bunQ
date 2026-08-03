interface AckEntry {
  readonly id: string;
}

function record(value: unknown): Record<string, unknown> | null {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : null;
}

function validatedIndices(value: unknown, size: number): number[] {
  if (!Array.isArray(value)) throw new Error('Invalid ACKB ignoredIndices response');
  const seen = new Set<number>();
  for (const index of value) {
    if (!Number.isInteger(index) || index < 0 || index >= size || seen.has(index)) {
      throw new Error('Invalid ACKB ignoredIndices response');
    }
    seen.add(index);
  }
  return [...seen];
}

function validatedIds(value: unknown): string[] {
  if (!Array.isArray(value) || value.some((id) => typeof id !== 'string')) {
    throw new Error('Invalid ACKB ignoredIds response');
  }
  return value as string[];
}

/** Parse structured ACKB evidence while preserving duplicate job-ID positions. */
export function ignoredAckIndices(data: unknown, batch: readonly AckEntry[]): ReadonlySet<number> {
  if (data === undefined) return new Set();
  const payload = record(data);
  if (!payload) throw new Error('Invalid ACKB response data');
  const hasIndices = Object.hasOwn(payload, 'ignoredIndices');
  const hasIds = Object.hasOwn(payload, 'ignoredIds');
  if (!hasIndices) throw new Error('Invalid ACKB ignoredIndices response');

  const ids = hasIds ? validatedIds(payload.ignoredIds) : null;
  const indices = validatedIndices(payload.ignoredIndices, batch.length);
  if (ids) {
    if (ids.length !== indices.length) throw new Error('Mismatched ACKB ignored evidence');
    for (let offset = 0; offset < indices.length; offset++) {
      if (batch[indices[offset]]?.id !== ids[offset]) {
        throw new Error('Mismatched ACKB ignored evidence');
      }
    }
  }
  return new Set(indices);
}

/** Parse a single ACK/FAIL response; historical applied responses have no data. */
export function terminalOutcomeWasApplied(data: unknown): boolean {
  if (data === undefined) return true;
  const payload = record(data);
  if (payload?.applied === false && payload.reason === 'already-finalized') return false;
  throw new Error('Invalid terminal outcome response data');
}
