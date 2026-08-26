import type { Job } from '../../../domain/types/job';

const FLOW_METADATA_KEYS = [
  '__parentId',
  '__parentQueue',
  '__childrenIds',
  '__flowParentId',
  '__flowParentIds',
] as const;

function dataRecord(value: unknown): Record<string, unknown> | null {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : null;
}

/** Merge user data while retaining the flow metadata owned by the broker. */
export function updatedPostgresJobData(job: Job, data: unknown): unknown {
  const current = dataRecord(job.data);
  const metadata = current
    ? Object.fromEntries(
        FLOW_METADATA_KEYS.filter((key) => Object.hasOwn(current, key)).map((key) => [
          key,
          current[key],
        ])
      )
    : {};
  if (Object.keys(metadata).length === 0) return data;
  const next = dataRecord(data);
  if (!next) throw new Error('flow job data must be an object');
  for (const key of Object.keys(next)) {
    if (key.startsWith('__')) throw new Error(`flow job data key is reserved: ${key}`);
  }
  return { ...next, ...metadata };
}
