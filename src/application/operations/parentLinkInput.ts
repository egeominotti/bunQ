import type { JobInput } from '../../domain/types/job';

function inputData(input: JobInput): Record<string, unknown> | null {
  return input.data && typeof input.data === 'object' && !Array.isArray(input.data)
    ? (input.data as Record<string, unknown>)
    : null;
}

/**
 * Queue.add parent options carry the parent queue in reserved metadata. A bare
 * parentId remains the legacy flow forward-reference contract and is linked
 * later when the parent declares the edge.
 */
export function parentLinkQueue(input: JobInput): string | null {
  if (!input.parentId) return null;
  const data = inputData(input);
  if (!data || String(data.__parentId) !== String(input.parentId)) return null;
  return typeof data.__parentQueue === 'string' && data.__parentQueue.length > 0
    ? data.__parentQueue
    : null;
}

export function isParentLinkInput(input: JobInput): boolean {
  return parentLinkQueue(input) !== null;
}
