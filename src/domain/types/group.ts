export interface GroupRateLimit {
  readonly max: number;
  readonly duration: number;
}

/** Worker-side defaults enforced authoritatively by the broker. */
export interface GroupPullOptions {
  readonly concurrency?: number;
  readonly limit?: GroupRateLimit;
  /** Internal batch affinity: undefined means any group; null means ungrouped only. */
  readonly affinity?: string | null;
}

export interface GroupRateLimitOverride extends GroupRateLimit {}

export const MAX_GROUP_PRIORITY = 2_097_151;

/** Validate the canonical string form shared by every broker and persistence backend. */
export function validateGroupId(value: unknown, required = false): string | null {
  if (value === undefined) return required ? 'groupId is required' : null;
  if (typeof value !== 'string') return 'groupId must be a string';
  if (value.length === 0) return 'groupId must be a non-empty string';
  if (value.length > 256) return 'groupId must be at most 256 characters';
  if (value.includes('\0')) return 'groupId must not contain NUL characters';
  return null;
}

export function assertGroupId(value: unknown): asserts value is string {
  const error = validateGroupId(value, true);
  if (error) throw new Error(error);
}

export function assertOptionalGroupId(value: unknown): asserts value is string | undefined {
  const error = validateGroupId(value);
  if (error) throw new Error(error);
}

export function validateGroupPriority(value: unknown): string | null {
  if (value === undefined) return null;
  if (typeof value !== 'number' || !Number.isFinite(value)) {
    return 'group.priority must be a finite number';
  }
  if (!Number.isInteger(value)) return 'group.priority must be an integer';
  if (value < 0 || value > MAX_GROUP_PRIORITY) {
    return `group.priority must be between 0 and ${MAX_GROUP_PRIORITY}`;
  }
  return null;
}

export function assertGroupPriority(value: unknown): void {
  const error = validateGroupPriority(value);
  if (error) throw new Error(error);
}

/** Normalize BullMQ-compatible numeric IDs without accepting arbitrary coercion. */
export function normalizeGroupId(value: unknown): string {
  if (typeof value !== 'string' && typeof value !== 'number') {
    throw new Error('groupId must be a string or number');
  }
  if (typeof value === 'number' && !Number.isSafeInteger(value)) {
    throw new Error('numeric groupId must be a safe integer');
  }
  const groupId = String(value);
  assertGroupId(groupId);
  return groupId;
}

export function validatePositiveSafeInteger(value: unknown, name: string): string | null {
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value <= 0) {
    return `${name} must be a positive safe integer`;
  }
  return null;
}

export function assertPositiveSafeInteger(value: unknown, name: string): asserts value is number {
  const error = validatePositiveSafeInteger(value, name);
  if (error) throw new Error(error);
}

export function validateGroupPullOptions(options: unknown): string | null {
  if (options === undefined) return null;
  if (!options || typeof options !== 'object' || Array.isArray(options)) {
    return 'group must be an object';
  }
  const group = options as Record<string, unknown>;
  if (
    group.affinity !== undefined &&
    group.affinity !== null &&
    typeof group.affinity !== 'string'
  ) {
    return 'group.affinity must be a string or null';
  }
  if (group.concurrency !== undefined) {
    const error = validatePositiveSafeInteger(group.concurrency, 'group.concurrency');
    if (error) return error;
  }
  if (group.limit !== undefined) {
    if (!group.limit || typeof group.limit !== 'object' || Array.isArray(group.limit)) {
      return 'group.limit must be an object';
    }
    const limit = group.limit as Record<string, unknown>;
    const maxError = validatePositiveSafeInteger(limit.max, 'group.limit.max');
    if (maxError) return maxError;
    const durationError = validatePositiveSafeInteger(limit.duration, 'group.limit.duration');
    if (durationError) return durationError;
  }
  return null;
}

export function assertGroupPullOptions(options: unknown): asserts options is GroupPullOptions {
  const error = validateGroupPullOptions(options);
  if (error) throw new Error(error);
}
