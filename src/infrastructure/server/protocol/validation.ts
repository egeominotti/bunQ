import { validateGroupPriority } from '../../../domain/types/group';

export function validateQueueName(name: string): string | null {
  if (!name || name.length === 0) return 'Queue name is required';
  if (name.length > 256) return 'Queue name too long (max 256 characters)';
  if (!/^[a-zA-Z0-9_\-.:]+$/.test(name)) return 'Queue name contains invalid characters';
  return null;
}

export { validateGroupId } from '../../../domain/types/group';

export function validateJobData(data: unknown): string | null {
  let json: string | undefined;
  try {
    json = JSON.stringify(data);
  } catch {
    return 'Job data must be JSON serializable';
  }
  if (json !== undefined && json.length > 10 * 1024 * 1024) {
    return 'Job data too large (max 10MB)';
  }
  return null;
}

export function validateNumericField(
  value: unknown,
  name: string,
  options: { min?: number; max?: number; required?: boolean } = {}
): string | null {
  const { min = 0, max = Number.MAX_SAFE_INTEGER, required = false } = options;
  if (value === undefined || value === null) return required ? `${name} is required` : null;
  if (typeof value !== 'number') return `${name} must be a number`;
  if (!Number.isFinite(value)) return `${name} must be a finite number`;
  if (
    !Number.isInteger(value) &&
    (name === 'priority' || name === 'attempts' || name === 'maxAttempts')
  ) {
    return `${name} must be an integer`;
  }
  if (value < min) return `${name} must be at least ${min}`;
  if (value > max) return `${name} must be at most ${max}`;
  return null;
}

export function validateBackoffField(value: unknown): string | null {
  if (value === undefined || value === null) return null;
  if (typeof value === 'object') {
    const object = value as Record<string, unknown>;
    if (object['type'] !== 'fixed' && object['type'] !== 'exponential') {
      return "backoff.type must be 'fixed' or 'exponential'";
    }
    return validateNumericField(object['delay'], 'backoff.delay', {
      min: 0,
      max: 24 * 60 * 60 * 1000,
      required: true,
    });
  }
  return validateNumericField(value, 'backoff', { min: 0, max: 24 * 60 * 60 * 1000 });
}

export function validateJobOptions(options: Record<string, unknown>): string | null {
  const validations = [
    options['groupId'] === undefined
      ? validateNumericField(options['priority'], 'priority', { min: -1000000, max: 1000000 })
      : validateGroupPriority(options['priority']),
    validateNumericField(options['delay'], 'delay', { min: 0, max: 365 * 24 * 60 * 60 * 1000 }),
    validateNumericField(options['timeout'], 'timeout', { min: 0, max: 24 * 60 * 60 * 1000 }),
    validateNumericField(options['maxAttempts'], 'maxAttempts', { min: 1, max: 1000 }),
    validateBackoffField(options['backoff']),
    validateNumericField(options['ttl'], 'ttl', { min: 0, max: 365 * 24 * 60 * 60 * 1000 }),
    validateNumericField(options['stallTimeout'], 'stallTimeout', {
      min: 0,
      max: 24 * 60 * 60 * 1000,
    }),
  ];
  for (const error of validations) {
    if (error) return error;
  }
  return null;
}
