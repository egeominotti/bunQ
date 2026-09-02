import type { AtomicFlowBatchInput } from '../../domain/types/flow';
import { validateGroupId, validateGroupPriority } from '../../domain/types/group';
import { isWellFormedJobId, normalizeJobPayload, type JobInput } from '../../domain/types/job';
import { validateFlowTopology } from './flowTopologyValidation';

const MAX_FLOW_JOBS = 10_000;
const MAX_JOB_DATA_BYTES = 10 * 1024 * 1024;
const MAX_FLOW_DATA_BYTES = 64 * 1024 * 1024;
const QUEUE_RE = /^[a-zA-Z0-9_\-.:]+$/;
const BOOLEAN_OPTIONS = [
  'lifo',
  'removeOnComplete',
  'removeOnFail',
  'durable',
  'failParentOnFailure',
  'removeDependencyOnFailure',
  'continueParentOnFailure',
  'ignoreDependencyOnFailure',
] as const;

function numeric(value: unknown, name: string, min: number, max: number, integer = false): void {
  if (value === undefined || value === null) return;
  if (typeof value !== 'number' || !Number.isFinite(value)) {
    throw new Error(`${name} must be a finite number`);
  }
  if (integer && !Number.isInteger(value)) throw new Error(`${name} must be an integer`);
  if (value < min || value > max) {
    throw new Error(`${name} must be between ${min} and ${max}`);
  }
}

function validateOptions(input: JobInput): void {
  if (input.groupId === undefined) {
    numeric(input.priority, 'priority', -1_000_000, 1_000_000, true);
  } else {
    const priorityError = validateGroupPriority(input.priority);
    if (priorityError) throw new Error(priorityError);
  }
  numeric(input.delay, 'delay', 0, 365 * 24 * 60 * 60 * 1_000);
  numeric(input.timeout, 'timeout', 0, 24 * 60 * 60 * 1_000);
  numeric(input.ttl, 'ttl', 0, 365 * 24 * 60 * 60 * 1_000);
  numeric(input.maxAttempts, 'attempts', 1, 1_000, true);
  numeric(input.stallTimeout, 'stallTimeout', 0, 24 * 60 * 60 * 1_000);
  numeric(input.stackTraceLimit, 'stackTraceLimit', 0, 10_000, true);
  numeric(input.keepLogs, 'keepLogs', 0, 1_000_000, true);
  numeric(input.sizeLimit, 'sizeLimit', 0, MAX_JOB_DATA_BYTES, true);
  numeric(input.timestamp, 'timestamp', 0, Number.MAX_SAFE_INTEGER);
  numeric(input.groupMaxSize, 'group.maxSize', 1, Number.MAX_SAFE_INTEGER, true);

  if (typeof input.backoff === 'object' && input.backoff !== null) {
    if (input.backoff.type !== 'fixed' && input.backoff.type !== 'exponential') {
      throw new Error("backoff.type must be 'fixed' or 'exponential'");
    }
    numeric(input.backoff.delay, 'backoff.delay', 0, 24 * 60 * 60 * 1_000);
  } else {
    numeric(input.backoff, 'backoff', 0, 24 * 60 * 60 * 1_000);
  }

  for (const name of BOOLEAN_OPTIONS) {
    const value = input[name];
    if (value !== undefined && typeof value !== 'boolean') {
      throw new Error(`${name} must be a boolean`);
    }
  }
  const groupError = validateGroupId(input.groupId);
  if (groupError) throw new Error(groupError);
  if (
    input.tags !== undefined &&
    (!Array.isArray(input.tags) ||
      input.tags.some((tag) => typeof tag !== 'string' || tag.length > 256))
  ) {
    throw new Error('tags must be an array of strings of at most 256 characters');
  }

  if (input.repeat !== undefined) {
    throw new Error('repeat is not supported inside an atomic flow');
  }
  if (input.uniqueKey !== undefined || input.dedup !== undefined) {
    throw new Error('deduplication is not supported inside an atomic flow');
  }
  if (input.debounceId !== undefined || input.debounceTtl !== undefined) {
    throw new Error('debounce is not supported inside an atomic flow');
  }

  const policies = [
    input.failParentOnFailure,
    input.removeDependencyOnFailure,
    input.continueParentOnFailure,
    input.ignoreDependencyOnFailure,
  ].filter((enabled) => enabled === true);
  if (policies.length > 1) {
    throw new Error('flow failure policies are mutually exclusive');
  }
  if (policies.length === 1 && !input.parentId) {
    throw new Error('a flow failure policy requires a parent');
  }
}

function validateData(data: unknown): { encodedLength: number; value: Record<string, unknown> } {
  if (typeof data !== 'object' || data === null || Array.isArray(data)) {
    throw new Error('flow job data must be an object');
  }
  let encoded: string | undefined;
  try {
    encoded = JSON.stringify(data);
  } catch {
    throw new Error('job data must be JSON serializable');
  }
  if (encoded === undefined) throw new Error('job data must be JSON serializable');
  if (encoded.length > MAX_JOB_DATA_BYTES) {
    throw new Error('Job data too large (max 10MB)');
  }
  const value = data as Record<string, unknown>;
  return { encodedLength: encoded.length, value };
}

function validateName(name: string): void {
  if (name.length === 0 || name.length > 256) {
    throw new Error('flow job name must be a non-empty string of at most 256 characters');
  }
}

function validateIdList(value: unknown, name: string): void {
  if (
    value !== undefined &&
    (!Array.isArray(value) ||
      value.some(
        (id) =>
          typeof id !== 'string' || id.length === 0 || id.length > 1_024 || !isWellFormedJobId(id)
      ))
  ) {
    throw new Error(`${name} must be an array of valid string job ids`);
  }
}

/** Validate the whole graph before any lock, storage write, or queue mutation. */
export function validateAtomicFlowBatch(batch: AtomicFlowBatchInput): void {
  if (!batch || typeof batch !== 'object' || !Array.isArray(batch.jobs)) {
    throw new Error('flow jobs must be an array');
  }
  if (batch.jobs.length > MAX_FLOW_JOBS) {
    throw new Error(`flow exceeds the ${MAX_FLOW_JOBS} job limit`);
  }

  const ids = new Set<string>();
  const dataById = new Map<string, Record<string, unknown>>();
  let totalDataBytes = 0;
  for (const [index, job] of batch.jobs.entries()) {
    if (!job || typeof job !== 'object') throw new Error(`flow jobs[${index}] must be an object`);
    if (typeof job.id !== 'string') throw new Error('flow job id must be a string');
    const id = job.id;
    if (!id || id.length > 1_024 || !isWellFormedJobId(id)) {
      throw new Error('flow job id is invalid or is not well-formed Unicode');
    }
    if (id.includes(':')) throw new Error('flow job id cannot contain a colon');
    if (ids.has(id)) throw new Error(`duplicate flow job id: ${id}`);
    ids.add(id);
    if (
      typeof job.queue !== 'string' ||
      !job.queue ||
      job.queue.length > 256 ||
      !QUEUE_RE.test(job.queue)
    ) {
      throw new Error(`invalid flow queue: ${job.queue}`);
    }
    if (!job.input || typeof job.input !== 'object') {
      throw new Error(`flow job input is invalid: ${id}`);
    }
    if (
      job.input.customId !== undefined &&
      (typeof job.input.customId !== 'string' || job.input.customId !== id)
    ) {
      throw new Error(`custom id does not match planned id: ${id}`);
    }
    if (
      job.input.parentId !== undefined &&
      (typeof job.input.parentId !== 'string' ||
        job.input.parentId.length === 0 ||
        !isWellFormedJobId(job.input.parentId))
    ) {
      throw new Error(`flow parent id is invalid: ${id}`);
    }
    validateIdList(job.input.dependsOn, 'dependsOn');
    validateIdList(job.input.childrenIds, 'childrenIds');
    const payload = normalizeJobPayload(job.input);
    validateName(payload.name);
    const validatedData = validateData(payload.data);
    totalDataBytes += validatedData.encodedLength;
    if (totalDataBytes > MAX_FLOW_DATA_BYTES) {
      throw new Error('flow data is too large (max 64MB per atomic batch)');
    }
    dataById.set(id, validatedData.value);
    validateOptions(job.input);
  }
  validateFlowTopology(batch, dataById);
}
