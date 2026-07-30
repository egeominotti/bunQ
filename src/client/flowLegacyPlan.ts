import { generateJobId, jobId, type JobId } from '../domain/types/job';
import type { AtomicFlowBatchInput, AtomicFlowJobInput } from '../domain/types/flow';
import type { FlowStep } from './flowTypes';
import { flowJobInput } from './flowOptions';

const MAX_FLOW_DEPTH = 100;
const MAX_FLOW_JOBS = 10_000;

function validateStep<T>(step: FlowStep<T>, depth = 0): void {
  if (!step || typeof step !== 'object') throw new Error('flow step must be an object');
  if (!step.name || typeof step.name !== 'string' || step.name.length > 256) {
    throw new Error('flow job name must be a non-empty string of at most 256 characters');
  }
  if (!step.queueName || typeof step.queueName !== 'string') {
    throw new Error('flow queueName is required');
  }
  if (depth > MAX_FLOW_DEPTH) {
    throw new Error(`flow exceeds the ${MAX_FLOW_DEPTH} level depth limit`);
  }
  if (step.children !== undefined && !Array.isArray(step.children)) {
    throw new Error('flow children must be an array');
  }
  if (typeof step.data !== 'object' || step.data === null || Array.isArray(step.data)) {
    throw new Error('flow job data must be an object');
  }
  for (const key of Object.keys(step.data as object)) {
    if (key === 'name' || key.startsWith('__')) {
      throw new Error(`flow job data key is reserved: ${key}`);
    }
  }
}

function validateFlatSteps<T>(steps: FlowStep<T>[]): void {
  if (steps.length > MAX_FLOW_JOBS) throw new Error(`flow exceeds the ${MAX_FLOW_JOBS} job limit`);
  for (const step of steps) {
    validateStep(step);
    if ((step.children?.length ?? 0) > 0) {
      throw new Error('nested children are only supported by addTree');
    }
  }
}

function allocate<T>(step: FlowStep<T>, ids: Set<string>): JobId {
  const raw = step.opts?.jobId;
  if (raw !== undefined && (!raw || raw.includes(':'))) {
    throw new Error('flow jobId must be non-empty and cannot contain a colon');
  }
  const id = raw === undefined ? generateJobId() : jobId(raw);
  if (ids.has(String(id))) throw new Error(`duplicate flow job id: ${String(id)}`);
  ids.add(String(id));
  return id;
}

function dataFor<T>(step: FlowStep<T>, internal: Record<string, unknown>): unknown {
  return { ...(step.data as object), name: step.name, ...internal };
}

export function planChain<T>(steps: FlowStep<T>[]): {
  batch: AtomicFlowBatchInput;
  ids: JobId[];
} {
  validateFlatSteps(steps);
  const seen = new Set<string>();
  const ids = steps.map((step) => allocate(step, seen));
  const jobs = steps.map((step, index): AtomicFlowJobInput => {
    const dependency = index > 0 ? ids[index - 1] : undefined;
    return {
      id: ids[index],
      queue: step.queueName,
      input: flowJobInput(
        dataFor(step, { __flowParentId: dependency ? String(dependency) : null }),
        step.opts ?? {},
        { dependsOn: dependency ? [dependency] : undefined }
      ),
    };
  });
  return { batch: { jobs }, ids };
}

export function planBulkThen<T>(
  parallel: FlowStep<T>[],
  final: FlowStep<T>
): { batch: AtomicFlowBatchInput; parallelIds: JobId[]; finalId: JobId } {
  validateFlatSteps([...parallel, final]);
  const seen = new Set<string>();
  const parallelIds = parallel.map((step) => allocate(step, seen));
  const finalId = allocate(final, seen);
  const parallelJobs = parallel.map(
    (step, index): AtomicFlowJobInput => ({
      id: parallelIds[index],
      queue: step.queueName,
      input: flowJobInput(
        dataFor(step, {
          __parentId: String(finalId),
          __parentQueue: final.queueName,
        }),
        step.opts ?? {},
        { parentId: finalId }
      ),
    })
  );
  const finalJob: AtomicFlowJobInput = {
    id: finalId,
    queue: final.queueName,
    input: flowJobInput(
      dataFor(final, {
        __flowParentIds: parallelIds.map(String),
        __childrenIds: parallelIds.map(String),
      }),
      final.opts ?? {},
      {
        dependsOn: parallelIds.length > 0 ? parallelIds : undefined,
        childrenIds: parallelIds.length > 0 ? parallelIds : undefined,
      }
    ),
  };
  return { batch: { jobs: [...parallelJobs, finalJob] }, parallelIds, finalId };
}

export function planTree<T>(root: FlowStep<T>): {
  batch: AtomicFlowBatchInput;
  ids: JobId[];
} {
  const jobs: AtomicFlowJobInput[] = [];
  const ids: JobId[] = [];
  const allocated = new Set<string>();
  const seen = new WeakSet<object>();

  const visit = (step: FlowStep<T>, dependency?: JobId, depth = 0): void => {
    validateStep(step, depth);
    if (seen.has(step as object)) throw new Error('flow contains a cycle or shared node');
    seen.add(step as object);
    if (jobs.length >= MAX_FLOW_JOBS) {
      throw new Error(`flow exceeds the ${MAX_FLOW_JOBS} job limit`);
    }
    const id = allocate(step, allocated);
    ids.push(id);
    jobs.push({
      id,
      queue: step.queueName,
      input: flowJobInput(
        dataFor(step, { __flowParentId: dependency ? String(dependency) : null }),
        step.opts ?? {},
        { dependsOn: dependency ? [dependency] : undefined }
      ),
    });
    for (const child of step.children ?? []) visit(child, id, depth + 1);
  };

  visit(root);
  return { batch: { jobs }, ids };
}
