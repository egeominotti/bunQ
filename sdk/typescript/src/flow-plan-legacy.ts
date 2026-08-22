import { randomUUID } from 'node:crypto';
import {
  type AtomicFlowJobInput,
  allocateFlowId,
  type FlowIdFactory,
  flowData,
  flowInput,
} from './flow-plan.js';
import type { FlowStep } from './flow-types.js';

const MAX_FLOW_JOBS = 10_000;
const QUEUE_RE = /^[a-zA-Z0-9_\-.:]+$/;

export interface ChainPlan {
  jobs: AtomicFlowJobInput[];
  ids: string[];
}

export interface FanInPlan {
  jobs: AtomicFlowJobInput[];
  parallelIds: string[];
  finalId: string;
}

function allocateSteps<T>(
  steps: FlowStep<T>[],
  ids: Set<string>,
  idFactory: FlowIdFactory
): string[] {
  if (steps.length > MAX_FLOW_JOBS) {
    throw new Error(`flow exceeds the ${MAX_FLOW_JOBS} job limit`);
  }
  return steps.map((step) => {
    if (!step || typeof step !== 'object') throw new Error('flow step must be an object');
    if (typeof step.name !== 'string' || !step.name || step.name.length > 256) {
      throw new Error('flow job name must be a non-empty string of at most 256 characters');
    }
    if (
      typeof step.queueName !== 'string' ||
      !step.queueName ||
      step.queueName.length > 256 ||
      !QUEUE_RE.test(step.queueName)
    ) {
      throw new Error('flow queueName is invalid');
    }
    const children = (step as FlowStep<T> & { children?: unknown }).children;
    if (children !== undefined && !Array.isArray(children)) {
      throw new Error('flow children must be an array');
    }
    if ((children?.length ?? 0) > 0) {
      throw new Error('nested children are not supported by this flow method');
    }
    return allocateFlowId(step.opts ?? {}, ids, idFactory);
  });
}

export function planChain<T>(
  steps: FlowStep<T>[],
  idFactory: FlowIdFactory = randomUUID
): ChainPlan {
  const ids = allocateSteps(steps, new Set(), idFactory);
  const jobs = steps.map((step, index): AtomicFlowJobInput => {
    const dependency = index > 0 ? ids[index - 1] : undefined;
    const data = flowData(step.name, step.data, {
      __flowParentId: dependency ?? null,
    });
    return {
      id: ids[index],
      queue: step.queueName,
      input: flowInput(data, step.opts ?? {}, {
        dependsOn: dependency ? [dependency] : undefined,
      }),
    };
  });
  return { jobs, ids };
}

export function planBulkThen<T>(
  parallel: FlowStep<T>[],
  final: FlowStep<T>,
  idFactory: FlowIdFactory = randomUUID
): FanInPlan {
  if (parallel.length >= MAX_FLOW_JOBS) {
    throw new Error(`flow exceeds the ${MAX_FLOW_JOBS} job limit`);
  }
  const ids = new Set<string>();
  const parallelIds = allocateSteps(parallel, ids, idFactory);
  const [finalId] = allocateSteps([final], ids, idFactory);
  const parallelJobs = parallel.map((step, index): AtomicFlowJobInput => ({
    id: parallelIds[index],
    queue: step.queueName,
    input: flowInput(
      flowData(step.name, step.data, {
        __parentId: finalId,
        __parentQueue: final.queueName,
      }),
      step.opts ?? {},
      { parentId: finalId }
    ),
  }));
  const finalJob: AtomicFlowJobInput = {
    id: finalId,
    queue: final.queueName,
    input: flowInput(
      flowData(final.name, final.data, {
        __flowParentIds: parallelIds,
        __childrenIds: parallelIds,
      }),
      final.opts ?? {},
      {
        dependsOn: parallelIds.length > 0 ? parallelIds : undefined,
        childrenIds: parallelIds.length > 0 ? parallelIds : undefined,
      }
    ),
  };
  return { jobs: [...parallelJobs, finalJob], parallelIds, finalId };
}
