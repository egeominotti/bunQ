import { generateJobId, jobId, type JobId } from '../domain/types/job';
import type { AtomicFlowBatchInput, AtomicFlowJobInput } from '../domain/types/flow';
import type { FlowJob, FlowOpts } from './flowTypes';
import { flowJobInput } from './flowOptions';

const MAX_FLOW_DEPTH = 100;
const MAX_FLOW_JOBS = 10_000;

export interface PlannedFlowNode<T> {
  readonly id: JobId;
  readonly name: string;
  readonly queueName: string;
  readonly data: T | undefined;
  readonly children?: PlannedFlowNode<T>[];
}

export interface FlowPlan<T> {
  readonly batch: AtomicFlowBatchInput;
  readonly roots: PlannedFlowNode<T>[];
}

function validateNodeShape(node: FlowJob<unknown>, depth: number): void {
  if (!node || typeof node !== 'object') throw new Error('flow node must be an object');
  if (!node.name || typeof node.name !== 'string') throw new Error('flow job name is required');
  if (!node.queueName || typeof node.queueName !== 'string') {
    throw new Error('flow queueName is required');
  }
  if (depth > MAX_FLOW_DEPTH) {
    throw new Error(`flow exceeds the ${MAX_FLOW_DEPTH} level depth limit`);
  }
  if (node.children !== undefined && !Array.isArray(node.children)) {
    throw new Error('flow children must be an array');
  }
  if (node.data !== undefined) {
    if (typeof node.data !== 'object' || node.data === null || Array.isArray(node.data)) {
      throw new Error('flow job data must be an object');
    }
    for (const key of Object.keys(node.data as object)) {
      if (key === 'name' || key.startsWith('__')) {
        throw new Error(`flow job data key is reserved: ${key}`);
      }
    }
  }
}

function plannedId(node: FlowJob<unknown>): JobId {
  const custom = node.opts?.jobId;
  if (custom === undefined) return generateJobId();
  if (!custom || custom.includes(':')) {
    throw new Error('flow jobId must be non-empty and cannot contain a colon');
  }
  return jobId(custom);
}

/** Compile one or more trees into a fully-resolved graph before contacting the broker. */
export function planFlows<T>(flows: FlowJob<T>[], options?: FlowOpts): FlowPlan<T> {
  const jobs: AtomicFlowJobInput[] = [];
  const ids = new Set<string>();
  const seen = new WeakSet<object>();
  let nodeCount = 0;

  const visit = (
    node: FlowJob<T>,
    parent: { id: JobId; queue: string } | undefined,
    depth: number
  ): PlannedFlowNode<T> => {
    validateNodeShape(node, depth);
    if (seen.has(node as object)) throw new Error('flow contains a cycle or shared node');
    seen.add(node as object);
    nodeCount++;
    if (nodeCount > MAX_FLOW_JOBS) {
      throw new Error(`flow exceeds the ${MAX_FLOW_JOBS} job limit`);
    }

    const id = plannedId(node);
    if (ids.has(String(id))) throw new Error(`duplicate flow job id: ${String(id)}`);
    ids.add(String(id));

    const children = (node.children ?? []).map((child) =>
      visit(child, { id, queue: node.queueName }, depth + 1)
    );
    const childIds = children.map((child) => child.id);
    const defaults = options?.queuesOptions?.[node.queueName];
    const opts = defaults ? { ...defaults, ...node.opts } : (node.opts ?? {});
    const internalData: Record<string, unknown> = {
      ...(node.data as object | undefined),
      name: node.name,
    };
    if (parent) {
      internalData.__parentId = String(parent.id);
      internalData.__parentQueue = parent.queue;
    }
    if (childIds.length > 0) internalData.__childrenIds = childIds.map(String);

    jobs.push({
      id,
      queue: node.queueName,
      input: flowJobInput(internalData, opts, {
        parentId: parent?.id,
        dependsOn: childIds.length > 0 ? childIds : undefined,
        childrenIds: childIds.length > 0 ? childIds : undefined,
      }),
    });
    return {
      id,
      name: node.name,
      queueName: node.queueName,
      data: node.data,
      children: children.length > 0 ? children : undefined,
    };
  };

  return {
    batch: { jobs },
    roots: flows.map((flow) => visit(flow, undefined, 0)),
  };
}
