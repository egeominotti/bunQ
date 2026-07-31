import { randomUUID } from 'node:crypto';

import type { FlowJob, FlowOptions } from './flow-types.js';
import { compact } from './frame.js';
import { type JobOptions, wireJobOptions } from './types.js';

const MAX_FLOW_DEPTH = 100;
const MAX_FLOW_JOBS = 10_000;
const QUEUE_RE = /^[a-zA-Z0-9_\-.:]+$/;

export type FlowIdFactory = () => string;

export interface AtomicFlowJobInput {
  id: string;
  queue: string;
  input: Record<string, unknown>;
}

export interface PlannedFlowNode<T = unknown> {
  id: string;
  name: string;
  queueName: string;
  data: T | undefined;
  children?: PlannedFlowNode<T>[];
}

export interface FlowPlan<T = unknown> {
  jobs: AtomicFlowJobInput[];
  roots: PlannedFlowNode<T>[];
}

interface PlanState {
  readonly ids: Set<string>;
  readonly seen: WeakSet<object>;
  readonly jobs: AtomicFlowJobInput[];
  readonly idFactory: FlowIdFactory;
  count: number;
}

interface FlowLinks {
  parentId?: string;
  dependsOn?: string[];
  childrenIds?: string[];
}

function assertNameAndQueue(name: unknown, queueName: unknown, depth: number): void {
  if (typeof name !== 'string' || name.length === 0 || name.length > 256) {
    throw new Error('flow job name must be a non-empty string of at most 256 characters');
  }
  if (
    typeof queueName !== 'string' ||
    queueName.length === 0 ||
    queueName.length > 256 ||
    !QUEUE_RE.test(queueName)
  ) {
    throw new Error('flow queueName is invalid');
  }
  if (depth > MAX_FLOW_DEPTH) {
    throw new Error(`flow exceeds the ${MAX_FLOW_DEPTH} level depth limit`);
  }
}

function assertAtomicOptions(opts: JobOptions): void {
  if (opts.repeat !== undefined) throw new Error('repeat is not supported inside an atomic flow');
  if (opts.deduplication !== undefined) {
    throw new Error('deduplication is not supported inside an atomic flow');
  }
  if (opts.debounce !== undefined) {
    throw new Error('debounce is not supported inside an atomic flow');
  }
  if (
    opts.parentId !== undefined ||
    opts.dependsOn !== undefined ||
    opts.childrenIds !== undefined
  ) {
    throw new Error('flow topology options are owned by FlowProducer');
  }
}

function assertUserData(data: unknown): void {
  if (!data || typeof data !== 'object' || Array.isArray(data)) return;
  for (const key of Object.keys(data)) {
    if (key === 'name' || key.startsWith('__')) {
      throw new Error(`flow job data key is reserved: ${key}`);
    }
  }
}

export function flowData(
  name: string,
  data: unknown,
  internal: Record<string, unknown> = {}
): Record<string, unknown> {
  assertUserData(data);
  if (data && typeof data === 'object' && !Array.isArray(data)) {
    return { ...(data as Record<string, unknown>), name, ...internal };
  }
  return data === undefined || data === null
    ? { name, ...internal }
    : { name, payload: data, ...internal };
}

export function flowInput(
  data: Record<string, unknown>,
  opts: JobOptions,
  links: FlowLinks = {}
): Record<string, unknown> {
  assertAtomicOptions(opts);
  const mapped = { ...wireJobOptions(opts) };
  delete mapped.jobId;
  delete mapped.parentId;
  delete mapped.dependsOn;
  delete mapped.childrenIds;
  return compact({
    data,
    ...mapped,
    customId: opts.jobId,
    parentId: links.parentId,
    dependsOn: links.dependsOn,
    childrenIds: links.childrenIds,
  });
}

export function allocateFlowId(
  opts: JobOptions,
  ids: Set<string>,
  idFactory: FlowIdFactory
): string {
  const id = opts.jobId ?? idFactory();
  if (typeof id !== 'string' || id.length === 0 || id.length > 1_024 || id.includes(':')) {
    throw new Error('flow jobId must be non-empty and cannot contain a colon');
  }
  if (ids.has(id)) throw new Error(`duplicate flow job id: ${id}`);
  ids.add(id);
  return id;
}

function visit<T>(
  node: FlowJob<T>,
  parent: { id: string; queue: string } | undefined,
  depth: number,
  options: FlowOptions | undefined,
  state: PlanState
): PlannedFlowNode<T> {
  if (!node || typeof node !== 'object') throw new Error('flow node must be an object');
  assertNameAndQueue(node.name, node.queueName, depth);
  if (node.children !== undefined && !Array.isArray(node.children)) {
    throw new Error('flow children must be an array');
  }
  if (state.seen.has(node as object)) throw new Error('flow contains a cycle or shared node');
  state.seen.add(node as object);
  state.count += 1;
  if (state.count > MAX_FLOW_JOBS) {
    throw new Error(`flow exceeds the ${MAX_FLOW_JOBS} job limit`);
  }

  const defaults = options?.queuesOptions?.[node.queueName];
  const opts = defaults ? { ...defaults, ...node.opts } : (node.opts ?? {});
  assertAtomicOptions(opts);
  const id = allocateFlowId(opts, state.ids, state.idFactory);
  const children = (node.children ?? []).map((child) =>
    visit(child, { id, queue: node.queueName }, depth + 1, options, state)
  );
  const childIds = children.map((child) => child.id);
  const internal: Record<string, unknown> = {};
  if (parent) {
    internal.__parentId = parent.id;
    internal.__parentQueue = parent.queue;
  }
  if (childIds.length > 0) internal.__childrenIds = childIds;
  const data = flowData(node.name, node.data, internal);
  state.jobs.push({
    id,
    queue: node.queueName,
    input: flowInput(data, opts, {
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
}

export function planFlows<T>(
  flows: FlowJob<T>[],
  options?: FlowOptions,
  idFactory: FlowIdFactory = randomUUID
): FlowPlan<T> {
  for (const defaults of Object.values(options?.queuesOptions ?? {})) {
    if ((defaults as Partial<JobOptions>).jobId !== undefined) {
      throw new Error('jobId cannot be a queue default');
    }
  }
  const state: PlanState = {
    ids: new Set(),
    seen: new WeakSet(),
    jobs: [],
    idFactory,
    count: 0,
  };
  const roots = flows.map((flow) => visit(flow, undefined, 0, options, state));
  return { jobs: state.jobs, roots };
}
