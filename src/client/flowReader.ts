import type { Job as DomainJob } from '../domain/types/job';
import { jobId } from '../domain/types/job';
import { createFlowJobObject, extractFlowJobPayload } from './flowJobFactory';
import type { FlowJobCallbacks } from './flowJobTypes';
import type { GetFlowOpts, JobNode } from './flowTypes';
import { getSharedManager } from './manager';
import type { TcpConnectionPool } from './tcpPool';

export interface FlowReaderContext {
  embedded: boolean;
  tcp: TcpConnectionPool | null;
  buildCallbacks: (queueName: string) => FlowJobCallbacks;
}

function traversalLimit(value: number | undefined, name: string): number {
  if (value === undefined) return Infinity;
  if (value === Infinity) return value;
  if (!Number.isInteger(value) || value < 0) {
    throw new Error(`${name} must be a non-negative integer`);
  }
  return value;
}

function childIdsFromTcp(job: Record<string, unknown>): string[] {
  const data =
    typeof job.data === 'object' && job.data !== null ? (job.data as Record<string, unknown>) : {};
  const raw = job.childrenIds ?? data.__childrenIds;
  if (raw === undefined || raw === null) return [];
  if (!Array.isArray(raw) || raw.some((id) => typeof id !== 'string')) {
    throw new Error(`Flow job ${String(job.id)} has invalid childrenIds`);
  }
  return raw;
}

function assertNoCycle(id: string, ancestors: ReadonlySet<string>): Set<string> {
  if (ancestors.has(id)) throw new Error(`Flow topology contains a cycle at job ${id}`);
  const next = new Set(ancestors);
  next.add(id);
  return next;
}

interface ExpectedParent {
  readonly id: string;
  readonly queue: string;
}

interface TraversalState {
  readonly depth: number;
  readonly maxChildren: number;
  readonly ancestors: ReadonlySet<string>;
  readonly expectedParent: ExpectedParent | null;
}

function assertParentLink(
  id: string,
  parentId: unknown,
  data: Record<string, unknown>,
  expected: ExpectedParent | null
): void {
  if (!expected) return;
  if (
    String(parentId) !== expected.id ||
    data.__parentId !== expected.id ||
    data.__parentQueue !== expected.queue
  ) {
    throw new Error(`Flow child ${id} does not point back to parent ${expected.id}`);
  }
}

async function buildEmbeddedNode<T>(
  context: FlowReaderContext,
  job: DomainJob,
  traversal: TraversalState
): Promise<JobNode<T>> {
  const { depth, maxChildren, ancestors, expectedParent } = traversal;
  const id = String(job.id);
  const nextAncestors = assertNoCycle(id, ancestors);
  const data = job.data as Record<string, unknown>;
  assertParentLink(id, job.parentId, data, expectedParent);
  const payload = extractFlowJobPayload(job);
  const node: JobNode<T> = {
    job: createFlowJobObject(id, payload.name, payload.data as T, job.queue, {
      callbacks: context.buildCallbacks(job.queue),
      snapshot: job,
    }),
  };
  if (depth <= 0 || job.childrenIds.length === 0 || maxChildren === 0) return node;

  const childIds =
    maxChildren === Infinity ? job.childrenIds : job.childrenIds.slice(0, maxChildren);
  const children: JobNode<T>[] = [];
  for (const childId of childIds) {
    const child = await getSharedManager().getJob(childId);
    if (!child) throw new Error(`Flow child job ${String(childId)} not found`);
    children.push(
      await buildEmbeddedNode(context, child, {
        depth: depth - 1,
        maxChildren,
        ancestors: nextAncestors,
        expectedParent: { id, queue: job.queue },
      })
    );
  }
  if (children.length > 0) node.children = children;
  return node;
}

async function getTcpJob(
  tcp: TcpConnectionPool,
  id: string,
  root: boolean
): Promise<Record<string, unknown> | null> {
  const response = await tcp.send({ cmd: 'GetJob', id });
  if (response.ok !== true) {
    if (root && response.error === 'Job not found') return null;
    throw new Error(
      typeof response.error === 'string' ? response.error : `Unable to fetch flow job ${id}`
    );
  }
  if (!response.job || typeof response.job !== 'object') {
    throw new Error(`GetJob returned no snapshot for flow job ${id}`);
  }
  return response.job as Record<string, unknown>;
}

async function buildTcpNode<T>(
  context: FlowReaderContext,
  job: Record<string, unknown>,
  traversal: TraversalState
): Promise<JobNode<T>> {
  const { depth, maxChildren, ancestors, expectedParent } = traversal;
  const tcp = context.tcp;
  if (!tcp) throw new Error('TCP connection not initialized');
  const id = String(job.id);
  const queueName = String(job.queue);
  const nextAncestors = assertNoCycle(id, ancestors);
  const data =
    typeof job.data === 'object' && job.data !== null ? (job.data as Record<string, unknown>) : {};
  assertParentLink(id, job.parentId, data, expectedParent);
  const payload = extractFlowJobPayload(job);
  const node: JobNode<T> = {
    job: createFlowJobObject(id, payload.name, payload.data as T, queueName, {
      callbacks: context.buildCallbacks(queueName),
      snapshot: job as unknown as DomainJob,
    }),
  };
  const childIds = childIdsFromTcp(job);
  if (depth <= 0 || childIds.length === 0 || maxChildren === 0) return node;

  const selected = maxChildren === Infinity ? childIds : childIds.slice(0, maxChildren);
  const children: JobNode<T>[] = [];
  for (const childId of selected) {
    const child = await getTcpJob(tcp, childId, false);
    if (!child) throw new Error(`Flow child job ${childId} not found`);
    children.push(
      await buildTcpNode(context, child, {
        depth: depth - 1,
        maxChildren,
        ancestors: nextAncestors,
        expectedParent: { id, queue: queueName },
      })
    );
  }
  if (children.length > 0) node.children = children;
  return node;
}

/** Read a flow without silently truncating transport or topology errors. */
export async function readFlow<T>(
  context: FlowReaderContext,
  opts: GetFlowOpts
): Promise<JobNode<T> | null> {
  const depth = traversalLimit(opts.depth, 'depth');
  const maxChildren = traversalLimit(opts.maxChildren, 'maxChildren');
  if (context.embedded) {
    const job = await getSharedManager().getJob(jobId(opts.id));
    if (!job || job.queue !== opts.queueName) return null;
    return buildEmbeddedNode(context, job, {
      depth,
      maxChildren,
      ancestors: new Set(),
      expectedParent: null,
    });
  }
  if (!context.tcp) throw new Error('TCP connection not initialized');
  const job = await getTcpJob(context.tcp, opts.id, true);
  if (!job || job.queue !== opts.queueName) return null;
  return buildTcpNode(context, job, {
    depth,
    maxChildren,
    ancestors: new Set(),
    expectedParent: null,
  });
}
