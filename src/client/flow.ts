/**
 * FlowProducer - Job chaining and pipelines
 * BullMQ v5 compatible
 */

import { EventEmitter } from 'events';
import { getSharedManager } from './manager';
import { TcpConnectionPool, getSharedPool, releaseSharedPool } from './tcpPool';
import { jobId } from '../domain/types/job';
import type { Job as DomainJob } from '../domain/types/job';
import type {
  FlowProducerOptions,
  FlowStep,
  FlowResult,
  FlowJob,
  JobNode,
  GetFlowOpts,
  FlowOpts,
} from './flowTypes';
import {
  createFlowJobObject,
  extractUserDataFromInternal,
  type FlowJobCallbacks,
} from './flowJobFactory';
import { pushJob, pushJobWithParent, cleanupJobs, type PushContext } from './flowPush';
import * as managementOps from './queue/operations/management';

// Re-export types for backwards compatibility
export type {
  FlowProducerOptions,
  FlowStep,
  FlowResult,
  FlowJob,
  JobNode,
  GetFlowOpts,
  FlowOpts,
} from './flowTypes';

const FORCE_EMBEDDED = Bun.env.BUNQUEUE_EMBEDDED === '1';

/**
 * FlowProducer creates job flows with automatic dependencies.
 *
 * @example
 * ```typescript
 * const flow = new FlowProducer();
 *
 * // Simple chain: A → B → C
 * const { jobIds } = await flow.addChain([
 *   { name: 'fetch', queueName: 'pipeline', data: { url: '...' } },
 *   { name: 'process', queueName: 'pipeline', data: {} },
 *   { name: 'store', queueName: 'pipeline', data: {} },
 * ]);
 *
 * // Parallel then merge
 * const result = await flow.addBulkThen(
 *   [
 *     { name: 'task1', queueName: 'parallel', data: { id: 1 } },
 *     { name: 'task2', queueName: 'parallel', data: { id: 2 } },
 *   ],
 *   { name: 'merge', queueName: 'final', data: {} }
 * );
 * ```
 */
export class FlowProducer extends EventEmitter {
  closing: Promise<void> = Promise.resolve();
  private readonly embedded: boolean;
  private readonly tcp: TcpConnectionPool | null;
  private readonly useSharedPool: boolean;

  constructor(opts: FlowProducerOptions = {}) {
    super();
    this.embedded = opts.embedded ?? FORCE_EMBEDDED;

    if (this.embedded) {
      this.tcp = null;
      this.useSharedPool = false;
    } else {
      const connOpts = opts.connection ?? {};
      const poolSize = connOpts.poolSize ?? 4;

      if (poolSize === 4 && !connOpts.token) {
        this.tcp = getSharedPool({
          host: connOpts.host,
          port: connOpts.port,
          poolSize,
          pingInterval: connOpts.pingInterval,
          commandTimeout: connOpts.commandTimeout,
          pipelining: connOpts.pipelining,
          maxInFlight: connOpts.maxInFlight,
        });
        this.useSharedPool = true;
      } else {
        this.tcp = new TcpConnectionPool({
          host: connOpts.host ?? 'localhost',
          port: connOpts.port ?? 6789,
          token: connOpts.token,
          poolSize,
          pingInterval: connOpts.pingInterval,
          commandTimeout: connOpts.commandTimeout,
          pipelining: connOpts.pipelining,
          maxInFlight: connOpts.maxInFlight,
        });
        this.useSharedPool = false;
      }
    }
  }

  /** Get push context for helper functions */
  private get pushCtx(): PushContext {
    return { embedded: this.embedded, tcp: this.tcp };
  }

  /** Close the connection pool (only if using dedicated pool) */
  close(): Promise<void> {
    if (this.tcp && !this.useSharedPool) {
      this.tcp.close();
    } else if (this.tcp && this.useSharedPool) {
      releaseSharedPool(this.tcp);
    }
    this.closing = Promise.resolve();
    return this.closing;
  }

  /** Disconnect from the server (BullMQ v5 compatible). Alias for close(). */
  disconnect(): Promise<void> {
    return this.close();
  }

  /** Wait until the FlowProducer is ready (BullMQ v5 compatible). */
  async waitUntilReady(): Promise<void> {
    if (this.embedded) return;
    if (this.tcp) await this.tcp.send({ cmd: 'Ping' });
  }

  // ============================================================================
  // BullMQ v5 Compatible Methods
  // ============================================================================

  /** Add a flow (BullMQ v5 compatible). Children are processed BEFORE their parent. */
  async add<T = unknown>(flow: FlowJob<T>, opts?: FlowOpts): Promise<JobNode<T>> {
    const createdJobIds: string[] = [];
    try {
      return await this.addFlowNode(flow, null, createdJobIds, opts);
    } catch (error) {
      // Atomic rollback: clean up all created jobs
      await cleanupJobs(this.pushCtx, createdJobIds);
      throw error;
    }
  }

  /** Add multiple flows (BullMQ v5 compatible). */
  async addBulk<T = unknown>(flows: FlowJob<T>[]): Promise<JobNode<T>[]> {
    const results: JobNode<T>[] = [];
    const allCreatedJobIds: string[] = [];
    try {
      for (const flow of flows) {
        const createdJobIds: string[] = [];
        const result = await this.addFlowNode(flow, null, createdJobIds);
        allCreatedJobIds.push(...createdJobIds);
        results.push(result);
      }
      return results;
    } catch (error) {
      // Atomic rollback: clean up all created jobs from all flows
      await cleanupJobs(this.pushCtx, allCreatedJobIds);
      throw error;
    }
  }

  /** Get a flow tree starting from a job (BullMQ v5 compatible). */
  async getFlow<T = unknown>(opts: GetFlowOpts): Promise<JobNode<T> | null> {
    const { id, queueName, depth, maxChildren } = opts;
    if (this.embedded) {
      return this.getFlowEmbedded<T>(id, queueName, depth ?? Infinity, maxChildren);
    }
    return this.getFlowTcp<T>(id, queueName, depth ?? Infinity, maxChildren);
  }

  // ============================================================================
  // Legacy bunqueue API Methods
  // ============================================================================

  /** Add a chain of jobs. Jobs execute sequentially: step[0] → step[1] → ... */
  async addChain<T = unknown>(steps: FlowStep<T>[]): Promise<FlowResult> {
    if (steps.length === 0) return { jobIds: [] };

    const jobIds: string[] = [];
    let prevId: string | null = null;

    try {
      for (const step of steps) {
        const data = { name: step.name, __flowParentId: prevId, ...(step.data as object) };
        const id = await pushJob(
          this.pushCtx,
          step.queueName,
          data,
          step.opts ?? {},
          prevId ? [prevId] : undefined
        );
        jobIds.push(id);
        prevId = id;
      }
    } catch (error) {
      await cleanupJobs(this.pushCtx, jobIds);
      throw error;
    }

    return { jobIds };
  }

  /** Add parallel jobs that converge to a final job. */
  async addBulkThen<T = unknown>(
    parallel: FlowStep<T>[],
    final: FlowStep<T>
  ): Promise<{ parallelIds: string[]; finalId: string }> {
    const parallelIds: string[] = [];
    try {
      // Push parallel jobs concurrently — they are independent by design
      const results = await Promise.allSettled(
        parallel.map(async (step) => {
          const data = { name: step.name, ...(step.data as object) };
          return pushJob(this.pushCtx, step.queueName, data, step.opts ?? {});
        })
      );

      // Collect successful IDs and check for failures
      const errors: unknown[] = [];
      for (const r of results) {
        if (r.status === 'fulfilled') {
          parallelIds.push(r.value);
        } else {
          errors.push(r.reason);
        }
      }

      if (errors.length > 0) {
        // Clean up successfully created jobs before throwing
        await cleanupJobs(this.pushCtx, parallelIds);
        throw errors[0] instanceof Error ? errors[0] : new Error(String(errors[0]));
      }

      const finalData = {
        name: final.name,
        __flowParentIds: parallelIds,
        ...(final.data as object),
      };
      const finalId = await pushJob(
        this.pushCtx,
        final.queueName,
        finalData,
        final.opts ?? {},
        parallelIds
      );

      return { parallelIds, finalId };
    } catch (error) {
      await cleanupJobs(this.pushCtx, parallelIds);
      throw error;
    }
  }

  /** Add a tree of jobs where children depend on parent. */
  async addTree<T = unknown>(root: FlowStep<T>): Promise<FlowResult> {
    const jobIds: string[] = [];
    try {
      await this.addTreeNode(root, null, jobIds);
    } catch (error) {
      await cleanupJobs(this.pushCtx, jobIds);
      throw error;
    }
    return { jobIds };
  }

  /**
   * Get the result of a completed parent job (embedded only).
   * @template R - Type of the job result
   */
  // eslint-disable-next-line @typescript-eslint/no-unnecessary-type-parameters
  getParentResult<R = unknown>(parentId: string): R | undefined {
    if (!this.embedded) throw new Error('getParentResult is only available in embedded mode');
    return getSharedManager().getResult(jobId(parentId)) as R | undefined;
  }

  /**
   * Get results from multiple parent jobs (embedded only).
   * @template R - Type of the job results
   */
  getParentResults<R = unknown>(parentIds: string[]): Map<string, R> {
    if (!this.embedded) throw new Error('getParentResults is only available in embedded mode');
    const manager = getSharedManager();
    const results = new Map<string, R>();
    for (const id of parentIds) {
      const result = manager.getResult(jobId(id));
      if (result !== undefined) results.set(id, result as R);
    }
    return results;
  }

  // ============================================================================
  // Private Methods
  // ============================================================================

  /** Build callbacks that wire flow job methods to actual management operations */
  private buildCallbacks(queueName: string): FlowJobCallbacks {
    const ctx = { name: queueName, embedded: this.embedded, tcp: this.tcp };
    return {
      embedded: this.embedded,
      tcp: this.tcp,
      updateData: (id, data) => managementOps.updateJobData(ctx, id, data),
      updateProgress: (id, progress) => managementOps.updateJobProgress(ctx, id, progress),
      log: (id, msg) => managementOps.addJobLog(ctx, id, msg).then(() => {}),
      promote: (id) => managementOps.promoteJob(ctx, id),
      remove: (id) => managementOps.removeAsync(ctx, id),
      changePriority: (id, opts) => managementOps.changeJobPriority(ctx, id, opts),
      changeDelay: (id, d) => managementOps.changeJobDelay(ctx, id, d),
      clearLogs: (id) => managementOps.clearJobLogs(ctx, id),
      retry: (id) => managementOps.retryJob(ctx, id),
      getState: (id) => {
        if (this.embedded) {
          return getSharedManager().getJobState(jobId(id));
        }
        if (!this.tcp) return Promise.resolve('unknown');
        return this.tcp
          .send({ cmd: 'GetState', id })
          .then((r) => (typeof r.state === 'string' ? r.state : 'unknown'));
      },
    };
  }

  private async getFlowEmbedded<T>(
    id: string,
    queueName: string,
    depth: number,
    maxChildren?: number
  ): Promise<JobNode<T> | null> {
    const job = await getSharedManager().getJob(jobId(id));
    if (job?.queue !== queueName) return null;
    return this.buildJobNode<T>(job, depth, maxChildren);
  }

  private async getFlowTcp<T>(
    id: string,
    queueName: string,
    depth: number,
    maxChildren?: number
  ): Promise<JobNode<T> | null> {
    if (!this.tcp) throw new Error('TCP connection not initialized');
    const response = await this.tcp.send({ cmd: 'GetJob', id });
    if (!response.ok || !response.job) return null;
    const jobData = response.job as Record<string, unknown>;
    if (jobData.queue !== queueName) return null;
    return this.buildJobNodeFromTcp<T>(jobData, depth, maxChildren);
  }

  private async buildJobNode<T>(
    job: DomainJob,
    depth: number,
    maxChildren?: number
  ): Promise<JobNode<T>> {
    const data = job.data as Record<string, unknown>;
    const name = typeof data.name === 'string' ? data.name : 'default';
    const userData = extractUserDataFromInternal(data) as T;
    const jobObj = createFlowJobObject(
      String(job.id),
      name,
      userData,
      job.queue,
      this.buildCallbacks(job.queue)
    );

    if (depth <= 0 || job.childrenIds.length === 0) return { job: jobObj };

    const childNodes: JobNode<T>[] = [];
    const childrenToFetch = maxChildren ? job.childrenIds.slice(0, maxChildren) : job.childrenIds;

    for (const childId of childrenToFetch) {
      const childJob = await getSharedManager().getJob(childId);
      if (childJob) childNodes.push(await this.buildJobNode<T>(childJob, depth - 1, maxChildren));
    }

    return { job: jobObj, children: childNodes.length > 0 ? childNodes : undefined };
  }

  private async buildJobNodeFromTcp<T>(
    jobData: Record<string, unknown>,
    depth: number,
    maxChildren?: number
  ): Promise<JobNode<T>> {
    const id = String(jobData.id);
    const queueName = String(jobData.queue);
    const data = jobData.data as Record<string, unknown> | null;
    const name = typeof data?.name === 'string' ? data.name : 'default';
    const userData = extractUserDataFromInternal(data ?? {}) as T;
    const rawChildrenIds = data?.__childrenIds;
    const childrenIds = Array.isArray(rawChildrenIds) ? (rawChildrenIds as string[]) : [];
    const jobObj = createFlowJobObject(
      id,
      name,
      userData,
      queueName,
      this.buildCallbacks(queueName)
    );

    if (depth <= 0 || childrenIds.length === 0 || !this.tcp) return { job: jobObj };

    const childNodes: JobNode<T>[] = [];
    const childrenToFetch = maxChildren ? childrenIds.slice(0, maxChildren) : childrenIds;

    for (const childId of childrenToFetch) {
      const response = await this.tcp.send({ cmd: 'GetJob', id: childId });
      if (response.ok && response.job) {
        childNodes.push(
          await this.buildJobNodeFromTcp<T>(
            response.job as Record<string, unknown>,
            depth - 1,
            maxChildren
          )
        );
      }
    }

    return { job: jobObj, children: childNodes.length > 0 ? childNodes : undefined };
  }

  private async addFlowNode<T>(
    node: FlowJob<T>,
    parentRef: { id: string; queue: string } | null,
    createdJobIds: string[],
    flowOpts?: FlowOpts
  ): Promise<JobNode<T>> {
    const childNodes: JobNode<T>[] = [];
    const childIds: string[] = [];

    if (node.children && node.children.length > 0) {
      const tempParentRef = { id: 'pending', queue: node.queueName };
      // Siblings are independent — create them concurrently
      const results = await Promise.all(
        node.children.map((child) =>
          this.addFlowNode(child, tempParentRef, createdJobIds, flowOpts)
        )
      );
      for (const childNode of results) {
        childNodes.push(childNode);
        childIds.push(childNode.job.id);
      }
    }

    // Merge node opts with per-queue defaults from FlowOpts
    const queueDefaults = flowOpts?.queuesOptions?.[node.queueName];
    const mergedOpts = queueDefaults ? { ...queueDefaults, ...node.opts } : (node.opts ?? {});

    const jobData: Record<string, unknown> = {
      name: node.name,
      ...(node.data as object | undefined),
    };
    if (parentRef) {
      jobData.__parentId = parentRef.id;
      jobData.__parentQueue = parentRef.queue;
    }
    if (childIds.length > 0) jobData.__childrenIds = childIds;

    const jobIdStr = await pushJobWithParent(this.pushCtx, {
      queueName: node.queueName,
      data: jobData,
      opts: mergedOpts,
      parentRef,
      childIds,
    });
    createdJobIds.push(jobIdStr);

    const job = createFlowJobObject(
      jobIdStr,
      node.name,
      node.data as T,
      node.queueName,
      this.buildCallbacks(node.queueName)
    );

    return { job, children: childNodes.length > 0 ? childNodes : undefined };
  }

  private async addTreeNode<T>(
    step: FlowStep<T>,
    parentId: string | null,
    jobIds: string[]
  ): Promise<string> {
    const data = { name: step.name, __flowParentId: parentId, ...(step.data as object) };
    const id = await pushJob(
      this.pushCtx,
      step.queueName,
      data,
      step.opts ?? {},
      parentId ? [parentId] : undefined
    );
    jobIds.push(id);

    if (step.children) {
      await Promise.all(step.children.map((child) => this.addTreeNode(child, id, jobIds)));
    }
    return id;
  }
}
