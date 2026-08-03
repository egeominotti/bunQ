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
import { createFlowJobObject, type FlowJobCallbacks } from './flowJobFactory';
import type { PushContext } from './flowPush';
import * as managementOps from './queue/operations/management';
import { commitFlow } from './flowAtomic';
import { assertFlowTcpOk } from './flowJobTypes';
import { planFlows, type PlannedFlowNode } from './flowPlan';
import { planBulkThen, planChain, planTree } from './flowLegacyPlan';
import { readFlow } from './flowReader';
import {
  getParentResult as readParentResult,
  getParentResults as readParentResults,
  type RuntimeResult,
} from './flowResults';

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
  closing: Promise<void> | null = null;
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
          maxCommandTimeouts: connOpts.maxCommandTimeouts,
          tls: connOpts.tls,
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
          maxCommandTimeouts: connOpts.maxCommandTimeouts,
          tls: connOpts.tls,
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
    if (this.closing) return this.closing;
    this.closing = Promise.resolve().then(() => {
      if (this.tcp && !this.useSharedPool) this.tcp.close();
      else if (this.tcp) releaseSharedPool(this.tcp);
    });
    return this.closing;
  }

  /** Disconnect from the server (BullMQ v5 compatible). Alias for close(). */
  disconnect(): Promise<void> {
    return this.close();
  }

  /** Wait until the FlowProducer is ready (BullMQ v5 compatible). */
  async waitUntilReady(): Promise<void> {
    if (this.embedded) return;
    if (this.tcp) assertFlowTcpOk(await this.tcp.send({ cmd: 'Ping' }), 'Ping');
  }

  // ============================================================================
  // BullMQ v5 Compatible Methods
  // ============================================================================

  /** Add a flow (BullMQ v5 compatible). Children are processed BEFORE their parent. */
  async add<T = unknown>(flow: FlowJob<T>, opts?: FlowOpts): Promise<JobNode<T>> {
    const plan = planFlows([flow], opts);
    const snapshots = await commitFlow(this.pushCtx, plan.batch);
    return this.buildPlannedNode(plan.roots[0], this.indexSnapshots(snapshots));
  }

  /** Add multiple flows (BullMQ v5 compatible). */
  async addBulk<T = unknown>(flows: FlowJob<T>[]): Promise<JobNode<T>[]> {
    const plan = planFlows(flows);
    const snapshots = await commitFlow(this.pushCtx, plan.batch);
    const snapshotsById = this.indexSnapshots(snapshots);
    return plan.roots.map((root) => this.buildPlannedNode(root, snapshotsById));
  }

  /** Get a flow tree starting from a job (BullMQ v5 compatible). */
  // biome-ignore lint/suspicious/useAwait: preserves the public Promise return contract.
  async getFlow<T = unknown>(opts: GetFlowOpts): Promise<JobNode<T> | null> {
    return readFlow<T>(
      {
        embedded: this.embedded,
        tcp: this.tcp,
        buildCallbacks: (queueName) => this.buildCallbacks(queueName),
      },
      opts
    );
  }

  // ============================================================================
  // Legacy bunqueue API Methods
  // ============================================================================

  /** Add a chain of jobs. Jobs execute sequentially: step[0] → step[1] → ... */
  async addChain<T = unknown>(steps: FlowStep<T>[]): Promise<FlowResult> {
    if (steps.length === 0) return { jobIds: [] };
    const plan = planChain(steps);
    await commitFlow(this.pushCtx, plan.batch);
    return { jobIds: plan.ids.map(String) };
  }

  /** Add parallel jobs that converge to a final job. */
  async addBulkThen<T = unknown>(
    parallel: FlowStep<T>[],
    final: FlowStep<T>
  ): Promise<{ parallelIds: string[]; finalId: string }> {
    const plan = planBulkThen(parallel, final);
    await commitFlow(this.pushCtx, plan.batch);
    return {
      parallelIds: plan.parallelIds.map(String),
      finalId: String(plan.finalId),
    };
  }

  /** Add a tree of jobs where children depend on parent. */
  async addTree<T = unknown>(root: FlowStep<T>): Promise<FlowResult> {
    const plan = planTree(root);
    await commitFlow(this.pushCtx, plan.batch);
    return { jobIds: plan.ids.map(String) };
  }

  /** Get the result of a completed parent job in embedded or TCP mode. */
  // eslint-disable-next-line @typescript-eslint/no-unnecessary-type-parameters
  getParentResult<R = unknown>(parentId: string): RuntimeResult<R | undefined> {
    return readParentResult<R>({ embedded: this.embedded, tcp: this.tcp }, parentId);
  }

  /** Get results from multiple parent jobs in input order in either runtime. */
  getParentResults<R = unknown>(parentIds: string[]): RuntimeResult<Map<string, R>> {
    return readParentResults<R>({ embedded: this.embedded, tcp: this.tcp }, parentIds);
  }

  // ============================================================================
  // Private Methods
  // ============================================================================

  /** Build callbacks that wire flow job methods to actual management operations */
  private buildCallbacks(queueName: string): FlowJobCallbacks {
    const ctx = { name: queueName, embedded: this.embedded, tcp: this.tcp };
    if (!this.embedded) {
      return {
        embedded: false,
        tcp: this.tcp,
        getState: (id) => {
          if (!this.tcp) return Promise.resolve('unknown');
          return this.tcp.send({ cmd: 'GetState', id }).then((response) => {
            if (response.ok !== true) {
              throw new Error(
                typeof response.error === 'string' ? response.error : 'GetState failed'
              );
            }
            return typeof response.state === 'string' ? response.state : 'unknown';
          });
        },
      };
    }
    return {
      embedded: true,
      updateData: (id, data) => managementOps.updateJobData(ctx, id, data),
      updateProgress: (id, progress) => managementOps.updateJobProgress(ctx, id, progress),
      log: (id, msg) => managementOps.addJobLog(ctx, id, msg).then(() => undefined),
      promote: (id) => managementOps.promoteJob(ctx, id),
      remove: (id) => managementOps.removeAsync(ctx, id),
      changePriority: (id, opts) => managementOps.changeJobPriority(ctx, id, opts),
      changeDelay: (id, d) => managementOps.changeJobDelay(ctx, id, d),
      clearLogs: (id, keepLogs) => managementOps.clearJobLogs(ctx, id, keepLogs),
      retry: (id) => managementOps.retryJob(ctx, id),
      getState: (id) => getSharedManager().getJobState(jobId(id)),
    };
  }

  private buildPlannedNode<T>(
    node: PlannedFlowNode<T>,
    snapshots: ReadonlyMap<string, DomainJob>
  ): JobNode<T> {
    const snapshot = snapshots.get(String(node.id));
    if (!snapshot) throw new Error(`Committed flow snapshot missing for ${String(node.id)}`);
    const children = node.children?.map((child) => this.buildPlannedNode(child, snapshots));
    return {
      job: createFlowJobObject(String(node.id), node.name, node.data as T, node.queueName, {
        callbacks: this.buildCallbacks(node.queueName),
        snapshot,
      }),
      children: children && children.length > 0 ? children : undefined,
    };
  }

  private indexSnapshots(snapshots: DomainJob[]): Map<string, DomainJob> {
    return new Map(snapshots.map((snapshot) => [String(snapshot.id), snapshot]));
  }
}
