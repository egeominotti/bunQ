/**
 * FlowProducer: atomic parent/children trees, chains and fan-in flows.
 */

import { Connection } from './connection.js';
import { CommandError } from './errors.js';
import { commitFlow } from './flow-commit.js';
import { type PlannedFlowNode, planFlows } from './flow-plan.js';
import { planBulkThen, planChain } from './flow-plan-legacy.js';
import type {
  FlowJob,
  FlowOptions,
  FlowProducerOptions,
  FlowStep,
  GetFlowOptions,
  JobNode,
} from './flow-types.js';
import { Job } from './job.js';
import type { JobResponse } from './responses.js';

export class FlowProducer {
  readonly connection: Connection;
  private readonly ownsConnection: boolean;

  constructor(opts: FlowProducerOptions = {}) {
    this.connection =
      opts.connection ??
      new Connection({
        host: opts.host,
        port: opts.port,
        token: opts.token,
        tls: opts.tls,
        logger: opts.logger,
        onTelemetry: opts.onTelemetry,
      });
    this.ownsConnection = opts.connection === undefined;
  }

  /** Add a flow tree in one broker-side atomic commit. */
  async add<T = unknown>(flow: FlowJob<T>, options?: FlowOptions): Promise<JobNode<T>> {
    const plan = planFlows([flow], options);
    const snapshots = await commitFlow(this.connection, plan.jobs);
    return this.buildNode(plan.roots[0], snapshots);
  }

  async addBulk<T = unknown>(flows: FlowJob<T>[]): Promise<JobNode<T>[]> {
    const plan = planFlows(flows);
    const snapshots = await commitFlow(this.connection, plan.jobs);
    return plan.roots.map((root) => this.buildNode(root, snapshots));
  }

  /** Fetch a flow tree starting from a job id (recursive over childrenIds). */
  getFlow<T = unknown>(opts: GetFlowOptions): Promise<JobNode<T> | null> {
    return this.fetchNode<T>(
      opts.id,
      opts.depth ?? Number.POSITIVE_INFINITY,
      opts.maxChildren,
      new Set()
    );
  }

  /** Add a sequential chain: step[0] → step[1] → ... via dependsOn. */
  async addChain<T = unknown>(steps: FlowStep<T>[]): Promise<{ jobIds: string[] }> {
    if (steps.length === 0) return { jobIds: [] };
    const plan = planChain(steps);
    await commitFlow(this.connection, plan.jobs);
    return { jobIds: plan.ids };
  }

  /** Parallel jobs converging into a final fan-in job. */
  async addBulkThen<T = unknown>(
    parallel: FlowStep<T>[],
    final: FlowStep<T>
  ): Promise<{ parallelIds: string[]; finalId: string }> {
    const plan = planBulkThen(parallel, final);
    await commitFlow(this.connection, plan.jobs);
    return { parallelIds: plan.parallelIds, finalId: plan.finalId };
  }

  close(): void {
    if (this.ownsConnection) this.connection.close();
  }

  // ----------------------------------------------------------------- internals

  private buildNode<T>(
    node: PlannedFlowNode<T>,
    snapshots: ReadonlyMap<string, Record<string, unknown>>
  ): JobNode<T> {
    const snapshot = snapshots.get(node.id);
    if (!snapshot) throw new Error(`Committed flow snapshot missing for ${node.id}`);
    const children = node.children?.map((child) => this.buildNode(child, snapshots));
    return {
      job: new Job<T>(snapshot, this.connection),
      children: children && children.length > 0 ? children : undefined,
    };
  }

  private async fetchNode<T>(
    id: string,
    depth: number,
    maxChildren: number | undefined,
    visited: Set<string>
  ): Promise<JobNode<T> | null> {
    if (visited.has(id)) return null; // cycle guard: id already on the current path
    visited.add(id);
    // A missing job — the root, or a child removed via removeOnComplete/cancel
    // (childrenIds is a static push-time list, never pruned) — yields null and
    // is skipped, returning the surviving partial tree instead of throwing.
    let response: JobResponse;
    try {
      response = await this.connection.call<JobResponse>({ cmd: 'GetJob', id });
    } catch (err) {
      // Only 'Job not found' means a removed node; a real server error must not
      // masquerade as a missing child and yield a misleading partial tree.
      if (err instanceof CommandError && /not found/i.test(err.message)) return null;
      throw err;
    }
    const raw = response.job;
    if (!raw) return null;
    const job = new Job<T>(raw, this.connection);
    if (depth <= 0 || job.childrenIds.length === 0) return { job };

    const limit = maxChildren ?? job.childrenIds.length;
    const children: JobNode<T>[] = [];
    for (const childId of job.childrenIds.slice(0, limit)) {
      const child = await this.fetchNode<T>(childId, depth - 1, maxChildren, visited);
      if (child) children.push(child);
    }
    return { job, children: children.length > 0 ? children : undefined };
  }
}
