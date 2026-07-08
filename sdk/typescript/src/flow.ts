/**
 * FlowProducer: parent/children job trees, chains and fan-in flows.
 * Mirrors the official client's TCP flow logic (bottom-up creation,
 * UpdateParent fix-up, rollback via Cancel on failure).
 */

import { Connection } from './connection.js';
import { CommandError } from './errors.js';
import type {
  FlowJob,
  FlowProducerOptions,
  FlowStep,
  GetFlowOptions,
  JobNode,
} from './flow-types.js';
import { compact } from './frame.js';
import { Job } from './job.js';
import type { JobResponse, OkResponse } from './responses.js';
import { type JobOptions, jobPayload, wireJobOptions } from './types.js';

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

  /** Add a flow tree. Children are created (and processed) BEFORE their parent. */
  async add<T = unknown>(flow: FlowJob<T>): Promise<JobNode<T>> {
    const created: string[] = [];
    try {
      return await this.addNode(flow, null, created);
    } catch (err) {
      await this.rollback(created);
      throw err;
    }
  }

  async addBulk<T = unknown>(flows: FlowJob<T>[]): Promise<JobNode<T>[]> {
    const created: string[] = [];
    const results: JobNode<T>[] = [];
    try {
      for (const flow of flows) {
        results.push(await this.addNode(flow, null, created));
      }
      return results;
    } catch (err) {
      await this.rollback(created);
      throw err;
    }
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
    const jobIds: string[] = [];
    let prevId: string | null = null;
    try {
      for (const step of steps) {
        const data: Record<string, unknown> = compact({
          ...jobPayload(step.name, step.data),
          __flowParentId: prevId ?? undefined,
        });
        // Connection.call compacts internally, so no outer compact() is needed
        // (nesting it confused generic inference of `data`/`response`).
        const response: OkResponse = await this.connection.call<OkResponse>({
          cmd: 'PUSH',
          queue: step.queueName,
          data,
          ...wireJobOptions(step.opts),
          dependsOn: prevId ? [prevId] : undefined,
        });
        const id = String(response.id);
        jobIds.push(id);
        prevId = id;
      }
      return { jobIds };
    } catch (err) {
      await this.rollback(jobIds);
      throw err;
    }
  }

  /** Parallel jobs converging into a final fan-in job. */
  async addBulkThen<T = unknown>(
    parallel: FlowStep<T>[],
    final: FlowStep<T>
  ): Promise<{ parallelIds: string[]; finalId: string }> {
    const parallelIds: string[] = [];
    try {
      const results = await Promise.allSettled(
        parallel.map(async (step) => {
          const response = await this.connection.call<OkResponse>(
            compact({
              cmd: 'PUSH',
              queue: step.queueName,
              data: jobPayload(step.name, step.data),
              ...wireJobOptions(step.opts),
            }) as { cmd: string }
          );
          return String(response.id);
        })
      );
      const errors: unknown[] = [];
      for (const r of results) {
        if (r.status === 'fulfilled') parallelIds.push(r.value);
        else errors.push(r.reason);
      }
      if (errors.length > 0) {
        await this.rollback(parallelIds);
        throw errors[0] instanceof Error ? errors[0] : new Error(String(errors[0]));
      }

      const finalData = {
        ...jobPayload(final.name, final.data),
        __flowParentIds: parallelIds,
      };
      const finalId = await this.pushWithParent(
        final.queueName,
        finalData,
        final.opts,
        null,
        parallelIds
      );
      return { parallelIds, finalId };
    } catch (err) {
      await this.rollback(parallelIds);
      throw err;
    }
  }

  close(): void {
    if (this.ownsConnection) this.connection.close();
  }

  // ----------------------------------------------------------------- internals

  private async addNode<T>(
    node: FlowJob<T>,
    parentRef: { id: string; queue: string } | null,
    created: string[]
  ): Promise<JobNode<T>> {
    const childNodes: JobNode<T>[] = [];
    const childIds: string[] = [];

    if (node.children && node.children.length > 0) {
      const tempParent = { id: 'pending', queue: node.queueName };
      const results = await Promise.all(
        node.children.map((child) => this.addNode(child, tempParent, created))
      );
      for (const childNode of results) {
        childNodes.push(childNode);
        childIds.push(childNode.job.id);
      }
    }

    const jobData: Record<string, unknown> = jobPayload(node.name, node.data);
    if (parentRef) {
      jobData.__parentId = parentRef.id;
      jobData.__parentQueue = parentRef.queue;
    }
    if (childIds.length > 0) jobData.__childrenIds = childIds;

    const id = await this.pushWithParent(node.queueName, jobData, node.opts, parentRef, childIds);
    created.push(id);

    const job = new Job<T>({ id, queue: node.queueName, data: jobData }, this.connection);
    return { job, children: childNodes.length > 0 ? childNodes : undefined };
  }

  private async pushWithParent(
    queue: string,
    data: Record<string, unknown>,
    opts: JobOptions | undefined,
    parentRef: { id: string; queue: string } | null,
    childIds: string[]
  ): Promise<string> {
    const response = await this.connection.call<OkResponse>(
      compact({
        cmd: 'PUSH',
        queue,
        data,
        ...wireJobOptions(opts),
        parentId: parentRef?.id,
        childrenIds: childIds.length > 0 ? childIds : undefined,
        dependsOn: childIds.length > 0 ? childIds : undefined,
      }) as { cmd: string }
    );
    const parentJobId = String(response.id);

    // fix up children with the real parent id (like the official client)
    for (const childId of childIds) {
      await this.connection.call({ cmd: 'UpdateParent', childId, parentId: parentJobId });
    }
    return parentJobId;
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

  private async rollback(jobIds: string[]): Promise<void> {
    await Promise.all(
      jobIds.map(async (id) => {
        try {
          await this.connection.call({ cmd: 'Cancel', id });
        } catch {
          // ignore cleanup errors
        }
      })
    );
  }
}
