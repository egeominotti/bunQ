/**
 * Engine - Public facade for the workflow engine
 * Manages lifecycle of internal Queue, Worker, and Store.
 */

import { Queue } from '../queue/queue';
import { Worker } from '../worker/worker';
import { WorkflowStore } from './store';
import { WorkflowExecutor } from './executor';
import { WorkflowEmitter } from './emitter';
import type { Workflow } from './workflow';
import type {
  EngineOptions,
  RunHandle,
  Execution,
  ExecutionState,
  ExecutionListOptions,
  RecoverResult,
  StepJobData,
  WorkflowEventType,
  WorkflowEventListener,
} from './types';

const DEFAULT_QUEUE_NAME = '__wf:steps';

export class Engine {
  private readonly queue: Queue;
  private readonly worker: Worker;
  private readonly store: WorkflowStore;
  private readonly executor: WorkflowExecutor;
  private readonly emitter: WorkflowEmitter;

  constructor(opts: EngineOptions = {}) {
    const queueName = opts.queueName ?? DEFAULT_QUEUE_NAME;

    this.queue = new Queue(queueName, {
      connection: opts.connection,
      embedded: opts.embedded,
      dataPath: opts.dataPath,
    });

    this.store = new WorkflowStore(opts.dataPath);
    this.emitter = new WorkflowEmitter();

    if (opts.onEvent) {
      this.emitter.onAny(opts.onEvent);
    }

    this.executor = new WorkflowExecutor(this.store, this.queue, this.emitter);

    this.worker = new Worker(
      queueName,
      async (job) => {
        const data = job.data as unknown as StepJobData;
        return await this.executor.processStep(data);
      },
      {
        connection: opts.connection,
        embedded: opts.embedded,
        dataPath: opts.dataPath,
        concurrency: opts.concurrency ?? 5,
      }
    );
  }

  /** Register a workflow definition */
  register(workflow: Workflow): this {
    this.executor.register(workflow);
    return this;
  }

  /** Start a new workflow execution */
  async start(workflowName: string, input?: unknown): Promise<RunHandle> {
    return await this.executor.start(workflowName, input);
  }

  /** Get execution state by ID */
  getExecution(id: string): Execution | null {
    return this.executor.getExecution(id);
  }

  /** List executions with optional filters */
  listExecutions(
    workflowName?: string,
    state?: ExecutionState,
    options?: ExecutionListOptions
  ): Execution[] {
    return this.executor.listExecutions(workflowName, state, options);
  }

  /**
   * Retry the compensation that parked a `compensation-stuck` run and continue the
   * unwind. Use once the cause of the failed reversal has been fixed.
   */
  async resumeCompensation(executionId: string): Promise<void> {
    return await this.executor.resumeCompensation(executionId);
  }

  /**
   * Abandon a parked unwind: the steps still un-compensated are recorded as skipped
   * and the run becomes terminal. Partial, but explicitly so.
   */
  /**
   * `async` on purpose, even though the work is synchronous: `resumeCompensation` is
   * async, and an operator writing recovery code under pressure reaches for
   * `Promise.allSettled([resume(id), abandon(id)])`. With a sync throw that expression
   * throws before `allSettled` is ever called, so the defensive form is the one that
   * blows up. Matching the sibling costs nothing while the API is experimental.
   */
  async abandonCompensation(executionId: string): Promise<void> {
    await Promise.resolve(this.executor.abandonCompensation(executionId));
  }

  /** Send a signal to a waiting execution */
  async signal(executionId: string, event: string, payload?: unknown): Promise<void> {
    return await this.executor.signal(executionId, event, payload);
  }

  /**
   * Recover orphaned executions after a crash/restart.
   * - 'running' executions: re-enqueued at their current step
   * - 'waiting' executions: timeout timers re-armed (or resumed if signal arrived)
   * - 'compensating' executions: compensation re-run (handlers must be idempotent)
   */
  async recover(): Promise<RecoverResult> {
    return await this.executor.recover();
  }

  // ============ Observability ============

  /** Subscribe to a specific workflow event type */
  on(type: WorkflowEventType, listener: WorkflowEventListener): this {
    this.emitter.on(type, listener);
    return this;
  }

  /** Subscribe to all workflow events */
  onAny(listener: WorkflowEventListener): this {
    this.emitter.onAny(listener);
    return this;
  }

  /** Unsubscribe from a specific event type */
  off(type: WorkflowEventType, listener: WorkflowEventListener): this {
    this.emitter.off(type, listener);
    return this;
  }

  /** Unsubscribe a catch-all listener */
  offAny(listener: WorkflowEventListener): this {
    this.emitter.offAny(listener);
    return this;
  }

  /** Subscribe to all events for a specific execution */
  subscribe(executionId: string, callback: WorkflowEventListener): () => void {
    const filter: WorkflowEventListener = (event) => {
      if (event.executionId === executionId) callback(event);
    };
    this.emitter.onAny(filter);
    return () => this.emitter.offAny(filter);
  }

  // ============ Cleanup ============

  /** Remove old completed/failed executions */
  cleanup(maxAgeMs: number, states?: ExecutionState[]): number {
    return this.store.cleanup(maxAgeMs, states);
  }

  /** Archive old executions to a separate table */
  archive(maxAgeMs: number, states?: ExecutionState[]): number {
    return this.store.archive(maxAgeMs, states);
  }

  /** Get archived execution count */
  getArchivedCount(): number {
    return this.store.getArchivedCount();
  }

  /** Shut down the engine */
  async close(force = false): Promise<void> {
    // Before the worker: a waitFor timer that fires during shutdown enqueues a step
    // job, and a queue closing underneath it turns an orderly shutdown into a rejected
    // add. Releasing the timers first makes the order irrelevant.
    this.executor.close(force);
    await this.worker.close(force);
    this.queue.close();
    this.store.close();
    this.emitter.removeAllListeners();
  }
}
