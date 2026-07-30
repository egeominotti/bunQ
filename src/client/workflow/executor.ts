/** WorkflowExecutor - Core execution logic */
import type { Queue } from '../queue/queue';
import type { Workflow } from './workflow';
import { assertNoIndexCollision, assertNoDuplicateWaitFor } from './workflow';
import type { WorkflowStore } from './store';
import type { WorkflowEmitter } from './emitter';
import type {
  Execution,
  ExecutionListOptions,
  StepJobData,
  RunHandle,
  RecoverResult,
  WorkflowNode,
} from './types';
import { WaitForSignalError, runCompensation } from './compensator';
import {
  abandonParkedCompensation,
  resumeCompensation,
  type RollbackDeps,
} from './rollbackControl';
import { recoverExecutions } from './recovery';
import { clearTimers, scheduleTimeoutCheck } from './waitFor';
import type { WaitForDeps } from './waitFor';
import type { TimerHandle } from './clock';
import { describeError } from './identity';
import { claimKey, decideAdmission } from './admission';
import { signalExecution, startExecution } from './executorLifecycle';
import { executeWorkflowNode } from './executorNodes';
import { bindExecutionDefinition, WorkflowDefinitionMismatchError } from './definitionGuard';

export class WorkflowExecutor {
  private readonly workflows = new Map<string, Workflow>();
  private readonly timeoutTimers = new Map<string, TimerHandle>();

  /**
   * Release every armed waitFor timer. The engine owns the executor's lifetime, so
   * `Engine.close()` must call this: an armed timer would otherwise fire into a
   * closing queue, and a caller that keeps the process alive after closing one engine
   * has no other handle on them.
   */
  close(): void {
    clearTimers(this.timeoutTimers);
  }

  private readonly updateFn: (e: Execution) => void;

  constructor(
    private readonly store: WorkflowStore,
    private readonly queue: Queue,
    private readonly emitter: WorkflowEmitter | null = null
  ) {
    this.updateFn = (e) => {
      this.store.update(e);
    };
  }

  register(workflow: Workflow): void {
    if (this.workflows.has(workflow.name)) {
      throw new Error(`Workflow "${workflow.name}" is already registered`);
    }
    if (workflow.nodes.length === 0) {
      throw new Error(`Workflow "${workflow.name}" has no steps`);
    }
    const names = workflow.getStepNames();
    const dupes = names.filter((n, i) => names.indexOf(n) !== i);
    if (dupes.length > 0) {
      throw new Error(`Duplicate step names in "${workflow.name}": ${dupes.join(', ')}`);
    }
    assertNoIndexCollision(workflow);
    assertNoDuplicateWaitFor(workflow);
    const definitionHash = workflow.seal();
    const conflict = this.store
      .listActive(workflow.name)
      .find(
        (exec) =>
          exec.state !== 'compensation-stuck' &&
          exec.definitionHash !== undefined &&
          exec.definitionHash !== definitionHash
      );
    if (conflict) {
      throw new WorkflowDefinitionMismatchError(conflict.id, workflow.name);
    }
    this.workflows.set(workflow.name, workflow);
  }

  async start(
    workflowName: string,
    input: unknown,
    parentExecutionId?: string
  ): Promise<RunHandle> {
    return await startExecution(this.lifecycleDeps, workflowName, input, parentExecutionId);
  }

  /**
   * Nodes this process is currently executing, keyed `<execution>:<nodeIndex>`.
   *
   * The cursor guard below rejects a job for a node the run has already left, but not
   * a SECOND job for the node it is on right now: both carry the same index. That is
   * the reachable duplicate, because `recover()` re-enqueues the current node of every
   * `running` execution and is documented as callable on a live engine. Without this
   * claim the node runs twice and each copy advances the run independently, doubling
   * every side effect after it while the run still ends `completed`.
   *
   * A claim, not a queue-level dedup: a deterministic `jobId` was tried and could
   * swallow a LEGITIMATE later re-enqueue of the same node, wedging the run forever.
   * This drops only a duplicate that overlaps in time.
   */
  private readonly nodesInFlight = new Set<string>();

  async processStep(data: StepJobData): Promise<unknown> {
    // The admission DECISION lives in `admission.ts` as a pure function; this method
    // only carries it out. Delivery is at-least-once, so the same node job arrives
    // twice routinely, and when one of the three guards was missing a duplicate re-ran
    // the node and every node after it: two advance chains on one execution, doubled
    // side effects, and a final `completed` that hid it. That took a long model
    // campaign to find because the reasoning was buried in a method that also read
    // SQLite and dispatched work.
    const exec = this.store.get(data.executionId);
    const admission = decideAdmission(exec, data.nodeIndex, this.nodesInFlight);
    if (admission.kind === 'reject' || !exec) return null;
    const claim = claimKey(data.executionId, data.nodeIndex);
    this.nodesInFlight.add(claim);
    try {
      return await this.runNode(data, exec);
    } finally {
      this.nodesInFlight.delete(claim);
    }
  }

  private async runNode(data: StepJobData, exec: Execution): Promise<unknown> {
    const wf = this.workflows.get(exec.workflowName);
    if (!wf) throw new Error(`Workflow "${exec.workflowName}" not registered`);
    bindExecutionDefinition(exec, wf, this.updateFn);

    // If waiting, set back to running for timeout re-check
    if (exec.state === 'waiting') exec.state = 'running';

    const node = wf.nodes[data.nodeIndex] as WorkflowNode | undefined;
    if (!node) {
      exec.state = 'completed';
      this.store.update(exec);
      this.emitter?.emitWorkflow('workflow:completed', exec.id, exec.workflowName, 'completed');
      return null;
    }

    try {
      await executeWorkflowNode(this.nodeDeps, exec, node, data.nodeIndex, wf);
    } catch (err) {
      if (err instanceof WaitForSignalError) return null;
      exec.state = 'failed';
      // Why the run failed — kept distinct from what the rollback then did.
      exec.failureReason = describeError(err);
      // Deliberately UNGUARDED, unlike the writes on the throwing paths in `runner.ts` and
      // `runSubWorkflow`. A throw here skips `compensate()` below, which sounds worse and
      // is not: disk still says `running`, `listRecoverable()` covers `running`, so the
      // next `recover()` re-drives this node and the unwind happens then. Swallowing it
      // would instead leave a run that looks failed and was never rolled back, with
      // nothing scheduled to notice. Guard this only alongside a durable signal that the
      // rollback is still owed.
      this.store.update(exec);
      this.emitter?.emitWorkflow('workflow:failed', exec.id, exec.workflowName, 'failed');
      await this.compensate(exec, wf);
      throw err;
    }
    return null;
  }

  async signal(executionId: string, event: string, payload: unknown): Promise<void> {
    await signalExecution(this.lifecycleDeps, executionId, event, payload);
  }

  private get lifecycleDeps() {
    return {
      store: this.store,
      queue: this.queue,
      workflows: this.workflows,
      emitter: this.emitter,
      timers: this.timeoutTimers,
      enqueue: (exec: Execution) => this.enqueue(exec),
    };
  }

  /** Retry the compensation that parked the run, then finish the unwind. */
  async resumeCompensation(executionId: string): Promise<void> {
    await resumeCompensation(this.rollbackDeps, executionId);
  }

  /** Give up on a parked unwind, recording the outstanding steps as skipped. */
  abandonCompensation(executionId: string): void {
    abandonParkedCompensation(this.rollbackDeps, executionId);
  }

  private get rollbackDeps(): RollbackDeps {
    return {
      store: this.store,
      emitter: this.emitter,
      workflows: this.workflows,
    };
  }

  getExecution(id: string): Execution | null {
    return this.store.get(id);
  }
  listExecutions(
    wfName?: string,
    state?: Execution['state'],
    options?: ExecutionListOptions
  ): Execution[] {
    return this.store.list(wfName, state, options);
  }

  private get nodeDeps() {
    return {
      store: this.store,
      emitter: this.emitter,
      updateFn: this.updateFn,
      advance: (exec: Execution, nextIdx: number, wf: Workflow) => this.advance(exec, nextIdx, wf),
      start: (name: string, input: unknown, parentId: string) => this.start(name, input, parentId),
      enqueue: (exec: Execution) => this.enqueue(exec),
      waitFor: this.waitForDeps,
    };
  }

  private async advance(exec: Execution, nextIdx: number, wf: Workflow) {
    exec.currentNodeIndex = nextIdx;
    this.store.update(exec);
    if (nextIdx >= wf.nodes.length) {
      exec.state = 'completed';
      this.store.update(exec);
      this.emitter?.emitWorkflow('workflow:completed', exec.id, exec.workflowName, 'completed');
    } else {
      await this.enqueue(exec);
    }
  }

  private async enqueue(exec: Execution) {
    const jobData: StepJobData = {
      executionId: exec.id,
      workflowName: exec.workflowName,
      nodeIndex: exec.currentNodeIndex,
    };
    // Deliberately NO deterministic jobId here.
    //
    // `<execution>:<nodeIndex>` was tried, to let the queue's custom-id dedup collapse
    // a duplicate enqueue. It buys nothing the cursor guard in processStep does not
    // already provide (a duplicate job is ignored there), and it introduces a liveness
    // risk in exchange: if the custom-id entry outlives the job it names, a legitimate
    // re-enqueue of the same node is swallowed and the run wedges permanently. A
    // generated model campaign produced exactly one unexplained `execution wedged in
    // "running"` with it enabled and none without. A duplicate job that is ignored is
    // strictly safer than a missing job that never arrives.
    await this.queue.add('wf:step', jobData);
  }

  private get waitForDeps(): WaitForDeps {
    return {
      store: this.store,
      emitter: this.emitter,
      advance: (e, next, w) => this.advance(e, next, w),
      compensate: (e, w) => this.compensate(e, w),
      scheduleTimeoutCheck: (id, name, i, ms) => {
        this.scheduleTimeoutCheck(id, name, i, ms);
      },
    };
  }

  private scheduleTimeoutCheck(execId: string, workflowName: string, nodeIdx: number, ms: number) {
    scheduleTimeoutCheck(
      { queue: this.queue, timers: this.timeoutTimers },
      execId,
      workflowName,
      nodeIdx,
      ms
    );
  }

  private async compensate(exec: Execution, wf: Workflow) {
    await runCompensation(exec, wf, this.store, this.emitter, this.workflows);
  }

  /** Recover orphaned executions after a crash/restart */
  async recover(): Promise<RecoverResult> {
    return await recoverExecutions({
      store: this.store,
      queue: this.queue,
      workflows: this.workflows,
      emitter: this.emitter,
      timeoutTimers: this.timeoutTimers,
      nodesInFlight: this.nodesInFlight,
      scheduleTimeoutCheck: (id, name, idx, ms) => {
        this.scheduleTimeoutCheck(id, name, idx, ms);
      },
    });
  }
}
