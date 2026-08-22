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
import { isWorkflowExecutionClosed, WorkflowExecutionFence } from './executionFence';
import { enqueueWorkflowStep } from './executorQueue';

export class WorkflowExecutor {
  private readonly workflows = new Map<string, Workflow>();
  private readonly timeoutTimers = new Map<string, TimerHandle>();
  private readonly fence = new WorkflowExecutionFence();

  /**
   * Release every armed waitFor timer. The engine owns the executor's lifetime, so
   * `Engine.close()` must call this: an armed timer would otherwise fire into a
   * closing queue, and a caller that keeps the process alive after closing one engine
   * has no other handle on them.
   */
  close(force = false): void {
    this.fence.close(force);
    clearTimers(this.timeoutTimers);
  }

  private readonly updateFn: (e: Execution) => void;

  constructor(
    private readonly store: WorkflowStore,
    private readonly queue: Queue,
    private readonly emitter: WorkflowEmitter | null = null
  ) {
    this.updateFn = (e) => {
      this.fence.assertActive();
      this.store.update(e);
    };
  }

  register(workflow: Workflow): void {
    this.fence.assertActive();
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
    this.fence.assertActive();
    return await startExecution(this.lifecycleDeps, workflowName, input, parentExecutionId);
  }

  /**
   * Nodes this process is currently executing, keyed `<execution>:<nodeIndex>`.
   *
   * The cursor rejects stale jobs but not two overlapping deliveries for the current
   * node. This process-local claim drops that overlap. Queue-level dedup is unsuitable:
   * a retained ID can swallow a legitimate recovery enqueue and wedge the run.
   */
  private readonly nodesInFlight = new Set<string>();

  async processStep(data: StepJobData): Promise<unknown> {
    if (!this.fence.isActive()) return null;
    // Admission is pure; this method only owns the in-flight claim and dispatch.
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
      this.fence.assertActive();
      exec.state = 'completed';
      this.updateFn(exec);
      this.emitter?.emitWorkflow('workflow:completed', exec.id, exec.workflowName, 'completed');
      return null;
    }

    try {
      await executeWorkflowNode(this.nodeDeps, exec, node, data.nodeIndex, wf);
    } catch (err) {
      if (!this.fence.isActive() || isWorkflowExecutionClosed(err)) return null;
      if (err instanceof WaitForSignalError) return null;
      this.fence.assertActive();
      exec.state = 'failed';
      // Why the run failed — kept distinct from what the rollback then did.
      exec.failureReason = describeError(err);
      // A failed transition must persist before compensation starts. If the guarded
      // write fails, recovery sees the old running row and re-drives this node.
      this.updateFn(exec);
      this.emitter?.emitWorkflow('workflow:failed', exec.id, exec.workflowName, 'failed');
      if (!this.fence.isActive()) return null;
      await this.compensate(exec, wf);
      throw err;
    }
    return null;
  }

  async signal(executionId: string, event: string, payload: unknown): Promise<void> {
    this.fence.assertActive();
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
      assertActive: this.fence.assertActive,
    };
  }

  /** Retry the compensation that parked the run, then finish the unwind. */
  async resumeCompensation(executionId: string): Promise<void> {
    this.fence.assertActive();
    await resumeCompensation(this.rollbackDeps, executionId);
  }

  /** Give up on a parked unwind, recording the outstanding steps as skipped. */
  abandonCompensation(executionId: string): void {
    this.fence.assertActive();
    abandonParkedCompensation(this.rollbackDeps, executionId);
  }

  private get rollbackDeps(): RollbackDeps {
    return {
      store: this.store,
      emitter: this.emitter,
      workflows: this.workflows,
      assertActive: this.fence.assertActive,
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
      assertActive: this.fence.assertActive,
    };
  }

  private async advance(exec: Execution, nextIdx: number, wf: Workflow) {
    this.fence.assertActive();
    exec.currentNodeIndex = nextIdx;
    this.updateFn(exec);
    if (nextIdx >= wf.nodes.length) {
      exec.state = 'completed';
      this.updateFn(exec);
      this.emitter?.emitWorkflow('workflow:completed', exec.id, exec.workflowName, 'completed');
    } else {
      await this.enqueue(exec);
    }
  }

  private async enqueue(exec: Execution) {
    await enqueueWorkflowStep(this.queue, exec, this.fence.assertActive);
  }

  private get waitForDeps(): WaitForDeps {
    return {
      store: this.store,
      emitter: this.emitter,
      advance: (e, next, w) => this.advance(e, next, w),
      compensate: (e, w) => this.compensate(e, w),
      updateFn: this.updateFn,
      assertActive: this.fence.assertActive,
      scheduleTimeoutCheck: (id, name, i, ms) => {
        this.scheduleTimeoutCheck(id, name, i, ms);
      },
    };
  }

  private scheduleTimeoutCheck(execId: string, workflowName: string, nodeIdx: number, ms: number) {
    scheduleTimeoutCheck(
      {
        queue: this.queue,
        timers: this.timeoutTimers,
        assertActive: this.fence.assertActive,
        isActive: () => this.fence.isActive(),
      },
      execId,
      workflowName,
      nodeIdx,
      ms
    );
  }

  private async compensate(exec: Execution, wf: Workflow) {
    this.fence.assertActive();
    await runCompensation(exec, wf, this.store, this.emitter, this.workflows, {
      assertActive: this.fence.assertActive,
    });
  }

  /** Recover orphaned executions after a crash/restart */
  async recover(): Promise<RecoverResult> {
    this.fence.assertActive();
    return await recoverExecutions({
      store: this.store,
      queue: this.queue,
      workflows: this.workflows,
      emitter: this.emitter,
      timeoutTimers: this.timeoutTimers,
      nodesInFlight: this.nodesInFlight,
      assertActive: this.fence.assertActive,
      scheduleTimeoutCheck: (id, name, idx, ms) => {
        this.scheduleTimeoutCheck(id, name, idx, ms);
      },
    });
  }
}
