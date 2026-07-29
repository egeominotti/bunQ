/** WorkflowExecutor - Core execution logic */
import type { Queue } from '../queue/queue';
import type { Workflow } from './workflow';
import { assertNoIndexCollision, assertNoDuplicateWaitFor, unusableEventName } from './workflow';
import type { WorkflowStore } from './store';
import type { WorkflowEmitter } from './emitter';
import type {
  Execution,
  StepJobData,
  RunHandle,
  RecoverResult,
  WorkflowNode,
  StepDefinition,
} from './types';
import {
  executeStepWithRetry,
  executeParallelSteps,
  executeSubWorkflow,
  buildContext,
} from './runner';
import { executeDoUntil, executeDoWhile, executeForEach, executeMap } from './loops';
import { WaitForSignalError, runCompensation } from './compensator';
import {
  abandonParkedCompensation,
  resumeCompensation,
  type RollbackDeps,
} from './rollbackControl';
import { recoverExecutions } from './recovery';
import { clearTimers, runWaitFor, scheduleTimeoutCheck } from './waitFor';
import type { WaitForDeps } from './waitFor';
import { clock, type TimerHandle } from './clock';
import { describeError } from './identity';
import { claimKey, decideAdmission } from './admission';

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
    const names = workflow.getStepNames();
    const dupes = names.filter((n, i) => names.indexOf(n) !== i);
    if (dupes.length > 0) {
      throw new Error(`Duplicate step names in "${workflow.name}": ${dupes.join(', ')}`);
    }
    assertNoIndexCollision(workflow);
    assertNoDuplicateWaitFor(workflow);
    this.workflows.set(workflow.name, workflow);
  }

  async start(
    workflowName: string,
    input: unknown,
    parentExecutionId?: string
  ): Promise<RunHandle> {
    const wf = this.workflows.get(workflowName);
    if (!wf) throw new Error(`Workflow "${workflowName}" not registered`);
    if (wf.nodes.length === 0) throw new Error(`Workflow "${workflowName}" has no steps`);
    const now = clock().now();
    const id = `wf_${now}_${clock().random().toString(36).slice(2, 10)}`;
    const exec: Execution = {
      id,
      workflowName,
      state: 'running',
      input,
      steps: {},
      currentNodeIndex: 0,
      signals: {},
      ...(parentExecutionId ? { parentExecutionId } : {}),
      createdAt: now,
      updatedAt: now,
    };
    this.store.save(exec);
    this.emitter?.emitWorkflow('workflow:started', id, workflowName, 'running', { input });
    await this.enqueue(exec);
    return { id, workflowName };
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
    // If waiting, set back to running for timeout re-check
    if (exec.state === 'waiting') exec.state = 'running';

    const wf = this.workflows.get(exec.workflowName);
    if (!wf) throw new Error(`Workflow "${exec.workflowName}" not registered`);

    const node = wf.nodes[data.nodeIndex] as WorkflowNode | undefined;
    if (!node) {
      exec.state = 'completed';
      this.store.update(exec);
      this.emitter?.emitWorkflow('workflow:completed', exec.id, exec.workflowName, 'completed');
      return null;
    }

    try {
      await this.executeNode(exec, node, data.nodeIndex, wf);
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
    // Same predicate as registration. A name that cannot be stored has to fail here
    // too: a caller who signals it would otherwise get a clean return for a delivery
    // that went nowhere.
    const bad = unusableEventName(event);
    if (bad) throw new Error(`Cannot signal an event ${bad}`);

    // A finished run cannot receive anything. Accepting it wrote the payload into the
    // persisted row and emitted `signal:received`, so a dashboard reported an approval
    // against a run that had already ended and a closed audit record was mutated after
    // the fact, while the caller got a clean return for a delivery that did nothing.
    // A signal racing a run to its end is real, and rejecting is how the caller finds
    // out (`test/repro-workflow-operator-signal.test.ts`).
    const current = this.store.get(executionId);
    if (current && current.state !== 'running' && current.state !== 'waiting') {
      throw new Error(
        `Execution "${executionId}" is "${current.state}" and cannot receive the signal "${event}"`
      );
    }
    const timer = this.timeoutTimers.get(executionId);
    if (timer) {
      clock().clearTimeout(timer);
      this.timeoutTimers.delete(executionId);
    }
    // Record the payload and claim the resume in a single transaction that touches
    // only the `signals` and `state` columns. Writing through the store — rather than
    // mutating a snapshot and calling update() — is what keeps a concurrently
    // executing step from overwriting the payload with its own stale `signals`
    // (test/repro-workflow-signal-lost-update.test.ts).
    //
    // The claim is a conditional `state = 'waiting'` UPDATE, so duplicate or
    // concurrent signals collapse to exactly one resume and every step after the
    // waitFor runs exactly once. A signal that lands before the run parks records
    // its payload and returns; runWaitFor then consumes it via parkForSignal().
    const outcome = this.store.recordSignal(executionId, event, payload);
    if (!outcome.found) throw new Error(`Execution "${executionId}" not found`);

    this.emitter?.emitSignal('signal:received', executionId, outcome.workflowName, event, payload);
    if (!outcome.resumed) return;

    await this.queue.add('wf:step', {
      executionId,
      workflowName: outcome.workflowName,
      nodeIndex: outcome.currentNodeIndex,
    } satisfies StepJobData);
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
  listExecutions(wfName?: string, state?: Execution['state']): Execution[] {
    return this.store.list(wfName, state);
  }

  private async executeNode(
    exec: Execution,
    node: WorkflowNode,
    idx: number,
    wf: Workflow
  ): Promise<void> {
    if (node.type === 'step') await this.runStep(exec, node.def, idx, wf);
    else if (node.type === 'branch') await this.runBranch(exec, node, idx, wf);
    else if (node.type === 'parallel') await this.runParallel(exec, node, idx, wf);
    else if (node.type === 'subWorkflow') await this.runSubWorkflow(exec, node, idx, wf);
    else if (node.type === 'doUntil') await this.runBody(exec, idx, wf, executeDoUntil, node.def);
    else if (node.type === 'doWhile') await this.runBody(exec, idx, wf, executeDoWhile, node.def);
    else if (node.type === 'forEach') await this.runBody(exec, idx, wf, executeForEach, node.def);
    else if (node.type === 'map') await this.runBody(exec, idx, wf, executeMap, node.def);
    else if (node.type === 'pivot') await this.runPivot(exec, idx, wf);
    else await runWaitFor(this.waitForDeps, exec, node, idx, wf);
  }

  private async runStep(exec: Execution, def: StepDefinition, idx: number, wf: Workflow) {
    const ctx = buildContext(exec);
    await executeStepWithRetry(def, ctx, exec, { emitter: this.emitter, updateFn: this.updateFn });
    await this.advance(exec, idx + 1, wf);
  }

  private async runBranch(
    exec: Execution,
    node: Extract<WorkflowNode, { type: 'branch' }>,
    idx: number,
    wf: Workflow
  ) {
    const pathName = node.def.condition(buildContext(exec));
    const pathSteps = node.def.paths.get(pathName);
    if (pathSteps && pathSteps.length > 0) {
      for (const step of pathSteps) {
        await executeStepWithRetry(step, buildContext(exec), exec, {
          emitter: this.emitter,
          updateFn: this.updateFn,
        });
      }
    }
    await this.advance(exec, idx + 1, wf);
  }

  private async runParallel(
    exec: Execution,
    node: Extract<WorkflowNode, { type: 'parallel' }>,
    idx: number,
    wf: Workflow
  ) {
    await executeParallelSteps(
      node.def.steps,
      buildContext(exec),
      exec,
      this.emitter,
      this.updateFn
    );
    await this.advance(exec, idx + 1, wf);
  }

  private async runSubWorkflow(
    exec: Execution,
    node: Extract<WorkflowNode, { type: 'subWorkflow' }>,
    idx: number,
    wf: Workflow
  ) {
    const subInput = node.inputMapper(buildContext(exec));
    const recordKey = `sub:${node.name}`;
    try {
      const { results, executionId } = await executeSubWorkflow(
        node.name,
        subInput,
        // The child must record who owns it: without this, recovery treats it as a
        // top-level run and drives it behind this parent's back.
        async (name, input) => {
          const handle = await this.start(name, input, exec.id);
          // Claim it in the parent's record BEFORE waiting on it. The record used to be
          // written only on completion, so a re-entry after a restart found nothing to
          // resume and started a second child.
          //
          // Awaited rather than done on a side branch: a detached `.then()` on a start
          // that REJECTS, which is what an unregistered child does, leaves an unhandled
          // rejection even though the caller handles the same rejection properly.
          exec.steps[recordKey] = { status: 'running', childExecutionId: handle.id };
          this.store.update(exec);
          return handle;
        },
        (id) => this.store.get(id),
        undefined,
        exec.steps[recordKey]?.childExecutionId
      );
      exec.steps[recordKey] = {
        status: 'completed',
        result: results,
        completedAt: clock().now(),
        childExecutionId: executionId,
      };
    } catch (error) {
      // Settle the record before letting the failure through.
      //
      // Only the success write existed, so every interesting outcome — the child failed,
      // parked in `compensation-stuck`, or timed out — left the record `running`.
      // `unwindSet` drops anything that is neither `completed` nor `failed` one line
      // before the `sub:` branch can admit it, so `unwindChild` was never called: a
      // parent whose child was parked with stock still reserved reversed its own steps,
      // reached the end of the pass and reported `rollbackStatus: 'completed'`
      // (`test/repro-workflow-child-park-inherit.test.ts`).
      //
      // It is also the truthful record on its own terms. A `sub:` step left `running`
      // after the parent has failed reads on a dashboard as a child still in flight.
      const claimed = exec.steps[recordKey];
      if (claimed) {
        exec.steps[recordKey] = {
          ...claimed,
          status: 'failed',
          error: describeError(error),
          completedAt: clock().now(),
        };
        // Guarded for the same reason as the failure write in `runner.ts`: `error` below
        // carries the real cause, and letting a write failure propagate instead replaced
        // it. The child's own diagnostic is what the rollback guide's field table points
        // an operator at, `... timed out` or `... is parked mid-rollback; resolve it with
        // resumeCompensation or abandonCompensation`, so destroying it takes away the
        // pointer to the child.
        //
        // This one does not self-heal, which is what made it worth guarding rather than
        // commenting: the next write succeeds, persists the wrong diagnostic, and the run
        // goes terminal carrying it
        // (`test/repro-workflow-step-error-masking.test.ts`).
        try {
          this.store.update(exec);
        } catch {
          // Deliberately swallowed; the in-memory record still settles the `sub:` step, and
          // `runNode` re-persists the whole execution one frame up.
        }
      }
      throw error;
    }
    await this.advance(exec, idx + 1, wf);
  }

  /**
   * Commit the saga. From here the run can still fail, but it can no longer be
   * rolled back: recovery past this point is forward-only.
   */
  private async runPivot(exec: Execution, idx: number, wf: Workflow) {
    exec.committedAt = idx;
    this.store.update(exec);
    await this.advance(exec, idx + 1, wf);
  }

  /**
   * Run a node body that manages its own step records (loops, forEach, map), then
   * move on. These differ only in which executor they delegate to.
   */
  private async runBody<TDef>(
    exec: Execution,
    idx: number,
    wf: Workflow,
    run: (
      def: TDef,
      exec: Execution,
      emitter: WorkflowEmitter | null,
      updateFn: (e: Execution) => void
    ) => Promise<void>,
    def: TDef
  ) {
    await run(def, exec, this.emitter, this.updateFn);
    await this.advance(exec, idx + 1, wf);
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
    return recoverExecutions({
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
