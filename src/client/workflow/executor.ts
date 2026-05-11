/** WorkflowExecutor - Core execution logic */
import type { Queue } from '../queue/queue';
import type { Workflow } from './workflow';
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
import { recoverExecutions } from './recovery';

export class WorkflowExecutor {
  private readonly workflows = new Map<string, Workflow>();
  private readonly timeoutTimers = new Map<string, ReturnType<typeof setTimeout>>();

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
    this.workflows.set(workflow.name, workflow);
  }

  async start(workflowName: string, input: unknown): Promise<RunHandle> {
    const wf = this.workflows.get(workflowName);
    if (!wf) throw new Error(`Workflow "${workflowName}" not registered`);
    if (wf.nodes.length === 0) throw new Error(`Workflow "${workflowName}" has no steps`);
    const now = Date.now();
    const id = `wf_${now}_${Math.random().toString(36).slice(2, 10)}`;
    const exec: Execution = {
      id,
      workflowName,
      state: 'running',
      input,
      steps: {},
      currentNodeIndex: 0,
      signals: {},
      createdAt: now,
      updatedAt: now,
    };
    this.store.save(exec);
    this.emitter?.emitWorkflow('workflow:started', id, workflowName, 'running', { input });
    await this.enqueue(exec);
    return { id, workflowName };
  }

  async processStep(data: StepJobData): Promise<unknown> {
    const exec = this.store.get(data.executionId);
    if (!exec || (exec.state !== 'running' && exec.state !== 'waiting')) return null;
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
      this.store.update(exec);
      this.emitter?.emitWorkflow('workflow:failed', exec.id, exec.workflowName, 'failed');
      await this.compensate(exec, wf);
      throw err;
    }
    return null;
  }

  async signal(executionId: string, event: string, payload: unknown): Promise<void> {
    const exec = this.store.get(executionId);
    if (!exec) throw new Error(`Execution "${executionId}" not found`);
    const timer = this.timeoutTimers.get(executionId);
    if (timer) {
      clearTimeout(timer);
      this.timeoutTimers.delete(executionId);
    }
    exec.signals[event] = payload;
    exec.state = 'running';
    this.store.update(exec);
    this.emitter?.emitSignal('signal:received', exec.id, exec.workflowName, event, payload);
    await this.enqueue(exec);
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
    else if (node.type === 'doUntil') await this.runLoop(exec, node, idx, wf, executeDoUntil);
    else if (node.type === 'doWhile') await this.runLoop(exec, node, idx, wf, executeDoWhile);
    else if (node.type === 'forEach') await this.runForEach(exec, node, idx, wf);
    else if (node.type === 'map') await this.runMap(exec, node, idx, wf);
    else await this.runWaitFor(exec, node, idx, wf);
  }

  private async runStep(exec: Execution, def: StepDefinition, idx: number, wf: Workflow) {
    const ctx = buildContext(exec);
    await executeStepWithRetry(def, ctx, exec, this.emitter, this.updateFn);
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
        await executeStepWithRetry(step, buildContext(exec), exec, this.emitter, this.updateFn);
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
    const result = await executeSubWorkflow(
      node.name,
      subInput,
      (name, input) => this.start(name, input),
      (id) => this.store.get(id)
    );
    exec.steps[`sub:${node.name}`] = { status: 'completed', result, completedAt: Date.now() };
    await this.advance(exec, idx + 1, wf);
  }

  private async runWaitFor(
    exec: Execution,
    node: Extract<WorkflowNode, { type: 'waitFor' }>,
    idx: number,
    wf: Workflow
  ) {
    if (exec.signals[node.event] !== undefined) {
      await this.advance(exec, idx + 1, wf);
      return;
    }
    const waitKey = `__waitFor:${node.event}`;
    if (node.timeout !== undefined) {
      const existing = exec.steps[waitKey] as { startedAt?: number } | undefined;
      const waitingSince = existing?.startedAt ?? Date.now();
      if (!existing) exec.steps[waitKey] = { status: 'running', startedAt: waitingSince };
      if (Date.now() - waitingSince >= node.timeout) {
        this.emitter?.emitSignal('signal:timeout', exec.id, exec.workflowName, node.event);
        exec.steps[waitKey] = {
          status: 'failed',
          startedAt: waitingSince,
          completedAt: Date.now(),
          error: `Signal "${node.event}" timed out after ${node.timeout}ms`,
        };
        exec.state = 'failed';
        this.store.update(exec);
        await this.compensate(exec, wf);
        throw new Error(`Signal "${node.event}" timed out`);
      }
      this.store.update(exec);
      const remaining = node.timeout - (Date.now() - waitingSince);
      this.scheduleTimeoutCheck(exec.id, exec.workflowName, exec.currentNodeIndex, remaining);
    }
    exec.state = 'waiting';
    this.store.update(exec);
    this.emitter?.emitWorkflow('workflow:waiting', exec.id, exec.workflowName, 'waiting');
    throw new WaitForSignalError(node.event);
  }

  private async runLoop(
    exec: Execution,
    node: Extract<WorkflowNode, { type: 'doUntil' }> | Extract<WorkflowNode, { type: 'doWhile' }>,
    idx: number,
    wf: Workflow,
    loopFn: typeof executeDoUntil
  ) {
    await loopFn(node.def, exec, this.emitter, this.updateFn);
    await this.advance(exec, idx + 1, wf);
  }

  private async runForEach(
    exec: Execution,
    node: Extract<WorkflowNode, { type: 'forEach' }>,
    idx: number,
    wf: Workflow
  ) {
    await executeForEach(node.def, exec, this.emitter, this.updateFn);
    await this.advance(exec, idx + 1, wf);
  }

  private async runMap(
    exec: Execution,
    node: Extract<WorkflowNode, { type: 'map' }>,
    idx: number,
    wf: Workflow
  ) {
    await executeMap(node.def, exec, this.emitter, this.updateFn);
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
    await this.queue.add('wf:step', jobData);
  }

  private scheduleTimeoutCheck(execId: string, workflowName: string, nodeIdx: number, ms: number) {
    const timer = setTimeout(() => {
      this.timeoutTimers.delete(execId);
      const jobData: StepJobData = { executionId: execId, workflowName, nodeIndex: nodeIdx };
      this.queue.add('wf:step', jobData as unknown as Record<string, unknown>).catch(() => {
        /* Queue may be closed */
      });
    }, ms);
    this.timeoutTimers.set(execId, timer);
  }

  private async compensate(exec: Execution, wf: Workflow) {
    await runCompensation(exec, wf, this.store, this.emitter);
  }

  /** Recover orphaned executions after a crash/restart */
  async recover(): Promise<RecoverResult> {
    return recoverExecutions({
      store: this.store,
      queue: this.queue,
      workflows: this.workflows,
      emitter: this.emitter,
      timeoutTimers: this.timeoutTimers,
      scheduleTimeoutCheck: (id, name, idx, ms) => {
        this.scheduleTimeoutCheck(id, name, idx, ms);
      },
    });
  }
}
