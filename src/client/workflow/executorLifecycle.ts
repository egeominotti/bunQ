/** Durable start and signal publication for WorkflowExecutor. */

import type { Queue } from '../queue/queue';
import { clock, type TimerHandle } from './clock';
import type { WorkflowEmitter } from './emitter';
import type { WorkflowStore } from './store';
import type { Execution, RunHandle, StepJobData } from './types';
import type { Workflow } from './workflow';
import { unusableEventName } from './workflow';
import { newExecutionId } from './identity';

interface LifecycleDeps {
  store: WorkflowStore;
  queue: Queue;
  workflows: ReadonlyMap<string, Workflow>;
  emitter: WorkflowEmitter | null;
  timers: Map<string, TimerHandle>;
  enqueue: (exec: Execution) => Promise<void>;
  assertActive: () => void;
}

export async function startExecution(
  deps: LifecycleDeps,
  workflowName: string,
  input: unknown,
  parentExecutionId?: string
): Promise<RunHandle> {
  deps.assertActive();
  const wf = deps.workflows.get(workflowName);
  if (!wf) throw new Error(`Workflow "${workflowName}" not registered`);
  if (wf.nodes.length === 0) throw new Error(`Workflow "${workflowName}" has no steps`);

  const now = clock().now();
  const id = newExecutionId();
  const exec: Execution = {
    id,
    workflowName,
    state: 'running',
    input,
    steps: {},
    currentNodeIndex: 0,
    signals: {},
    definitionHash: wf.seal(),
    ...(parentExecutionId ? { parentExecutionId } : {}),
    createdAt: now,
    updatedAt: now,
  };
  deps.assertActive();
  deps.store.save(exec);

  try {
    deps.emitter?.emitWorkflow('workflow:started', id, workflowName, 'running', { input });
    deps.assertActive();
    await deps.enqueue(exec);
    deps.assertActive();
  } catch (error) {
    // start() cannot return an id when publication fails. Removing the row keeps both
    // top-level starts and sub-workflow starts from creating an unreachable orphan.
    let failure = error;
    try {
      deps.assertActive();
    } catch (closedError) {
      failure = closedError;
    }
    try {
      deps.store.remove(id);
    } catch {
      // If shutdown already closed SQLite, recovery owns the still-running row.
    }
    throw failure;
  }
  return { id, workflowName };
}

export async function signalExecution(
  deps: LifecycleDeps,
  executionId: string,
  event: string,
  payload: unknown
): Promise<void> {
  deps.assertActive();
  const bad = unusableEventName(event);
  if (bad) throw new Error(`Cannot signal an event ${bad}`);

  // recordSignal owns the read/check/write transaction. In particular it rejects a
  // duplicate key before writing, so concurrent deliveries are first-writer-wins.
  const outcome = deps.store.recordSignal(executionId, event, payload);
  if (!outcome.found) throw new Error(`Execution "${executionId}" not found`);

  deps.emitter?.emitSignal('signal:received', executionId, outcome.workflowName, event, payload);
  if (!outcome.resumed) return;

  try {
    deps.assertActive();
    const timer = deps.timers.get(executionId);
    if (timer) {
      clock().clearTimeout(timer);
      deps.timers.delete(executionId);
    }
    await deps.queue.add('wf:step', {
      executionId,
      workflowName: outcome.workflowName,
      nodeIndex: outcome.currentNodeIndex,
    } satisfies StepJobData);
    deps.assertActive();
  } catch (error) {
    // Preserve the accepted payload but release the publication claim. Recovery sees
    // a waiting execution with its signal present and republishes the same node.
    let failure = error;
    try {
      deps.assertActive();
    } catch (closedError) {
      failure = closedError;
    }
    try {
      deps.store.restoreSignalWait(executionId, event, outcome.currentNodeIndex);
    } catch {
      // A closed store leaves a recoverable running row with its signal preserved.
    }
    throw failure;
  }
}
