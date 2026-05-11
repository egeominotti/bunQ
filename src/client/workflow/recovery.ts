/**
 * Recovery logic for orphaned workflow executions after crash/restart
 *
 * Handles three states:
 * - 'running': re-enqueue the step at currentNodeIndex
 * - 'waiting': re-arm timeout timer or resume if signal already arrived
 * - 'compensating': re-run compensation from the beginning (must be idempotent)
 */

import type { Queue } from '../queue/queue';
import type { Workflow } from './workflow';
import type { WorkflowStore } from './store';
import type { WorkflowEmitter } from './emitter';
import type { Execution, StepJobData, RecoverResult, WorkflowNode } from './types';
import { runCompensation } from './compensator';

export interface RecoverDeps {
  store: WorkflowStore;
  queue: Queue;
  workflows: Map<string, Workflow>;
  emitter: WorkflowEmitter | null;
  timeoutTimers: Map<string, ReturnType<typeof setTimeout>>;
  scheduleTimeoutCheck: (id: string, wfName: string, nodeIdx: number, ms: number) => void;
}

export async function recoverExecutions(deps: RecoverDeps): Promise<RecoverResult> {
  const { store, workflows } = deps;
  const executions = store.listRecoverable();
  const result: RecoverResult = { running: 0, waiting: 0, compensating: 0, total: 0 };

  for (const exec of executions) {
    const wf = workflows.get(exec.workflowName);
    if (!wf) continue;

    if (exec.state === 'running') {
      await enqueueExecution(exec, deps.queue);
      result.running++;
    } else if (exec.state === 'waiting') {
      await recoverWaiting(exec, wf, deps);
      result.waiting++;
    } else if (exec.state === 'compensating') {
      await runCompensation(exec, wf, store, deps.emitter);
      result.compensating++;
    }
  }

  result.total = result.running + result.waiting + result.compensating;
  return result;
}

async function enqueueExecution(exec: Execution, queue: Queue): Promise<void> {
  const jobData: StepJobData = {
    executionId: exec.id,
    workflowName: exec.workflowName,
    nodeIndex: exec.currentNodeIndex,
  };
  await queue.add('wf:step', jobData);
}

async function recoverWaiting(exec: Execution, wf: Workflow, deps: RecoverDeps): Promise<void> {
  const node = wf.nodes[exec.currentNodeIndex] as WorkflowNode | undefined;
  if (node?.type !== 'waitFor') {
    await enqueueExecution(exec, deps.queue);
    return;
  }

  // Check if signal already arrived while we were down
  if (exec.signals[node.event] !== undefined) {
    exec.state = 'running';
    deps.store.update(exec);
    await enqueueExecution(exec, deps.queue);
    return;
  }

  // Re-arm timeout if configured
  if (node.timeout !== undefined) {
    if (deps.timeoutTimers.has(exec.id)) return;

    const waitKey = `__waitFor:${node.event}`;
    const waitRecord = exec.steps[waitKey] as { startedAt?: number } | undefined;
    const waitingSince = waitRecord?.startedAt ?? exec.updatedAt;
    const elapsed = Date.now() - waitingSince;
    const remaining = node.timeout - elapsed;

    if (remaining <= 0) {
      exec.state = 'running';
      deps.store.update(exec);
      await enqueueExecution(exec, deps.queue);
    } else {
      deps.scheduleTimeoutCheck(exec.id, exec.workflowName, exec.currentNodeIndex, remaining);
    }
  }
}
