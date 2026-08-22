import { clock } from './clock';
import { assertWorkflowActive } from './executionFence';
import type { Execution } from './types';

const MAX_TIMER_DELAY_MS = 2_147_483_647;

export interface SubWorkflowRunOptions {
  pollIntervalMs?: number;
  maxWaitMs?: number;
  existingChildId?: string;
  assertActive?: () => void;
}

/** Start or resume a child execution and poll its durable terminal state. */
export async function executeSubWorkflow(
  workflowName: string,
  input: unknown,
  startFn: (name: string, input: unknown) => Promise<{ id: string }>,
  getFn: (id: string) => Execution | null,
  options: SubWorkflowRunOptions = {}
): Promise<{ results: Record<string, unknown>; executionId: string }> {
  const pollIntervalMs = options.pollIntervalMs ?? 100;
  const maxWaitMs = options.maxWaitMs ?? 300_000;
  const assertActive = options.assertActive ?? assertWorkflowActive;
  assertActive();
  if (!Number.isFinite(pollIntervalMs) || pollIntervalMs <= 0) {
    throw new Error('Sub-workflow pollIntervalMs must be finite and greater than 0');
  }
  if (!Number.isFinite(maxWaitMs) || maxWaitMs <= 0) {
    throw new Error('Sub-workflow maxWaitMs must be finite and greater than 0');
  }

  // Resume a durable child claimed by an earlier entry into this parent node.
  assertActive();
  const existing = options.existingChildId ? getFn(options.existingChildId) : null;
  const handle = existing ? { id: existing.id } : await startFn(workflowName, input);
  assertActive();
  const startedAt = existing?.createdAt ?? getFn(handle.id)?.createdAt ?? clock().now();

  for (;;) {
    assertActive();
    const subExec = getFn(handle.id);
    if (subExec?.state === 'completed') {
      const results: Record<string, unknown> = {};
      for (const [name, record] of Object.entries(subExec.steps)) {
        if (record.status === 'completed') results[name] = record.result;
      }
      return { results, executionId: handle.id };
    }
    if (subExec?.state === 'failed') {
      throw new Error(`Sub-workflow "${workflowName}" (${handle.id}) failed`);
    }
    if (subExec?.state === 'compensation-stuck') {
      throw new Error(
        `Sub-workflow "${workflowName}" (${handle.id}) is parked mid-rollback ` +
          '(compensation-stuck); resolve it with resumeCompensation or abandonCompensation'
      );
    }
    const remaining = maxWaitMs - (clock().now() - startedAt);
    if (remaining <= 0) {
      throw new Error(
        `Sub-workflow "${workflowName}" (${handle.id}) timed out after ${maxWaitMs}ms`
      );
    }
    await new Promise<void>((resolve) =>
      clock().setTimeout(() => resolve(), Math.min(pollIntervalMs, remaining, MAX_TIMER_DELAY_MS))
    );
    assertActive();
  }
}
