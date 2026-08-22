/** Durable workflow-node publication shared by execution and recovery. */

import type { Queue } from '../queue/queue';
import type { Execution, StepJobData } from './types';

/**
 * Publish one node without a deterministic job ID. The cursor and in-flight guards
 * discard overlapping duplicates, while queue-level dedup could suppress a legitimate
 * re-enqueue after recovery and wedge the execution permanently.
 */
export async function enqueueWorkflowStep(
  queue: Queue,
  exec: Execution,
  assertActive: () => void
): Promise<void> {
  assertActive();
  const jobData: StepJobData = {
    executionId: exec.id,
    workflowName: exec.workflowName,
    nodeIndex: exec.currentNodeIndex,
  };
  await queue.add('wf:step', jobData);
}
