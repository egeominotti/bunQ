import type { Engine, Execution, ExecutionState } from '../src/client/workflow';

export async function waitForWorkflowState(
  engine: Engine,
  executionId: string,
  state: ExecutionState,
  timeoutMs = 5_000
): Promise<Execution | null> {
  const deadline = Date.now() + timeoutMs;
  let execution = engine.getExecution(executionId);

  while (execution?.state !== state && Date.now() < deadline) {
    await Bun.sleep(10);
    execution = engine.getExecution(executionId);
  }

  return execution;
}
