import type { Execution, ExecutionState, StepRecord } from './types';
import { pack, unpack } from './storeCodec';

export interface ExecutionMeta {
  rollbackStatus?: Execution['rollbackStatus'];
  failureReason?: string;
  committedAt?: number;
  parentExecutionId?: string;
  decisions?: Record<string, unknown>;
  definitionHash?: string;
}

export function packExecutionMeta(exec: Execution): Uint8Array | null {
  const meta: ExecutionMeta = {};
  if (exec.rollbackStatus !== undefined) meta.rollbackStatus = exec.rollbackStatus;
  if (exec.failureReason !== undefined) meta.failureReason = exec.failureReason;
  if (exec.committedAt !== undefined) meta.committedAt = exec.committedAt;
  if (exec.parentExecutionId !== undefined) meta.parentExecutionId = exec.parentExecutionId;
  if (exec.decisions !== undefined) meta.decisions = exec.decisions;
  if (exec.definitionHash !== undefined) meta.definitionHash = exec.definitionHash;
  return Object.keys(meta).length > 0 ? pack(meta) : null;
}

export function decodeExecution(row: Record<string, unknown>): Execution {
  return {
    id: row.id as string,
    workflowName: row.workflow_name as string,
    state: row.state as ExecutionState,
    input: unpack(row.input as Uint8Array | null),
    steps: (unpack(row.steps as Uint8Array | null) as Record<string, StepRecord> | null) ?? {},
    currentNodeIndex: row.current_node_index as number,
    resolvedSteps: row.resolved_steps
      ? (unpack(row.resolved_steps as Uint8Array | null) as string[])
      : undefined,
    signals: (unpack(row.signals as Uint8Array | null) as Record<string, unknown> | null) ?? {},
    createdAt: row.created_at as number,
    updatedAt: row.updated_at as number,
    ...((unpack(row.meta as Uint8Array | null) as ExecutionMeta | null) ?? {}),
  };
}
