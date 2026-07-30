/** Pure record selection and context construction for saga compensation. */

import { clock } from './clock';
import { idempotencyKey, loopBaseName } from './identity';
import { buildContext, findStepDef } from './runner';
import type { CompensationStatus, Execution, StepRecord } from './types';
import type { Workflow } from './workflow';

/**
 * Eligible records in reverse start order. Failed records are included because their
 * external effect may have succeeded before its response was lost.
 */
export function unwindSet(exec: Execution, wf: Workflow): [string, StepRecord][] {
  if (exec.committedAt !== undefined) return [];

  return Object.entries(exec.steps)
    .filter(([name, record]) => {
      if (name.startsWith('__')) return false;
      if (record.status !== 'completed' && record.status !== 'failed') return false;
      // Indexed loop records are authoritative; the bare name is only the last-result
      // mirror and compensating both would undo the final iteration twice.
      if (isLoopBody(wf, name) && exec.steps[`${name}:0`] !== undefined) return false;
      if (name.startsWith('sub:')) return record.childExecutionId !== undefined;
      if (findStepDef(wf, name) !== null) return true;
      // A removed definition must halt rather than disappear from an unwind it owed.
      return record.compensation !== undefined || record.compensatable === true;
    })
    .reverse();
}

export function settle(record: StepRecord, status: CompensationStatus, error?: string): void {
  record.compensation = { status, at: clock().now(), ...(error ? { error } : {}) };
}

/**
 * Rebind a loop's bare step name to this occurrence and restore forEach item metadata.
 */
export function compensationContext(
  exec: Execution,
  baseCtx: ReturnType<typeof buildContext>,
  name: string,
  record: StepRecord
) {
  const occurrence = record.occurrence ?? 0;
  const base = loopBaseName(name);
  const ctx = {
    ...baseCtx,
    steps: { ...baseCtx.steps, [base]: record.result, [name]: record.result },
    idempotencyKey: idempotencyKey(exec.id, name, occurrence, 'compensate'),
    forwardIdempotencyKey: record.idempotencyKey,
  };
  if (record.loopIndex === undefined) return ctx;
  return {
    ...ctx,
    steps: { ...ctx.steps, __item: record.loopItem, __index: record.loopIndex },
  };
}

function isLoopBody(wf: Workflow, name: string): boolean {
  return wf.nodes.some(
    (node) =>
      ((node.type === 'doUntil' || node.type === 'doWhile') &&
        node.def.steps.some((step) => step.name === name)) ||
      (node.type === 'forEach' && node.def.step.name === name)
  );
}
