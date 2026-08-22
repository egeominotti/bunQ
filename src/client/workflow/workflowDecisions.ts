/** Durable control-flow decision journal. */

import type { Execution } from './types';

const hasOwn = (value: object, key: PropertyKey): boolean => Object.hasOwn(value, key);

/**
 * Return an already persisted decision, or evaluate and persist it before the caller
 * performs any effects selected by that decision.
 */
export async function resolveDecision<T>(
  exec: Execution,
  key: string,
  evaluate: () => T | Promise<T>,
  updateFn: (exec: Execution) => void,
  assertActive: () => void
): Promise<T> {
  assertActive();
  const decisions = (exec.decisions ??= {});
  if (hasOwn(decisions, key)) return decisions[key] as T;

  const value = await evaluate();
  assertActive();
  decisions[key] = value;
  updateFn(exec);
  return value;
}

export function branchDecisionKey(nodeIndex: number): string {
  return `branch:${nodeIndex}`;
}

export function forEachItemsDecisionKey(stepName: string): string {
  return `forEach:${stepName}:items`;
}

export function loopDecisionKey(
  kind: 'doUntil' | 'doWhile',
  firstStepName: string,
  iteration: number
): string {
  return `${kind}:${firstStepName}:${iteration}`;
}

export function subWorkflowInputDecisionKey(nodeIndex: number): string {
  return `subWorkflow:${nodeIndex}:input`;
}
