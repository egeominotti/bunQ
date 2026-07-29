/**
 * Invariants the workflow engine must hold after every generated command.
 *
 * These are written to be independent of the engine's implementation: they are
 * checked against the persisted `Execution` plus a ledger of what the scripted
 * handlers actually did. Each one corresponds to a way the engine has been, or
 * could plausibly be, wrong:
 *
 *   I1 signal fidelity      — a delivered signal is never lost or altered.
 *                             (the lost-update bug: an in-flight step's stale
 *                             snapshot overwrote signals and hung the run)
 *   I2 monotonic progress   — currentNodeIndex never moves backwards.
 *   I3 no resurrection      — a terminal execution stays terminal.
 *   I4 waitFor gating       — no step positioned after a waitFor ever runs before
 *                             that signal has been delivered. (the silently
 *                             dropped waitFor inside a branch path)
 *   I5 retry bound          — a step never records more attempts than its retry.
 *   I6 name domain          — exec.steps only ever contains names the spec can
 *                             produce; no phantom or mangled keys.
 *   I7 compensation sanity  — every compensation that ran belongs to a step that
 *                             really completed and really declared a handler.
 */

import type { Execution } from '../../src/client/workflow';
import { declaredNames, stepsGatedBy, type Ledger, type WorkflowSpec } from './workflow-spec';

export interface InvariantState {
  spec: WorkflowSpec;
  ledger: Ledger;
  /** Signals handed to engine.signal(), in delivery order. */
  delivered: Map<string, unknown>;
  /** Highest node index observed so far. */
  maxNodeIndex: number;
  /** True once a terminal state has been observed. */
  sawTerminal: boolean;
  terminalState: string | null;
  /** Steps declared with a compensate handler. */
  compensable: Set<string>;
  /** Retry budget per step name. */
  retryBudget: Map<string, number>;
}

export class InvariantViolation extends Error {}

const TERMINAL = new Set(['completed', 'failed']);

export function checkInvariants(state: InvariantState, exec: Execution | null): void {
  if (!exec) return;
  const fail = (id: string, detail: string): never => {
    throw new InvariantViolation(
      `${id} violated: ${detail}\nspec=${JSON.stringify(state.spec)}\n` +
        `ledger=${JSON.stringify(state.ledger)}\nexec=${JSON.stringify({
          state: exec.state,
          currentNodeIndex: exec.currentNodeIndex,
          signals: exec.signals,
          steps: exec.steps,
        })}`
    );
  };

  // W-COMP-ONCE — a compensate handler is dispatched AT MOST ONCE per eligible record,
  // for the whole life of the run, including across restart()/recover().
  //
  // This is the invariant a real defect slipped past: recover() snapshotted every
  // recoverable row up front, then drove them one at a time, so a sub-workflow already
  // unwound through its parent was driven a second time from a stale copy. Both rows
  // still ended `failed` with `rollbackStatus: 'completed'`, so no state assertion
  // could see it. Only counting dispatches can.
  const dispatched = new Map<string, number>();
  for (const name of state.ledger.compensations) {
    dispatched.set(name, (dispatched.get(name) ?? 0) + 1);
  }
  for (const [name, count] of dispatched) {
    if (count > 1) {
      fail('W-COMP-ONCE', `"${name}" compensated ${count} times; a rollback must never repeat`);
    }
  }

  // I1 — every delivered signal is present, unchanged.
  for (const [event, payload] of state.delivered) {
    if (!(event in exec.signals)) {
      fail('I1 signal fidelity', `signal "${event}" was delivered but is absent from exec.signals`);
    }
    if (JSON.stringify(exec.signals[event]) !== JSON.stringify(payload)) {
      fail(
        'I1 signal fidelity',
        `signal "${event}" payload changed: expected ${JSON.stringify(payload)}, got ${JSON.stringify(exec.signals[event])}`
      );
    }
  }

  // I2 — progress never rewinds.
  if (exec.currentNodeIndex < state.maxNodeIndex) {
    fail(
      'I2 monotonic progress',
      `currentNodeIndex went ${state.maxNodeIndex} -> ${exec.currentNodeIndex}`
    );
  }
  state.maxNodeIndex = Math.max(state.maxNodeIndex, exec.currentNodeIndex);

  // I3 — terminal is forever.
  if (state.sawTerminal && exec.state !== state.terminalState) {
    fail(
      'I3 no resurrection',
      `execution left terminal state ${state.terminalState} for ${exec.state}`
    );
  }
  if (TERMINAL.has(exec.state)) {
    state.sawTerminal = true;
    state.terminalState = exec.state;
  }

  // I4 — a step behind a waitFor cannot have run before its signal arrived.
  const gated = stepsGatedBy(state.spec);
  for (const name of state.ledger.steps) {
    const event = gated.get(name);
    if (event !== undefined && !state.delivered.has(event)) {
      fail(
        'I4 waitFor gating',
        `step "${name}" ran but its gating signal "${event}" was never delivered`
      );
    }
  }

  // I5 — retries stay inside the declared budget.
  for (const [name, record] of Object.entries(exec.steps)) {
    const budget = state.retryBudget.get(baseName(name));
    if (budget !== undefined && (record.attempts ?? 0) > budget) {
      fail(
        'I5 retry bound',
        `step "${name}" recorded ${record.attempts} attempts, budget ${budget}`
      );
    }
  }

  // I6 — no phantom keys in the step map.
  const allowed = declaredNames(state.spec);
  for (const name of Object.keys(exec.steps)) {
    if (!allowed.has(name) && !name.startsWith('__') && !name.startsWith('sub:')) {
      fail('I6 name domain', `unexpected step key "${name}"`);
    }
  }

  // I7 — compensations only ever belong to genuinely completed, compensable steps.
  for (const name of state.ledger.compensations) {
    if (!state.compensable.has(name)) {
      fail('I7 compensation sanity', `"${name}" was compensated but declares no handler`);
    }
    const record = exec.steps[name];
    if (record && record.status !== 'completed' && record.status !== 'failed') {
      fail('I7 compensation sanity', `"${name}" was compensated while in status ${record.status}`);
    }
  }
}

/** `notify:2` -> `notify` (forEach indexed records share the declared budget). */
function baseName(name: string): string {
  const idx = name.lastIndexOf(':');
  if (idx <= 0) return name;
  const tail = name.slice(idx + 1);
  return /^\d+$/.test(tail) ? name.slice(0, idx) : name;
}
