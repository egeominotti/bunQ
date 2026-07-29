/**
 * SignalCoordinator — sole owner of the `signals` column.
 *
 * Signal payloads cannot be persisted the way step state is. A worker holds one
 * in-memory `Execution` for the whole duration of a node, so any full-row write from
 * that snapshot silently reverts a signal that landed while the node was running —
 * the approval disappears and the run parks forever
 * (test/repro-workflow-signal-lost-update.test.ts).
 *
 * So every mutation of `signals` goes through here instead: read-modify-write inside
 * a transaction, and the two state transitions that pair with it ('waiting' ->
 * 'running' on resume, 'running' -> 'waiting' on park) expressed as conditional
 * UPDATEs. Making those conditional is also what collapses duplicate or concurrent
 * signals to exactly one resume, at the database rather than relying on the event
 * loop never yielding at the right moment.
 */

import type { Database } from 'bun:sqlite';
import { pack, unpack } from './storeCodec';
import type { ParkOutcome, SignalOutcome } from './types';
import { clock } from './clock';

/**
 * Has this signal arrived? A KEY test, deliberately not a value test.
 *
 * `payload` is optional on `Engine.signal()`, so "the human said go" with nothing to
 * carry is recorded as `signals[event] = undefined`. The codec is configured with
 * `structuredClone: true` and round-trips that faithfully — the key is present, the
 * value is `undefined` — so `signals[event] !== undefined` answers "did it arrive?"
 * with NO for the most idiomatic call in the whole human-in-the-loop API.
 *
 * That disagreed with `record()`, which claims the resume without inspecting the
 * payload: the run resumed, re-entered the waitFor, was told no signal had arrived,
 * and parked again. With a timeout it then expired and compensated work the approver
 * had just authorised (`test/repro-workflow-signal-no-payload.test.ts`).
 *
 * The predicate itself is `Object.hasOwn`, and an earlier revision of this comment
 * claimed `in` was safe here because the object is always a fresh decode. That was
 * wrong twice over: `in` walks the prototype chain regardless of where the object came
 * from, and the EVENT NAME is user-supplied even when the object is not.
 */
export function hasSignal(signals: Record<string, unknown>, event: string): boolean {
  // `Object.hasOwn`, not `in`, and not `signals[event] !== undefined`. All three read
  // like the same question and only one of them asks it.
  //
  // `signals` is a plain object used as a map, so `in` walks the prototype chain:
  // `'toString' in {}` is true, and a gate named `toString`, `constructor`, `valueOf`
  // or `hasOwnProperty` opened the instant it parked, with nobody having signalled
  // anything. The value test that preceded it failed identically, because
  // `signals['toString']` is the inherited function and not `undefined`.
  //
  // The names are not exotic in a real vocabulary, and an event name read from config
  // or user input is attacker-influenced. The failure is silent and it opens the one
  // control that exists to stop things
  // (`test/repro-workflow-prototype-gate.test.ts`, found by the generated-input suite
  // in `test/workflow-properties.test.ts`).
  return Object.hasOwn(signals, event);
}

export class SignalCoordinator {
  private readonly read: ReturnType<Database['prepare']>;
  private readonly write: ReturnType<Database['prepare']>;
  private readonly claimResume: ReturnType<Database['prepare']>;
  private readonly claimPark: ReturnType<Database['prepare']>;

  constructor(private readonly db: Database) {
    this.read = db.prepare(
      `SELECT workflow_name, state, current_node_index, signals FROM workflow_executions WHERE id = ?`
    );
    this.write = db.prepare(
      `UPDATE workflow_executions SET signals = ?, updated_at = ? WHERE id = ?`
    );
    this.claimResume = db.prepare(
      `UPDATE workflow_executions SET state = 'running', updated_at = ? WHERE id = ? AND state = 'waiting'`
    );
    // 'waiting' is accepted as a source state so re-parking is idempotent: a clamped
    // or partial timeout re-arm re-enters the same waitFor node while the row is
    // still 'waiting', and must be allowed to park (and re-arm) again.
    this.claimPark = db.prepare(
      `UPDATE workflow_executions SET state = 'waiting', updated_at = ? WHERE id = ? AND state IN ('running', 'waiting')`
    );
  }

  /**
   * Record a signal payload and, if the run is parked at a `waitFor`, atomically
   * claim the single resume for this caller.
   */
  record(id: string, event: string, payload: unknown): SignalOutcome {
    const tx = this.db.transaction((): SignalOutcome => {
      const row = this.read.get(id) as Record<string, unknown> | null;
      if (!row) return { found: false, resumed: false, workflowName: '', currentNodeIndex: 0 };

      const signals = this.decode(row);
      signals[event] = payload;
      const now = clock().now();
      this.write.run(pack(signals), now, id);

      const claimed = this.claimResume.run(now, id) as { changes: number };
      return {
        found: true,
        resumed: claimed.changes === 1,
        workflowName: row.workflow_name as string,
        currentNodeIndex: row.current_node_index as number,
      };
    });
    return tx();
  }

  /**
   * Park a running execution at a `waitFor`, unless the awaited signal has already
   * been recorded.
   *
   * Closes the window where a signal lands after `runWaitFor` read its in-memory
   * snapshot but before the run reaches state `waiting`: without the re-check the
   * payload is stored, nobody claims the resume, and the run hangs.
   */
  park(id: string, event: string): ParkOutcome {
    const tx = this.db.transaction((): ParkOutcome => {
      const row = this.read.get(id) as Record<string, unknown> | null;
      const signals = this.decode(row);
      if (!row) return { signalPresent: false, parked: false, signals };
      if (hasSignal(signals, event)) return { signalPresent: true, parked: false, signals };
      const claimed = this.claimPark.run(clock().now(), id) as { changes: number };
      return { signalPresent: false, parked: claimed.changes === 1, signals };
    });
    return tx();
  }

  private decode(row: Record<string, unknown> | null): Record<string, unknown> {
    return (unpack(row?.signals as Uint8Array | null) as Record<string, unknown> | null) ?? {};
  }
}
