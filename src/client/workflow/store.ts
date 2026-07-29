/**
 * WorkflowStore - SQLite persistence for workflow executions
 */

import { Database } from 'bun:sqlite';
import { pack, unpack } from './storeCodec';
import { SignalCoordinator } from './storeSignals';
import type { Execution, ExecutionState, ParkOutcome, SignalOutcome, StepRecord } from './types';
import { clock } from './clock';

const CREATE_TABLE = `
CREATE TABLE IF NOT EXISTS workflow_executions (
  id TEXT PRIMARY KEY,
  workflow_name TEXT NOT NULL,
  state TEXT NOT NULL DEFAULT 'running',
  input BLOB,
  steps BLOB,
  current_node_index INTEGER NOT NULL DEFAULT 0,
  resolved_steps BLOB,
  signals BLOB,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
)`;

const CREATE_ARCHIVE_TABLE = `
CREATE TABLE IF NOT EXISTS workflow_executions_archive (
  id TEXT PRIMARY KEY,
  workflow_name TEXT NOT NULL,
  state TEXT NOT NULL,
  input BLOB,
  steps BLOB,
  current_node_index INTEGER NOT NULL DEFAULT 0,
  resolved_steps BLOB,
  signals BLOB,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  archived_at INTEGER NOT NULL
)`;

const CREATE_IDX_NAME = `CREATE INDEX IF NOT EXISTS idx_wf_name ON workflow_executions(workflow_name)`;
const CREATE_IDX_STATE = `CREATE INDEX IF NOT EXISTS idx_wf_state ON workflow_executions(state)`;

/** Rollback bookkeeping, stored as one blob so the schema migration stays trivial. */
interface ExecutionMeta {
  rollbackStatus?: Execution['rollbackStatus'];
  failureReason?: string;
  committedAt?: number;
  parentExecutionId?: string;
}

function packMeta(exec: Execution): Uint8Array | null {
  const meta: ExecutionMeta = {};
  if (exec.rollbackStatus !== undefined) meta.rollbackStatus = exec.rollbackStatus;
  if (exec.failureReason !== undefined) meta.failureReason = exec.failureReason;
  if (exec.committedAt !== undefined) meta.committedAt = exec.committedAt;
  if (exec.parentExecutionId !== undefined) meta.parentExecutionId = exec.parentExecutionId;
  return Object.keys(meta).length > 0 ? pack(meta) : null;
}

export class WorkflowStore {
  private readonly db: Database;
  private readonly stmts: {
    upsert: ReturnType<Database['prepare']>;
    get: ReturnType<Database['prepare']>;
    updateState: ReturnType<Database['prepare']>;
    list: ReturnType<Database['prepare']>;
    listByName: ReturnType<Database['prepare']>;
    listByState: ReturnType<Database['prepare']>;
    listByBoth: ReturnType<Database['prepare']>;
    listRecoverable: ReturnType<Database['prepare']>;
  };
  /** Sole owner of the `signals` column — see storeSignals.ts */
  private readonly signals: SignalCoordinator;

  constructor(dbPath?: string) {
    this.db = new Database(dbPath ?? ':memory:', { create: true });
    this.db.run('PRAGMA journal_mode = WAL');
    // The Engine hands the SAME dataPath to this store and to its embedded
    // Queue/Worker, so two connections share one file. Without a busy timeout a
    // read-then-upgrade inside signal()/parkForSignal() can surface SQLITE_BUSY
    // straight to the caller, with no retry, the moment the queue happens to be
    // writing. Five seconds matches the server's own persistence layer.
    this.db.run('PRAGMA busy_timeout = 5000');
    this.db.run(CREATE_TABLE);
    this.db.run(CREATE_ARCHIVE_TABLE);
    this.db.run(CREATE_IDX_NAME);
    this.db.run(CREATE_IDX_STATE);
    // Rollback bookkeeping arrived after the original schema. One nullable blob
    // rather than three columns keeps the migration to a single guarded statement
    // per table; SQLite has no ADD COLUMN IF NOT EXISTS, so the throw IS the check.
    for (const table of ['workflow_executions', 'workflow_executions_archive']) {
      try {
        this.db.run(`ALTER TABLE ${table} ADD COLUMN meta BLOB`);
      } catch {
        /* already migrated */
      }
    }

    this.stmts = {
      upsert: this.db.prepare(`
        INSERT INTO workflow_executions
        (id, workflow_name, state, input, steps, current_node_index, resolved_steps, signals, created_at, updated_at, meta)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `),
      get: this.db.prepare(`SELECT * FROM workflow_executions WHERE id = ?`),
      // NOTE: `signals` is deliberately absent. That column is owned exclusively by
      // recordSignal()/parkForSignal() — see the comment on `update()`.
      updateState: this.db.prepare(`
        UPDATE workflow_executions
        SET state = ?, steps = ?, current_node_index = ?, resolved_steps = ?, updated_at = ?, meta = ?
        WHERE id = ?
      `),
      list: this.db.prepare(`SELECT * FROM workflow_executions ORDER BY created_at DESC LIMIT 100`),
      listByName: this.db.prepare(
        `SELECT * FROM workflow_executions WHERE workflow_name = ? ORDER BY created_at DESC LIMIT 100`
      ),
      listByState: this.db.prepare(
        `SELECT * FROM workflow_executions WHERE state = ? ORDER BY created_at DESC LIMIT 100`
      ),
      listByBoth: this.db.prepare(
        `SELECT * FROM workflow_executions WHERE workflow_name = ? AND state = ? ORDER BY created_at DESC LIMIT 100`
      ),
      listRecoverable: this.db.prepare(
        `SELECT * FROM workflow_executions WHERE state IN ('running', 'waiting', 'compensating') ORDER BY updated_at ASC`
      ),
    };

    this.signals = new SignalCoordinator(this.db);
  }

  /**
   * INSERT a brand-new execution. Not an upsert in spirit, despite the statement.
   *
   * A plain INSERT, not an upsert: the statement used to be `INSERT OR REPLACE`, so a
   * duplicate execution id silently OVERWROTE a live run instead of failing. Ids carry
   * a random component, which makes that vanishingly rare in production and reachable
   * in a simulation, where a seeded generator draws from a far smaller space. A lost
   * execution is the worst possible presentation of a collision; a constraint error is
   * the best.
   *
   * This is the ONLY writer of `signals` outside SignalCoordinator, and it is safe
   * only because its single caller passes a fresh execution whose signals are `{}`.
   * Calling it on a live run would write a stale snapshot over signals delivered
   * since it was read, which is exactly the lost-update SignalCoordinator exists to
   * prevent. Use `update()` for anything that already exists.
   */
  save(exec: Execution): void {
    if (Object.keys(exec.signals).length > 0) {
      throw new Error(
        'WorkflowStore.save() is for new executions only; it would overwrite delivered signals. Use update().'
      );
    }
    this.stmts.upsert.run(
      exec.id,
      exec.workflowName,
      exec.state,
      pack(exec.input),
      pack(exec.steps),
      exec.currentNodeIndex,
      exec.resolvedSteps ? pack(exec.resolvedSteps) : null,
      pack(exec.signals),
      exec.createdAt,
      exec.updatedAt,
      packMeta(exec)
    );
  }

  get(id: string): Execution | null {
    const row = this.stmts.get.get(id) as Record<string, unknown> | null;
    return row ? this.rowToExecution(row) : null;
  }

  /**
   * Persist the step-level columns of an execution.
   *
   * Deliberately does NOT write `signals`. A worker holds one in-memory `Execution`
   * for the whole duration of a node, so its `signals` snapshot goes stale as soon as
   * `recordSignal()` writes to the row. Rewriting the column from that stale snapshot
   * silently destroyed signals delivered mid-step and parked the run forever
   * (`test/repro-workflow-signal-lost-update.test.ts`). Signal payloads are owned
   * exclusively by `recordSignal()`/`parkForSignal()`, which read-modify-write the
   * column inside a transaction.
   */
  update(exec: Execution): void {
    exec.updatedAt = clock().now();
    this.stmts.updateState.run(
      exec.state,
      pack(exec.steps),
      exec.currentNodeIndex,
      exec.resolvedSteps ? pack(exec.resolvedSteps) : null,
      exec.updatedAt,
      packMeta(exec),
      exec.id
    );
  }

  /**
   * Record a signal payload and, if the run is parked at a `waitFor`, atomically
   * claim the single resume for this caller.
   */
  recordSignal(id: string, event: string, payload: unknown): SignalOutcome {
    return this.signals.record(id, event, payload);
  }

  /**
   * Park a running execution at a `waitFor`, unless the awaited signal has already
   * been recorded (in which case the caller must advance instead).
   */
  parkForSignal(id: string, event: string): ParkOutcome {
    return this.signals.park(id, event);
  }

  list(workflowName?: string, state?: ExecutionState): Execution[] {
    let rows: Record<string, unknown>[];
    if (workflowName && state) {
      rows = this.stmts.listByBoth.all(workflowName, state) as Record<string, unknown>[];
    } else if (workflowName) {
      rows = this.stmts.listByName.all(workflowName) as Record<string, unknown>[];
    } else if (state) {
      rows = this.stmts.listByState.all(state) as Record<string, unknown>[];
    } else {
      rows = this.stmts.list.all() as Record<string, unknown>[];
    }
    return rows.map((r) => this.rowToExecution(r));
  }

  /**
   * Executions in a recoverable state that recovery may drive ON ITS OWN.
   *
   * A `subWorkflow` child is a row like any other, so the state filter alone offered
   * it to `recover()` as if it were a top-level run. Driving it independently re-ran
   * its steps and produced fresh records with no `compensation`, which defeats the
   * "a step that already carries an outcome is not re-run" guard: the parent's later
   * unwind dispatched the child's reversal a SECOND time, against a provider that had
   * already been refunded (`test/repro-model-child-recovered-alone.test.ts`; found by
   * the state-machine model, seed 1267197984).
   *
   * A child's lifecycle belongs to its parent, which unwinds it through `unwindChild`.
   * One exception: if the parent row is gone, nothing owns the child any more, so it
   * is returned rather than stranded forever in a non-terminal state.
   */
  listRecoverable(): Execution[] {
    const rows = this.stmts.listRecoverable.all() as Record<string, unknown>[];
    const all = rows.map((r) => this.rowToExecution(r));
    return all.filter((e) => !e.parentExecutionId || this.get(e.parentExecutionId) === null);
  }

  /**
   * Delete executions at least `maxAgeMs` old in terminal states.
   *
   * The cutoff is INCLUSIVE. A strict `<` makes `cleanup(0)`, the documented way to
   * flush everything terminal right now, skip every row whose `updated_at` lands on
   * the current millisecond, which is precisely where a run that just finished lands
   * (`test/repro-workflow-archive-boundary.test.ts`).
   */
  cleanup(maxAgeMs: number, states: string[] = ['completed', 'failed']): number {
    const cutoff = clock().now() - maxAgeMs;
    const placeholders = states.map(() => '?').join(',');
    const stmt = this.db.prepare(
      `DELETE FROM workflow_executions WHERE updated_at <= ? AND state IN (${placeholders})`
    );
    const result = stmt.run(cutoff, ...states) as { changes: number };
    return result.changes;
  }

  /** Archive executions at least `maxAgeMs` old to the archive table. Cutoff inclusive, as in `cleanup`. */
  archive(maxAgeMs: number, states: string[] = ['completed', 'failed']): number {
    const cutoff = clock().now() - maxAgeMs;
    const now = clock().now();
    const placeholders = states.map(() => '?').join(',');
    const rows = this.db
      .prepare(
        `SELECT * FROM workflow_executions WHERE updated_at <= ? AND state IN (${placeholders}) LIMIT 1000`
      )
      .all(cutoff, ...states) as Record<string, unknown>[];

    if (rows.length === 0) return 0;

    const insertArchive = this.db.prepare(`
      INSERT OR REPLACE INTO workflow_executions_archive
      (id, workflow_name, state, input, steps, current_node_index, resolved_steps, signals, created_at, updated_at, archived_at, meta)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);
    const deleteOriginal = this.db.prepare(`DELETE FROM workflow_executions WHERE id = ?`);

    const tx = this.db.transaction(() => {
      for (const row of rows) {
        insertArchive.run(
          row.id as string,
          row.workflow_name as string,
          row.state as string,
          row.input as Uint8Array,
          row.steps as Uint8Array,
          row.current_node_index as number,
          row.resolved_steps as Uint8Array | null,
          row.signals as Uint8Array,
          row.created_at as number,
          row.updated_at as number,
          now,
          (row.meta as Uint8Array | null) ?? null
        );
        deleteOriginal.run(row.id as string);
      }
    });
    tx();
    return rows.length;
  }

  /** Get archived execution count */
  getArchivedCount(): number {
    const row = this.db
      .prepare(`SELECT COUNT(*) as cnt FROM workflow_executions_archive`)
      .get() as { cnt: number };
    return row.cnt;
  }

  close(): void {
    this.db.close();
  }

  private rowToExecution(row: Record<string, unknown>): Execution {
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
}
