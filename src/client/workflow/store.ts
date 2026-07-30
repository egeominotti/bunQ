/**
 * WorkflowStore - SQLite persistence for workflow executions
 */

import { Database } from 'bun:sqlite';
import { pack } from './storeCodec';
import { SignalCoordinator } from './storeSignals';
import type {
  Execution,
  ExecutionListOptions,
  ExecutionState,
  ParkOutcome,
  SignalOutcome,
} from './types';
import { clock } from './clock';
import { decodeExecution, packExecutionMeta } from './storeExecutionCodec';
import { archivedExecutionCount, archiveExecutions, cleanupExecutions } from './storeMaintenance';
import { ExecutionListing } from './storeListing';

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
const CREATE_LIST_INDEXES = [
  `CREATE INDEX IF NOT EXISTS idx_wf_created ON workflow_executions(created_at DESC, id DESC)`,
  `CREATE INDEX IF NOT EXISTS idx_wf_name_created ON workflow_executions(workflow_name, created_at DESC, id DESC)`,
  `CREATE INDEX IF NOT EXISTS idx_wf_state_created ON workflow_executions(state, created_at DESC, id DESC)`,
  `CREATE INDEX IF NOT EXISTS idx_wf_name_state_created ON workflow_executions(workflow_name, state, created_at DESC, id DESC)`,
  `CREATE INDEX IF NOT EXISTS idx_wf_state_updated ON workflow_executions(state, updated_at ASC, id ASC)`,
];

export class WorkflowStore {
  private readonly db: Database;
  private readonly stmts: {
    upsert: ReturnType<Database['prepare']>;
    get: ReturnType<Database['prepare']>;
    updateState: ReturnType<Database['prepare']>;
    childrenByName: ReturnType<Database['prepare']>;
    listActiveByName: ReturnType<Database['prepare']>;
    listRecoverable: ReturnType<Database['prepare']>;
    remove: ReturnType<Database['prepare']>;
  };
  /** Sole owner of the `signals` column — see storeSignals.ts */
  private readonly signals: SignalCoordinator;
  private readonly listing: ExecutionListing;

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
    for (const statement of CREATE_LIST_INDEXES) this.db.run(statement);
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
      childrenByName: this.db.prepare(
        `SELECT * FROM workflow_executions WHERE workflow_name = ? ORDER BY created_at ASC, id ASC`
      ),
      listActiveByName: this.db.prepare(
        `SELECT * FROM workflow_executions
         WHERE workflow_name = ?
           AND state IN ('running', 'waiting', 'compensating', 'compensation-stuck', 'failed')`
      ),
      listRecoverable: this.db.prepare(
        `SELECT * FROM workflow_executions
         WHERE state IN ('running', 'waiting', 'compensating', 'failed')
         ORDER BY updated_at ASC, id ASC`
      ),
      remove: this.db.prepare(`DELETE FROM workflow_executions WHERE id = ?`),
    };

    this.signals = new SignalCoordinator(this.db);
    this.listing = new ExecutionListing(this.db);
  }

  /**
   * Insert a fresh execution. A plain INSERT exposes id collisions instead of
   * replacing live runs. This is the only signal writer outside SignalCoordinator and
   * is safe because callers pass a new execution with no signals.
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
      packExecutionMeta(exec)
    );
  }

  get(id: string): Execution | null {
    const row = this.stmts.get.get(id) as Record<string, unknown> | null;
    return row ? decodeExecution(row) : null;
  }

  findChild(parentExecutionId: string, workflowName: string): Execution | null {
    const rows = this.stmts.childrenByName.all(workflowName) as Record<string, unknown>[];
    return (
      rows.map(decodeExecution).find((child) => child.parentExecutionId === parentExecutionId) ??
      null
    );
  }

  /** Remove an execution whose initial queue publication failed. */
  remove(id: string): boolean {
    const result = this.stmts.remove.run(id) as { changes: number };
    return result.changes === 1;
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
      packExecutionMeta(exec),
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
   * Restore the wait claim if publishing its resume job failed.
   *
   * The signal remains durable, so recover() can publish the resume again.
   */
  restoreSignalWait(id: string, event: string, nodeIndex: number): boolean {
    return this.signals.restoreWaiting(id, event, nodeIndex);
  }

  /**
   * Park a running execution at a `waitFor`, unless the awaited signal has already
   * been recorded (in which case the caller must advance instead).
   */
  parkForSignal(id: string, event: string): ParkOutcome {
    return this.signals.park(id, event);
  }

  list(workflowName?: string, state?: ExecutionState, options?: ExecutionListOptions): Execution[] {
    return this.listing.list(workflowName, state, options);
  }

  /** Runs whose registered definition must remain available and structurally stable. */
  listActive(workflowName: string): Execution[] {
    const rows = this.stmts.listActiveByName.all(workflowName) as Record<string, unknown>[];
    return rows
      .map(decodeExecution)
      .filter((exec) => exec.state !== 'failed' || exec.rollbackStatus === undefined);
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
    const all = rows.map(decodeExecution);
    return all.filter((execution) => {
      if (execution.state === 'failed' && execution.rollbackStatus !== undefined) return false;
      return !execution.parentExecutionId || this.get(execution.parentExecutionId) === null;
    });
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
    return cleanupExecutions(this.db, maxAgeMs, states);
  }

  /** Archive executions at least `maxAgeMs` old to the archive table. Cutoff inclusive, as in `cleanup`. */
  archive(maxAgeMs: number, states: string[] = ['completed', 'failed']): number {
    return archiveExecutions(this.db, maxAgeMs, states);
  }

  /** Get archived execution count */
  getArchivedCount(): number {
    return archivedExecutionCount(this.db);
  }

  close(): void {
    this.db.close();
  }
}
