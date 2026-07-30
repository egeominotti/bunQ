import type { Database } from 'bun:sqlite';
import type { Execution, ExecutionListOptions, ExecutionState } from './types';
import { decodeExecution } from './storeExecutionCodec';

const DEFAULT_LIMIT = 100;
const MAX_LIMIT = 1000;
const ORDER_AND_PAGE = 'ORDER BY created_at DESC, id DESC LIMIT ? OFFSET ?';

/** Prepared, consistently ordered execution-list queries. */
export class ExecutionListing {
  private readonly all;
  private readonly byName;
  private readonly byState;
  private readonly byBoth;

  constructor(db: Database) {
    this.all = db.prepare(`SELECT * FROM workflow_executions ${ORDER_AND_PAGE}`);
    this.byName = db.prepare(
      `SELECT * FROM workflow_executions WHERE workflow_name = ? ${ORDER_AND_PAGE}`
    );
    this.byState = db.prepare(
      `SELECT * FROM workflow_executions WHERE state = ? ${ORDER_AND_PAGE}`
    );
    this.byBoth = db.prepare(
      `SELECT * FROM workflow_executions WHERE workflow_name = ? AND state = ? ${ORDER_AND_PAGE}`
    );
  }

  list(
    workflowName?: string,
    state?: ExecutionState,
    options: ExecutionListOptions = {}
  ): Execution[] {
    const { limit, offset } = pageBounds(options);
    let rows: Record<string, unknown>[];
    if (workflowName !== undefined && state !== undefined) {
      rows = this.byBoth.all(workflowName, state, limit, offset) as Record<string, unknown>[];
    } else if (workflowName !== undefined) {
      rows = this.byName.all(workflowName, limit, offset) as Record<string, unknown>[];
    } else if (state !== undefined) {
      rows = this.byState.all(state, limit, offset) as Record<string, unknown>[];
    } else {
      rows = this.all.all(limit, offset) as Record<string, unknown>[];
    }
    return rows.map(decodeExecution);
  }
}

function pageBounds(options: ExecutionListOptions): { limit: number; offset: number } {
  const limit = options.limit ?? DEFAULT_LIMIT;
  const offset = options.offset ?? 0;
  if (!Number.isSafeInteger(limit) || limit < 1 || limit > MAX_LIMIT) {
    throw new RangeError(`Workflow execution list limit must be an integer from 1 to ${MAX_LIMIT}`);
  }
  if (!Number.isSafeInteger(offset) || offset < 0) {
    throw new RangeError('Workflow execution list offset must be a non-negative integer');
  }
  return { limit, offset };
}
