import type { Database } from 'bun:sqlite';
import { clock } from './clock';

export function cleanupExecutions(
  db: Database,
  maxAgeMs: number,
  states: string[] = ['completed', 'failed']
): number {
  const cutoff = clock().now() - maxAgeMs;
  const placeholders = states.map(() => '?').join(',');
  const stmt = db.prepare(
    `DELETE FROM workflow_executions WHERE updated_at <= ? AND state IN (${placeholders})`
  );
  const result = stmt.run(cutoff, ...states) as { changes: number };
  return result.changes;
}

export function archiveExecutions(
  db: Database,
  maxAgeMs: number,
  states: string[] = ['completed', 'failed']
): number {
  const cutoff = clock().now() - maxAgeMs;
  const now = clock().now();
  const placeholders = states.map(() => '?').join(',');
  const rows = db
    .prepare(
      `SELECT * FROM workflow_executions
       WHERE updated_at <= ? AND state IN (${placeholders})
       ORDER BY updated_at ASC, id ASC
       LIMIT 1000`
    )
    .all(cutoff, ...states) as Record<string, unknown>[];

  if (rows.length === 0) return 0;

  const insertArchive = db.prepare(`
    INSERT OR REPLACE INTO workflow_executions_archive
    (id, workflow_name, state, input, steps, current_node_index, resolved_steps,
     signals, created_at, updated_at, archived_at, meta)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);
  const deleteOriginal = db.prepare(`DELETE FROM workflow_executions WHERE id = ?`);
  const tx = db.transaction(() => {
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

export function archivedExecutionCount(db: Database): number {
  const row = db.prepare(`SELECT COUNT(*) as cnt FROM workflow_executions_archive`).get() as {
    cnt: number;
  };
  return row.cnt;
}
