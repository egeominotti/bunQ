/**
 * Retention must include an execution whose updatedAt equals the cutoff.
 * Freeze Date.now only around synchronous store operations so this boundary is
 * exercised once, regardless of SQLite latency or scheduler contention.
 */
import { describe, expect, spyOn, test } from 'bun:test';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { WorkflowStore } from '../src/client/workflow/store';
import type { Execution } from '../src/client/workflow/types';

const STAMP = 1_700_000_000_000;

function terminalExec(id: string, stamp: number): Execution {
  return {
    id,
    workflowName: 'boundary',
    state: 'completed',
    input: {},
    steps: {},
    currentNodeIndex: 0,
    signals: {},
    createdAt: stamp,
    updatedAt: stamp,
  };
}

describe('archive/cleanup include the cutoff millisecond', () => {
  test.each(['archive', 'cleanup'] as const)(
    '%s(0) includes the exact cutoff and preserves newer or nonterminal executions',
    (operation) => {
      const directory = mkdtempSync(join(tmpdir(), `bq-${operation}-boundary-`));
      let store: WorkflowStore | undefined;
      try {
        store = new WorkflowStore(join(directory, 'wf.db'));
        store.save(terminalExec('at-cutoff', STAMP));
        store.save(terminalExec('newer', STAMP + 1));
        store.save({ ...terminalExec('running', STAMP), state: 'running' });

        const now = spyOn(Date, 'now').mockReturnValue(STAMP);
        try {
          expect(store.get('at-cutoff')?.updatedAt).toBe(Date.now());
          now.mockClear();
          expect(store[operation](0, ['completed'])).toBe(1);
          expect(now).toHaveBeenCalled();
          expect(store.get('at-cutoff')).toBeNull();
          expect(store.get('newer')?.updatedAt).toBe(STAMP + 1);
          expect(store.get('running')?.state).toBe('running');
          expect(store.getArchivedCount()).toBe(operation === 'archive' ? 1 : 0);
          expect(store[operation](0, ['completed'])).toBe(0);
        } finally {
          now.mockRestore();
        }
      } finally {
        try {
          store?.close();
        } finally {
          rmSync(directory, { recursive: true, force: true });
        }
      }
    }
  );
});
