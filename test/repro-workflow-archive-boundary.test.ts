/**
 * REPRO — `archive(0)` and `cleanup(0)` skip an execution updated in the CURRENT
 * millisecond, so "archive everything terminal, now" silently archives nothing.
 *
 * Both queries filter with `updated_at < ?` against `cutoff = Date.now() - maxAgeMs`.
 * With `maxAgeMs: 0` the cutoff IS the current millisecond, and a strict `<` excludes
 * every row whose `updated_at` lands on it. A run that has just completed is exactly
 * such a row, which is the normal case for the documented "flush everything terminal"
 * call.
 *
 * This is not a theoretical boundary: it took down
 * `test/workflow-docs-examples.test.ts` in a sandbox run, where an execution completed
 * and was archived inside the same millisecond and `archive(0)` returned 0.
 *
 * The boundary is exercised DETERMINISTICALLY rather than hoped for. Each iteration
 * stamps the row with `t0 = Date.now()` and calls `archive(0)` immediately; when
 * `Date.now()` still reads `t0` afterwards, the cutoff computed inside `archive` was
 * certainly `t0`, so `updated_at === cutoff` held and the boundary was really hit. The
 * loop asserts it reached that state at least once, so the test cannot pass by never
 * exercising the case it exists to cover.
 */

import { afterEach, describe, expect, test } from 'bun:test';
import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { WorkflowStore } from '../src/client/workflow/store';
import type { Execution } from '../src/client/workflow/types';

let store: WorkflowStore | undefined;
afterEach(() => {
  store?.close();
  store = undefined;
});

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
  test('archive(0) removes an execution updated in the current millisecond', async () => {
    const dir = await mkdtemp(join(tmpdir(), 'bq-archive-boundary-'));
    store = new WorkflowStore(join(dir, 'wf.db'));

    let boundaryExercised = false;
    for (let i = 0; i < 2000 && !boundaryExercised; i++) {
      const t0 = Date.now();
      store.save(terminalExec(`run-${i}`, t0));
      const archived = store.archive(0, ['completed']);
      // Read the clock AFTER the call: if it has not moved, the `Date.now()` inside
      // archive() cannot have read anything but t0.
      boundaryExercised = Date.now() === t0;
      expect(archived, `iteration ${i} left a terminal execution unarchived`).toBe(1);
    }

    expect(
      boundaryExercised,
      'the same-millisecond case was never reached, so this test proved nothing'
    ).toBe(true);
  }, 30_000);

  test('cleanup(0) deletes an execution updated in the current millisecond', async () => {
    const dir = await mkdtemp(join(tmpdir(), 'bq-cleanup-boundary-'));
    store = new WorkflowStore(join(dir, 'wf.db'));

    let boundaryExercised = false;
    for (let i = 0; i < 2000 && !boundaryExercised; i++) {
      const t0 = Date.now();
      store.save(terminalExec(`run-${i}`, t0));
      const deleted = store.cleanup(0, ['completed']);
      boundaryExercised = Date.now() === t0;
      expect(deleted, `iteration ${i} left a terminal execution in the live table`).toBe(1);
    }

    expect(
      boundaryExercised,
      'the same-millisecond case was never reached, so this test proved nothing'
    ).toBe(true);
  }, 30_000);
});
