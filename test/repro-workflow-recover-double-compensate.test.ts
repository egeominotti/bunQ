/**
 * REPRO — `recover()` rolls a sub-workflow back TWICE, and the execution record
 * shows a clean unwind afterwards.
 *
 * `recoverExecutions` snapshots every recoverable row once (`store.listRecoverable()`)
 * and then drives them one at a time with `await` in between. Driving a PARENT unwinds
 * its child through `unwindChild`, which loads the child FRESH and settles every one of
 * its records. The loop's own copy of that child is still the pre-crash snapshot, where
 * `compensation === undefined` on every record, so `unwind()`'s "never twice" guard
 * (`if (record.compensation) continue`) reads the stale copy and re-dispatches every
 * compensate handler.
 *
 * The `inFlight` claim does not cover this: the two unwinds do not overlap in time, so
 * the claim is free by the time the stale child is reached.
 *
 * Consequence: a refund/release/deprovision runs twice after a crash, and both rows end
 * as `failed` / `rollbackStatus: 'completed'` — the duplicate leaves no trace.
 *
 * Phase 1 runs in a SEPARATE process and is SIGKILLed, because the defect needs a real
 * crash: `inFlight` is module state, so an in-process "restart" would still be holding
 * the claim and would mask this by doing nothing at all.
 */

import { afterEach, describe, expect, test } from 'bun:test';
import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Engine, Workflow } from '../src/client/workflow';

let engine: Engine | undefined;
afterEach(async () => {
  await engine?.close(true).catch(() => {});
  engine = undefined;
});

describe('recover() unwinds a sub-workflow exactly once', () => {
  test('a child already unwound through its parent is not unwound again', async () => {
    const dir = await mkdtemp(join(tmpdir(), 'bq-recover-dup-'));
    const dataPath = join(dir, 'wf.db');
    const repo = new URL('..', import.meta.url).pathname;

    // ---- phase 1: crash with parent AND child both persisted as `compensating` ----
    // The child's rollback blocks forever, so SIGKILL lands with the child's records
    // unsettled -- exactly the on-disk state a crash mid-refund leaves behind.
    const script = join(dir, 'crash.ts');
    await Bun.write(
      script,
      `import { Engine, Workflow } from '${repo}src/client/workflow';
const child = new Workflow('c-svc').step('provision', async () => ({ id: 'res-1' }), {
  timeout: 600000,
  compensate: async () => { console.log('UNDO'); await new Promise(() => {}); },
});
const parent = new Workflow('p-svc')
  .subWorkflow('c-svc', () => ({}))
  .step('boom', async () => { throw new Error('parent failed'); });
const e = new Engine({ embedded: true, dataPath: ${JSON.stringify(dataPath)} });
e.register(child); e.register(parent);
await e.start('p-svc');
`
    );

    const proc = Bun.spawn(['bun', 'run', script], { stdout: 'pipe', stderr: 'pipe' });
    let out = '';
    try {
      const reader = proc.stdout.getReader();
      const deadline = Date.now() + 20_000;
      // `reader.read()` blocks until the child writes. The deadline is only checked
      // BETWEEN reads, so a child that prints nothing parked here forever: the test
      // hit its own timeout, the spawned process was never killed, and orphans with
      // PPID 1 accumulated across runs and skewed every timing-sensitive suite on the
      // machine. The read is now raced against the deadline.
      while (!out.includes('UNDO') && Date.now() < deadline) {
        const chunk = await Promise.race([
          reader.read(),
          Bun.sleep(Math.max(0, deadline - Date.now())).then(() => null),
        ]);
        if (!chunk || chunk.done) break;
        out += new TextDecoder().decode(chunk.value);
      }
    } finally {
      // Runs even if the loop above throws, which is the whole point.
      proc.kill(9);
      await proc.exited;
    }
    expect(out, 'phase 1 never reached the child rollback').toContain('UNDO');
    await Bun.sleep(300);

    // ---- phase 2: fresh process state, recover ----
    const undo: string[] = [];
    const child = new Workflow('c-svc').step('provision', async () => ({ id: 'res-1' }), {
      compensate: async () => {
        undo.push('undo-child-provision');
      },
    });
    const parent = new Workflow('p-svc')
      .subWorkflow('c-svc', () => ({}))
      .step('boom', async () => {
        throw new Error('parent failed');
      });

    engine = new Engine({ embedded: true, dataPath });
    engine.register(child);
    engine.register(parent);

    const before = engine.listExecutions().map((e) => e.state);
    expect(before.filter((s) => s === 'compensating')).toHaveLength(2);

    await engine.recover();
    await Bun.sleep(500);

    // The rollback of a resource must be dispatched once, not once per driver.
    expect(undo, 'the child rollback ran more than once').toEqual(['undo-child-provision']);
  }, 90_000);
});
