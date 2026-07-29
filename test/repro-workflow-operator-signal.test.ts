/**
 * REPRO — two defects that corrupt the one thing this module exists to provide: a
 * trustworthy account of what happened.
 *
 * 1. A `compensate` handler that throws a non-Error had its payload destroyed. The
 *    catch did `err instanceof Error ? err.message : String(err)`, and `String({...})`
 *    is `"[object Object]"`. Throwing a structured error is normal for an HTTP client
 *    (`throw { code: 500, detail: 'provider down' }`), and this is recorded on a run
 *    parked in `compensation-stuck` — the state that exists precisely so an operator
 *    has something to act on. They get a string that says nothing.
 *
 * 2. `signal()` on a run that has already finished was accepted, written into the
 *    persisted row and announced with a `signal:received` event. A dashboard then
 *    reports an approval as received against a run that ended before it arrived, and a
 *    closed audit record is mutated after the fact. The caller learns nothing: the
 *    call returns cleanly for a delivery that had no effect and could not have had one.
 *
 * The second fix rejects rather than silently ignoring. A signal racing a run to its
 * end is a real possibility, and turning that race into a rejection asks the caller to
 * handle it. That is the point: they now find out. Writing into a terminal record and
 * saying nothing leaves them believing the opposite of what happened.
 */

import { afterEach, describe, expect, test } from 'bun:test';
import { Engine, Workflow } from '../src/client/workflow';
import { waitForWorkflowState } from './workflowTestUtils';

let engine: Engine | undefined;
afterEach(async () => {
  await engine?.close(true).catch(() => {});
  engine = undefined;
});

describe('a parked unwind keeps the diagnostic it was parked with', () => {
  test('a compensate handler throwing an object records the object, not [object Object]', async () => {
    engine = new Engine({ embedded: true });
    engine.register(
      new Workflow('structured')
        .step('charge', async () => ({ txId: 'tx_1' }), {
          retry: 1,
          compensate: async () => {
            throw { code: 502, detail: 'refund endpoint unreachable' };
          },
        })
        .step('boom', async () => {
          throw new Error('downstream rejected');
        }, { retry: 1 })
    );

    const run = await engine.start('structured');
    expect(await waitForWorkflowState(engine, run.id, 'compensation-stuck')).toBeTruthy();

    const recorded = engine.getExecution(run.id)?.steps.charge?.compensation?.error ?? '';
    expect(recorded, 'the payload the provider sent back was destroyed').not.toBe(
      '[object Object]'
    );
    expect(recorded).toContain('502');
    expect(recorded).toContain('refund endpoint unreachable');
  }, 40_000);

  test('a handler throwing a string or a number still reads sensibly', async () => {
    for (const thrown of ['plain string', 42] as const) {
      engine = new Engine({ embedded: true });
      engine.register(
        new Workflow(`thrown-${typeof thrown}`)
          .step('a', async () => ({}), {
            retry: 1,
            compensate: async () => {
              throw thrown;
            },
          })
          .step('boom', async () => {
            throw new Error('x');
          }, { retry: 1 })
      );
      const run = await engine.start(`thrown-${typeof thrown}`);
      expect(await waitForWorkflowState(engine, run.id, 'compensation-stuck')).toBeTruthy();
      expect(engine.getExecution(run.id)?.steps.a?.compensation?.error).toContain(String(thrown));
      await engine.close(true);
      engine = undefined;
    }
  }, 40_000);
});

describe('a finished run does not accept a signal', () => {
  test('signalling a completed run is rejected and leaves the record untouched', async () => {
    engine = new Engine({ embedded: true });
    engine.register(new Workflow('done').step('a', async () => ({ ok: true })));

    const run = await engine.start('done');
    expect(await waitForWorkflowState(engine, run.id, 'completed')).toBeTruthy();

    await expect(engine.signal(run.id, 'late', { by: 'too-late' })).rejects.toThrow(/completed/);

    expect(
      engine.getExecution(run.id)?.signals,
      'a closed audit record was mutated after the fact'
    ).toEqual({});
  }, 40_000);

  test('signalling a live run still works', async () => {
    engine = new Engine({ embedded: true });
    engine.register(
      new Workflow('live').waitFor('go', { timeout: 20_000 }).step('after', async () => ({}))
    );

    const run = await engine.start('live');
    await waitForWorkflowState(engine, run.id, 'waiting');
    await engine.signal(run.id, 'go', { by: 'ops' });

    expect(await waitForWorkflowState(engine, run.id, 'completed')).toBeTruthy();
    expect(engine.getExecution(run.id)?.signals).toEqual({ go: { by: 'ops' } });
  }, 40_000);
});

describe('a parallel group reports every failure, not the first', () => {
  test('failureReason names both failed steps', async () => {
    // `steps.md` promises "an AggregateError containing every failure". The throw did
    // carry them all; the PERSISTED reason took only `errors[0].message`, so a group
    // where two steps failed recorded one cause and whoever read it went looking for a
    // single problem that was not the only problem.
    engine = new Engine({ embedded: true });
    engine.register(
      new Workflow('par-fail').parallel((w) =>
        w
          .step('charge', async () => {
            throw new Error('card declined');
          }, { retry: 1 })
          .step('reserve', async () => {
            throw new Error('warehouse offline');
          }, { retry: 1 })
          .step('notify', async () => ({ ok: true }), { retry: 1 })
      )
    );

    const run = await engine.start('par-fail');
    expect(await waitForWorkflowState(engine, run.id, 'failed')).toBeTruthy();

    const reason = engine.getExecution(run.id)?.failureReason ?? '';
    expect(reason, 'the first failure must still be there').toContain('card declined');
    expect(reason, 'and so must the second').toContain('warehouse offline');
  }, 40_000);

  test('a single failure reads as itself, with no aggregate wrapper', async () => {
    engine = new Engine({ embedded: true });
    engine.register(
      new Workflow('par-one').parallel((w) =>
        w
          .step('a', async () => {
            throw new Error('only this one');
          }, { retry: 1 })
          .step('b', async () => ({ ok: true }), { retry: 1 })
      )
    );

    const run = await engine.start('par-one');
    expect(await waitForWorkflowState(engine, run.id, 'failed')).toBeTruthy();
    expect(engine.getExecution(run.id)?.failureReason).toBe('only this one');
  }, 40_000);
});
