/**
 * REGRESSION — the four blocking defects found by an adversarial review of the saga
 * hardening change set, plus the two severe ones fixed alongside them.
 *
 * Each test failed before its fix. None of them is reachable through the ordinary
 * suites, which is why they survived a green `test:sandbox`:
 *
 *   - the process-exit one is a process-lifetime property, invisible to a test runner
 *     that keeps the process alive on purpose;
 *   - the duplicate-node and double-compensation ones need two concurrent drivers of
 *     the same execution;
 *   - the abandon one needs a sub-workflow AND a parked unwind AND an abandon, a
 *     three-way combination no single-feature test covers.
 *
 * The process-exit case is deliberately a spawned child: asserting "this process can
 * exit" is impossible from inside a runner that must not exit.
 */

import { afterEach, describe, expect, test } from 'bun:test';
import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Engine, Workflow } from '../src/client/workflow';

let engine: Engine | undefined;
afterEach(async () => {
  await engine?.close(true);
  engine = undefined;
});

async function settle(e: Engine, id: string, want: string, ms = 20_000) {
  const deadline = Date.now() + ms;
  while (e.getExecution(id)?.state !== want && Date.now() < deadline) await Bun.sleep(25);
  return e.getExecution(id)?.state;
}

// ---------------------------------------------------------------------------
// BLOCKER 1 — an armed waitFor timer kept the process alive after close()
// ---------------------------------------------------------------------------

describe('a parked waitFor does not pin the process', () => {
  test('a child process with a 40-day approval gate still exits after close()', async () => {
    const dir = await mkdtemp(join(tmpdir(), 'bq-exit-'));
    const script = join(dir, 'parked.ts');
    const repo = new URL('..', import.meta.url).pathname;

    // 40 days exceeds setTimeout's 32-bit ceiling, so it exercises the clamp that
    // introduced the regression: the clamp turned "fires immediately" into a real
    // ~24.8-day handle, and an un-unref'd handle of that length is a process pin.
    await Bun.write(
      script,
      `import { Engine, Workflow } from '${repo}src/client/workflow';
import { shutdownManager } from '${repo}src/client';

const flow = new Workflow('parked')
  .step('one', async () => ({ ok: true }))
  .waitFor('never-arrives', { timeout: 40 * 24 * 60 * 60 * 1000 })
  .step('two', async () => ({ done: true }));

const engine = new Engine({ embedded: true });
engine.register(flow);
const run = await engine.start('parked');
const dl = Date.now() + 15000;
while (engine.getExecution(run.id)?.state !== 'waiting' && Date.now() < dl) await Bun.sleep(50);
if (engine.getExecution(run.id)?.state !== 'waiting') { console.log('NEVER_PARKED'); process.exit(2); }
await engine.close(true);
await shutdownManager();
console.log('CLOSED');
`
    );

    const proc = Bun.spawn(['bun', 'run', script], {
      env: { ...process.env, BUNQUEUE_EMBEDDED: '1' },
      stdout: 'pipe',
      stderr: 'pipe',
    });

    const exited = await Promise.race([
      proc.exited,
      Bun.sleep(25_000).then(() => 'TIMEOUT' as const),
    ]);
    const out = await new Response(proc.stdout).text();

    if (exited === 'TIMEOUT') {
      proc.kill();
      throw new Error(`process did not exit; a timer is pinning it. stdout: ${out}`);
    }
    expect(out).toContain('CLOSED');
    expect(exited).toBe(0);
  }, 40_000);
});

// ---------------------------------------------------------------------------
// BLOCKER 2 — abandonCompensation left sub-workflow records with no outcome
// ---------------------------------------------------------------------------

describe('every eligible record carries an outcome after abandon', () => {
  test('a sub-workflow record behind a failed compensation is settled, not skipped', async () => {
    const child = new Workflow('child-svc').step('provision', async () => ({ id: 'res-1' }), {
      compensate: async () => {
        /* the child can undo itself */
      },
    });

    // The FIRST step to be unwound throws, parking the run before the sub: record is
    // reached. That is the shape that exposed the gap: `abandonCompensation` filtered
    // eligibility with findStepDef(), which has no sub: case, so the record was
    // silently passed over and finished with compensation === undefined.
    const parent = new Workflow('parent-svc')
      .step('reserve', async () => ({ held: true }), {
        compensate: async () => {
          /* never reached: the unwind halts before it */
        },
      })
      .subWorkflow('child-svc', () => ({}))
      .step('charge', async () => ({ paid: true }), {
        compensate: async () => {
          throw new Error('provider refused the refund');
        },
      })
      .step('confirm', async () => {
        throw new Error('confirmation failed');
      });

    engine = new Engine({ embedded: true });
    engine.register(child);
    engine.register(parent);

    const run = await engine.start('parent-svc');
    expect(await settle(engine, run.id, 'compensation-stuck')).toBe('compensation-stuck');

    engine.abandonCompensation(run.id);

    const exec = engine.getExecution(run.id);
    expect(exec?.state).toBe('failed');

    // The guarantee the guide states: after abandon, exactly one outcome, never zero.
    const eligible = Object.entries(exec?.steps ?? {}).filter(
      ([name]) => name === 'reserve' || name === 'charge' || name.startsWith('sub:')
    );
    expect(eligible.length).toBeGreaterThanOrEqual(3);
    for (const [name, record] of eligible) {
      expect(record.compensation, `"${name}" was left with no outcome`).toBeDefined();
    }
    expect(exec?.steps['sub:child-svc'].compensation?.status).toBe('compensation-skipped');
  }, 30_000);
});

// ---------------------------------------------------------------------------
// BLOCKER 3 — recover() re-ran a compensation that was already in flight
// ---------------------------------------------------------------------------

describe('a compensation never runs twice', () => {
  test('recover() over a live unwind does not re-dispatch settled handlers', async () => {
    const undo: string[] = [];
    let release: (() => void) | undefined;
    const held = new Promise<void>((r) => {
      release = r;
    });

    const flow = new Workflow('double-undo')
      .step('a', async () => ({ ok: true }), {
        compensate: async () => {
          undo.push('undo-a');
          await held; // hold the unwind open so recover() lands mid-flight
        },
      })
      .step('b', async () => {
        throw new Error('b failed');
      });

    engine = new Engine({ embedded: true });
    engine.register(flow);
    const run = await engine.start('double-undo');

    const deadline = Date.now() + 10_000;
    while (undo.length === 0 && Date.now() < deadline) await Bun.sleep(20);
    expect(undo).toEqual(['undo-a']);

    // The engine is alive and mid-unwind. recover() lists `compensating` executions,
    // so this is a supported call that used to start a second unwind of the same run.
    await engine.recover();
    await Bun.sleep(200);

    release?.();
    expect(await settle(engine, run.id, 'failed')).toBe('failed');
    expect(undo).toEqual(['undo-a']);
  }, 30_000);
});

// ---------------------------------------------------------------------------
// BLOCKER 4 — a duplicate node job re-ran the node and every node after it
// ---------------------------------------------------------------------------

describe('a duplicate node job is ignored', () => {
  test('recover() on a live run does not create a second advance chain', async () => {
    const ran: string[] = [];

    const flow = new Workflow('advance-once')
      .step('one', async () => {
        ran.push('one');
        await Bun.sleep(150);
        return { ok: true };
      })
      .step('two', async () => {
        ran.push('two');
        return { ok: true };
      });

    engine = new Engine({ embedded: true });
    engine.register(flow);
    const run = await engine.start('advance-once');

    // Land inside the first step, while the run is `running` at node 0.
    const deadline = Date.now() + 5_000;
    while (ran.length === 0 && Date.now() < deadline) await Bun.sleep(10);
    await engine.recover();

    expect(await settle(engine, run.id, 'completed')).toBe('completed');
    await Bun.sleep(300); // let any stray duplicate land

    expect(ran).toEqual(['one', 'two']);
  }, 30_000);
});

// ---------------------------------------------------------------------------
// SEVERE — a wedged compensate handler must park the run, not hang it forever
// ---------------------------------------------------------------------------

describe('a compensate handler is bounded by its step timeout', () => {
  test('a handler that never resolves parks the run instead of hanging', async () => {
    const flow = new Workflow('wedged-undo')
      .step('a', async () => ({ ok: true }), {
        timeout: 300,
        compensate: () => new Promise<void>(() => {}), // never settles
      })
      .step('b', async () => {
        throw new Error('b failed');
      });

    engine = new Engine({ embedded: true });
    engine.register(flow);
    const run = await engine.start('wedged-undo');

    // Before the fix this sat in `compensating` forever: not parked, so an operator
    // had no run to resume or abandon, and recover() blocked on it at every startup.
    expect(await settle(engine, run.id, 'compensation-stuck', 15_000)).toBe('compensation-stuck');
    expect(engine.getExecution(run.id)?.steps.a.compensation?.status).toBe('compensation-failed');
  }, 30_000);
});

// ---------------------------------------------------------------------------
// SEVERE — a committed child must not be reported as rolled back
// ---------------------------------------------------------------------------

describe('a sub-workflow past its pivot is not reported as compensated', () => {
  test('the parent parks rather than claiming a rollback that did not happen', async () => {
    const child = new Workflow('committed-child')
      .step('reserve', async () => ({ held: true }), {
        compensate: async () => {
          throw new Error('must never run: the child is committed');
        },
      })
      .pivot()
      .step('ship', async () => ({ shipped: true }));

    const parent = new Workflow('parent-of-committed')
      .subWorkflow('committed-child', () => ({}))
      .step('confirm', async () => {
        throw new Error('confirmation failed');
      });

    engine = new Engine({ embedded: true });
    engine.register(child);
    engine.register(parent);

    const run = await engine.start('parent-of-committed');
    expect(await settle(engine, run.id, 'compensation-stuck')).toBe('compensation-stuck');

    const record = engine.getExecution(run.id)?.steps['sub:committed-child'];
    expect(record?.compensation?.status).toBe('compensation-failed');
    expect(record?.compensation?.error).toContain('committed');
  }, 30_000);
});

// ---------------------------------------------------------------------------
// A step name is not a loop iteration just because it contains a colon
// ---------------------------------------------------------------------------

describe('colon-suffixed step names are not mistaken for loop iterations', () => {
  test('a step named like a loop body plus a suffix keeps its own rollback', async () => {
    const calls: string[] = [];

    // `charge:extra` merely starts with `charge:`. Resolving loop bodies with a bare
    // prefix match made it resolve to the LOOP's definition: the loop's refund ran
    // twice and this step's own refund never ran at all.
    const flow = new Workflow('shadowed-name')
      .doUntil(
        (_ctx, i) => i >= 1,
        (w) =>
          w.step('charge', async () => ({ a: 1 }), {
            compensate: async () => {
              calls.push('undo-charge');
            },
          }),
        { maxIterations: 3 }
      )
      .step('charge:extra', async () => ({ b: 2 }), {
        compensate: async () => {
          calls.push('undo-charge:extra');
        },
      })
      .step('boom', async () => {
        throw new Error('fail');
      });

    engine = new Engine({ embedded: true });
    engine.register(flow);
    const runInfo = await engine.start('shadowed-name');
    expect(await settle(engine, runInfo.id, 'failed')).toBe('failed');

    // Reverse start order, and each exactly once.
    expect(calls).toEqual(['undo-charge:extra', 'undo-charge']);
  }, 30_000);

  test('a plain step is not dropped from the unwind by an unrelated `:0` sibling', async () => {
    const calls: string[] = [];

    // No loops anywhere, so the register-time index-collision check has nothing to
    // compare against and both names are legal. Excluding any record with a `:0`
    // sibling silently dropped `foo` from the unwind while still reporting the
    // rollback as complete.
    const flow = new Workflow('no-loops')
      .step('foo', async () => ({ a: 1 }), {
        compensate: async () => {
          calls.push('undo-foo');
        },
      })
      .step('foo:0', async () => ({ b: 2 }), {
        compensate: async () => {
          calls.push('undo-foo:0');
        },
      })
      .step('boom', async () => {
        throw new Error('fail');
      });

    engine = new Engine({ embedded: true });
    engine.register(flow);
    const runInfo = await engine.start('no-loops');
    expect(await settle(engine, runInfo.id, 'failed')).toBe('failed');

    const exec = engine.getExecution(runInfo.id);
    expect(calls).toEqual(['undo-foo:0', 'undo-foo']);
    expect(exec?.steps.foo.compensation).toBeDefined();
    expect(exec?.rollbackStatus).toBe('completed');
  }, 30_000);

  test('a compensate handler reads its own result, not a colon-sibling step', async () => {
    let seen: unknown;

    // `payment:charge` is a normal step name. Stripping at the LAST colon rebound it
    // onto `payment`, so its rollback was handed the wrong step's output.
    const flow = new Workflow('colon-sibling')
      .step('payment', async () => ({ who: 'PAYMENT-STEP' }))
      .step('payment:charge', async () => ({ who: 'CHARGE-STEP' }), {
        compensate: async (ctx) => {
          seen = (ctx.steps as Record<string, unknown>)['payment'];
        },
      })
      .step('boom', async () => {
        throw new Error('fail');
      });

    engine = new Engine({ embedded: true });
    engine.register(flow);
    const runInfo = await engine.start('colon-sibling');
    expect(await settle(engine, runInfo.id, 'failed')).toBe('failed');

    expect(seen).toEqual({ who: 'PAYMENT-STEP' });
  }, 30_000);

  test('a loop body whose own name contains a colon keeps its own rollback', async () => {
    const calls: string[] = [];

    // The `/:\d+$/` rule was applied to the REMAINDER without anchoring its start, so
    // it only asked "does this end in a numeric segment?". `charge:extra:1` therefore
    // matched base `charge` (remainder `:extra:1` ends in `:1`) and `findStepDef`
    // returned the wrong definition. Observed before the fix: `undo-charge` ran FOUR
    // times, `undo-charge:extra` ran ZERO times, and the run still reported
    // `rollbackStatus: 'completed'` with every record marked `compensated` — a
    // rollback alert that reads clean over reversals that provably did not happen.
    //
    // `charge` is declared first on purpose: findStepDef returns the first match.
    const flow = new Workflow('colon-loop-bodies')
      .doUntil(
        (_ctx, i) => i >= 2,
        (w) =>
          w.step('charge', async () => ({ from: 'charge' }), {
            compensate: async () => {
              calls.push('undo-charge');
            },
          }),
        { maxIterations: 5 }
      )
      .doUntil(
        (_ctx, i) => i >= 2,
        (w) =>
          w.step('charge:extra', async () => ({ from: 'charge:extra' }), {
            compensate: async () => {
              calls.push('undo-charge:extra');
            },
          }),
        { maxIterations: 5 }
      )
      .step('boom', async () => {
        throw new Error('fail');
      });

    engine = new Engine({ embedded: true });
    engine.register(flow);
    const runInfo = await engine.start('colon-loop-bodies');
    expect(await settle(engine, runInfo.id, 'failed')).toBe('failed');

    // Two iterations each, each undone exactly once, in reverse start order.
    expect(calls).toEqual([
      'undo-charge:extra',
      'undo-charge:extra',
      'undo-charge',
      'undo-charge',
    ]);
    expect(engine.getExecution(runInfo.id)?.rollbackStatus).toBe('completed');
  }, 30_000);
});
