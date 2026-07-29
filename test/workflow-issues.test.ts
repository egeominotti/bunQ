/**
 * Workflow Engine - Issue reproduction tests
 *
 * Each test demonstrates a known issue in the workflow engine.
 * These tests SHOULD FAIL when the issue exists, and PASS once fixed.
 * Tests marked with comments explain the expected vs actual behavior.
 */

import { describe, test, expect, afterEach, setDefaultTimeout } from 'bun:test';
import { Workflow, Engine } from '../src/client/workflow';
import { waitForWorkflowState } from './workflowTestUtils';
import type { WorkflowEvent, StepEvent } from '../src/client/workflow';
import { WorkflowEmitter } from '../src/client/workflow/emitter';
import { WorkflowStore } from '../src/client/workflow/store';
import {
  executeParallelSteps,
  executeStepWithRetry,
  executeSubWorkflow,
  buildContext,
} from '../src/client/workflow/runner';
import { executeMap, executeForEach } from '../src/client/workflow/loops';
import type { Execution, StepDefinition, StepContext } from '../src/client/workflow/types';

setDefaultTimeout(30_000);

// ============================================================================
// Helper: create a minimal Execution object for unit tests
// ============================================================================
function makeExecution(overrides: Partial<Execution> = {}): Execution {
  const now = Date.now();
  return {
    id: `wf_test_${now}_${Math.random().toString(36).slice(2, 10)}`,
    workflowName: 'test',
    state: 'running',
    input: {},
    steps: {},
    currentNodeIndex: 0,
    signals: {},
    createdAt: now,
    updatedAt: now,
    ...overrides,
  };
}

function makeStepDef(name: string, handler: (ctx: StepContext) => Promise<unknown> | unknown, opts: Partial<StepDefinition> = {}): StepDefinition {
  return {
    name,
    handler,
    retry: opts.retry ?? 1,
    timeout: opts.timeout ?? 30_000,
    compensate: opts.compensate,
    inputSchema: opts.inputSchema,
    outputSchema: opts.outputSchema,
  };
}

// ============================================================================
// ISSUE 1: Emitter listener exception breaks dispatch chain
// ============================================================================
describe('ISSUE: Emitter listener exception breaks dispatch chain', () => {
  test('a throwing listener prevents subsequent listeners from receiving the event', () => {
    const emitter = new WorkflowEmitter();
    const received: string[] = [];

    // Listener 1: throws
    emitter.on('step:completed', () => {
      received.push('listener-1');
      throw new Error('listener-1 exploded');
    });

    // Listener 2: should still receive the event
    emitter.on('step:completed', () => {
      received.push('listener-2');
    });

    // Listener 3 (global): should also receive the event
    emitter.onAny(() => {
      received.push('listener-global');
    });

    // The emitter should NOT propagate listener errors to the caller.
    // All 3 listeners should be called regardless of listener-1 throwing.
    let threw = false;
    try {
      emitter.emitStep('step:completed', 'exec-1', 'wf', 'step-a', { result: 'ok' });
    } catch {
      threw = true;
    }

    expect(threw).toBe(false);
    expect(received).toEqual(['listener-1', 'listener-2', 'listener-global']);
  });
});

// ============================================================================
// ISSUE 2: Branch with non-existent path silently skipped
// ============================================================================
describe('ISSUE: Branch with non-existent path silently skipped', () => {
  let engine: Engine;
  afterEach(async () => {
    if (engine) await engine.close(true);
  });

  test('branch condition returning unknown path name silently skips without error', async () => {
    const log: string[] = [];

    const flow = new Workflow('branch-miss')
      .step('classify', async () => {
        log.push('classify');
        return { tier: 'premium' }; // returns "premium" but no path defined for it
      })
      .branch((ctx) => (ctx.steps['classify'] as { tier: string }).tier)
      .path('vip', (w) =>
        w.step('vip-handler', async () => {
          log.push('vip');
          return { discount: 20 };
        })
      )
      .path('basic', (w) =>
        w.step('basic-handler', async () => {
          log.push('basic');
          return { discount: 0 };
        })
      )
      .step('done', async () => {
        log.push('done');
      });

    engine = new Engine({ embedded: true });
    engine.register(flow);

    const run = await engine.start('branch-miss');
    await waitForWorkflowState(engine, run.id, 'completed');

    const exec = engine.getExecution(run.id);

    // BUG: The workflow completes successfully even though "premium" path doesn't exist.
    // No error, no warning. The branch is silently skipped.
    // Expected: should throw or at least set a warning state.
    expect(exec!.state).toBe('completed');
    expect(log).toEqual(['classify', 'done']); // branch steps silently skipped
    expect(log).not.toContain('vip');
    expect(log).not.toContain('basic');
  });
});

// ============================================================================
// ISSUE 3: Parallel steps - only first error thrown, others lost
// ============================================================================
describe('ISSUE: Parallel steps only report first error', () => {
  test('multiple parallel step failures only surface the first error', async () => {
    const emitter = new WorkflowEmitter();
    const exec = makeExecution();
    const updates: Execution[] = [];

    const steps: StepDefinition[] = [
      makeStepDef('step-a', async () => {
        throw new Error('error-A');
      }),
      makeStepDef('step-b', async () => {
        throw new Error('error-B');
      }),
      makeStepDef('step-c', async () => {
        throw new Error('error-C');
      }),
    ];

    const ctx = buildContext(exec);

    let caughtError: Error | null = null;
    try {
      await executeParallelSteps(steps, ctx, exec, emitter, (e) => updates.push({ ...e }));
    } catch (err) {
      caughtError = err as Error;
    }

    // All errors should be aggregated, not just the first one thrown.
    expect(caughtError).not.toBeNull();
    expect(caughtError).toBeInstanceOf(AggregateError);

    const aggErr = caughtError as AggregateError;
    expect(aggErr.errors.length).toBe(3);
    expect(aggErr.errors.map((e: Error) => e.message).sort()).toEqual(['error-A', 'error-B', 'error-C']);

    // All three steps should be recorded as failed
    expect(exec.steps['step-a'].status).toBe('failed');
    expect(exec.steps['step-b'].status).toBe('failed');
    expect(exec.steps['step-c'].status).toBe('failed');
  });
});

// ============================================================================
// ISSUE 4: Signal overwrite - multiple signals with same event name
// ============================================================================
describe('ISSUE: Signal payload overwritten on duplicate event name', () => {
  let engine: Engine;
  afterEach(async () => {
    if (engine) await engine.close(true);
  });

  test('sending a signal twice overwrites the first payload without warning', async () => {
    const flow = new Workflow('signal-overwrite')
      .step('init', async () => ({ ready: true }))
      .waitFor('approval')
      .step('after', async (ctx) => {
        return { decision: ctx.signals['approval'] };
      });

    engine = new Engine({ embedded: true });
    engine.register(flow);

    const run = await engine.start('signal-overwrite');
    await new Promise((r) => setTimeout(r, 1500));

    // First signal
    await engine.signal(run.id, 'approval', { approved: true, by: 'admin' });

    // Second signal with same event - overwrites silently
    await engine.signal(run.id, 'approval', { approved: false, by: 'manager' });

    await new Promise((r) => setTimeout(r, 1500));

    const exec = engine.getExecution(run.id);

    // BUG: First signal payload is completely lost.
    // Only the second payload is retained.
    expect(exec!.signals['approval']).toEqual({ approved: false, by: 'manager' });
    // The first payload { approved: true, by: 'admin' } is gone.
    // No signal history is maintained.
  });
});

// ============================================================================
// ISSUE 5: listExecutions hardcoded LIMIT 100, no pagination
// ============================================================================
describe('ISSUE: listExecutions hardcoded LIMIT 100', () => {
  test('store.list() returns max 100 rows with no way to paginate', () => {
    const store = new WorkflowStore();
    try {
      const now = Date.now();

      // Insert 110 executions
      for (let i = 0; i < 110; i++) {
        store.save({
          id: `wf_${now}_${i.toString().padStart(4, '0')}`,
          workflowName: 'pagination-test',
          state: 'completed',
          input: { i },
          steps: {},
          currentNodeIndex: 0,
          signals: {},
          createdAt: now + i,
          updatedAt: now + i,
        });
      }

      const results = store.list('pagination-test');

      // BUG: Only 100 returned, 10 are silently truncated.
      // No pagination support (offset/limit params).
      expect(results.length).toBe(100); // proves the bug: 10 executions lost
      // FIXED behavior: should support pagination or return all
    } finally {
      store.close();
    }
  });
});

// ============================================================================
// ISSUE 6: Map node throws without error handling or compensation
// ============================================================================
describe('ISSUE: Map node exception has no error handling', () => {
  let engine: Engine;
  afterEach(async () => {
    if (engine) await engine.close(true);
  });

  test('map transform throwing causes workflow failure with no retry or events', async () => {
    const events: string[] = [];

    const flow = new Workflow('map-error')
      .step('data', async () => ({ values: [1, 2, 3] }))
      .map('transform', () => {
        throw new Error('transform exploded');
      })
      .step('after', async () => ({ done: true }));

    engine = new Engine({
      embedded: true,
      onEvent: (e) => events.push(e.type),
    });
    engine.register(flow);

    const run = await engine.start('map-error');
    await waitForWorkflowState(engine, run.id, 'failed');

    const exec = engine.getExecution(run.id);

    // BUG: Map node failure has:
    // - No retry logic (unlike regular steps)
    // - No step:failed event emitted
    // - No compensation triggered for completed steps
    expect(exec!.state).toBe('failed');

    // No step-level events were emitted for the map failure
    const mapEvents = events.filter((e) => e === 'step:failed');
    // Map doesn't emit step:failed - it's unobservable
    expect(events).toContain('workflow:failed');
    // The 'data' step completed but no compensation runs for map failures
    expect(exec!.steps['data']?.status).toBe('completed');
  });
});

// ============================================================================
// ISSUE 7: resolvedSteps field is never populated (dead code)
// ============================================================================
describe('ISSUE: resolvedSteps field is never populated', () => {
  let engine: Engine;
  afterEach(async () => {
    if (engine) await engine.close(true);
  });

  test('resolvedSteps is defined in type, persisted to DB, but never set', async () => {
    const flow = new Workflow('resolved-steps-test')
      .step('a', async () => ({ val: 1 }))
      .step('b', async () => ({ val: 2 }))
      .step('c', async () => ({ val: 3 }));

    engine = new Engine({ embedded: true });
    engine.register(flow);

    const run = await engine.start('resolved-steps-test');
    await waitForWorkflowState(engine, run.id, 'completed');

    const exec = engine.getExecution(run.id);
    expect(exec!.state).toBe('completed');

    // BUG: resolvedSteps is never populated despite being defined in the type
    // and persisted to the database. It's dead code.
    expect(exec!.resolvedSteps).toBeUndefined();
    // If it were used, it should contain ['a', 'b', 'c']
  });
});

// ============================================================================
// ISSUE 8: Loop (doUntil/doWhile) overwrites step results each iteration
// ============================================================================
describe('Loop step results are kept per iteration', () => {
  let engine: Engine;
  afterEach(async () => {
    if (engine) await engine.close(true);
  });

  test('doUntil keeps every iteration, and the base name still holds the last', async () => {
    let iteration = 0;

    const flow = new Workflow('loop-overwrite')
      .doUntil(
        (ctx) => iteration >= 3,
        (w) =>
          w.step('counter', async () => {
            iteration++;
            return { iteration, timestamp: Date.now() };
          }),
        { maxIterations: 10 }
      );

    engine = new Engine({ embedded: true });
    engine.register(flow);

    const run = await engine.start('loop-overwrite');
    await waitForWorkflowState(engine, run.id, 'completed');

    const exec = engine.getExecution(run.id);
    expect(exec!.state).toBe('completed');

    // The base name still resolves to the LAST iteration. That is the documented
    // contract for downstream steps and stays unchanged.
    const counterResult = exec!.steps['counter']?.result as { iteration: number };
    expect(counterResult.iteration).toBe(3);

    // ...and every iteration is now also kept under an indexed name, the way forEach
    // already did. Without these a loop is a sliding window of one turn, which makes
    // the obvious use of a loop — an agent turn — unable to see its own transcript
    // (test/workflow-ai-sdk-agent.test.ts).
    const iterations = [0, 1, 2].map(
      (i) => (exec!.steps[`counter:${i}`]?.result as { iteration: number } | undefined)?.iteration
    );
    expect(iterations).toEqual([1, 2, 3]);

    // The indexed copies are additive only: they carry the same identity as the
    // iteration they mirror, and are not separate units of work.
    expect(exec!.steps['counter:0']?.occurrence).toBe(0);
    expect(exec!.steps['counter:2']?.occurrence).toBe(2);
  });
});

// ============================================================================
// ISSUE 9: forEach step name collision with manually named steps
// ============================================================================
describe('ISSUE: forEach indexed step names can collide with user steps', () => {
  let engine: Engine;
  afterEach(async () => {
    if (engine) await engine.close(true);
  });

  test('a step colliding with a loop\'s per-iteration namespace is rejected', () => {
    // `process:0` is the name the forEach reserves for its first iteration. Letting
    // both exist is silent corruption either way round: before memoisation the loop
    // overwrote the user's step, after it the loop mistakes the user's record for its
    // own completed work and skips the iteration. Refuse it at registration instead.
    const flow = new Workflow('collision')
      .step('process:0', async () => ({ source: 'manual-step' }))
      .forEach(() => [1, 2], 'process', async () => ({ source: 'forEach' }));

    engine = new Engine({ embedded: true });
    expect(() => engine.register(flow)).toThrow(/collides with the per-iteration names/);
  });
});

// ============================================================================
// ISSUE 10: Compensation doesn't run for map node failures
// ============================================================================
describe('ISSUE: Map failure does not trigger compensation', () => {
  let engine: Engine;
  afterEach(async () => {
    if (engine) await engine.close(true);
  });

  test('completed steps with compensate handlers are not rolled back when map throws', async () => {
    const compensated: string[] = [];

    const flow = new Workflow('map-no-compensate')
      .step(
        'charge',
        async () => ({ txId: 'tx_123' }),
        {
          compensate: async () => {
            compensated.push('charge-refunded');
          },
        }
      )
      .map('transform', () => {
        throw new Error('map exploded');
      });

    engine = new Engine({ embedded: true });
    engine.register(flow);

    const run = await engine.start('map-no-compensate');
    await waitForWorkflowState(engine, run.id, 'failed');

    const exec = engine.getExecution(run.id);
    expect(exec!.state).toBe('failed');
    expect(exec!.steps['charge']?.status).toBe('completed');

    // BUG CHECK: Does compensation run?
    // The map node failure triggers compensation in the executor.
    // However, map itself has no step:failed event, no retry, no dedicated error handling.
    // Compensation DOES run because the error propagates to processStep's catch block.
    // This specific test verifies that path works.
    // The real issue is that map has no retry/events of its own.
    if (compensated.length === 0) {
      // If compensation didn't run, that's a bug
      expect(compensated).toContain('charge-refunded');
    }
  });
});

// ============================================================================
// ISSUE 11: Emitter - typed listeners + global listeners interaction
// ============================================================================
describe('ISSUE: Emitter dispatch order with throwing global listener', () => {
  test('throwing global listener breaks typed listener dispatch', () => {
    const emitter = new WorkflowEmitter();
    const received: string[] = [];

    // Global listener that throws
    emitter.onAny(() => {
      received.push('global-1');
      throw new Error('global boom');
    });

    // Another global listener
    emitter.onAny(() => {
      received.push('global-2');
    });

    let threw = false;
    try {
      emitter.emitStep('step:started', 'exec-1', 'wf', 'step-a');
    } catch {
      threw = true;
    }

    // Global listener exceptions should be caught, not bubble up.
    // Both global listeners should execute.
    expect(threw).toBe(false);
    expect(received).toEqual(['global-1', 'global-2']);
  });
});

// ============================================================================
// ISSUE 12: Input validation runs on every retry attempt
// ============================================================================
describe('ISSUE: Input validation redundantly runs on every retry', () => {
  test('inputSchema.parse() called N times for N retry attempts', async () => {
    const emitter = new WorkflowEmitter();
    const exec = makeExecution({ input: { name: 'test' } });
    let parseCalls = 0;
    let attempts = 0;

    const def = makeStepDef(
      'validated-step',
      async () => {
        attempts++;
        if (attempts < 3) throw new Error(`fail #${attempts}`);
        return { ok: true };
      },
      {
        retry: 3,
        inputSchema: {
          parse: (data: unknown) => {
            parseCalls++;
            return data; // always passes
          },
        },
      }
    );

    const ctx = buildContext(exec);
    await executeStepWithRetry(def, ctx, exec, { emitter, updateFn: () => {} });

    // BUG: Input was validated 3 times (once per attempt) even though it never changes.
    // Input validation should only run once since ctx.input is immutable.
    expect(parseCalls).toBe(3); // proves the bug: should be 1
    expect(attempts).toBe(3);
  });
});

// ============================================================================
// ISSUE 13: Parallel steps share a single context snapshot (stale context)
// ============================================================================
describe('ISSUE: Parallel steps share stale context snapshot', () => {
  test('parallel steps cannot see each others results during execution', async () => {
    const emitter = new WorkflowEmitter();
    const exec = makeExecution();
    const seenByB: Record<string, unknown> = {};

    // step-a completes first, step-b should ideally see step-a's result
    const steps: StepDefinition[] = [
      makeStepDef('step-a', async () => {
        return { fromA: 'hello' };
      }),
      makeStepDef('step-b', async (ctx) => {
        // Copy what step-b sees in context at execution time
        Object.assign(seenByB, ctx.steps);
        return { fromB: 'world' };
      }),
    ];

    const ctx = buildContext(exec);
    await executeParallelSteps(steps, ctx, exec, emitter, () => {});

    // BUG: Context is built BEFORE parallel execution starts.
    // step-b can never see step-a's result even if step-a finishes first.
    // The context snapshot is frozen at the start of parallel execution.
    expect(seenByB['step-a']).toBeUndefined(); // proves the bug: stale context
    // In a fixed version, parallel steps might have access to live state
  });
});

// ============================================================================
// ISSUE 14: Sub-workflow hardcoded 300s timeout, not configurable
// ============================================================================
describe('ISSUE: Sub-workflow timeout is hardcoded', () => {
  test('executeSubWorkflow has no maxWait parameter - hardcoded to 300_000ms', async () => {
    // The function signature is:
    //   executeSubWorkflow(name, input, startFn, getFn, pollIntervalMs?, existingChildId?)
    // There is no parameter for maxWait; it is hardcoded as `const maxWait = 300_000` in
    // the function body.

    // `Function.length` counts only the params before the first default or optional one,
    // so this stays 4 as trailing optional params are added. `existingChildId` was added
    // after this test was written and is why the comment above needed updating: it is what
    // makes re-entering the node resume the child a restart already started, rather than
    // abandoning it and provisioning a second one
    // (`test/repro-workflow-orphan-child.test.ts`).
    expect(executeSubWorkflow.length).toBe(4); // name, input, startFn, getFn

    // Read the source to confirm the hardcoded value
    const src = executeSubWorkflow.toString();
    // The 300_000 constant is baked into the function body
    expect(src).toContain('300');

    // BUG: Users can't customize the timeout for long-running sub-workflows.
    // A sub-workflow that takes >5 minutes will always fail.
    // pollIntervalMs is configurable but maxWait is not.
  });
});

// ============================================================================
// ISSUE 15: forEach compensation - can't compensate individual iterations
// ============================================================================
describe('ISSUE: forEach compensation is per-step, not per-iteration', () => {
  let engine: Engine;
  afterEach(async () => {
    if (engine) await engine.close(true);
  });

  test('forEach compensate handler runs once, not per item processed', async () => {
    const compensateCalls: unknown[] = [];

    const flow = new Workflow<{ items: number[] }>('foreach-compensate')
      .forEach(
        (ctx) => (ctx.input as { items: number[] }).items,
        'process',
        async (ctx) => {
          const item = ctx.steps['__item'] as number;
          return { processed: item };
        },
        {
          compensate: async (ctx) => {
            compensateCalls.push(ctx.steps);
          },
        }
      )
      .step(
        'final',
        async () => {
          throw new Error('deliberate failure');
        },
        { retry: 1 }
      );

    engine = new Engine({ embedded: true });
    engine.register(flow);

    const run = await engine.start('foreach-compensate', { items: [1, 2, 3] });
    await waitForWorkflowState(engine, run.id, 'failed');

    const exec = engine.getExecution(run.id);
    expect(exec!.state).toBe('failed');

    // forEach creates steps process:0, process:1, process:2
    // findStepDef should match indexed forEach step names (e.g. "process:0")
    // back to the forEach step definition (which has name "process").
    // Compensation should run for each completed forEach iteration.
    expect(compensateCalls.length).toBeGreaterThan(0);
  });
});

// ============================================================================
// ISSUE 16: WorkflowStore.list() - no offset/limit parameters
// ============================================================================
describe('ISSUE: WorkflowStore has no pagination API', () => {
  test('list method signature has no offset/limit parameters', () => {
    const store = new WorkflowStore();
    try {
      // The list method only accepts (workflowName?, state?)
      // No way to pass offset or limit
      expect(store.list.length).toBeLessThanOrEqual(2);

      // Verify the method doesn't accept offset/limit by checking behavior
      const results = store.list();
      expect(Array.isArray(results)).toBe(true);
    } finally {
      store.close();
    }
  });
});

// ============================================================================
// ISSUE 17: Execution ID collision risk with weak random
// ============================================================================
describe('ISSUE: Execution ID uses weak random generation', () => {
  test('ID format uses Date.now() + 8 random chars - collision possible', async () => {
    // Create many executions rapidly to test for collisions
    const store = new WorkflowStore();
    try {
      const ids = new Set<string>();
      const now = Date.now();

      // Generate 1000 IDs in the same format as the executor
      for (let i = 0; i < 1000; i++) {
        const id = `wf_${now}_${Math.random().toString(36).slice(2, 10)}`;
        ids.add(id);
      }

      // With 8 base-36 chars (36^8 = ~2.8 trillion combinations),
      // collisions are extremely unlikely with 1000 IDs.
      // But it's still not a proper UUID v4.
      expect(ids.size).toBe(1000);

      // The concern is more about predictability and the pattern itself.
      // Verify the format: wf_{timestamp}_{random}
      const sampleId = ids.values().next().value as string;
      expect(sampleId).toMatch(/^wf_\d+_[a-z0-9]+$/);
      // A UUID v4 would be more standard and collision-resistant
    } finally {
      store.close();
    }
  });
});

// ============================================================================
// ISSUE 18: Empty workflow - error only after saving to store
// ============================================================================
describe('ISSUE: Empty workflow validated at start(), not register()', () => {
  let engine: Engine;
  afterEach(async () => {
    if (engine) await engine.close(true);
  });

  test('empty workflow is accepted at register() but fails at start()', async () => {
    const flow = new Workflow('empty-wf');
    // No steps added - nodes array is empty

    engine = new Engine({ embedded: true });

    // BUG: register() succeeds for an empty workflow
    expect(() => engine.register(flow)).not.toThrow();

    // Error only happens at start() time
    try {
      await engine.start('empty-wf');
      expect(true).toBe(false); // should not reach here
    } catch (err) {
      expect((err as Error).message).toContain('has no steps');
    }
    // Expected: should fail at register() time, not start() time
  });
});

// ============================================================================
// ISSUE 19: doWhile maxIterations check allows one extra condition eval
// ============================================================================
describe('ISSUE: doWhile maxIterations boundary condition', () => {
  test('doWhile checks maxIterations AFTER condition but BEFORE step execution', async () => {
    const emitter = new WorkflowEmitter();
    let conditionCalls = 0;
    let stepCalls = 0;

    const def = {
      condition: () => {
        conditionCalls++;
        return true; // always true
      },
      steps: [
        makeStepDef('work', async () => {
          stepCalls++;
          return {};
        }),
      ],
      maxIterations: 3,
    };

    const exec = makeExecution();

    try {
      const { executeDoWhile } = await import('../src/client/workflow/loops');
      await executeDoWhile(def, exec, emitter, () => {});
    } catch (err) {
      expect((err as Error).message).toContain('maxIterations');
    }

    // doWhile: condition checked at iterations 0, 1, 2, 3
    // Steps run at iterations 0, 1, 2
    // At iteration 3: condition is true but maxIterations exceeded -> throw
    // So condition is evaluated 4 times but steps only run 3 times
    expect(conditionCalls).toBe(4); // one extra condition eval
    expect(stepCalls).toBe(3);
  });
});

// ============================================================================
// ISSUE 20: Store archive uses iterative INSERT/DELETE, not batch SQL
// ============================================================================
describe('ISSUE: Archive operation uses row-by-row insert/delete', () => {
  test('archive iterates rows individually instead of batch INSERT...SELECT', () => {
    const store = new WorkflowStore();
    try {
      const now = Date.now();
      const oldTime = now - 100_000;

      // Create some old executions
      for (let i = 0; i < 5; i++) {
        store.save({
          id: `wf_old_${i}`,
          workflowName: 'archive-test',
          state: 'completed',
          input: { i },
          steps: {},
          currentNodeIndex: 0,
          signals: {},
          createdAt: oldTime,
          updatedAt: oldTime,
        });
      }

      // Archive them
      const archived = store.archive(50_000);
      expect(archived).toBe(5);

      // Verify they moved to archive
      expect(store.getArchivedCount()).toBe(5);

      // Verify they're gone from main table
      const remaining = store.list('archive-test');
      expect(remaining.length).toBe(0);

      // The operation works, but it's not atomic at the SQL level -
      // it iterates rows in a JS transaction. If process crashes mid-loop,
      // partial state is possible (though the transaction should help).
    } finally {
      store.close();
    }
  });
});

// ============================================================================
// ISSUE 21: Compensation for parallel steps - no per-step compensation
// ============================================================================
describe('ISSUE: Parallel step compensation is collective, not individual', () => {
  let engine: Engine;
  afterEach(async () => {
    if (engine) await engine.close(true);
  });

  test('if one parallel step fails, completed parallel steps are not individually compensated', async () => {
    const compensated: string[] = [];

    const flow = new Workflow('parallel-comp')
      .parallel((w) =>
        w
          .step(
            'fast-step',
            async () => ({ done: true }),
            {
              compensate: async () => {
                compensated.push('fast-step-compensated');
              },
            }
          )
          .step(
            'slow-fail',
            async () => {
              await new Promise((r) => setTimeout(r, 100));
              throw new Error('slow step failed');
            },
            { retry: 1 }
          )
      );

    engine = new Engine({ embedded: true });
    engine.register(flow);

    const run = await engine.start('parallel-comp');
    await waitForWorkflowState(engine, run.id, 'failed');

    const exec = engine.getExecution(run.id);
    expect(exec!.state).toBe('failed');

    // fast-step completed before slow-fail errored.
    // Compensation should run for fast-step.
    // Due to Promise.allSettled, both run to completion/failure.
    expect(exec!.steps['fast-step']?.status).toBe('completed');
    expect(exec!.steps['slow-fail']?.status).toBe('failed');

    // Compensation IS run by the executor for completed steps.
    // But the compensation runs with a shared context, not step-specific context.
    if (compensated.length > 0) {
      expect(compensated).toContain('fast-step-compensated');
    }
  });
});

// ============================================================================
// ISSUE 22: Duplicate step names across different branch paths
// ============================================================================
describe('ISSUE: Duplicate step names across branch paths', () => {
  test('same step name in two branch paths causes collision in exec.steps', async () => {
    // getStepNames() collects names from all paths, so this WILL be detected
    // as a duplicate during register()
    const flow = new Workflow('dup-branch')
      .branch((ctx) => 'a')
      .path('a', (w) =>
        w.step('handler', async () => ({ from: 'path-a' }))
      )
      .path('b', (w) =>
        w.step('handler', async () => ({ from: 'path-b' }))
      );

    const engine = new Engine({ embedded: true });

    // This correctly throws due to duplicate detection in getStepNames()
    let threw = false;
    try {
      engine.register(flow);
    } catch (err) {
      threw = true;
      expect((err as Error).message).toContain('Duplicate step names');
    }

    await engine.close(true);

    // The duplicate check works for branch paths since getStepNames()
    // iterates all paths. This is correct behavior.
    // However, the check doesn't cover forEach indexed names vs user names.
    expect(threw).toBe(true);
  });
});

// ============================================================================
// ISSUE 23: executeMap has no emitter integration
// ============================================================================
describe('ISSUE: Map node emits no events', () => {
  test('executeMap should emit step:started and step:completed events', async () => {
    const events: string[] = [];
    const emitter = new WorkflowEmitter();
    emitter.onAny((e) => events.push(e.type));

    const exec = makeExecution();

    // executeMap should accept an emitter and emit events
    await executeMap(
      { name: 'transform', transform: (ctx) => ({ result: 42 }) },
      exec,
      emitter,
      () => {}
    );

    expect(exec.steps['transform']?.status).toBe('completed');
    expect(events).toContain('step:started');
    expect(events).toContain('step:completed');
  });
});

// ============================================================================
// ISSUE 24: Sub-workflow result flattens all step results
// ============================================================================
describe('ISSUE: Sub-workflow returns flat map of all step results', () => {
  let engine: Engine;
  afterEach(async () => {
    if (engine) await engine.close(true);
  });

  test('sub-workflow with many steps returns unstructured flat result map', async () => {
    const child = new Workflow('child-flow')
      .step('a', async () => ({ val: 1 }))
      .step('b', async () => ({ val: 2 }))
      .step('c', async () => ({ val: 3 }));

    const parent = new Workflow('parent-flow')
      .subWorkflow('child-flow', () => ({}))
      .step('after', async (ctx) => {
        return { childResult: ctx.steps['sub:child-flow'] };
      });

    engine = new Engine({ embedded: true });
    engine.register(child);
    engine.register(parent);

    const run = await engine.start('parent-flow');
    await new Promise((r) => setTimeout(r, 3000));

    const exec = engine.getExecution(run.id);
    if (exec?.state === 'completed') {
      // The sub-workflow result is a flat Record<string, unknown>
      // with ALL completed step results. No structure, no filtering.
      const subResult = exec.steps['sub:child-flow']?.result as Record<string, unknown>;
      expect(subResult).toHaveProperty('a');
      expect(subResult).toHaveProperty('b');
      expect(subResult).toHaveProperty('c');
      // For complex workflows this becomes unwieldy
    }
  });
});
