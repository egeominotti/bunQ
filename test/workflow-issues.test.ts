/**
 * Workflow Engine - regression and edge-contract tests
 *
 * Confirmed defects stay here as permanent assertions after they are fixed.
 * Intentional semantics are named as contracts so a passing test never reads like
 * evidence that a known bug still exists.
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

function makeStepDef(
  name: string,
  handler: (ctx: StepContext) => Promise<unknown> | unknown,
  opts: Partial<StepDefinition> = {}
): StepDefinition {
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
// Listener isolation
// ============================================================================
describe('Emitter listener isolation', () => {
  test('a throwing listener does not prevent subsequent listeners receiving the event', () => {
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
// Branches are total
// ============================================================================
describe('Branch with a non-existent path fails explicitly', () => {
  let engine: Engine;
  afterEach(async () => {
    if (engine) await engine.close(true);
  });

  test('branch condition returning an unknown path fails before downstream work', async () => {
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
    await waitForWorkflowState(engine, run.id, 'failed');

    const exec = engine.getExecution(run.id);
    expect(exec!.state).toBe('failed');
    expect(exec?.failureReason).toContain('premium');
    expect(log).toEqual(['classify']);
    expect(log).not.toContain('vip');
    expect(log).not.toContain('basic');
  });
});

// ============================================================================
// Parallel failures retain every cause
// ============================================================================
describe('Parallel failure aggregation', () => {
  test('multiple parallel step failures surface every error', async () => {
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
      await executeParallelSteps(steps, ctx, exec, {
        emitter,
        updateFn: (e) => updates.push({ ...e }),
      });
    } catch (err) {
      caughtError = err as Error;
    }

    // All errors should be aggregated, not just the first one thrown.
    expect(caughtError).not.toBeNull();
    expect(caughtError).toBeInstanceOf(AggregateError);

    const aggErr = caughtError as AggregateError;
    expect(aggErr.errors.length).toBe(3);
    expect(aggErr.errors.map((e: Error) => e.message).sort()).toEqual([
      'error-A',
      'error-B',
      'error-C',
    ]);

    // All three steps should be recorded as failed
    expect(exec.steps['step-a'].status).toBe('failed');
    expect(exec.steps['step-b'].status).toBe('failed');
    expect(exec.steps['step-c'].status).toBe('failed');
  });
});

// ============================================================================
// Signal payloads are first-writer-wins
// ============================================================================
describe('Duplicate signal payloads are rejected', () => {
  let engine: Engine;
  afterEach(async () => {
    if (engine) await engine.close(true);
  });

  test('sending a signal twice preserves the accepted payload', async () => {
    const flow = new Workflow('signal-overwrite')
      .step('init', async () => ({ ready: true }))
      .waitFor('approval')
      .step('after', async (ctx) => {
        return { decision: ctx.signals['approval'] };
      });

    engine = new Engine({ embedded: true });
    engine.register(flow);

    const run = await engine.start('signal-overwrite');
    await waitForWorkflowState(engine, run.id, 'waiting');

    const accepted = { approved: true, by: 'admin' };
    await engine.signal(run.id, 'approval', accepted);

    await expect(
      engine.signal(run.id, 'approval', { approved: false, by: 'manager' })
    ).rejects.toThrow(/already received|cannot receive/i);

    await waitForWorkflowState(engine, run.id, 'completed');

    const exec = engine.getExecution(run.id);
    expect(exec!.signals['approval']).toEqual(accepted);
  });
});

// ============================================================================
// Bounded execution listing remains fully pageable
// ============================================================================
describe('Execution listing pagination', () => {
  test('the default page is bounded and the remaining rows are retrievable', () => {
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

      const first = store.list('pagination-test');
      const second = store.list('pagination-test', undefined, { limit: 20, offset: 100 });

      expect(first.length).toBe(100);
      expect(second.length).toBe(10);
      expect(new Set([...first, ...second].map((exec) => exec.id)).size).toBe(110);
    } finally {
      store.close();
    }
  });
});

// ============================================================================
// Map node failures are observable and durable
// ============================================================================
describe('Map node exception handling', () => {
  let engine: Engine;
  afterEach(async () => {
    if (engine) await engine.close(true);
  });

  test('map transform throwing records the failure and emits step:failed', async () => {
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

    expect(exec!.state).toBe('failed');
    const mapEvents = events.filter((e) => e === 'step:failed');
    expect(mapEvents).toHaveLength(1);
    expect(exec?.steps.transform?.status).toBe('failed');
    expect(exec?.steps.transform?.error).toContain('transform exploded');
    expect(events).toContain('workflow:failed');
    // The data step remains eligible for the generic failure unwind.
    expect(exec!.steps['data']?.status).toBe('completed');
  });
});

// ============================================================================
// Branch-selected step names are durable
// ============================================================================
describe('Resolved branch steps', () => {
  let engine: Engine;
  afterEach(async () => {
    if (engine) await engine.close(true);
  });

  test('resolvedSteps contains only the selected branch path', async () => {
    const flow = new Workflow('resolved-steps-test')
      .step('a', async () => ({ val: 1 }))
      .branch(() => 'selected')
      .path('selected', (path) => path.step('b', async () => ({ val: 2 })))
      .path('skipped', (path) => path.step('c', async () => ({ val: 3 })));

    engine = new Engine({ embedded: true });
    engine.register(flow);

    const run = await engine.start('resolved-steps-test');
    await waitForWorkflowState(engine, run.id, 'completed');

    const exec = engine.getExecution(run.id);
    expect(exec!.state).toBe('completed');

    expect(exec!.resolvedSteps).toEqual(['b']);
    expect(exec!.steps.b?.status).toBe('completed');
    expect(exec!.steps.c).toBeUndefined();
  });
});

// ============================================================================
// Loop iterations keep indexed results
// ============================================================================
describe('Loop step results are kept per iteration', () => {
  let engine: Engine;
  afterEach(async () => {
    if (engine) await engine.close(true);
  });

  test('doUntil keeps every iteration, and the base name still holds the last', async () => {
    let iteration = 0;

    const flow = new Workflow('loop-overwrite').doUntil(
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
// forEach indexed namespace collision
// ============================================================================
describe('forEach indexed namespace protection', () => {
  let engine: Engine;
  afterEach(async () => {
    if (engine) await engine.close(true);
  });

  test("a step colliding with a loop's per-iteration namespace is rejected", () => {
    // `process:0` is the name the forEach reserves for its first iteration. Letting
    // both exist is silent corruption either way round: before memoisation the loop
    // overwrote the user's step, after it the loop mistakes the user's record for its
    // own completed work and skips the iteration. Refuse it at registration instead.
    const flow = new Workflow('collision')
      .step('process:0', async () => ({ source: 'manual-step' }))
      // oxlint-disable-next-line array-callback-return -- Workflow.forEach extracts items
      .forEach(
        () => [1, 2],
        'process',
        async () => ({ source: 'forEach' })
      );

    engine = new Engine({ embedded: true });
    expect(() => engine.register(flow)).toThrow(/collides with the per-iteration names/);
  });
});

// ============================================================================
// A map failure triggers the generic unwind
// ============================================================================
describe('Map failure compensation', () => {
  let engine: Engine;
  afterEach(async () => {
    if (engine) await engine.close(true);
  });

  test('completed steps with compensate handlers are rolled back when map throws', async () => {
    const compensated: string[] = [];

    const flow = new Workflow('map-no-compensate')
      .step('charge', async () => ({ txId: 'tx_123' }), {
        compensate: async () => {
          compensated.push('charge-refunded');
        },
      })
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

    expect(compensated).toEqual(['charge-refunded']);
    expect(exec?.steps.transform?.status).toBe('failed');
  });
});

// ============================================================================
// Typed and global emitter listeners are independently isolated
// ============================================================================
describe('Emitter global-listener isolation', () => {
  test('a throwing global listener does not break later global listeners', () => {
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
// Input validation is cached within one retry episode
// ============================================================================
describe('Input validation across retries', () => {
  test('inputSchema.parse() is called once for a stable input', async () => {
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
    await executeStepWithRetry(def, ctx, exec, { emitter, updateFn: () => undefined });

    expect(parseCalls).toBe(1);
    expect(attempts).toBe(3);
  });
});

// ============================================================================
// Parallel siblings share the pre-group context by design
// ============================================================================
describe('Parallel context snapshot contract', () => {
  test('parallel steps cannot see each others results during execution', async () => {
    const emitter = new WorkflowEmitter();
    const exec = makeExecution();
    const seenByB: Record<string, unknown> = {};

    // Siblings are concurrent and therefore cannot depend on each other's result.
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
    await executeParallelSteps(steps, ctx, exec, { emitter, updateFn: () => undefined });

    expect(seenByB['step-a']).toBeUndefined();
  });
});

// ============================================================================
// Sub-workflow timeout and polling are configurable
// ============================================================================
describe('Sub-workflow polling bounds', () => {
  test('the configured bounds are captured by the durable node', () => {
    const flow = new Workflow('custom-child-bounds').subWorkflow('child', () => ({}), {
      timeout: 45_000,
      pollInterval: 250,
    });
    const node = flow.nodes[0];
    expect(node?.type).toBe('subWorkflow');
    if (node?.type !== 'subWorkflow') throw new Error('unexpected node');
    expect(node.timeout).toBe(45_000);
    expect(node.pollInterval).toBe(250);
  });
});

// ============================================================================
// forEach compensation is per iteration
// ============================================================================
describe('forEach per-iteration compensation', () => {
  let engine: Engine;
  afterEach(async () => {
    if (engine) await engine.close(true);
  });

  test('forEach compensate handler runs once for every processed item', async () => {
    const compensateCalls: { item: unknown; index: unknown }[] = [];

    const flow = new Workflow<{ items: number[] }>('foreach-compensate')
      // oxlint-disable-next-line array-callback-return -- Workflow.forEach extracts items
      .forEach(
        (ctx) => (ctx.input as { items: number[] }).items,
        'process',
        async (ctx) => {
          const item = ctx.steps['__item'] as number;
          return { processed: item };
        },
        {
          compensate: async (ctx) => {
            compensateCalls.push({
              item: ctx.steps.__item,
              index: ctx.steps.__index,
            });
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

    expect(compensateCalls).toEqual([
      { item: 3, index: 2 },
      { item: 2, index: 1 },
      { item: 1, index: 0 },
    ]);
  });
});

// ============================================================================
// WorkflowStore.list() validates page bounds
// ============================================================================
describe('WorkflowStore pagination API', () => {
  test('list accepts offset/limit and rejects unsafe bounds', () => {
    const store = new WorkflowStore();
    try {
      expect(store.list(undefined, undefined, { limit: 10, offset: 0 })).toEqual([]);
      expect(() => store.list(undefined, undefined, { limit: 0 })).toThrow(/limit/);
      expect(() => store.list(undefined, undefined, { offset: -1 })).toThrow(/offset/);
    } finally {
      store.close();
    }
  });
});

// ============================================================================
// Execution IDs use opaque 128-bit entropy
// ============================================================================
describe('Execution ID generation', () => {
  test('new runs receive distinct opaque 128-bit identifiers', async () => {
    const engine = new Engine({ embedded: true });
    try {
      engine.register(new Workflow('id-format').step('work', () => null));
      const ids = await Promise.all(
        Array.from({ length: 10 }, () => engine.start('id-format').then((run) => run.id))
      );
      expect(new Set(ids).size).toBe(ids.length);
      for (const id of ids) expect(id).toMatch(/^wf_[0-9a-f]{32}$/);
    } finally {
      await engine.close(true);
    }
  });
});

// ============================================================================
// Empty workflows fail during registration
// ============================================================================
describe('Empty workflow validation', () => {
  let engine: Engine;
  afterEach(async () => {
    if (engine) await engine.close(true);
  });

  test('empty workflow is rejected at register()', () => {
    const flow = new Workflow('empty-wf');
    // No steps added - nodes array is empty

    engine = new Engine({ embedded: true });

    expect(() => engine.register(flow)).toThrow('has no steps');
  });
});

// ============================================================================
// doWhile checks whether it can stop at the exact iteration bound
// ============================================================================
describe('doWhile maxIterations boundary', () => {
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
      await executeDoWhile(def, exec, emitter, () => undefined);
    } catch (err) {
      expect((err as Error).message).toContain('maxIterations');
    }

    // The condition at iteration 3 is necessary: false would terminate cleanly
    // after exactly three bodies, while true proves a fourth body is required and
    // therefore trips the bound.
    expect(conditionCalls).toBe(4);
    expect(stepCalls).toBe(3);
  });
});

// ============================================================================
// Archive moves eligible rows transactionally
// ============================================================================
describe('Archive operation', () => {
  test('archive moves eligible rows out of the live table', () => {
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
    } finally {
      store.close();
    }
  });
});

// ============================================================================
// Parallel siblings retain per-step compensation
// ============================================================================
describe('Parallel step compensation', () => {
  let engine: Engine;
  afterEach(async () => {
    if (engine) await engine.close(true);
  });

  test('if one parallel step fails, completed siblings are compensated', async () => {
    const compensated: string[] = [];

    const flow = new Workflow('parallel-comp').parallel((w) =>
      w
        .step('fast-step', async () => ({ done: true }), {
          compensate: async () => {
            compensated.push('fast-step-compensated');
          },
        })
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

    expect(compensated).toEqual(['fast-step-compensated']);
  });
});

// ============================================================================
// Duplicate step names across different branch paths
// ============================================================================
describe('Duplicate branch-path step names', () => {
  test('the same step name in two paths is rejected at registration', async () => {
    const flow = new Workflow('dup-branch')
      .branch((ctx) => 'a')
      .path('a', (w) => w.step('handler', async () => ({ from: 'path-a' })))
      .path('b', (w) => w.step('handler', async () => ({ from: 'path-b' })));

    const engine = new Engine({ embedded: true });

    expect(() => engine.register(flow)).toThrow(/Duplicate step names/);
    await engine.close(true);
  });
});

// ============================================================================
// Map lifecycle emits step events
// ============================================================================
describe('Map node events', () => {
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
      () => undefined
    );

    expect(exec.steps['transform']?.status).toBe('completed');
    expect(events).toContain('step:started');
    expect(events).toContain('step:completed');
  });
});

// ============================================================================
// Sub-workflow result contract is a flat map of completed child results
// ============================================================================
describe('Sub-workflow result contract', () => {
  let engine: Engine;
  afterEach(async () => {
    if (engine) await engine.close(true);
  });

  test('sub-workflow returns a flat map of completed child results', async () => {
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
    await waitForWorkflowState(engine, run.id, 'completed');

    const exec = engine.getExecution(run.id);
    expect(exec?.state).toBe('completed');
    const subResult = exec?.steps['sub:child-flow']?.result as Record<string, unknown>;
    expect(subResult).toEqual({
      a: { val: 1 },
      b: { val: 2 },
      c: { val: 3 },
    });
  });
});
