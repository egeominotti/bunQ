/**
 * Workflow Engine - Tests for new features:
 * 1. Step retry with backoff
 * 2. Parallel steps
 * 3. Signal timeout
 * 4. Cleanup / archival
 * 5. Observability (events)
 * 6. Nested workflows (sub-workflow)
 */

import { describe, test, expect, afterEach, setDefaultTimeout } from 'bun:test';
import { Workflow, Engine } from '../src/client/workflow';
import type { WorkflowEvent, StepEvent } from '../src/client/workflow';
import { waitForWorkflowState } from './workflowTestUtils';

setDefaultTimeout(30_000);

describe('Workflow Engine - Step Retry', () => {
  let engine: Engine;
  afterEach(async () => {
    if (engine) await engine.close(true);
  });

  test('retries a failing step before giving up', async () => {
    let attempts = 0;

    const flow = new Workflow('retry-test')
      .step(
        'flaky',
        async () => {
          attempts++;
          if (attempts < 3) throw new Error(`fail #${attempts}`);
          return { ok: true };
        },
        { retry: 3 }
      );

    engine = new Engine({ embedded: true });
    engine.register(flow);

    const run = await engine.start('retry-test');
    const exec = await waitForWorkflowState(engine, run.id, 'completed');
    expect(exec!.state).toBe('completed');
    expect(attempts).toBe(3);
    expect(exec!.steps['flaky'].status).toBe('completed');
    expect(exec!.steps['flaky'].attempts).toBe(3);
  });

  test('fails after exhausting all retries', async () => {
    let attempts = 0;

    const flow = new Workflow('retry-exhaust')
      .step(
        'always-fail',
        async () => {
          attempts++;
          throw new Error('nope');
        },
        { retry: 2 }
      );

    engine = new Engine({ embedded: true });
    engine.register(flow);

    const run = await engine.start('retry-exhaust');
    const exec = await waitForWorkflowState(engine, run.id, 'failed');
    expect(exec!.state).toBe('failed');
    expect(attempts).toBe(2);
    expect(exec!.steps['always-fail'].status).toBe('failed');
    expect(exec!.steps['always-fail'].attempts).toBe(2);
  });

  test('retry: 1 means no retry (fail immediately)', async () => {
    let attempts = 0;

    const flow = new Workflow('no-retry')
      .step(
        'once',
        async () => {
          attempts++;
          throw new Error('boom');
        },
        { retry: 1 }
      );

    engine = new Engine({ embedded: true });
    engine.register(flow);

    const run = await engine.start('no-retry');
    const exec = await waitForWorkflowState(engine, run.id, 'failed');
    expect(exec!.state).toBe('failed');
    expect(attempts).toBe(1);
  });

  test('retry + compensation: compensation runs once after all retries exhausted', async () => {
    let attempts = 0;
    const compensations: string[] = [];

    const flow = new Workflow('retry-compensate')
      .step(
        'setup',
        async () => {
          return { done: true };
        },
        {
          compensate: async () => {
            compensations.push('undo-setup');
          },
        }
      )
      .step(
        'flaky',
        async () => {
          attempts++;
          throw new Error('always fails');
        },
        { retry: 2 }
      );

    engine = new Engine({ embedded: true });
    engine.register(flow);

    const run = await engine.start('retry-compensate');
    const exec = await waitForWorkflowState(engine, run.id, 'failed');
    expect(exec!.state).toBe('failed');
    expect(attempts).toBe(2);
    expect(compensations).toEqual(['undo-setup']);
  });
});

describe('Workflow Engine - Parallel Steps', () => {
  let engine: Engine;
  afterEach(async () => {
    if (engine) await engine.close(true);
  });

  test('runs steps in parallel and collects results', async () => {
    const order: string[] = [];
    const startTimes: Record<string, number> = {};

    const flow = new Workflow('parallel-test')
      .step('init', async () => {
        return { ready: true };
      })
      .parallel((w) =>
        w
          .step('email', async () => {
            startTimes['email'] = Date.now();
            order.push('email-start');
            await new Promise((r) => setTimeout(r, 200));
            order.push('email-end');
            return { sent: true };
          })
          .step('sms', async () => {
            startTimes['sms'] = Date.now();
            order.push('sms-start');
            await new Promise((r) => setTimeout(r, 100));
            order.push('sms-end');
            return { sent: true };
          })
          .step('webhook', async () => {
            startTimes['webhook'] = Date.now();
            order.push('webhook-start');
            await new Promise((r) => setTimeout(r, 150));
            order.push('webhook-end');
            return { delivered: true };
          })
      )
      .step('finalize', async () => {
        return { done: true };
      });

    engine = new Engine({ embedded: true });
    engine.register(flow);

    const run = await engine.start('parallel-test');
    await new Promise((r) => setTimeout(r, 3000));

    const exec = engine.getExecution(run.id);
    expect(exec!.state).toBe('completed');
    expect(exec!.steps['email'].status).toBe('completed');
    expect(exec!.steps['sms'].status).toBe('completed');
    expect(exec!.steps['webhook'].status).toBe('completed');
    expect(exec!.steps['finalize'].status).toBe('completed');

    // All parallel steps should have started before any ended
    // (proves they ran concurrently, not sequentially)
    const starts = order.filter((o) => o.endsWith('-start'));
    const firstEnd = order.findIndex((o) => o.endsWith('-end'));
    expect(starts.length).toBe(3);
    expect(firstEnd).toBeGreaterThanOrEqual(3); // All 3 starts before any end

    // Start times should be very close (within 50ms)
    const times = Object.values(startTimes);
    const spread = Math.max(...times) - Math.min(...times);
    expect(spread).toBeLessThan(50);
  });

  test('parallel step failure triggers compensation of prior steps', async () => {
    const compensations: string[] = [];

    const flow = new Workflow('parallel-fail')
      .step(
        'setup',
        async () => ({ ready: true }),
        {
          compensate: async () => {
            compensations.push('undo-setup');
          },
        }
      )
      .parallel((w) =>
        w
          .step('ok-step', async () => {
            await new Promise((r) => setTimeout(r, 50));
            return { ok: true };
          })
          .step(
            'bad-step',
            async () => {
              throw new Error('parallel failure');
            },
            { retry: 1 }
          )
      );

    engine = new Engine({ embedded: true });
    engine.register(flow);

    const run = await engine.start('parallel-fail');
    await new Promise((r) => setTimeout(r, 2000));

    const exec = engine.getExecution(run.id);
    expect(exec!.state).toBe('failed');
    expect(compensations).toContain('undo-setup');
  });

  test('parallel() with empty builder throws', () => {
    expect(() => {
      new Workflow('empty-parallel').parallel((w) => w);
    }).toThrow('parallel() requires at least one step');
  });
});

describe('Workflow Engine - Signal Timeout', () => {
  let engine: Engine;
  afterEach(async () => {
    if (engine) await engine.close(true);
  });

  test('signal timeout fails the workflow when no signal arrives', async () => {
    const compensations: string[] = [];

    const flow = new Workflow('timeout-signal')
      .step(
        'submit',
        async () => ({ submitted: true }),
        {
          compensate: async () => {
            compensations.push('undo-submit');
          },
        }
      )
      .waitFor('approval', { timeout: 1000 });

    engine = new Engine({ embedded: true });
    engine.register(flow);

    const run = await engine.start('timeout-signal');
    // Wait for step to complete + delayed job to fire after timeout
    await new Promise((r) => setTimeout(r, 5000));

    const exec = engine.getExecution(run.id);
    expect(exec!.state).toBe('failed');
    // The waitFor synthetic step should show the timeout error
    expect(exec!.steps['__waitFor:approval']?.error).toContain('timed out');
    // Compensation should have run for completed steps
    expect(compensations).toContain('undo-submit');
  });

  test('signal arriving before timeout succeeds normally', async () => {
    const flow = new Workflow('signal-ok')
      .step('submit', async () => ({ submitted: true }))
      .waitFor('approval', { timeout: 10_000 })
      .step('process', async (ctx) => {
        return { decision: ctx.signals['approval'] };
      });

    engine = new Engine({ embedded: true });
    engine.register(flow);

    const run = await engine.start('signal-ok');
    await new Promise((r) => setTimeout(r, 800));

    expect(engine.getExecution(run.id)!.state).toBe('waiting');

    await engine.signal(run.id, 'approval', { approved: true });
    await new Promise((r) => setTimeout(r, 1000));

    const exec = engine.getExecution(run.id);
    expect(exec!.state).toBe('completed');
    expect((exec!.steps['process'].result as { decision: unknown }).decision).toEqual({
      approved: true,
    });
  });
});

describe('Workflow Engine - Cleanup & Archival', () => {
  let engine: Engine;
  afterEach(async () => {
    if (engine) await engine.close(true);
  });

  test('cleanup removes old completed executions', async () => {
    const flow = new Workflow('cleanup-test').step('do', async () => ({ done: true }));

    engine = new Engine({ embedded: true });
    engine.register(flow);

    await engine.start('cleanup-test');
    await engine.start('cleanup-test');
    await engine.start('cleanup-test');
    await new Promise((r) => setTimeout(r, 1500));

    const before = engine.listExecutions('cleanup-test');
    expect(before.length).toBe(3);
    expect(before.every((e) => e.state === 'completed')).toBe(true);

    // Cleanup with maxAge=0 removes everything completed
    const removed = engine.cleanup(0);
    expect(removed).toBe(3);

    const after = engine.listExecutions('cleanup-test');
    expect(after.length).toBe(0);
  });

  test('cleanup only removes specified states', async () => {
    const flow = new Workflow('cleanup-states')
      .step('do', async () => ({ done: true }));

    engine = new Engine({ embedded: true });
    engine.register(flow);

    await engine.start('cleanup-states');
    await new Promise((r) => setTimeout(r, 1000));

    // Only remove 'failed' — our completed execution should survive
    const removed = engine.cleanup(0, ['failed']);
    expect(removed).toBe(0);

    const remaining = engine.listExecutions('cleanup-states');
    expect(remaining.length).toBe(1);
  });

  test('archive moves executions to archive table', async () => {
    const flow = new Workflow('archive-test').step('do', async () => ({ done: true }));

    engine = new Engine({ embedded: true });
    engine.register(flow);

    await engine.start('archive-test');
    await engine.start('archive-test');
    await new Promise((r) => setTimeout(r, 1500));

    expect(engine.listExecutions('archive-test').length).toBe(2);

    const archived = engine.archive(0);
    expect(archived).toBe(2);

    // Main table should be empty
    expect(engine.listExecutions('archive-test').length).toBe(0);

    // Archive table should have them
    expect(engine.getArchivedCount()).toBe(2);
  });
});

describe('Workflow Engine - Observability', () => {
  let engine: Engine;
  afterEach(async () => {
    if (engine) await engine.close(true);
  });

  test('emits step and workflow lifecycle events', async () => {
    const events: WorkflowEvent[] = [];

    const flow = new Workflow('events-test')
      .step('a', async () => ({ x: 1 }))
      .step('b', async () => ({ y: 2 }));

    engine = new Engine({ embedded: true });
    engine.onAny((e) => events.push(e));
    engine.register(flow);

    const run = await engine.start('events-test');
    await new Promise((r) => setTimeout(r, 1500));

    const types = events.map((e) => e.type);
    expect(types).toContain('workflow:started');
    expect(types).toContain('step:started');
    expect(types).toContain('step:completed');
    expect(types).toContain('workflow:completed');

    // All events reference our execution
    expect(events.every((e) => e.executionId === run.id)).toBe(true);
  });

  test('emits step:retry events on retry', async () => {
    let attempts = 0;
    const retryEvents: StepEvent[] = [];

    const flow = new Workflow('retry-events')
      .step(
        'flaky',
        async () => {
          attempts++;
          if (attempts < 3) throw new Error('fail');
          return { ok: true };
        },
        { retry: 3 }
      );

    engine = new Engine({ embedded: true });
    engine.on('step:retry', (e) => retryEvents.push(e as StepEvent));
    engine.register(flow);

    const run = await engine.start('retry-events');
    await waitForWorkflowState(engine, run.id, 'completed');

    expect(retryEvents.length).toBe(2); // 2 retries before success on 3rd
    expect(retryEvents[0].stepName).toBe('flaky');
    expect(retryEvents[0].attempt).toBe(1);
    expect(retryEvents[1].attempt).toBe(2);
  });

  test('emits workflow:failed and workflow:compensating on failure', async () => {
    const lifecycleTypes: string[] = [];

    const flow = new Workflow('fail-events')
      .step(
        'setup',
        async () => ({ done: true }),
        { compensate: async () => {} }
      )
      .step(
        'boom',
        async () => {
          throw new Error('fail');
        },
        { retry: 1 }
      );

    engine = new Engine({ embedded: true });
    engine.on('workflow:failed', () => lifecycleTypes.push('failed'));
    engine.on('workflow:compensating', () => lifecycleTypes.push('compensating'));
    engine.register(flow);

    await engine.start('fail-events');
    await new Promise((r) => setTimeout(r, 2000));

    expect(lifecycleTypes).toContain('failed');
    expect(lifecycleTypes).toContain('compensating');
  });

  test('onEvent constructor option works', async () => {
    const events: string[] = [];

    const flow = new Workflow('onevent-test').step('a', async () => ({ ok: true }));

    engine = new Engine({
      embedded: true,
      onEvent: (e) => events.push(e.type),
    });
    engine.register(flow);

    await engine.start('onevent-test');
    await new Promise((r) => setTimeout(r, 1500));

    expect(events).toContain('workflow:started');
    expect(events).toContain('workflow:completed');
  });

  test('off removes a listener', async () => {
    const events: string[] = [];
    const listener = (e: WorkflowEvent) => events.push(e.type);

    const flow = new Workflow('off-test').step('a', async () => ({ ok: true }));

    engine = new Engine({ embedded: true });
    engine.on('step:completed', listener);
    engine.off('step:completed', listener);
    engine.register(flow);

    await engine.start('off-test');
    await new Promise((r) => setTimeout(r, 1000));

    expect(events.filter((t) => t === 'step:completed').length).toBe(0);
  });
});

describe('Workflow Engine - Nested Workflows', () => {
  let engine: Engine;
  afterEach(async () => {
    if (engine) await engine.close(true);
  });

  test('sub-workflow executes child and passes results to parent', async () => {
    const child = new Workflow('payment')
      .step('validate', async (ctx) => {
        const input = ctx.input as { amount: number };
        return { valid: input.amount > 0 };
      })
      .step('charge', async (ctx) => {
        const input = ctx.input as { amount: number };
        return { txId: `tx_${input.amount}` };
      });

    const parent = new Workflow('order')
      .step('create-order', async () => {
        return { orderId: 'ORD-1', total: 99.99 };
      })
      .subWorkflow('payment', (ctx) => ({
        amount: (ctx.steps['create-order'] as { total: number }).total,
      }))
      .step('confirm', async (ctx) => {
        const paymentResult = ctx.steps['sub:payment'] as Record<string, unknown>;
        return { confirmed: true, payment: paymentResult };
      });

    engine = new Engine({ embedded: true });
    engine.register(child);
    engine.register(parent);

    const run = await engine.start('order');
    await new Promise((r) => setTimeout(r, 10_000));

    const exec = engine.getExecution(run.id);
    expect(exec!.state).toBe('completed');
    expect(exec!.steps['create-order'].status).toBe('completed');
    expect(exec!.steps['sub:payment'].status).toBe('completed');
    expect(exec!.steps['confirm'].status).toBe('completed');

    // Sub-workflow results should be available
    const subResult = exec!.steps['sub:payment'].result as Record<string, unknown>;
    expect(subResult['validate']).toEqual({ valid: true });
    expect(subResult['charge']).toEqual({ txId: 'tx_99.99' });
  });

  test('sub-workflow failure fails the parent', async () => {
    const compensations: string[] = [];

    const child = new Workflow('bad-child')
      .step(
        'fail',
        async () => {
          throw new Error('child failed');
        },
        { retry: 1 }
      );

    const parent = new Workflow('parent-with-bad-child')
      .step(
        'setup',
        async () => ({ ready: true }),
        { compensate: async () => compensations.push('undo-setup') }
      )
      .subWorkflow('bad-child', () => ({}));

    engine = new Engine({ embedded: true });
    engine.register(child);
    engine.register(parent);

    const run = await engine.start('parent-with-bad-child');
    await new Promise((r) => setTimeout(r, 10_000));

    const exec = engine.getExecution(run.id);
    expect(exec!.state).toBe('failed');
    expect(compensations).toContain('undo-setup');
  });
});

describe('Workflow Engine - Combined Features', () => {
  let engine: Engine;
  afterEach(async () => {
    if (engine) await engine.close(true);
  });

  test('e-commerce pipeline: retry + parallel + compensation', async () => {
    let chargeAttempts = 0;
    const compensations: string[] = [];
    const notifications: string[] = [];

    const flow = new Workflow('ecommerce')
      .step(
        'create-order',
        async () => {
          return { orderId: 'ORD-777' };
        },
        { compensate: async () => compensations.push('cancel-order') }
      )
      .step(
        'charge-payment',
        async () => {
          chargeAttempts++;
          if (chargeAttempts < 2) throw new Error('gateway timeout');
          return { txId: 'tx_abc' };
        },
        {
          retry: 3,
          compensate: async () => compensations.push('refund'),
        }
      )
      .parallel((w) =>
        w
          .step('send-email', async () => {
            notifications.push('email');
            return { email: true };
          })
          .step('send-sms', async () => {
            notifications.push('sms');
            return { sms: true };
          })
      )
      .step('finalize', async () => {
        return { complete: true };
      });

    engine = new Engine({ embedded: true });
    engine.register(flow);

    const run = await engine.start('ecommerce');
    await new Promise((r) => setTimeout(r, 10_000));

    const exec = engine.getExecution(run.id);
    expect(exec!.state).toBe('completed');
    expect(chargeAttempts).toBe(2); // Failed once, succeeded on retry
    expect(notifications).toContain('email');
    expect(notifications).toContain('sms');
    expect(compensations.length).toBe(0); // No compensation needed
  });
});
