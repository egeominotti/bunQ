/**
 * Workflow Engine - Realistic scenario tests
 * Tests real-world patterns, edge cases, and failure modes.
 */

import { describe, test, expect, afterEach } from 'bun:test';
import { Workflow, Engine } from '../src/client/workflow';
import { waitForWorkflowState } from './workflowTestUtils';

describe('Workflow Engine - Realistic Scenarios', () => {
  let engine: Engine;

  afterEach(async () => {
    if (engine) await engine.close(true);
  });

  // ============ E-commerce Order Flow ============

  test('e-commerce order: full happy path', async () => {
    const events: string[] = [];
    const db: Record<string, unknown> = {};

    const orderFlow = new Workflow<{ orderId: string; amount: number }>('order-flow')
      .step('validate-order', async (ctx) => {
        events.push('validate');
        const input = ctx.input as { orderId: string; amount: number };
        if (input.amount <= 0) throw new Error('Invalid amount');
        return { valid: true, orderId: input.orderId };
      })
      .step('reserve-stock', async (ctx) => {
        events.push('reserve-stock');
        db['stock-reserved'] = true;
        return { reserved: true };
      }, {
        compensate: async () => {
          events.push('release-stock');
          db['stock-reserved'] = false;
        },
      })
      .step('charge-payment', async (ctx) => {
        events.push('charge');
        db['payment-charged'] = true;
        return { transactionId: 'tx_123' };
      }, {
        compensate: async () => {
          events.push('refund');
          db['payment-charged'] = false;
        },
      })
      .step('send-confirmation', async (ctx) => {
        events.push('confirm');
        const txId = (ctx.steps['charge-payment'] as { transactionId: string }).transactionId;
        return { emailSent: true, txId };
      });

    engine = new Engine({ embedded: true });
    engine.register(orderFlow);

    const run = await engine.start('order-flow', { orderId: 'ORD-1', amount: 99.99 });
    await waitForWorkflowState(engine, run.id, 'completed');

    const exec = engine.getExecution(run.id);
    expect(exec!.state).toBe('completed');
    expect(events).toEqual(['validate', 'reserve-stock', 'charge', 'confirm']);
    expect(db['stock-reserved']).toBe(true);
    expect(db['payment-charged']).toBe(true);

    // Verify step results chain correctly
    const confirmResult = exec!.steps['send-confirmation'].result as { emailSent: boolean; txId: string };
    expect(confirmResult.txId).toBe('tx_123');
  });

  test('e-commerce order: payment fails → compensation runs', async () => {
    const events: string[] = [];
    const db: Record<string, unknown> = {};

    const orderFlow = new Workflow('order-fail')
      .step('reserve-stock', async () => {
        events.push('reserve');
        db['stock'] = 'reserved';
        return { ok: true };
      }, {
        compensate: async () => {
          events.push('release-stock');
          db['stock'] = 'available';
        },
      })
      .step('charge-payment', async () => {
        events.push('charge-attempt');
        throw new Error('Card declined');
      }, {
        retry: 1,
        compensate: async () => {
          events.push('refund');
        },
      });

    engine = new Engine({ embedded: true });
    engine.register(orderFlow);

    const run = await engine.start('order-fail');
    await waitForWorkflowState(engine, run.id, 'failed');

    const exec = engine.getExecution(run.id);
    expect(exec!.state).toBe('failed');
    expect(exec!.steps['reserve-stock'].status).toBe('completed');
    expect(exec!.steps['charge-payment'].status).toBe('failed');
    expect(exec!.steps['charge-payment'].error).toContain('Card declined');

    // Compensation: reserve-stock should be rolled back
    expect(events).toContain('release-stock');
    expect(db['stock']).toBe('available');
  });

  // ============ Branching Scenarios ============

  test('branch: different paths produce different results', async () => {
    const results: string[] = [];

    const flow = new Workflow('tier-flow')
      .step('classify', async (ctx) => {
        const input = ctx.input as { total: number };
        return { tier: input.total > 100 ? 'premium' : 'basic' };
      })
      .branch((ctx) => (ctx.steps['classify'] as { tier: string }).tier)
      .path('premium', (w) =>
        w.step('premium-handler', async () => {
          results.push('premium');
          return { shipping: 'express' };
        })
      )
      .path('basic', (w) =>
        w.step('basic-handler', async () => {
          results.push('basic');
          return { shipping: 'standard' };
        })
      )
      .step('done', async () => {
        results.push('done');
      });

    engine = new Engine({ embedded: true });
    engine.register(flow);

    // Premium path
    const run1 = await engine.start('tier-flow', { total: 200 });
    await waitForWorkflowState(engine, run1.id, 'completed');
    expect(engine.getExecution(run1.id)!.state).toBe('completed');

    // Basic path
    const run2 = await engine.start('tier-flow', { total: 50 });
    await waitForWorkflowState(engine, run2.id, 'completed');
    expect(engine.getExecution(run2.id)!.state).toBe('completed');

    expect(results).toContain('premium');
    expect(results).toContain('basic');
    // Both should reach 'done'
    expect(results.filter((r) => r === 'done').length).toBe(2);
  });

  // ============ Wait For Signal ============

  test('approval workflow: submit → wait → approve → process', async () => {
    const log: string[] = [];

    const flow = new Workflow('expense')
      .step('submit-expense', async (ctx) => {
        log.push('submitted');
        return { amount: (ctx.input as { amount: number }).amount };
      })
      .waitFor('manager-approval')
      .step('process-reimbursement', async (ctx) => {
        const approval = ctx.signals['manager-approval'] as { approved: boolean };
        log.push(approval.approved ? 'reimbursed' : 'rejected');
        return { processed: true };
      });

    engine = new Engine({ embedded: true });
    engine.register(flow);

    const run = await engine.start('expense', { amount: 500 });
    await new Promise((r) => setTimeout(r, 800));

    // Should be paused waiting
    let exec = engine.getExecution(run.id);
    expect(exec!.state).toBe('waiting');
    expect(log).toEqual(['submitted']);

    // Manager approves
    await engine.signal(run.id, 'manager-approval', { approved: true });
    await new Promise((r) => setTimeout(r, 800));

    exec = engine.getExecution(run.id);
    expect(exec!.state).toBe('completed');
    expect(log).toEqual(['submitted', 'reimbursed']);
  });

  test('signal with rejection path', async () => {
    const log: string[] = [];

    const flow = new Workflow('expense-reject')
      .step('submit', async () => {
        log.push('submit');
        return {};
      })
      .waitFor('decision')
      .step('handle-decision', async (ctx) => {
        const decision = ctx.signals['decision'] as { approved: boolean };
        if (!decision.approved) {
          log.push('rejected');
          return { status: 'rejected' };
        }
        log.push('approved');
        return { status: 'approved' };
      });

    engine = new Engine({ embedded: true });
    engine.register(flow);

    const run = await engine.start('expense-reject');
    await waitForWorkflowState(engine, run.id, 'waiting');

    await engine.signal(run.id, 'decision', { approved: false });
    await waitForWorkflowState(engine, run.id, 'completed');

    const exec = engine.getExecution(run.id);
    expect(exec!.state).toBe('completed');
    expect(log).toEqual(['submit', 'rejected']);
    expect((exec!.steps['handle-decision'].result as { status: string }).status).toBe('rejected');
  });

  // ============ Multiple Concurrent Executions ============

  test('multiple concurrent executions of same workflow', async () => {
    const results = new Map<string, number>();

    const flow = new Workflow('counter')
      .step('compute', async (ctx) => {
        const input = ctx.input as { n: number };
        // Simulate async work
        await new Promise((r) => setTimeout(r, 50));
        return { result: input.n * 2 };
      });

    engine = new Engine({ embedded: true, concurrency: 10 });
    engine.register(flow);

    // Start 10 concurrent executions
    const runs = await Promise.all(
      Array.from({ length: 10 }, (_, i) => engine.start('counter', { n: i }))
    );

    await Promise.all(runs.map((r) => waitForWorkflowState(engine, r.id, 'completed')));

    // All should complete
    for (const run of runs) {
      const exec = engine.getExecution(run.id);
      expect(exec!.state).toBe('completed');
      const res = exec!.steps['compute'].result as { result: number };
      results.set(run.id, res.result);
    }

    expect(results.size).toBe(10);
  });

  // ============ Step Context Passing ============

  test('steps accumulate results through context', async () => {
    let finalCtx: Record<string, unknown> = {};

    const flow = new Workflow('accumulate')
      .step('a', async () => ({ users: ['alice'] }))
      .step('b', async (ctx) => {
        const prev = ctx.steps['a'] as { users: string[] };
        return { users: [...prev.users, 'bob'] };
      })
      .step('c', async (ctx) => {
        const prev = ctx.steps['b'] as { users: string[] };
        finalCtx = { allUsers: [...prev.users, 'charlie'], input: ctx.input };
        return finalCtx;
      });

    engine = new Engine({ embedded: true });
    engine.register(flow);

    const run = await engine.start('accumulate', { source: 'test' });
    await waitForWorkflowState(engine, run.id, 'completed');

    const exec = engine.getExecution(run.id);
    expect(exec!.state).toBe('completed');
    expect(finalCtx).toEqual({
      allUsers: ['alice', 'bob', 'charlie'],
      input: { source: 'test' },
    });
  });

  // ============ Edge Cases ============

  test('workflow with single step', async () => {
    const flow = new Workflow('single')
      .step('only', async () => ({ done: true }));

    engine = new Engine({ embedded: true });
    engine.register(flow);

    const run = await engine.start('single');
    await waitForWorkflowState(engine, run.id, 'completed');

    const exec = engine.getExecution(run.id);
    expect(exec!.state).toBe('completed');
  });

  test('step timeout works', async () => {
    const flow = new Workflow('timeout-test')
      .step('slow', async () => {
        await new Promise((r) => setTimeout(r, 5000));
        return { done: true };
      }, { timeout: 200, retry: 1 });

    engine = new Engine({ embedded: true });
    engine.register(flow);

    const run = await engine.start('timeout-test');
    await waitForWorkflowState(engine, run.id, 'failed');

    const exec = engine.getExecution(run.id);
    expect(exec!.state).toBe('failed');
    expect(exec!.steps['slow'].error).toContain('timed out');
  });

  test('getExecution returns null for unknown ID', () => {
    engine = new Engine({ embedded: true });
    expect(engine.getExecution('nonexistent')).toBeNull();
  });

  test('signal to unknown execution throws', async () => {
    engine = new Engine({ embedded: true });
    await expect(engine.signal('nope', 'event', {})).rejects.toThrow('not found');
  });

  // ============ Persistence ============

  test('execution state persists across queries', async () => {
    const flow = new Workflow('persist-test')
      .step('work', async () => {
        await new Promise((r) => setTimeout(r, 50));
        return { data: 'persisted' };
      });

    engine = new Engine({ embedded: true });
    engine.register(flow);

    const run = await engine.start('persist-test');
    await waitForWorkflowState(engine, run.id, 'completed');

    // Query multiple times — should be consistent
    const exec1 = engine.getExecution(run.id);
    const exec2 = engine.getExecution(run.id);
    expect(exec1!.state).toBe('completed');
    expect(exec2!.state).toBe('completed');
    expect(exec1!.steps['work'].result).toEqual({ data: 'persisted' });
    expect(exec2!.steps['work'].result).toEqual({ data: 'persisted' });
  });
});
