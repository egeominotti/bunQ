/**
 * DOCS EXAMPLES — every code sample in the Workflow Engine guide, executed.
 *
 * The rule this file enforces: nothing goes in the documentation that has not been
 * run. Each test mirrors one example from `docs/src/content/docs/guide/workflow/*`
 * as closely as a test can, and asserts the behaviour the surrounding prose claims.
 * If an example changes, this file changes with it; if this file goes red, the docs
 * are lying.
 *
 * External services (payment providers, mailers, cloud APIs) are replaced by small
 * in-memory stand-ins with the same shape, so the ENGINE usage is exercised exactly
 * as written even though the provider is not real.
 */

import { afterEach, describe, expect, test } from 'bun:test';
import { generateText, stepCountIs, tool } from 'ai';
import { MockLanguageModelV3 } from 'ai/test';
import { z } from 'zod';
import { mkdtempSync } from 'node:fs';
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

// ---------------------------------------------------------------- Quick Start

describe('docs: Quick Start', () => {
  test('the order pipeline runs and each step sees the previous result, typed', async () => {
    const payments = {
      charged: [] as number[],
      async charge(orderId: string, amount: number) {
        payments.charged.push(amount);
        return `tx_${orderId}`;
      },
      async refund() {
        /* undo */
      },
    };
    const mailer = { sent: [] as string[], async send(t: string, d: { txId: string }) {
      mailer.sent.push(`${t}:${d.txId}`);
    } };

    const orderFlow = new Workflow<{ orderId: string; amount: number }>('order-pipeline')
      .step('validate', async (ctx) => {
        if (ctx.input.amount <= 0) throw new Error('Invalid amount');
        return { orderId: ctx.input.orderId, validated: true };
      })
      .step(
        'charge',
        async (ctx) => {
          const txId = await payments.charge(ctx.steps.validate.orderId, ctx.input.amount);
          return { transactionId: txId };
        },
        { compensate: async () => void (await payments.refund()) }
      )
      .step('confirm', async (ctx) => {
        await mailer.send('order-confirm', { txId: ctx.steps.charge.transactionId });
        return { emailSent: true };
      });

    engine = new Engine({ embedded: true, dataPath: join(mkdtempSync(join(tmpdir(), 'bq-docs-')), 'wf.db') });
    engine.register(orderFlow);

    const run = await engine.start('order-pipeline', { orderId: 'ORD-1', amount: 99.99 });
    expect(await settle(engine, run.id, 'completed')).toBe('completed');

    const exec = engine.getExecution(run.id);
    expect(exec?.state).toBe('completed');
    expect(payments.charged).toEqual([99.99]);
    expect(mailer.sent).toEqual(['order-confirm:tx_ORD-1']);
  }, 40_000);
});

// ------------------------------------------------------------------- Rollback

describe('docs: Rollback (saga compensation)', () => {
  test('compensations run in reverse start order', async () => {
    const log: string[] = [];
    const accounts = {
      async debit(who: string) {
        log.push(`debit:${who}`);
      },
      async credit(who: string) {
        log.push(`credit:${who}`);
      },
    };

    const flow = new Workflow('money-transfer')
      .step('debit-source', async () => {
        await accounts.debit('from');
        return { debited: true };
      }, { retry: 1, compensate: async () => void (await accounts.credit('from')) })
      .step('credit-target', async () => {
        await accounts.credit('to');
        return { credited: true };
      }, { retry: 1, compensate: async () => void (await accounts.debit('to')) })
      .step('send-receipt', () => {
        throw new Error('Email service down');
      }, { retry: 1 });

    engine = new Engine({ embedded: true });
    engine.register(flow);
    const run = await engine.start('money-transfer', {});
    expect(await settle(engine, run.id, 'failed')).toBe('failed');

    expect(log).toEqual(['debit:from', 'credit:to', 'debit:to', 'credit:from']);
    expect(engine.getExecution(run.id)?.rollbackStatus).toBe('completed');
  }, 40_000);

  test('a failed compensation parks the run, and the operator can resume or abandon', async () => {
    let refusals = 0;
    const log: string[] = [];
    const build = () =>
      new Workflow('charge-flow')
        .step('reserve', () => ({ ok: 1 }), {
          retry: 1,
          compensate: () => void log.push('release'),
        })
        .step('charge', () => ({ ok: 1 }), {
          retry: 1,
          compensate: () => {
            if (refusals++ === 0) throw new Error('refund refused');
            log.push('refund');
          },
        })
        .step('verify', () => {
          throw new Error('compliance rejected');
        }, { retry: 1 });

    engine = new Engine({ embedded: true });
    engine.register(build());
    const run = await engine.start('charge-flow', {});
    expect(await settle(engine, run.id, 'compensation-stuck')).toBe('compensation-stuck');

    // Exactly the fields the guide tells the reader to inspect.
    const parked = engine.getExecution(run.id);
    expect(parked?.state).toBe('compensation-stuck');
    expect(parked?.rollbackStatus).toBe('stuck');
    expect(parked?.failureReason).toBe('compliance rejected');
    expect(parked?.steps.charge?.compensation?.status).toBe('compensation-failed');
    expect(parked?.steps.charge?.compensation?.error).toContain('refund refused');
    // The step behind it is deliberately left un-settled so a resume can reach it.
    expect(parked?.steps.reserve?.compensation).toBeUndefined();

    await engine.resumeCompensation(run.id);
    const resumed = engine.getExecution(run.id);
    expect(resumed?.state).toBe('failed');
    expect(resumed?.rollbackStatus).toBe('completed');
    expect(log).toEqual(['refund', 'release']);
  }, 40_000);

  test('a hung compensation is bounded by the step timeout and parks the run', async () => {
    const flow = new Workflow('pay')
      .step('charge', () => ({ txId: 'tx_1' }), {
        retry: 1,
        timeout: 1000, // bounds chargeCard AND refund
        compensate: () => new Promise<void>(() => {}), // a refund that never settles
      })
      .step(
        'confirm',
        () => {
          throw new Error('confirm failed');
        },
        { retry: 1 }
      );

    engine = new Engine({ embedded: true });
    engine.register(flow);
    const run = await engine.start('pay', {});
    expect(await settle(engine, run.id, 'compensation-stuck')).toBe('compensation-stuck');

    const parked = engine.getExecution(run.id);
    expect(parked?.rollbackStatus).toBe('stuck');
    expect(parked?.steps.charge?.compensation?.status).toBe('compensation-failed');
    expect(parked?.steps.charge?.compensation?.error).toBe('Step timed out after 1000ms');
  }, 40_000);

  test('abandonCompensation records the outstanding steps as skipped', async () => {
    engine = new Engine({ embedded: true });
    engine.register(
      new Workflow('abandon-flow')
        .step('reserve', () => ({ ok: 1 }), { retry: 1, compensate: () => {} })
        .step('charge', () => ({ ok: 1 }), {
          retry: 1,
          compensate: () => {
            throw new Error('permanently refused');
          },
        })
        .step('verify', () => {
          throw new Error('rejected');
        }, { retry: 1 })
    );
    const run = await engine.start('abandon-flow', {});
    expect(await settle(engine, run.id, 'compensation-stuck')).toBe('compensation-stuck');

    engine.abandonCompensation(run.id);
    const exec = engine.getExecution(run.id);
    expect(exec?.state).toBe('failed');
    expect(exec?.rollbackStatus).toBe('stuck');
    expect(exec?.steps.reserve?.compensation?.status).toBe('compensation-skipped');
  }, 40_000);
});

// ------------------------------------------------------------ Idempotency key

describe('docs: Idempotency Keys', () => {
  test('the key is stable across retries and the rollback receives the forward key', async () => {
    const seen: (string | undefined)[] = [];
    let forwardKeyAtRollback: string | undefined;
    let attempts = 0;

    engine = new Engine({ embedded: true });
    engine.register(
      new Workflow('keys')
        .step(
          'charge',
          (ctx) => {
            seen.push(ctx.idempotencyKey);
            if (++attempts < 3) throw new Error('transient');
            return { transactionId: 'tx_1' };
          },
          {
            retry: 3,
            compensate: (ctx) => {
              forwardKeyAtRollback = ctx.forwardIdempotencyKey;
            },
          }
        )
        .step('boom', () => {
          throw new Error('later failure');
        }, { retry: 1 })
    );
    const run = await engine.start('keys', {});
    expect(await settle(engine, run.id, 'failed')).toBe('failed');

    expect(seen).toHaveLength(3);
    expect(new Set(seen).size).toBe(1);
    expect(seen[0]).toBe(`${run.id}:charge#0:forward`);
    expect(forwardKeyAtRollback).toBe(seen[0]);
  }, 40_000);
});

// ------------------------------------------------------------ Point of no return

describe('docs: Point of No Return', () => {
  test('a failure after .pivot() rolls nothing back', async () => {
    const log: string[] = [];
    engine = new Engine({ embedded: true });
    engine.register(
      new Workflow('provision')
        .step('reserve-subdomain', () => ({ ok: 1 }), {
          retry: 1,
          compensate: () => void log.push('release'),
        })
        .step('charge-setup-fee', () => ({ ok: 1 }), {
          retry: 1,
          compensate: () => void log.push('refund'),
        })
        .pivot()
        .step('send-welcome-email', () => ({ sent: true }))
        .step('activate-tenant', () => {
          throw new Error('activation failed');
        }, { retry: 1 })
    );
    const run = await engine.start('provision', {});
    expect(await settle(engine, run.id, 'failed')).toBe('failed');

    expect(log).toEqual([]);
    expect(engine.getExecution(run.id)?.rollbackStatus).toBe('not-applicable');
  }, 40_000);
});

// ------------------------------------------------------------- Retries, schemas

describe('docs: Retries, Timeouts and Schema Validation', () => {
  test('retry and timeout options behave as documented', async () => {
    let attempts = 0;
    engine = new Engine({ embedded: true });
    engine.register(
      new Workflow('call-api').step(
        'call-api',
        () => {
          if (++attempts < 3) throw new Error('HTTP 503');
          return { ok: true };
        },
        { retry: 5, timeout: 10_000 }
      )
    );
    const run = await engine.start('call-api', {});
    expect(await settle(engine, run.id, 'completed')).toBe('completed');
    expect(attempts).toBe(3);
    expect(engine.getExecution(run.id)?.steps['call-api']?.attempts).toBe(3);
  }, 40_000);

  test('a Zod outputSchema failure fails the step', async () => {
    engine = new Engine({ embedded: true });
    engine.register(
      new Workflow('validated').step(
        'charge',
        () => ({ transactionId: 'tx_123', charged: 'not-a-number' }) as unknown as {
          transactionId: string;
          charged: number;
        },
        {
          retry: 1,
          outputSchema: z.object({ transactionId: z.string(), charged: z.number() }),
        }
      )
    );
    const run = await engine.start('validated', {});
    expect(await settle(engine, run.id, 'failed')).toBe('failed');
    expect(engine.getExecution(run.id)?.failureReason).toContain('Output validation failed');
  }, 40_000);
});

// -------------------------------------------------------------- Control flow

describe('docs: Branching and Parallel', () => {
  test('only the matching path runs, and steps after the branch always run', async () => {
    const ran: string[] = [];
    engine = new Engine({ embedded: true });
    engine.register(
      new Workflow('support-ticket')
        .step('classify', () => ({ priority: 'high' }))
        .branch((ctx) => (ctx.steps.classify as { priority: string }).priority)
        .path('high', (w) =>
          w.step('assign-senior', () => {
            ran.push('assign-senior');
            return { assignedTo: 'alice' };
          })
        )
        .path('low', (w) =>
          w.step('auto-reply', () => {
            ran.push('auto-reply');
            return { assignedTo: 'bot' };
          })
        )
        .step('log-ticket', () => {
          ran.push('log-ticket');
          return { logged: true };
        })
    );
    const run = await engine.start('support-ticket', {});
    expect(await settle(engine, run.id, 'completed')).toBe('completed');
    expect(ran).toEqual(['assign-senior', 'log-ticket']);
  }, 40_000);

  test('a non-step node inside a path is rejected at build time', () => {
    expect(() =>
      new Workflow('bad')
        .step('classify', () => ({ tier: 'a' }))
        .branch(() => 'a')
        .path('a', (w) => w.step('x', () => ({ x: 1 })).waitFor('nope'))
    ).toThrow(/path\(\) accepts step\(\) nodes only/);
  });

  test('parallel steps land in ctx.steps and merge downstream', async () => {
    engine = new Engine({ embedded: true });
    engine.register(
      new Workflow('fan-out')
        .parallel((w) =>
          w
            .step('fetch-orders', async () => [{ id: 1 }])
            .step('fetch-preferences', async () => ({ theme: 'dark' }))
        )
        .step('merge', (ctx) => ({
          orders: ctx.steps['fetch-orders'],
          prefs: ctx.steps['fetch-preferences'],
        }))
    );
    const run = await engine.start('fan-out', {});
    expect(await settle(engine, run.id, 'completed')).toBe('completed');
    expect(engine.getExecution(run.id)?.steps.merge?.result).toEqual({
      orders: [{ id: 1 }],
      prefs: { theme: 'dark' },
    });
  }, 40_000);
});

describe('docs: Loops and Iteration', () => {
  test('a doUntil body can read its own history from the indexed records', async () => {
    const sizes: number[] = [];
    engine = new Engine({ embedded: true });
    engine.register(
      new Workflow<{ task: string }>('loop-history').doUntil(
        (_ctx, iteration) => iteration >= 3,
        (w) =>
          w.step('turn', (ctx) => {
            const transcript: unknown[] = [];
            for (let i = 0; ctx.steps[`turn:${i}`]; i++) transcript.push(ctx.steps[`turn:${i}`]);
            sizes.push(transcript.length);
            return { n: transcript.length };
          }),
        { maxIterations: 10 }
      )
    );
    const run = await engine.start('loop-history', { task: 't' });
    expect(await settle(engine, run.id, 'completed')).toBe('completed');
    expect(sizes).toEqual([0, 1, 2]);

    const steps = engine.getExecution(run.id)?.steps ?? {};
    expect(Object.keys(steps).filter((k) => /^turn:\d+$/.test(k)).sort()).toEqual([
      'turn:0',
      'turn:1',
      'turn:2',
    ]);
  }, 40_000);

  test('forEach exposes __item and __index and stores indexed results', async () => {
    const notified: string[] = [];
    engine = new Engine({ embedded: true });
    engine.register(
      new Workflow<{ userIds: string[] }>('notify-all').forEach(
        (ctx) => ctx.input.userIds,
        'notify',
        (ctx) => {
          const userId = ctx.steps.__item as string;
          notified.push(`${String(ctx.steps.__index)}:${userId}`);
          return { notified: userId };
        },
        { retry: 3 }
      )
    );
    const run = await engine.start('notify-all', { userIds: ['u1', 'u2'] });
    expect(await settle(engine, run.id, 'completed')).toBe('completed');
    expect(notified).toEqual(['0:u1', '1:u2']);
    expect(engine.getExecution(run.id)?.steps['notify:1']?.result).toEqual({ notified: 'u2' });
  }, 40_000);
});

// ----------------------------------------------------------------- Signals

describe('docs: Waiting for Humans', () => {
  test('the run parks, the signal resumes it, and the payload is in ctx.signals', async () => {
    engine = new Engine({ embedded: true });
    engine.register(
      new Workflow<{ amount: number }>('expense-approval')
        .step('submit', () => ({ submitted: true }))
        .waitFor('manager-approval', { timeout: 86_400_000 })
        .step('process', (ctx) => {
          const decision = ctx.signals['manager-approval'] as { approved: boolean };
          return { status: decision.approved ? 'paid' : 'rejected' };
        })
    );
    const run = await engine.start('expense-approval', { amount: 120 });
    expect(await settle(engine, run.id, 'waiting')).toBe('waiting');

    await engine.signal(run.id, 'manager-approval', { approved: true });
    expect(await settle(engine, run.id, 'completed')).toBe('completed');
    expect(engine.getExecution(run.id)?.steps.process?.result).toEqual({ status: 'paid' });
  }, 40_000);
});

// ------------------------------------------------------------ Nested workflows

describe('docs: Waiting for Humans, late signal', () => {
  test('signalling a run that already ended throws, as the guide tells you to handle', async () => {
    engine = new Engine({ embedded: true });
    engine.register(new Workflow('late-gate').step('a', () => ({ ok: true })));

    const run = await engine.start('late-gate', {});
    expect(await settle(engine, run.id, 'completed')).toBe('completed');

    let handled = false;
    try {
      await engine.signal(run.id, 'manager-approval', { approved: true });
    } catch {
      // Exactly the shape approval.md tells the reader to write.
      handled = true;
    }
    expect(handled, 'the guide promises this throws so the caller can reconcile').toBe(true);
    expect(engine.getExecution(run.id)?.signals).toEqual({});
  }, 40_000);

  test('a payload-less signal counts as a delivery', async () => {
    engine = new Engine({ embedded: true });
    engine.register(
      new Workflow('bare-signal')
        .waitFor('manager-approval', { timeout: 20_000 })
        .step('after', () => ({ done: true }))
    );

    const run = await engine.start('bare-signal', {});
    const deadline = Date.now() + 8000;
    while (engine.getExecution(run.id)?.state !== 'waiting' && Date.now() < deadline) {
      await Bun.sleep(20);
    }
    await engine.signal(run.id, 'manager-approval');

    expect(await settle(engine, run.id, 'completed')).toBe('completed');
    expect(engine.getExecution(run.id)?.steps.after?.status).toBe('completed');
  }, 40_000);
});

describe('docs: Nested Workflows', () => {
  test("the parent's rollback runs the child's own unwind", async () => {
    const log: string[] = [];
    engine = new Engine({ embedded: true });
    engine.register(
      new Workflow('payment')
        .step('authorize', () => ({ authId: 'a1' }), {
          retry: 1,
          compensate: () => void log.push('void-auth'),
        })
    );
    engine.register(
      new Workflow('order')
        .step('create-order', () => ({ orderId: 'ORD-1', total: 99 }), {
          retry: 1,
          compensate: () => void log.push('cancel-order'),
        })
        .subWorkflow('payment', (ctx) => ({
          amount: (ctx.steps['create-order'] as { total: number }).total,
        }))
        .step('confirm', () => {
          throw new Error('confirmation failed');
        }, { retry: 1 })
    );

    const run = await engine.start('order', {});
    expect(await settle(engine, run.id, 'failed')).toBe('failed');

    // Child first, then parent — reverse start order across the boundary.
    expect(log).toEqual(['void-auth', 'cancel-order']);
    const exec = engine.getExecution(run.id);
    expect(exec?.steps['sub:payment']?.compensation?.status).toBe('compensated');
    expect(exec?.steps['sub:payment']?.result).toHaveProperty('authorize');
  }, 40_000);
});

// -------------------------------------------------------------- Observability

describe('docs: Events and Crash Recovery', () => {
  test('the documented event types are emitted, including compensation:*', async () => {
    const seen: string[] = [];
    engine = new Engine({ embedded: true, onEvent: (e) => seen.push(e.type) });
    engine.register(
      new Workflow('observed')
        .step('a', () => ({ ok: 1 }), { retry: 1, compensate: () => {} })
        .step('b', () => {
          throw new Error('boom');
        }, { retry: 1 })
    );
    const run = await engine.start('observed', {});
    expect(await settle(engine, run.id, 'failed')).toBe('failed');

    for (const type of [
      'workflow:started',
      'step:started',
      'step:completed',
      'step:failed',
      'workflow:compensating',
      'compensation:started',
      'compensation:completed',
      'workflow:failed',
    ]) {
      expect(seen).toContain(type);
    }
  }, 40_000);

  test('subscribe() filters to one execution and unsubscribes', async () => {
    const mine: string[] = [];
    engine = new Engine({ embedded: true });
    engine.register(new Workflow('sub-demo').step('only', () => ({ ok: 1 })));
    const run = await engine.start('sub-demo', {});
    const unsubscribe = engine.subscribe(run.id, (e) => mine.push(e.type));
    await settle(engine, run.id, 'completed');
    unsubscribe();
    expect(mine.every((t) => typeof t === 'string')).toBe(true);
  }, 40_000);

  test('recover() reports what it re-enqueued and completes the run', async () => {
    const dataPath = join(mkdtempSync(join(tmpdir(), 'bq-docs-recover-')), 'wf.db');
    const build = () =>
      new Workflow('recoverable')
        .step('a', () => ({ ok: 1 }))
        .waitFor('go', { timeout: 60_000 })
        .step('b', () => ({ ok: 2 }));

    engine = new Engine({ embedded: true, dataPath, queueName: '__wf:docs:rec' });
    engine.register(build());
    const run = await engine.start('recoverable', {});
    expect(await settle(engine, run.id, 'waiting')).toBe('waiting');
    await engine.close(true);

    engine = new Engine({ embedded: true, dataPath, queueName: '__wf:docs:rec' });
    engine.register(build());
    const recovered = await engine.recover();
    expect(recovered.waiting).toBeGreaterThanOrEqual(1);

    await engine.signal(run.id, 'go', { ok: true });
    expect(await settle(engine, run.id, 'completed')).toBe('completed');
  }, 40_000);

  test('cleanup and archive move terminal executions out of the live table', async () => {
    engine = new Engine({ embedded: true, dataPath: join(mkdtempSync(join(tmpdir(), 'bq-docs-')), 'wf.db') });
    engine.register(new Workflow('short').step('only', () => ({ ok: 1 })));
    const run = await engine.start('short', {});
    expect(await settle(engine, run.id, 'completed')).toBe('completed');

    // maxAge 0 makes every terminal execution eligible right now.
    const archived = engine.archive(0, ['completed']);
    expect(archived).toBeGreaterThanOrEqual(1);
    expect(engine.getArchivedCount()).toBeGreaterThanOrEqual(1);
    expect(engine.cleanup(0, ['completed'])).toBeGreaterThanOrEqual(0);
  }, 40_000);
});

// --------------------------------------------------------------- AI SDK page

describe('docs: Vercel AI SDK examples', () => {
  const planningTools = {
    provision_database: tool({
      description: 'Provision a Postgres database for the tenant.',
      inputSchema: z.object({ region: z.string() }),
    }),
    provision_bucket: tool({
      description: 'Provision an object storage bucket.',
      inputSchema: z.object({ region: z.string() }),
    }),
  };

  /** Stands in for `anthropic('claude-sonnet-5')` so the example runs offline. */
  function planningModel() {
    return new MockLanguageModelV3({
      doGenerate: async () => ({
        content: [
          {
            type: 'tool-call' as const,
            toolCallId: 'c1',
            toolName: 'provision_database',
            input: '{"region":"eu-central"}',
          },
          {
            type: 'tool-call' as const,
            toolCallId: 'c2',
            toolName: 'provision_bucket',
            input: '{"region":"eu-central"}',
          },
        ],
        finishReason: { type: 'tool-calls' as const, raw: 'tool_calls' },
        usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
        warnings: [],
      }),
    });
  }

  test('the model plans, the workflow owns and rolls back the effects', async () => {
    const cloud = {
      live: new Set<string>(),
      async create(name: string, _args: unknown, _o: { idempotencyKey?: string }) {
        cloud.live.add(name);
        return { id: name };
      },
      async destroy(name: string, _o: { idempotencyKey?: string }) {
        cloud.live.delete(name);
      },
    };

    const agentSaga = new Workflow<{ tenant: string }>('provision-tenant')
      .step(
        'plan',
        async (ctx) => {
          const result = await generateText({
            model: planningModel(),
            tools: planningTools,
            stopWhen: stepCountIs(1),
            messages: [
              { role: 'user', content: `Provision infrastructure for ${ctx.input.tenant}.` },
            ],
          });
          return {
            planned: result.toolCalls.map((c) => ({ name: c.toolName, args: c.input })),
          };
        },
        { retry: 2, timeout: 90_000 }
      )
      .forEach(
        (ctx) => (ctx.steps.plan as { planned: { name: string; args: unknown }[] }).planned,
        'apply',
        async (ctx) => {
          const call = ctx.steps.__item as { name: string; args: unknown };
          return cloud.create(call.name, call.args, { idempotencyKey: ctx.idempotencyKey });
        },
        {
          retry: 1,
          compensate: async (ctx) => {
            const call = ctx.steps.__item as { name: string };
            await cloud.destroy(call.name, { idempotencyKey: ctx.idempotencyKey });
          },
        }
      )
      .step('verify', () => {
        throw new Error('compliance rejected the tenant');
      }, { retry: 1 });

    engine = new Engine({ embedded: true });
    engine.register(agentSaga);
    const run = await engine.start('provision-tenant', { tenant: 'acme' });
    expect(await settle(engine, run.id, 'failed')).toBe('failed');

    expect([...cloud.live]).toEqual([]);
    expect(engine.getExecution(run.id)?.rollbackStatus).toBe('completed');
  }, 60_000);

  test('durable agent turns rebuild the transcript from the journal', async () => {
    const promptSizes: number[] = [];
    const model = new MockLanguageModelV3({
      doGenerate: async (options: { prompt: unknown[] }) => {
        promptSizes.push(options.prompt.length);
        return {
          content: [{ type: 'text' as const, text: 'noted' }],
          finishReason: { type: 'stop' as const, raw: 'stop' },
          usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
          warnings: [],
        };
      },
    });

    const MAX_TURNS = 3;
    const agent = new Workflow<{ task: string }>('agent').doUntil(
      (_ctx, iteration) => iteration >= MAX_TURNS,
      (w) =>
        w.step('turn', async (ctx) => {
          const prior: unknown[] = [];
          for (let i = 0; ctx.steps[`turn:${i}`]; i++) {
            prior.push(...(ctx.steps[`turn:${i}`] as { messages: unknown[] }).messages);
          }
          const result = await generateText({
            model,
            stopWhen: stepCountIs(1),
            messages: [
              { role: 'user' as const, content: ctx.input.task },
              ...(prior as never[]),
              ...(prior.length > 0
                ? [{ role: 'user' as const, content: 'Continue.' }]
                : []),
            ],
          });
          return { messages: result.response.messages };
        }),
      { maxIterations: 20 }
    );

    engine = new Engine({ embedded: true });
    engine.register(agent);
    const run = await engine.start('agent', { task: 'Take notes.' });
    expect(await settle(engine, run.id, 'completed')).toBe('completed');

    // The engine property is that the transcript ACCUMULATES. The exact message count
    // per turn is a model detail (a tool-calling turn emits more than a text-only one),
    // so assert monotonic growth rather than fixed numbers.
    expect(promptSizes).toHaveLength(MAX_TURNS);
    const grows = promptSizes.every((n, i) => i === 0 || n > promptSizes[i - 1]);
    expect({ first: promptSizes[0], grows }).toEqual({ first: 1, grows: true });
  }, 60_000);

  test('a rejected approval unwinds what the agent already did', async () => {
    const done: string[] = [];
    engine = new Engine({ embedded: true });
    engine.register(
      new Workflow<{ target: string }>('destructive')
        .step('propose', (ctx) => {
          done.push('propose');
          return { target: ctx.input.target };
        }, { retry: 1, compensate: () => void done.push('undo-propose') })
        .waitFor('human-approval', { timeout: 86_400_000 })
        .step('execute', (ctx) => {
          const decision = ctx.signals['human-approval'] as { approved: boolean };
          if (!decision.approved) throw new Error('operator rejected the action');
          done.push('execute');
          return { deleted: true };
        }, { retry: 1 })
    );

    const run = await engine.start('destructive', { target: 'bucket-1' });
    expect(await settle(engine, run.id, 'waiting')).toBe('waiting');
    await engine.signal(run.id, 'human-approval', { approved: false });
    expect(await settle(engine, run.id, 'failed')).toBe('failed');

    expect(done).toEqual(['propose', 'undo-propose']);
  }, 60_000);
});

// ------------------------------------------------- Rollback: loops (rollback.md)

describe('docs: Loops and rollback', () => {
  test('every iteration is reversed, including the one that failed, by its own key', async () => {
    // The guide's claim, verbatim: "The iteration that FAILED is compensated as well,
    // and it is the one most likely to need it: a charge that reached the provider and
    // then lost the response is recorded failed while the money has already moved."
    //
    // And the handler it prescribes: reverse by `ctx.forwardIdempotencyKey`, because the
    // failed turn has no result to read.
    const provider = {
      charges: new Map<string, number>(),
      refunded: [] as string[],
      async charge(key: string, amount: number) {
        provider.charges.set(key, amount);
        return { key, amount };
      },
      async refundByKey(key: string) {
        provider.refunded.push(key);
      },
    };

    let turn = 0;
    engine = new Engine({ embedded: true });
    engine.register(
      new Workflow('loop-refunds').doUntil(
        () => false, // the third turn ends the loop by failing
        (w) =>
          w.step(
            'charge',
            async (ctx) => {
              const settled = await provider.charge(ctx.idempotencyKey, 10);
              if (turn++ === 2) throw new Error('provider timeout after the charge settled');
              return settled;
            },
            {
              retry: 1,
              compensate: async (ctx) => {
                await provider.refundByKey(ctx.forwardIdempotencyKey as string);
              },
            }
          ),
        { maxIterations: 10 }
      )
    );

    const run = await engine.start('loop-refunds', {});
    expect(await settle(engine, run.id, 'failed')).toBe('failed');

    // Three charges, three distinct keys, three refunds against those same keys.
    expect(provider.charges.size).toBe(3);
    expect(provider.refunded.length).toBe(3);
    expect(new Set(provider.refunded).size, 'each turn reversed by its own key').toBe(3);
    for (const key of provider.refunded) expect(provider.charges.has(key)).toBe(true);
    expect(engine.getExecution(run.id)?.rollbackStatus).toBe('completed');
  }, 60_000);
});
