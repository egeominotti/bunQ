/**
 * Workflow Engine - E2E Production Scenarios
 *
 * Simulates real production workflows with:
 * - Actual async I/O delays
 * - External service failures
 * - Race conditions
 * - Multi-step compensation chains
 * - Long-running workflows with signals
 * - Concurrent workflows competing for resources
 */

import { describe, test, expect, afterEach } from 'bun:test';
import { Workflow, Engine } from '../src/client/workflow';
import { waitForWorkflowState } from './workflowTestUtils';

// ============ Simulated Services ============

/** Simulated database */
class FakeDB {
  private data = new Map<string, unknown>();
  private log: string[] = [];

  async insert(table: string, record: Record<string, unknown>): Promise<string> {
    await this.delay(10 + Math.random() * 20);
    const id = `${table}_${Date.now()}_${Math.random().toString(36).slice(2, 6)}`;
    this.data.set(id, { ...record, id });
    this.log.push(`INSERT ${table}:${id}`);
    return id;
  }

  async update(id: string, patch: Record<string, unknown>): Promise<void> {
    await this.delay(5 + Math.random() * 10);
    const existing = this.data.get(id) as Record<string, unknown> | undefined;
    if (!existing) throw new Error(`Record ${id} not found`);
    this.data.set(id, { ...existing, ...patch });
    this.log.push(`UPDATE ${id}`);
  }

  async delete(id: string): Promise<void> {
    await this.delay(5);
    this.data.delete(id);
    this.log.push(`DELETE ${id}`);
  }

  get(id: string): unknown {
    return this.data.get(id);
  }

  getLog(): string[] {
    return [...this.log];
  }

  private delay(ms: number): Promise<void> {
    return new Promise((r) => setTimeout(r, ms));
  }
}

/** Simulated payment gateway */
class FakePaymentGateway {
  private charges = new Map<string, { amount: number; status: string }>();
  shouldFail = false;
  failCount = 0;
  private maxFails = 0;

  setFailure(times: number): void {
    this.shouldFail = true;
    this.maxFails = times;
    this.failCount = 0;
  }

  async charge(amount: number, currency: string): Promise<string> {
    await new Promise((r) => setTimeout(r, 30 + Math.random() * 50));
    if (this.shouldFail && this.failCount < this.maxFails) {
      this.failCount++;
      throw new Error(`Payment gateway timeout (attempt ${this.failCount})`);
    }
    const txId = `ch_${Date.now()}`;
    this.charges.set(txId, { amount, status: 'captured' });
    return txId;
  }

  async refund(txId: string): Promise<void> {
    await new Promise((r) => setTimeout(r, 20));
    const charge = this.charges.get(txId);
    if (charge) charge.status = 'refunded';
  }

  getCharge(txId: string): { amount: number; status: string } | undefined {
    return this.charges.get(txId);
  }
}

/** Simulated email service */
class FakeMailer {
  sent: Array<{ to: string; subject: string; body: string }> = [];

  async send(to: string, subject: string, body: string): Promise<void> {
    await new Promise((r) => setTimeout(r, 15));
    this.sent.push({ to, subject, body });
  }
}

/** Simulated inventory service */
class FakeInventory {
  private stock = new Map<string, number>();
  private reservations = new Map<string, { sku: string; qty: number }>();

  setStock(sku: string, qty: number): void {
    this.stock.set(sku, qty);
  }

  async reserve(sku: string, qty: number): Promise<string> {
    await new Promise((r) => setTimeout(r, 20));
    const available = this.stock.get(sku) ?? 0;
    if (available < qty) throw new Error(`Insufficient stock for ${sku}: ${available} < ${qty}`);
    this.stock.set(sku, available - qty);
    const resId = `res_${Date.now()}`;
    this.reservations.set(resId, { sku, qty });
    return resId;
  }

  async release(resId: string): Promise<void> {
    await new Promise((r) => setTimeout(r, 10));
    const res = this.reservations.get(resId);
    if (res) {
      const current = this.stock.get(res.sku) ?? 0;
      this.stock.set(res.sku, current + res.qty);
      this.reservations.delete(resId);
    }
  }

  getStock(sku: string): number {
    return this.stock.get(sku) ?? 0;
  }
}

// ============ Tests ============

describe('Workflow E2E - Production Scenarios', () => {
  let engine: Engine;

  afterEach(async () => {
    if (engine) await engine.close(true);
  });

  test('SCENARIO 1: Full e-commerce order pipeline with real service simulation', async () => {
    const db = new FakeDB();
    const payments = new FakePaymentGateway();
    const mailer = new FakeMailer();
    const inventory = new FakeInventory();

    inventory.setStock('WIDGET-A', 100);

    type OrderInput = { customerId: string; sku: string; qty: number; amount: number; email: string };

    const orderPipeline = new Workflow<OrderInput>('order-pipeline')
      .step('validate-order', async (ctx) => {
        const input = ctx.input as OrderInput;
        if (!input.customerId) throw new Error('Missing customerId');
        if (input.amount <= 0) throw new Error('Invalid amount');
        if (input.qty <= 0) throw new Error('Invalid quantity');
        return { validated: true };
      })
      .step('create-order-record', async (ctx) => {
        const input = ctx.input as OrderInput;
        const orderId = await db.insert('orders', {
          customerId: input.customerId,
          sku: input.sku,
          qty: input.qty,
          amount: input.amount,
          status: 'pending',
        });
        return { orderId };
      }, {
        compensate: async (ctx) => {
          const orderId = (ctx.steps['create-order-record'] as { orderId: string }).orderId;
          await db.update(orderId, { status: 'cancelled' });
        },
      })
      .step('reserve-inventory', async (ctx) => {
        const input = ctx.input as OrderInput;
        const reservationId = await inventory.reserve(input.sku, input.qty);
        return { reservationId };
      }, {
        compensate: async (ctx) => {
          const resId = (ctx.steps['reserve-inventory'] as { reservationId: string }).reservationId;
          await inventory.release(resId);
        },
      })
      .step('process-payment', async (ctx) => {
        const input = ctx.input as OrderInput;
        const transactionId = await payments.charge(input.amount, 'EUR');
        return { transactionId };
      }, {
        compensate: async (ctx) => {
          const txId = (ctx.steps['process-payment'] as { transactionId: string }).transactionId;
          await payments.refund(txId);
        },
      })
      .step('finalize-order', async (ctx) => {
        const orderId = (ctx.steps['create-order-record'] as { orderId: string }).orderId;
        const txId = (ctx.steps['process-payment'] as { transactionId: string }).transactionId;
        await db.update(orderId, { status: 'confirmed', transactionId: txId });
        return { finalized: true };
      })
      .step('send-confirmation-email', async (ctx) => {
        const input = ctx.input as OrderInput;
        const orderId = (ctx.steps['create-order-record'] as { orderId: string }).orderId;
        await mailer.send(
          input.email,
          `Order ${orderId} confirmed`,
          `Your order of ${input.qty}x ${input.sku} has been confirmed.`
        );
        return { emailSent: true };
      });

    engine = new Engine({ embedded: true, concurrency: 3 });
    engine.register(orderPipeline);

    const run = await engine.start('order-pipeline', {
      customerId: 'cust_001',
      sku: 'WIDGET-A',
      qty: 5,
      amount: 49.99,
      email: 'customer@example.com',
    });

    await waitForWorkflowState(engine, run.id, 'completed');

    const exec = engine.getExecution(run.id);
    expect(exec!.state).toBe('completed');

    // Verify all steps completed
    for (const name of ['validate-order', 'create-order-record', 'reserve-inventory', 'process-payment', 'finalize-order', 'send-confirmation-email']) {
      expect(exec!.steps[name]?.status).toBe('completed');
    }

    // Verify side effects
    expect(inventory.getStock('WIDGET-A')).toBe(95); // 100 - 5
    expect(mailer.sent.length).toBe(1);
    expect(mailer.sent[0].to).toBe('customer@example.com');

    // Verify DB operations happened
    const dbLog = db.getLog();
    expect(dbLog.some((l) => l.startsWith('INSERT orders:'))).toBe(true);
    expect(dbLog.some((l) => l.startsWith('UPDATE'))).toBe(true);
  });

  test('SCENARIO 2: Payment failure triggers full compensation chain', async () => {
    const db = new FakeDB();
    const payments = new FakePaymentGateway();
    const inventory = new FakeInventory();

    inventory.setStock('GADGET-B', 50);
    payments.shouldFail = true;
    payments.setFailure(999); // Always fail

    const compensationLog: string[] = [];

    const orderFlow = new Workflow('order-compensate')
      .step('create-order', async () => {
        const id = await db.insert('orders', { status: 'pending' });
        return { orderId: id };
      }, {
        compensate: async (ctx) => {
          compensationLog.push('cancel-order');
          const id = (ctx.steps['create-order'] as { orderId: string }).orderId;
          await db.update(id, { status: 'cancelled' });
        },
      })
      .step('reserve-stock', async () => {
        const resId = await inventory.reserve('GADGET-B', 10);
        return { reservationId: resId };
      }, {
        compensate: async (ctx) => {
          compensationLog.push('release-stock');
          const resId = (ctx.steps['reserve-stock'] as { reservationId: string }).reservationId;
          await inventory.release(resId);
        },
      })
      .step('charge-card', async () => {
        // This will always fail
        await payments.charge(199.99, 'EUR');
        return {};
      });

    engine = new Engine({ embedded: true });
    engine.register(orderFlow);

    const run = await engine.start('order-compensate');
    await waitForWorkflowState(engine, run.id, 'failed');

    const exec = engine.getExecution(run.id);
    expect(exec!.state).toBe('failed');

    // Compensation should have run in reverse
    expect(compensationLog).toEqual(['release-stock', 'cancel-order']);

    // Stock should be restored
    expect(inventory.getStock('GADGET-B')).toBe(50);
  });

  test('SCENARIO 3: User onboarding with KYC approval (human-in-the-loop)', async () => {
    const db = new FakeDB();
    const mailer = new FakeMailer();

    type OnboardingInput = { name: string; email: string; documents: string[] };

    const onboarding = new Workflow<OnboardingInput>('kyc-onboarding')
      .step('create-account', async (ctx) => {
        const input = ctx.input as OnboardingInput;
        const userId = await db.insert('users', {
          name: input.name,
          email: input.email,
          status: 'pending_kyc',
        });
        return { userId };
      })
      .step('submit-kyc', async (ctx) => {
        const input = ctx.input as OnboardingInput;
        const kycId = await db.insert('kyc_submissions', {
          documents: input.documents,
          status: 'pending_review',
        });
        await mailer.send('compliance@company.com', 'New KYC submission', `Review KYC ${kycId}`);
        return { kycId };
      })
      .waitFor('kyc-review')
      .step('process-review', async (ctx) => {
        const review = ctx.signals['kyc-review'] as { approved: boolean; reviewer: string; notes?: string };
        const userId = (ctx.steps['create-account'] as { userId: string }).userId;
        const kycId = (ctx.steps['submit-kyc'] as { kycId: string }).kycId;

        if (review.approved) {
          await db.update(userId, { status: 'active' });
          await db.update(kycId, { status: 'approved', reviewer: review.reviewer });
        } else {
          await db.update(userId, { status: 'rejected' });
          await db.update(kycId, { status: 'rejected', reviewer: review.reviewer, notes: review.notes });
        }
        return { approved: review.approved };
      })
      .step('send-result-email', async (ctx) => {
        const input = ctx.input as OnboardingInput;
        const result = ctx.steps['process-review'] as { approved: boolean };
        const subject = result.approved ? 'Account Approved!' : 'Account Application Update';
        const body = result.approved
          ? `Welcome ${input.name}! Your account is now active.`
          : `Sorry ${input.name}, your application needs additional review.`;
        await mailer.send(input.email, subject, body);
        return { notified: true };
      });

    engine = new Engine({ embedded: true });
    engine.register(onboarding);

    // Start onboarding
    const run = await engine.start('kyc-onboarding', {
      name: 'Alice',
      email: 'alice@example.com',
      documents: ['passport.pdf', 'utility_bill.pdf'],
    });

    await new Promise((r) => setTimeout(r, 1500));

    // Should be waiting for KYC review
    let exec = engine.getExecution(run.id);
    expect(exec!.state).toBe('waiting');
    expect(exec!.steps['create-account'].status).toBe('completed');
    expect(exec!.steps['submit-kyc'].status).toBe('completed');

    // Compliance team email was sent
    expect(mailer.sent.length).toBe(1);
    expect(mailer.sent[0].to).toBe('compliance@company.com');

    // Simulate: compliance team reviews after some time
    await new Promise((r) => setTimeout(r, 200));
    await engine.signal(run.id, 'kyc-review', {
      approved: true,
      reviewer: 'compliance_bot',
    });

    await new Promise((r) => setTimeout(r, 1500));

    exec = engine.getExecution(run.id);
    expect(exec!.state).toBe('completed');

    // User got approval email
    expect(mailer.sent.length).toBe(2);
    expect(mailer.sent[1].to).toBe('alice@example.com');
    expect(mailer.sent[1].subject).toBe('Account Approved!');

    // DB state is correct
    const userId = (exec!.steps['create-account'].result as { userId: string }).userId;
    const user = db.get(userId) as { status: string };
    expect(user.status).toBe('active');
  });

  test('SCENARIO 4: Tiered processing with branch + compensation', async () => {
    const actions: string[] = [];

    const flow = new Workflow('tiered-order')
      .step('analyze', async (ctx) => {
        const input = ctx.input as { amount: number; region: string };
        actions.push('analyze');
        return {
          tier: input.amount > 1000 ? 'enterprise' : 'self-serve',
          riskScore: input.region === 'high-risk' ? 90 : 10,
        };
      })
      .branch((ctx) => {
        const analysis = ctx.steps['analyze'] as { tier: string; riskScore: number };
        if (analysis.riskScore > 80) return 'manual-review';
        return analysis.tier;
      })
      .path('enterprise', (w) =>
        w
          .step('assign-account-manager', async () => {
            actions.push('assign-am');
            return { manager: 'john@company.com' };
          })
          .step('create-enterprise-contract', async () => {
            actions.push('enterprise-contract');
            return { contractId: 'ENT-001' };
          })
      )
      .path('self-serve', (w) =>
        w.step('auto-provision', async () => {
          actions.push('auto-provision');
          return { provisioned: true };
        })
      )
      .path('manual-review', (w) =>
        w.step('flag-for-review', async () => {
          actions.push('flagged');
          return { flagged: true, reason: 'high-risk region' };
        })
      )
      .step('complete', async () => {
        actions.push('complete');
      });

    engine = new Engine({ embedded: true });
    engine.register(flow);

    // Launch all 3 paths concurrently
    const [run1, run2, run3] = await Promise.all([
      engine.start('tiered-order', { amount: 5000, region: 'EU' }),
      engine.start('tiered-order', { amount: 50, region: 'high-risk' }),
      engine.start('tiered-order', { amount: 50, region: 'US' }),
    ]);

    await Promise.all(
      [run1.id, run2.id, run3.id].map((id) => waitForWorkflowState(engine, id, 'completed'))
    );

    // All should complete
    expect(engine.getExecution(run1.id)!.state).toBe('completed');
    expect(engine.getExecution(run2.id)!.state).toBe('completed');
    expect(engine.getExecution(run3.id)!.state).toBe('completed');

    // Enterprise path: assign-am + enterprise-contract
    expect(actions).toContain('assign-am');
    expect(actions).toContain('enterprise-contract');
    // High-risk path: flagged
    expect(actions).toContain('flagged');
    // Self-serve path: auto-provision
    expect(actions).toContain('auto-provision');
    // All 3 should complete
    expect(actions.filter((a) => a === 'complete').length).toBe(3);
  });

  test('SCENARIO 5: Concurrent orders competing for limited inventory', async () => {
    const inventory = new FakeInventory();
    inventory.setStock('LIMITED-ITEM', 3); // Only 3 in stock

    const completedOrders: string[] = [];
    const failedOrders: string[] = [];

    const orderFlow = new Workflow('limited-stock-order')
      .step('reserve', async (ctx) => {
        const input = ctx.input as { orderId: string };
        const resId = await inventory.reserve('LIMITED-ITEM', 1);
        return { reservationId: resId, orderId: input.orderId };
      }, {
        compensate: async (ctx) => {
          const resId = (ctx.steps['reserve'] as { reservationId: string }).reservationId;
          await inventory.release(resId);
        },
      })
      .step('process', async (ctx) => {
        const orderId = (ctx.steps['reserve'] as { orderId: string }).orderId;
        await new Promise((r) => setTimeout(r, 50));
        return { processed: true, orderId };
      });

    engine = new Engine({ embedded: true, concurrency: 10 });
    engine.register(orderFlow);

    // Launch 5 orders for 3 items — 2 should fail
    const runs = await Promise.all(
      Array.from({ length: 5 }, (_, i) =>
        engine.start('limited-stock-order', { orderId: `ORD-${i}` })
      )
    );

    await new Promise((r) => setTimeout(r, 3000));

    for (const run of runs) {
      const exec = engine.getExecution(run.id);
      if (exec!.state === 'completed') {
        completedOrders.push(run.id);
      } else {
        failedOrders.push(run.id);
      }
    }

    // Exactly 3 should succeed (limited stock)
    expect(completedOrders.length).toBe(3);
    expect(failedOrders.length).toBe(2);

    // Failed orders should have compensation (stock released)
    // Final stock should be 0 (3 reserved by successful orders)
    expect(inventory.getStock('LIMITED-ITEM')).toBe(0);
  });

  test('SCENARIO 6: Multi-stage deployment pipeline', async () => {
    const deployLog: string[] = [];
    let buildArtifact: string | null = null;

    const deployPipeline = new Workflow('deploy')
      .step('build', async (ctx) => {
        const input = ctx.input as { repo: string; branch: string; version: string };
        deployLog.push(`build:${input.repo}@${input.branch}`);
        await new Promise((r) => setTimeout(r, 50));
        buildArtifact = `artifact-${input.version}`;
        return { artifact: buildArtifact, buildTime: 42 };
      })
      .step('test', async (ctx) => {
        const artifact = (ctx.steps['build'] as { artifact: string }).artifact;
        deployLog.push(`test:${artifact}`);
        await new Promise((r) => setTimeout(r, 30));
        return { passed: true, coverage: 87.5 };
      })
      .step('deploy-staging', async (ctx) => {
        const artifact = (ctx.steps['build'] as { artifact: string }).artifact;
        deployLog.push(`deploy-staging:${artifact}`);
        await new Promise((r) => setTimeout(r, 40));
        return { environment: 'staging', url: 'https://staging.app.com' };
      })
      .waitFor('staging-approval')
      .step('deploy-production', async (ctx) => {
        const artifact = (ctx.steps['build'] as { artifact: string }).artifact;
        const approval = ctx.signals['staging-approval'] as { approvedBy: string };
        deployLog.push(`deploy-prod:${artifact}:approved-by:${approval.approvedBy}`);
        await new Promise((r) => setTimeout(r, 40));
        return { environment: 'production', url: 'https://app.com' };
      })
      .step('notify', async (ctx) => {
        const input = ctx.input as { repo: string; version: string };
        deployLog.push(`notify:${input.version}-deployed`);
        return { notified: true };
      });

    engine = new Engine({ embedded: true });
    engine.register(deployPipeline);

    const run = await engine.start('deploy', {
      repo: 'myapp',
      branch: 'main',
      version: '2.1.0',
    });

    await new Promise((r) => setTimeout(r, 2000));

    // Should be waiting for staging approval
    let exec = engine.getExecution(run.id);
    expect(exec!.state).toBe('waiting');
    expect(deployLog).toEqual([
      'build:myapp@main',
      'test:artifact-2.1.0',
      'deploy-staging:artifact-2.1.0',
    ]);

    // Tech lead approves staging
    await engine.signal(run.id, 'staging-approval', { approvedBy: 'tech-lead' });
    await new Promise((r) => setTimeout(r, 1500));

    exec = engine.getExecution(run.id);
    expect(exec!.state).toBe('completed');
    expect(deployLog).toEqual([
      'build:myapp@main',
      'test:artifact-2.1.0',
      'deploy-staging:artifact-2.1.0',
      'deploy-prod:artifact-2.1.0:approved-by:tech-lead',
      'notify:2.1.0-deployed',
    ]);
  });

  test('SCENARIO 7: Data pipeline with step results aggregation', async () => {
    type PipelineInput = { sources: string[] };

    const pipeline = new Workflow<PipelineInput>('data-pipeline')
      .step('extract', async (ctx) => {
        const input = ctx.input as PipelineInput;
        // Simulate extracting data from multiple sources
        const records: Array<{ source: string; count: number }> = [];
        for (const source of input.sources) {
          await new Promise((r) => setTimeout(r, 10));
          records.push({ source, count: Math.floor(Math.random() * 1000) + 100 });
        }
        return { records, totalSources: input.sources.length };
      })
      .step('transform', async (ctx) => {
        const extracted = ctx.steps['extract'] as { records: Array<{ source: string; count: number }> };
        await new Promise((r) => setTimeout(r, 20));
        const transformed = extracted.records.map((r) => ({
          ...r,
          normalized: r.count / 1000,
          processedAt: Date.now(),
        }));
        return { transformed, totalRecords: transformed.length };
      })
      .step('load', async (ctx) => {
        const data = ctx.steps['transform'] as {
          transformed: Array<{ source: string; normalized: number }>;
          totalRecords: number;
        };
        await new Promise((r) => setTimeout(r, 15));
        return {
          loaded: data.totalRecords,
          destinations: ['warehouse', 'analytics'],
          summary: data.transformed.map((t) => `${t.source}:${t.normalized.toFixed(2)}`),
        };
      })
      .step('report', async (ctx) => {
        const input = ctx.input as PipelineInput;
        const load = ctx.steps['load'] as { loaded: number; destinations: string[] };
        return {
          pipeline: 'data-pipeline',
          sources: input.sources.length,
          recordsLoaded: load.loaded,
          destinations: load.destinations,
          completedAt: Date.now(),
        };
      });

    engine = new Engine({ embedded: true });
    engine.register(pipeline);

    const run = await engine.start('data-pipeline', {
      sources: ['postgres', 'mongodb', 'redis', 's3'],
    });

    await waitForWorkflowState(engine, run.id, 'completed');

    const exec = engine.getExecution(run.id);
    expect(exec!.state).toBe('completed');

    const report = exec!.steps['report'].result as {
      sources: number;
      recordsLoaded: number;
      destinations: string[];
    };
    expect(report.sources).toBe(4);
    expect(report.recordsLoaded).toBe(4);
    expect(report.destinations).toEqual(['warehouse', 'analytics']);

    // Verify the full chain of data transformation
    const extract = exec!.steps['extract'].result as { totalSources: number };
    const transform = exec!.steps['transform'].result as { totalRecords: number };
    expect(extract.totalSources).toBe(4);
    expect(transform.totalRecords).toBe(4);
  });
});
