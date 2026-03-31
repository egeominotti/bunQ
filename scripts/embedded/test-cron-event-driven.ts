#!/usr/bin/env bun
/**
 * Test: CronScheduler event-driven timer precision
 * Verifies that cron jobs fire at the correct time using setTimeout
 * instead of the old 1s polling interval.
 */

// Force embedded mode BEFORE imports
process.env.BUNQUEUE_EMBEDDED = '1';

import { Queue, Worker } from '../../src/client';
import { shutdownManager } from '../../src/client/manager';

async function main() {
  console.log('=== Test CronScheduler Event-Driven Timer ===\n');

  let passed = 0;
  let failed = 0;

  // ── Test 1: Cron fires at precise interval (not polling at 1s) ──
  console.log('1. Testing PRECISE TIMER (repeatEvery: 300ms)...');
  try {
    const queue = new Queue<{ test: string }>('cron-precise', { embedded: true });
    const execTimes: number[] = [];
    const startTime = Date.now();

    const worker = new Worker<{ test: string }>(
      'cron-precise',
      async () => {
        execTimes.push(Date.now() - startTime);
        return { ok: true };
      },
      { concurrency: 1, embedded: true }
    );

    await queue.add('precise-cron', { test: 'precision' }, {
      repeat: { every: 300, limit: 3 },
    });

    await Bun.sleep(1500);
    await worker.close();

    if (execTimes.length >= 3) {
      const firstExec = execTimes[0];
      if (firstExec < 800) {
        console.log(`   ✅ First exec at ${firstExec}ms (event-driven, not 1s polling)`);
        console.log(`      All exec times: [${execTimes.map((t) => t + 'ms').join(', ')}]`);
        passed++;
      } else {
        console.log(`   ❌ First exec at ${firstExec}ms (too slow, likely polling)`);
        failed++;
      }
    } else {
      console.log(`   ❌ Only ${execTimes.length} executions (expected >=3)`);
      failed++;
    }

    queue.obliterate();
  } catch (e) {
    console.log(`   ❌ Precise timer test failed: ${e}`);
    failed++;
  }

  // ── Test 2: Cron added after start fires quickly ──
  console.log('\n2. Testing DYNAMIC CRON ADD (add while running)...');
  try {
    const queue = new Queue<{ test: string }>('cron-dynamic', { embedded: true });
    const execTimes: number[] = [];
    const startTime = Date.now();

    const worker = new Worker<{ test: string }>(
      'cron-dynamic',
      async () => {
        execTimes.push(Date.now() - startTime);
        return { ok: true };
      },
      { concurrency: 1, embedded: true }
    );

    // Wait 200ms then add a cron with 150ms interval
    await Bun.sleep(200);
    await queue.add('dynamic-cron', { test: 'dynamic' }, {
      repeat: { every: 150, limit: 3 },
    });

    await Bun.sleep(800);
    await worker.close();

    if (execTimes.length >= 2) {
      const firstExec = execTimes[0];
      if (firstExec < 800) {
        console.log(`   ✅ Dynamic cron first exec at ${firstExec}ms (${execTimes.length} total)`);
        passed++;
      } else {
        console.log(`   ❌ Dynamic cron first exec at ${firstExec}ms (too slow)`);
        failed++;
      }
    } else {
      console.log(`   ❌ Only ${execTimes.length} executions`);
      failed++;
    }

    queue.obliterate();
  } catch (e) {
    console.log(`   ❌ Dynamic cron test failed: ${e}`);
    failed++;
  }

  // ── Test 3: Short interval cron fires rapidly ──
  console.log('\n3. Testing RAPID CRON (repeatEvery: 100ms)...');
  try {
    const queue = new Queue<{ test: string }>('cron-rapid', { embedded: true });
    const execTimes: number[] = [];
    const startTime = Date.now();

    const worker = new Worker<{ test: string }>(
      'cron-rapid',
      async () => {
        execTimes.push(Date.now() - startTime);
        return { ok: true };
      },
      { concurrency: 1, embedded: true }
    );

    await queue.add('rapid-cron', { test: 'rapid' }, {
      repeat: { every: 100, limit: 5 },
    });

    await Bun.sleep(1000);
    await worker.close();

    if (execTimes.length >= 4) {
      // With old 1s polling, max 1 execution per second
      // With event-driven, should get ~5 in 1s
      console.log(`   ✅ ${execTimes.length} executions in 1s (event-driven beats 1s polling)`);
      console.log(`      Exec times: [${execTimes.map((t) => t + 'ms').join(', ')}]`);
      passed++;
    } else {
      console.log(`   ❌ Only ${execTimes.length} executions in 1s (expected >=4)`);
      failed++;
    }

    queue.obliterate();
  } catch (e) {
    console.log(`   ❌ Rapid cron test failed: ${e}`);
    failed++;
  }

  // ── Summary ──
  console.log('\n=== Summary ===');
  console.log(`Passed: ${passed}`);
  console.log(`Failed: ${failed}`);

  // Shutdown shared QueueManager singleton (clears all background task intervals
  // and the CronScheduler's 60s safety fallback setInterval that keeps the process alive)
  shutdownManager();

  if (failed > 0) {
    process.exit(1);
  }

  process.exit(0);
}

main().catch((err) => {
  console.error('Fatal error:', err);
  shutdownManager();
  process.exit(1);
});
