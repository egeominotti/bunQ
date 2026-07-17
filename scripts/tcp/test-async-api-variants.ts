#!/usr/bin/env bun
/**
 * Test awaitable client API variants over TCP.
 *
 * The fire-and-forget forms give no ordering guarantee over the
 * multi-connection pool and discard server counts. Every variant here must
 * resolve only after the server has processed the command, and return the
 * server's data where the wire provides it.
 */

import { Queue, Worker } from '../../src/client';

const TCP_PORT = parseInt(process.env.TCP_PORT ?? '16789', 10);
const conn = { connection: { port: TCP_PORT } };

let passed = 0;
let failed = 0;
function check(cond: boolean, label: string, detail = ''): void {
  if (cond) {
    console.log(`   ✅ ${label}`);
    passed++;
  } else {
    console.log(`   ❌ ${label} ${detail}`);
    failed++;
  }
}

async function main() {
  console.log('=== Test Async API Variants (TCP) ===\n');

  // ── 1. pauseAsync / resumeAsync / drainAsync / obliterateAsync ──
  console.log('1. Control variants...');
  {
    const q = new Queue<{ i: number }>('tcp-async-control', conn);
    await q.obliterateAsync();

    await q.pauseAsync();
    check((await q.isPausedAsync()) === true, 'pauseAsync observable via isPausedAsync');
    await q.resumeAsync();
    check((await q.isPausedAsync()) === false, 'resumeAsync observable via isPausedAsync');

    await q.add('a', { i: 1 });
    await q.add('b', { i: 2 });
    const drained = await q.drainAsync();
    check(drained === 2, `drainAsync returns server count (got ${drained})`);

    const fresh = await q.add('c', { i: 3 });
    check((await q.getJob(fresh.id)) !== null, 'add right after drainAsync survives');

    await q.obliterateAsync();
    const post = await q.add('d', { i: 4 });
    check((await q.getJob(post.id)) !== null, 'add right after obliterateAsync survives');
    q.obliterate();
    await q.close();
  }

  // ── 2. rate limit / concurrency async setters ──
  console.log('\n2. Rate limit / concurrency setters...');
  {
    const q = new Queue<{ i: number }>('tcp-async-limits', conn);
    await q.obliterateAsync();
    for (let i = 0; i < 3; i++) await q.add('job', { i });

    await q.setGlobalRateLimitAsync(2, 60_000);
    const { TcpConnectionPool } = await import('../../src/client');
    const tcp = new TcpConnectionPool({ port: TCP_PORT });
    await tcp.connect();
    const p1 = await tcp.send({ cmd: 'PULL', queue: 'tcp-async-limits' });
    const p2 = await tcp.send({ cmd: 'PULL', queue: 'tcp-async-limits' });
    const p3 = await tcp.send({ cmd: 'PULL', queue: 'tcp-async-limits' });
    check(
      Boolean(p1.job) && Boolean(p2.job) && !p3.job,
      'setGlobalRateLimitAsync(2, 60s) enforced immediately after resolution'
    );

    await q.removeGlobalRateLimitAsync();
    const p4 = await tcp.send({ cmd: 'PULL', queue: 'tcp-async-limits' });
    check(Boolean(p4.job), 'removeGlobalRateLimitAsync lifts the limit immediately');

    await q.setGlobalConcurrencyAsync(5);
    check(true, 'setGlobalConcurrencyAsync resolves (server ok)');
    await q.removeGlobalConcurrencyAsync();
    check(true, 'removeGlobalConcurrencyAsync resolves (server ok)');

    await tcp.close();
    q.obliterate();
    await q.close();
  }

  // ── 3. stall / DLQ config async setters round-trip server-side ──
  console.log('\n3. Config setters...');
  {
    const q = new Queue<{ i: number }>('tcp-async-config', conn);

    await q.setStallConfigAsync({ stallInterval: 44_000, maxStalls: 9 });
    const stall = await q.getStallConfigAsync();
    check(
      stall.stallInterval === 44_000 && stall.maxStalls === 9,
      'setStallConfigAsync visible via getStallConfigAsync',
      JSON.stringify(stall)
    );

    await q.setDlqConfigAsync({ autoRetry: true, maxEntries: 77 });
    const dlqCfg = await q.getDlqConfigAsync();
    check(
      dlqCfg.autoRetry === true && dlqCfg.maxEntries === 77,
      'setDlqConfigAsync visible via getDlqConfigAsync',
      JSON.stringify(dlqCfg)
    );

    q.obliterate();
    await q.close();
  }

  // ── 4. DLQ: retryDlqAsync / purgeDlqAsync counts + getDlqJobsAsync ──
  console.log('\n4. DLQ variants (remote listing + counts)...');
  {
    const QUEUE = 'tcp-async-dlq';
    const q = new Queue<{ marker: string }>(QUEUE, conn);
    await q.obliterateAsync();

    await q.add('boom', { marker: 'dead-1' }, { attempts: 1 });
    await q.add('boom', { marker: 'dead-2' }, { attempts: 1 });

    let processed = 0;
    await new Promise<void>((resolve) => {
      const w = new Worker(
        QUEUE,
        async () => {
          processed++;
          if (processed >= 2) setTimeout(() => void w.close().then(resolve), 300);
          throw new Error('forced failure');
        },
        { connection: conn.connection, concurrency: 1 }
      );
    });
    await Bun.sleep(500);

    const deadJobs = await q.getDlqJobsAsync();
    check(
      deadJobs.length === 2 && deadJobs.every((j) => String(j.data.marker).startsWith('dead-')),
      `getDlqJobsAsync lists remote DLQ (got ${deadJobs.length})`
    );
    const capped = await q.getDlqJobsAsync(1);
    check(capped.length === 1, 'getDlqJobsAsync honours count cap');

    const retried = await q.retryDlqAsync();
    check(retried === 2, `retryDlqAsync returns server count (got ${retried})`);

    // Retried jobs are waiting again; obliterate and rebuild one dead job for purge.
    await q.obliterateAsync();
    await q.add('boom', { marker: 'dead-3' }, { attempts: 1 });
    await new Promise<void>((resolve) => {
      const w = new Worker(
        QUEUE,
        async () => {
          setTimeout(() => void w.close().then(resolve), 300);
          throw new Error('forced failure');
        },
        { connection: conn.connection, concurrency: 1 }
      );
    });
    await Bun.sleep(500);

    const purged = await q.purgeDlqAsync();
    check(purged === 1, `purgeDlqAsync returns server count (got ${purged})`);
    check((await q.getDlqJobsAsync()).length === 0, 'DLQ empty after purgeDlqAsync');

    q.obliterate();
    await q.close();
  }

  console.log(`\n=== Summary ===\nPassed: ${passed}\nFailed: ${failed}`);
  process.exit(failed === 0 ? 0 : 1);
}

main().catch((e) => {
  console.error('Fatal:', e);
  process.exit(1);
});
