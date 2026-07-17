#!/usr/bin/env bun
/**
 * Test rate-limit duration window + TTL over the wire (TCP Mode)
 *
 * Covers the two wire-level gaps:
 * - RateLimit `duration`: "N per window" instead of the hardwired 1s bucket.
 * - RateLimit `ttl`: temporary limit expires broker-side (no client timer),
 *   as used by queue.rateLimit(expireTimeMs).
 */

import { Queue, TcpConnectionPool } from '../../src/client';

const TCP_PORT = parseInt(process.env.TCP_PORT ?? '16789', 10);

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
  console.log('=== Test Rate Limit Window + TTL (TCP) ===\n');

  const tcp = new TcpConnectionPool({ port: TCP_PORT });
  await tcp.connect();

  // ── 1. duration honored over the wire ──
  console.log('1. RateLimit with duration (2 per 60s)...');
  {
    const QUEUE = 'tcp-rl-window';
    const q = new Queue<{ i: number }>(QUEUE, { connection: { port: TCP_PORT } });
    await q.obliterateAsync();

    for (let i = 0; i < 3; i++) await q.add('job', { i });
    const set = await tcp.send({ cmd: 'RateLimit', queue: QUEUE, limit: 2, duration: 60_000 });
    check(set.ok === true, 'RateLimit with duration accepted');

    const p1 = await tcp.send({ cmd: 'PULL', queue: QUEUE });
    const p2 = await tcp.send({ cmd: 'PULL', queue: QUEUE });
    check(Boolean(p1.job) && Boolean(p2.job), 'first 2 pulls inside the window succeed');

    // The buggy 1s bucket refills here; a real 60s window must stay closed.
    await Bun.sleep(1_100);
    const p3 = await tcp.send({ cmd: 'PULL', queue: QUEUE });
    check(!p3.job, '3rd pull after 1.1s is still limited (60s window)', JSON.stringify(p3.job));

    await tcp.send({ cmd: 'RateLimitClear', queue: QUEUE });
    q.obliterate();
    await q.close();
  }

  // ── 2. TTL expires broker-side ──
  console.log('\n2. RateLimit with ttl (limit 1, ttl 400ms)...');
  {
    const QUEUE = 'tcp-rl-ttl';
    const q = new Queue<{ i: number }>(QUEUE, { connection: { port: TCP_PORT } });
    await q.obliterateAsync();

    for (let i = 0; i < 4; i++) await q.add('job', { i });
    const set = await tcp.send({ cmd: 'RateLimit', queue: QUEUE, limit: 1, ttl: 400 });
    check(set.ok === true, 'RateLimit with ttl accepted');

    const p1 = await tcp.send({ cmd: 'PULL', queue: QUEUE });
    const p2 = await tcp.send({ cmd: 'PULL', queue: QUEUE });
    check(Boolean(p1.job) && !p2.job, 'limit enforced while ttl active');

    // 700ms > ttl. Burst of 2 immediate pulls: impossible under the leftover
    // 1-token/second trickle, possible only if the limiter was really cleared.
    await Bun.sleep(700);
    const p3 = await tcp.send({ cmd: 'PULL', queue: QUEUE });
    const p4 = await tcp.send({ cmd: 'PULL', queue: QUEUE });
    check(
      Boolean(p3.job) && Boolean(p4.job),
      'after ttl expiry pulls run at full speed (burst of 2)',
      `p3=${Boolean(p3.job)} p4=${Boolean(p4.job)}`
    );

    q.obliterate();
    await q.close();
  }

  // ── 3. queue.rateLimit(expireTimeMs) end-to-end ──
  console.log('\n3. queue.rateLimit(500) expires broker-side...');
  {
    const QUEUE = 'tcp-rl-client-ttl';
    const q = new Queue<{ i: number }>(QUEUE, { connection: { port: TCP_PORT } });
    await q.obliterateAsync();

    for (let i = 0; i < 3; i++) await q.add('job', { i });
    await q.rateLimit(500);

    const p1 = await tcp.send({ cmd: 'PULL', queue: QUEUE });
    check(Boolean(p1.job), 'one job passes under the temporary limit');

    await Bun.sleep(800);
    const p2 = await tcp.send({ cmd: 'PULL', queue: QUEUE });
    const p3 = await tcp.send({ cmd: 'PULL', queue: QUEUE });
    check(
      Boolean(p2.job) && Boolean(p3.job),
      'after expiry the queue is unlimited again (burst of 2)',
      `p2=${Boolean(p2.job)} p3=${Boolean(p3.job)}`
    );

    q.obliterate();
    await q.close();
  }

  // ── 4. invalid duration/ttl over the wire degrade to defaults ──
  console.log('\n4. Invalid duration/ttl degrade to defaults (no error)...');
  {
    const QUEUE = 'tcp-rl-invalid';
    const q = new Queue<{ i: number }>(QUEUE, { connection: { port: TCP_PORT } });
    await q.obliterateAsync();
    for (let i = 0; i < 3; i++) await q.add('job', { i });

    const r1 = await tcp.send({ cmd: 'RateLimit', queue: QUEUE, limit: 1, duration: -5, ttl: 'x' });
    check(r1.ok === true, 'invalid duration/ttl accepted without error');

    const p1 = await tcp.send({ cmd: 'PULL', queue: QUEUE });
    const p2 = await tcp.send({ cmd: 'PULL', queue: QUEUE });
    check(Boolean(p1.job) && !p2.job, 'limit still enforced with default 1s window');

    // Invalid ttl must mean PERMANENT: still limited well past any bogus expiry.
    await Bun.sleep(400);
    const p3 = await tcp.send({ cmd: 'PULL', queue: QUEUE });
    check(!p3.job, 'invalid ttl means permanent limit (still limited at 400ms)');

    await tcp.send({ cmd: 'RateLimitClear', queue: QUEUE });
    q.obliterate();
    await q.close();
  }

  // ── 5. re-set before expiry replaces the ttl ──
  console.log('\n5. Re-set before expiry replaces the ttl...');
  {
    const QUEUE = 'tcp-rl-replace';
    const q = new Queue<{ i: number }>(QUEUE, { connection: { port: TCP_PORT } });
    await q.obliterateAsync();
    for (let i = 0; i < 2; i++) await q.add('job', { i });

    await tcp.send({ cmd: 'RateLimit', queue: QUEUE, limit: 1, ttl: 300 });
    await tcp.send({ cmd: 'RateLimit', queue: QUEUE, limit: 1, duration: 60_000 });

    // The old 300ms ttl must not clear the new permanent limit.
    await Bun.sleep(500);
    const p1 = await tcp.send({ cmd: 'PULL', queue: QUEUE });
    const p2 = await tcp.send({ cmd: 'PULL', queue: QUEUE });
    check(Boolean(p1.job) && !p2.job, 'stale ttl did not clear the replacing limit');

    await tcp.send({ cmd: 'RateLimitClear', queue: QUEUE });
    q.obliterate();
    await q.close();
  }

  await tcp.close();
  console.log(`\n=== Summary ===\nPassed: ${passed}\nFailed: ${failed}`);
  process.exit(failed === 0 ? 0 : 1);
}

main().catch((e) => {
  console.error('Fatal:', e);
  process.exit(1);
});
