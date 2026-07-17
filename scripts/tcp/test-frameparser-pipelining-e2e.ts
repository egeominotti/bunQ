#!/usr/bin/env bun
/**
 * Test FrameParser pipelining E2E (TCP Mode)
 *
 * Exercises the byte/framing hot path that the O(F²)->O(F) FrameParser cursor
 * change touches, end to end over a real TCP connection:
 *   - heavy pipelined push (many small frames coalesced into one TCP read)
 *   - large payloads (single frames that span multiple TCP segments -> partial
 *     frame reassembly across reads)
 *   - bulk push (large single frames)
 *   - worker exactly-once processing + byte-exact payload integrity
 * A framing bug (dropped/duplicated/corrupted frame, bad partial reassembly)
 * shows up here as a wrong count or a payload mismatch.
 */

import { Queue, Worker } from '../../src/client';

const TCP_PORT = parseInt(process.env.TCP_PORT ?? '16789');
const conn = { connection: { port: TCP_PORT } } as const;

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

/** Deterministic byte pattern so payload corruption is detectable. */
function payload(seed: number, size: number): string {
  let s = '';
  const base = `seed:${seed}:`;
  while (s.length < size) s += base;
  return s.slice(0, size);
}

async function main() {
  console.log('=== Test FrameParser Pipelining E2E (TCP) ===\n');

  // ── 1. Heavy pipelined push: many small frames coalesced per read ──
  console.log('1. Pipelined push (2000 concurrent adds, autobatch OFF)...');
  {
    const q = new Queue<{ i: number }>('fp-e2e-pipe', {
      ...conn,
      autoBatch: { enabled: false }, // force one frame per add -> coalescing on the wire
    });
    await q.obliterateAsync();

    const N = 2000;
    const adds = [];
    for (let i = 0; i < N; i++) adds.push(q.add('job', { i }));
    const results = await Promise.all(adds);
    const ids = new Set(results.map((r) => r.id));
    check(ids.size === N, `all ${N} adds returned unique ids`, `(got ${ids.size})`);

    await Bun.sleep(200);
    const counts = await q.getJobCounts();
    check(counts.waiting === N, `server reports ${N} waiting`, `(got ${counts.waiting})`);
    q.obliterate();
    await Bun.sleep(100);
    await q.close();
  }

  // ── 2. Large payloads spanning multiple TCP segments ──
  console.log('\n2. Large payloads (256KB) — frames span multiple TCP reads...');
  {
    const q = new Queue<{ i: number; data: string }>('fp-e2e-large', conn);
    await q.obliterateAsync();

    const SIZE = 256 * 1024; // > typical socket read chunk -> multi-segment frame
    const M = 20;
    const ids: string[] = [];
    for (let i = 0; i < M; i++) {
      const job = await q.add('big', { i, data: payload(i, SIZE) });
      ids.push(job.id);
    }
    // Read each back and verify byte-exact (proves partial-frame reassembly + round-trip framing)
    let intact = 0;
    for (let i = 0; i < M; i++) {
      const j = await q.getJob(ids[i]);
      const d = j?.data as { i: number; data: string } | undefined;
      if (d && d.i === i && d.data === payload(i, SIZE)) intact++;
    }
    check(intact === M, `all ${M} large payloads byte-exact after round-trip`, `(got ${intact})`);
    q.obliterate();
    await Bun.sleep(100);
    await q.close();
  }

  // ── 3. Bulk + worker exactly-once + integrity under concurrent load ──
  console.log('\n3. Bulk push + worker exactly-once + integrity...');
  {
    const q = new Queue<{ i: number; tag: string }>('fp-e2e-proc', conn);
    await q.obliterateAsync();

    const TOTAL = 5000;
    const BULK = 100;
    // mix bulk frames (large) with the worker pulling/acking (more frames) concurrently
    const jobsTemplate = (off: number) =>
      Array.from({ length: BULK }, (_, k) => ({
        name: 'b',
        data: { i: off + k, tag: payload(off + k, 64) },
      }));

    const seen = new Map<number, number>(); // i -> times processed
    const worker = new Worker<{ i: number; tag: string }>(
      'fp-e2e-proc',
      async (job) => {
        const d = job.data as { i: number; tag: string };
        seen.set(d.i, (seen.get(d.i) ?? 0) + 1);
        // integrity: tag must match the deterministic pattern for this i
        if (d.tag !== payload(d.i, 64)) throw new Error(`payload mismatch at i=${d.i}`);
        return { i: d.i };
      },
      { ...conn, concurrency: 20, batchSize: 50, useLocks: false, heartbeatInterval: 0 }
    );

    const t0 = Date.now();
    for (let off = 0; off < TOTAL; off += BULK) await q.addBulk(jobsTemplate(off));

    // wait until all processed (or timeout)
    while (seen.size < TOTAL && Date.now() - t0 < 60000) await Bun.sleep(50);

    check(seen.size === TOTAL, `all ${TOTAL} jobs processed`, `(got ${seen.size})`);
    const dups = [...seen.values()].filter((c) => c > 1).length;
    check(dups === 0, 'no job processed more than once (exactly-once)', `(${dups} dups)`);
    // no integrity errors surfaced as failed jobs
    await Bun.sleep(200);
    const counts = await q.getJobCounts();
    check(
      counts.failed === 0,
      'zero failed jobs (no payload mismatches)',
      `(${counts.failed} failed)`
    );

    await worker.close();
    q.obliterate();
    await Bun.sleep(100);
    await q.close();
  }

  // ── 4. Boundary: payload split exactly across the 4-byte length prefix region ──
  console.log('\n4. Many mid-size jobs (variable sizes) round-trip intact...');
  {
    const q = new Queue<{ i: number; data: string }>('fp-e2e-vary', conn);
    await q.obliterateAsync();

    const sizes = [1, 3, 7, 4095, 4096, 4097, 65535, 65536, 65537, 131072];
    const ids: string[] = [];
    for (let i = 0; i < sizes.length; i++) {
      const job = await q.add('v', { i, data: payload(i * 31 + 1, sizes[i]) });
      ids.push(job.id);
    }
    let ok = 0;
    for (let i = 0; i < sizes.length; i++) {
      const j = await q.getJob(ids[i]);
      const d = j?.data as { i: number; data: string } | undefined;
      if (d && d.data === payload(i * 31 + 1, sizes[i]) && d.data.length === sizes[i]) ok++;
    }
    check(ok === sizes.length, `all ${sizes.length} boundary-size payloads intact`, `(got ${ok})`);
    q.obliterate();
    await Bun.sleep(100);
    await q.close();
  }

  console.log(`\n=== Summary ===\nPassed: ${passed}\nFailed: ${failed}`);
  process.exit(failed === 0 ? 0 : 1);
}

main().catch((e) => {
  console.error('Fatal:', e);
  process.exit(1);
});
