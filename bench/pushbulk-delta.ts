/**
 * Trimmed push/bulk throughput bench (embedded + TCP), for before/after deltas.
 * Skips the pull-bound Process test. Reports median of R repetitions per cell
 * to reduce single-run noise. Requires a TCP server on :6789 for the TCP half.
 * Run: bun run bench/pushbulk-delta.ts
 */
import { Queue as EmbeddedQueue } from '../src/client';
import { Queue as TcpQueue } from '../src/client';

const SCALES = [1000, 5000, 10000, 50000];
const BULK_SIZE = 100;
const REPS = 3;
const PAYLOAD = { data: 'x'.repeat(100) };

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const median = (xs: number[]) => xs.slice().sort((a, b) => a - b)[Math.floor(xs.length / 2)];

async function embeddedPush(scale: number): Promise<number> {
  const q = new EmbeddedQueue(`d-ep-${scale}-${Date.now()}-${Math.floor(performance.now())}`, {
    embedded: true,
  });
  await sleep(20);
  const t = performance.now();
  for (let i = 0; i < scale; i++) await q.add('job', PAYLOAD);
  return Math.round((scale / (performance.now() - t)) * 1000);
}

async function embeddedBulk(scale: number): Promise<number> {
  const q = new EmbeddedQueue(`d-eb-${scale}-${Date.now()}-${Math.floor(performance.now())}`, {
    embedded: true,
  });
  const jobs = Array.from({ length: BULK_SIZE }, (_, i) => ({ name: 'b', data: { ...PAYLOAD, i } }));
  const iter = Math.floor(scale / BULK_SIZE);
  await sleep(20);
  const t = performance.now();
  for (let i = 0; i < iter; i++) await q.addBulk(jobs);
  return Math.round(((iter * BULK_SIZE) / (performance.now() - t)) * 1000);
}

async function tcpPush(scale: number): Promise<number> {
  const q = new TcpQueue(`d-tp-${scale}-${Date.now()}-${Math.floor(performance.now())}`, {
    connection: { host: 'localhost', port: 6789, poolSize: 32 },
  });
  await sleep(150);
  const t = performance.now();
  for (let i = 0; i < scale; i += 100) {
    const batch = Math.min(100, scale - i);
    const p = [];
    for (let j = 0; j < batch; j++) p.push(q.add('job', PAYLOAD));
    await Promise.all(p);
  }
  const ops = Math.round((scale / (performance.now() - t)) * 1000);
  await sleep(30);
  await q.close();
  return ops;
}

async function tcpBulk(scale: number): Promise<number> {
  const q = new TcpQueue(`d-tb-${scale}-${Date.now()}-${Math.floor(performance.now())}`, {
    connection: { host: 'localhost', port: 6789, poolSize: 32 },
  });
  const jobs = Array.from({ length: BULK_SIZE }, (_, i) => ({ name: 'b', data: { ...PAYLOAD, i } }));
  const iter = Math.floor(scale / BULK_SIZE);
  await sleep(150);
  const t = performance.now();
  for (let i = 0; i < iter; i++) await q.addBulk(jobs);
  const ops = Math.round(((iter * BULK_SIZE) / (performance.now() - t)) * 1000);
  await sleep(30);
  await q.close();
  return ops;
}

async function bestOf(fn: (s: number) => Promise<number>, scale: number): Promise<number> {
  const runs: number[] = [];
  for (let r = 0; r < REPS; r++) runs.push(await fn(scale));
  return median(runs);
}

async function main() {
  // check tcp
  let tcp = false;
  try {
    const t = new TcpQueue('d-conn', { connection: { host: 'localhost', port: 6789 } });
    await sleep(300);
    await t.close();
    tcp = true;
  } catch {
    /* embedded only */
  }
  console.log(`\nPUSH/BULK DELTA BENCH (median of ${REPS}) — tcp=${tcp}\n`);
  console.log('mode      scale    push(ops/s)   bulk(ops/s)');
  for (const s of SCALES) {
    const ep = await bestOf(embeddedPush, s);
    const eb = await bestOf(embeddedBulk, s);
    console.log(`EMB     ${String(s).padStart(7)}  ${String(ep).padStart(11)}  ${String(eb).padStart(12)}`);
  }
  if (tcp) {
    for (const s of SCALES) {
      const tp = await bestOf(tcpPush, s);
      const tb = await bestOf(tcpBulk, s);
      console.log(`TCP     ${String(s).padStart(7)}  ${String(tp).padStart(11)}  ${String(tb).padStart(12)}`);
    }
  }
  console.log('');
  process.exit(0);
}
main();
