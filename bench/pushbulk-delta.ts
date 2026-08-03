/**
 * Trimmed push/bulk throughput bench (embedded + TCP), for before/after deltas.
 * Skips the pull-bound Process test. Reports median of R repetitions per cell
 * to reduce single-run noise. Set BENCH_HOST/BENCH_PORT for the TCP half.
 * Run: bun run bench/pushbulk-delta.ts
 */
import { Queue as EmbeddedQueue } from '../src/client';
import { Queue as TcpQueue } from '../src/client';
import { shutdownManager } from '../src/client/manager';
import { closeAll } from './native-benchmark-integrity';

const SCALES = [1000, 5000, 10000, 50000];
const BULK_SIZE = 100;
const REPS = 3;
const PAYLOAD = { data: 'x'.repeat(100) };
const BENCH_HOST = process.env.BENCH_HOST ?? 'localhost';
const BENCH_PORT = Number.parseInt(process.env.BENCH_PORT ?? '6789', 10);

if (!Number.isInteger(BENCH_PORT) || BENCH_PORT < 1 || BENCH_PORT > 65_535) {
  throw new Error(
    `BENCH_PORT must be an integer from 1 to 65535, received ${process.env.BENCH_PORT}`
  );
}

const connection = (poolSize = 32) => ({ host: BENCH_HOST, port: BENCH_PORT, poolSize });

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const median = (xs: number[]) => xs.slice().sort((a, b) => a - b)[Math.floor(xs.length / 2)];

async function embeddedPush(scale: number): Promise<number> {
  const q = new EmbeddedQueue(`d-ep-${scale}-${Date.now()}-${Math.floor(performance.now())}`, {
    embedded: true,
  });
  try {
    await sleep(20);
    const t = performance.now();
    for (let i = 0; i < scale; i++) await q.add('job', PAYLOAD);
    return Math.round((scale / (performance.now() - t)) * 1000);
  } finally {
    await closeAll([q]);
  }
}

async function embeddedBulk(scale: number): Promise<number> {
  const q = new EmbeddedQueue(`d-eb-${scale}-${Date.now()}-${Math.floor(performance.now())}`, {
    embedded: true,
  });
  const jobs = Array.from({ length: BULK_SIZE }, (_, i) => ({
    name: 'b',
    data: { ...PAYLOAD, i },
  }));
  const iter = Math.floor(scale / BULK_SIZE);
  try {
    await sleep(20);
    const t = performance.now();
    for (let i = 0; i < iter; i++) await q.addBulk(jobs);
    return Math.round(((iter * BULK_SIZE) / (performance.now() - t)) * 1000);
  } finally {
    await closeAll([q]);
  }
}

async function tcpPush(scale: number): Promise<number> {
  const q = new TcpQueue(`d-tp-${scale}-${Date.now()}-${Math.floor(performance.now())}`, {
    embedded: false,
    connection: connection(),
  });
  try {
    await sleep(150);
    const t = performance.now();
    for (let i = 0; i < scale; i += 100) {
      const batch = Math.min(100, scale - i);
      const p = [];
      for (let j = 0; j < batch; j++) p.push(q.add('job', PAYLOAD));
      await Promise.all(p);
    }
    return Math.round((scale / (performance.now() - t)) * 1000);
  } finally {
    await sleep(30);
    await closeAll([q]);
  }
}

async function tcpBulk(scale: number): Promise<number> {
  const q = new TcpQueue(`d-tb-${scale}-${Date.now()}-${Math.floor(performance.now())}`, {
    embedded: false,
    connection: connection(),
  });
  const jobs = Array.from({ length: BULK_SIZE }, (_, i) => ({
    name: 'b',
    data: { ...PAYLOAD, i },
  }));
  const iter = Math.floor(scale / BULK_SIZE);
  try {
    await sleep(150);
    const t = performance.now();
    for (let i = 0; i < iter; i++) await q.addBulk(jobs);
    return Math.round(((iter * BULK_SIZE) / (performance.now() - t)) * 1000);
  } finally {
    await sleep(30);
    await closeAll([q]);
  }
}

async function bestOf(fn: (s: number) => Promise<number>, scale: number): Promise<number> {
  const runs: number[] = [];
  for (let r = 0; r < REPS; r++) runs.push(await fn(scale));
  return median(runs);
}

async function runCampaign(): Promise<void> {
  let tcp = false;
  const probe = new TcpQueue(`d-conn-${Date.now()}`, {
    embedded: false,
    connection: { ...connection(1), commandTimeout: 2000 },
  });
  try {
    await probe.getJobCounts();
    tcp = true;
  } catch {
    /* embedded only */
  } finally {
    await closeAll([probe]);
  }
  console.log(
    `\nPUSH/BULK DELTA BENCH (median of ${REPS}) — tcp=${tcp} endpoint=${BENCH_HOST}:${BENCH_PORT}\n`
  );
  console.log('mode      scale    push(ops/s)   bulk(ops/s)');
  for (const s of SCALES) {
    const ep = await bestOf(embeddedPush, s);
    const eb = await bestOf(embeddedBulk, s);
    console.log(
      `EMB     ${String(s).padStart(7)}  ${String(ep).padStart(11)}  ${String(eb).padStart(12)}`
    );
  }
  if (tcp) {
    for (const s of SCALES) {
      const tp = await bestOf(tcpPush, s);
      const tb = await bestOf(tcpBulk, s);
      console.log(
        `TCP     ${String(s).padStart(7)}  ${String(tp).padStart(11)}  ${String(tb).padStart(12)}`
      );
    }
  }
  console.log('');
}

async function main(): Promise<void> {
  try {
    await runCampaign();
  } finally {
    shutdownManager();
  }
}

if (import.meta.main) {
  main().catch((error) => {
    console.error(error);
    process.exitCode = 1;
  });
}
