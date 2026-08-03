/**
 * TCP serde benchmark — server in a SEPARATE process (avoids the event-loop
 * starvation that makes comprehensive.ts crash with "Command timeout").
 *
 * Stresses the request/response serde hot path:
 *   - pipelined push (autobatch OFF): many individual frames per TCP packet
 *     => exercises FrameParser.addData concat + per-frame slice, and the
 *        per-response frame(pack()) on the way back.
 *   - round-trip latency: p50/p99 of single awaited push.
 *   - bulk push (PUSHB): amortized single-frame baseline.
 *
 * Reports MEDIAN of RUNS iterations after a warmup. Fresh server + db per run.
 *
 * Run: bun run bench/tcp-bench.ts
 */
import { Queue } from '../src/client';
import { join } from 'node:path';

const PORT = Number(Bun.env.BENCH_PORT ?? 6799);
const N = Number(Bun.env.BENCH_N ?? 20000);
const RUNS = Number(Bun.env.BENCH_RUNS ?? 5);
const LATENCY_SAMPLES = 5000;
const BENCH_TMP_DIR = Bun.env.BENCH_TMP_DIR ?? '/tmp';
const PAYLOAD = {
  user: 'u_12345',
  action: 'process',
  ts: 1718000000000,
  meta: { a: 1, b: 'x', c: true },
};

export function resolveServerRateLimit(jobCount: number, configured?: string): number {
  if (configured !== undefined) {
    const value = Number(configured);
    if (!Number.isSafeInteger(value) || value <= 0) {
      throw new Error('RATE_LIMIT_MAX_REQUESTS must be a positive safe integer');
    }
    return value;
  }
  return Math.max(10_000, Math.ceil(jobCount) + 1_000, LATENCY_SAMPLES + 1_000);
}

const SERVER_RATE_LIMIT = resolveServerRateLimit(N, Bun.env.RATE_LIMIT_MAX_REQUESTS);
const SERVER_RATE_LIMIT_SOURCE = Bun.env.RATE_LIMIT_MAX_REQUESTS ? 'explicit' : 'derived';
const SERVER_RATE_LIMIT_WINDOW = Number(Bun.env.RATE_LIMIT_WINDOW_MS ?? 60_000);

function median(xs: number[]): number {
  const s = [...xs].sort((a, b) => a - b);
  const m = s.length >> 1;
  return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2;
}
const opsPerSec = (n: number, ms: number) => Math.round((n / ms) * 1000);

async function waitForPort(port: number, timeoutMs = 15000): Promise<void> {
  const start = Bun.nanoseconds();
  while ((Bun.nanoseconds() - start) / 1e6 < timeoutMs) {
    try {
      const sock = await Bun.connect({
        hostname: '127.0.0.1',
        port,
        socket: { data: () => undefined },
      });
      sock.end();
      return;
    } catch {
      await new Promise((r) => setTimeout(r, 100));
    }
  }
  throw new Error(`server not ready on :${port}`);
}

async function spawnServer(db: string): Promise<{ stop: () => Promise<void> }> {
  const proc = Bun.spawn([process.execPath, 'run', 'src/main.ts'], {
    cwd: `${import.meta.dir}/..`,
    env: {
      ...process.env,
      TCP_PORT: String(PORT),
      HTTP_PORT: String(PORT + 1),
      BUNQUEUE_DATA_PATH: db,
      LOG_LEVEL: 'error',
      RATE_LIMIT_MAX_REQUESTS: String(SERVER_RATE_LIMIT),
    },
    stdout: 'ignore',
    stderr: 'ignore',
  });
  await waitForPort(PORT);
  return {
    stop: async () => {
      proc.kill();
      await proc.exited;
    },
  };
}

async function cleanupDb(path: string): Promise<void> {
  for (const file of [path, `${path}-wal`, `${path}-shm`]) {
    await Bun.file(file)
      .delete()
      .catch(() => undefined);
  }
}

function newQueue(name: string, autoBatch: boolean): Queue {
  return new Queue(name, {
    connection: { host: '127.0.0.1', port: PORT, maxInFlight: 512 },
    autoBatch: autoBatch ? undefined : { enabled: false },
  });
}

// --- pipelined push: N concurrent individual adds (autobatch OFF) ---
async function benchPipelined(): Promise<number> {
  const q = newQueue('pipe', false);
  await q.add('warm', PAYLOAD); // open pool
  const t0 = Bun.nanoseconds();
  const CHUNK = 1000; // bound in-flight to pool capacity
  for (let i = 0; i < N; i += CHUNK) {
    const ps: Promise<unknown>[] = [];
    for (let j = i; j < Math.min(i + CHUNK, N); j++) ps.push(q.add('j', PAYLOAD));
    await Promise.all(ps);
  }
  const ms = (Bun.nanoseconds() - t0) / 1e6;
  await q.close();
  return opsPerSec(N, ms);
}

// --- bulk push (PUSHB) ---
async function benchBulk(): Promise<number> {
  const q = newQueue('bulk', true);
  const jobs = Array.from({ length: N }, () => ({ name: 'j', data: PAYLOAD }));
  const t0 = Bun.nanoseconds();
  const CHUNK = 500;
  for (let i = 0; i < N; i += CHUNK) await q.addBulk(jobs.slice(i, i + CHUNK));
  const ms = (Bun.nanoseconds() - t0) / 1e6;
  await q.close();
  return opsPerSec(N, ms);
}

// --- round-trip latency: single awaited push, p50/p99 (us) ---
async function benchLatency(): Promise<{ p50: number; p99: number }> {
  const q = newQueue('lat', false);
  await q.add('warm', PAYLOAD);
  const lat = new Float64Array(LATENCY_SAMPLES);
  for (let i = 0; i < LATENCY_SAMPLES; i++) {
    const t0 = Bun.nanoseconds();
    await q.add('j', PAYLOAD);
    lat[i] = (Bun.nanoseconds() - t0) / 1000; // us
  }
  await q.close();
  const s = Array.from(lat).sort((a, b) => a - b);
  return {
    p50: Math.round(s[Math.floor(LATENCY_SAMPLES * 0.5)]),
    p99: Math.round(s[Math.floor(LATENCY_SAMPLES * 0.99)]),
  };
}

async function main() {
  console.log(
    `TCP serde bench — N=${N}, RUNS=${RUNS}, port=${PORT}, ` +
      `protocolRateLimit=${SERVER_RATE_LIMIT}/${SERVER_RATE_LIMIT_WINDOW}ms (${SERVER_RATE_LIMIT_SOURCE})\n`
  );

  const pipe: number[] = [];
  const bulk: number[] = [];
  let lat = { p50: 0, p99: 0 };

  // warmup run (discarded)
  {
    const db = join(BENCH_TMP_DIR, `bq-tcpbench-warm-${process.pid}.db`);
    const srv = await spawnServer(db);
    await benchPipelined();
    await srv.stop();
    await cleanupDb(db);
    await new Promise((r) => setTimeout(r, 300));
  }

  for (let run = 0; run < RUNS; run++) {
    const db = join(BENCH_TMP_DIR, `bq-tcpbench-${process.pid}-${run}.db`);
    const srv = await spawnServer(db);
    pipe.push(await benchPipelined());
    await srv.stop();
    await new Promise((r) => setTimeout(r, 200));

    const srv2 = await spawnServer(db + 'b');
    bulk.push(await benchBulk());
    await srv2.stop();
    await new Promise((r) => setTimeout(r, 200));

    if (run === RUNS - 1) {
      const srv3 = await spawnServer(db + 'l');
      lat = await benchLatency();
      await srv3.stop();
    }

    for (const path of [db, db + 'b', db + 'l']) await cleanupDb(path);
    process.stdout.write(
      `  run ${run + 1}/${RUNS}: pipe=${pipe[run].toLocaleString()} bulk=${bulk[run].toLocaleString()}\n`
    );
  }

  console.log('\n=== RESULTS (median) ===');
  console.log(
    `pipelined push (autobatch off)  ${median(pipe).toLocaleString().padStart(12)} ops/s`
  );
  console.log(
    `bulk push (PUSHB)               ${median(bulk).toLocaleString().padStart(12)} ops/s`
  );
  console.log(`round-trip latency              p50=${lat.p50}us  p99=${lat.p99}us`);
  process.exit(0);
}

if (import.meta.main) await main();
