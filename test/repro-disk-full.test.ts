/**
 * DISK-FULL (ENOSPC / SQLITE_FULL) — behavior when the real disk fills up.
 *
 * OPTIONAL suite: it needs a tiny real filesystem it can actually fill. It
 * self-detects capability at module load and SKIPs cleanly when the
 * environment cannot provide one — `bun test` stays green everywhere.
 *
 *   - macOS: creates a 16 MB HFS+ ram disk via `hdiutil attach -nomount
 *     ram://32768` + `diskutil erasevolume` (no sudo needed). Detached in
 *     afterAll with try/finally — never left mounted, even on failure.
 *   - Linux (CI): a loopback mount needs root, so instead a pre-provisioned
 *     small filesystem path can be supplied via BUNQUEUE_TINYFS (e.g. a small
 *     tmpfs mounted by the workflow). Unset → documented SKIP.
 *
 * These tests spawn the REAL server (Bun.spawn src/main.ts) with
 * BUNQUEUE_DATA_PATH on the tiny volume, fill the volume, and assert:
 *
 *   DF1 (durable path):
 *     (1) the server does NOT crash on SQLITE_FULL — it stays responsive to
 *         Ping on a fresh connection;
 *     (2) failed durable pushes return an error to the client (ok:false +
 *         error message), never a fake id (no silent accept-then-lose);
 *     (3) every job whose PUSH was acknowledged ok before the wall is still
 *         queryable (GetState !== 'unknown');
 *     (4) after freeing space, new durable pushes succeed again.
 *
 *   DF2 (write-buffered path):
 *     buffered pushes ack ok even while the disk is full (the documented
 *     10ms-window trade-off) — but the degradation is SURFACED, not hidden:
 *     the StorageStatus wire command flips diskFull:true (and GET /health
 *     reports status 'degraded' with storage.diskFull) once the WriteBuffer
 *     flush hits the wall, and flips back to false after space is freed and
 *     a durable write succeeds (safeWrite clears the flag on success —
 *     src/infrastructure/persistence/sqlite.ts).
 *
 * Rejected durable writes must never remain visible in memory. Otherwise the
 * client can safely retry an explicit rejection while the rejected generation
 * is still executable, producing an avoidable duplicate delivery.
 */

import { afterAll, afterEach, describe, expect, it } from 'bun:test';
import { existsSync, readdirSync, rmSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { QueueManager } from '../src/application/queueManager';
import { TcpClient } from '../src/client/tcp/client';

// ---------------------------------------------------------------------------
// Capability detection (module load — must be synchronous so it.skipIf works)
// ---------------------------------------------------------------------------

interface TinyFs {
  /** Mounted directory we are allowed to fill completely. */
  dir: string;
  kind: 'ramdisk-macos' | 'tinyfs-env';
  /** macOS only: the /dev/diskN backing the ram disk, for detach. */
  device?: string;
}

let tiny: TinyFs | null = null;
let skipReason = 'unknown';

function detectMacRamDisk(): void {
  // 32768 sectors × 512 B = 16 MB — big enough for the server's SQLite
  // migrations + WAL, small enough to fill in well under a second.
  const attach = Bun.spawnSync(['hdiutil', 'attach', '-nomount', 'ram://32768'], {
    stdin: 'ignore',
    stdout: 'pipe',
    stderr: 'pipe',
  });
  if (attach.exitCode !== 0) {
    skipReason = `hdiutil attach failed (exit ${attach.exitCode}): ${attach.stderr.toString().trim()}`;
    return;
  }
  const device = attach.stdout.toString().trim().split(/\s+/)[0] ?? '';
  if (!/^\/dev\/disk\d+$/.test(device)) {
    skipReason = `hdiutil attach returned unexpected device "${device}"`;
    Bun.spawnSync(['hdiutil', 'detach', device, '-force'], { stdin: 'ignore' });
    return;
  }
  const volName = `BQTINY${process.pid}`;
  const erase = Bun.spawnSync(['diskutil', 'erasevolume', 'HFS+', volName, device], {
    stdin: 'ignore',
    stdout: 'pipe',
    stderr: 'pipe',
  });
  const dir = `/Volumes/${volName}`;
  if (erase.exitCode !== 0 || !existsSync(dir)) {
    skipReason = `diskutil erasevolume failed (exit ${erase.exitCode}): ${erase.stderr.toString().trim()}`;
    Bun.spawnSync(['hdiutil', 'detach', device, '-force'], { stdin: 'ignore' });
    return;
  }
  // Prove we can actually write on it before committing to the suite.
  try {
    writeFileSync(join(dir, '.probe'), 'ok');
    rmSync(join(dir, '.probe'), { force: true });
  } catch (err) {
    skipReason = `ram disk not writable: ${err instanceof Error ? err.message : String(err)}`;
    Bun.spawnSync(['hdiutil', 'detach', device, '-force'], { stdin: 'ignore' });
    return;
  }
  tiny = { dir, kind: 'ramdisk-macos', device };
}

function detectLinuxTinyFs(): void {
  const dir = process.env.BUNQUEUE_TINYFS;
  if (!dir) {
    skipReason =
      'Linux: loopback mounts need root; set BUNQUEUE_TINYFS to a pre-provisioned small ' +
      'filesystem (e.g. `sudo mount -t tmpfs -o size=16m tmpfs /mnt/tinyfs` in the workflow) to enable';
    return;
  }
  if (!existsSync(dir)) {
    skipReason = `BUNQUEUE_TINYFS="${dir}" does not exist`;
    return;
  }
  try {
    writeFileSync(join(dir, '.probe'), 'ok');
    rmSync(join(dir, '.probe'), { force: true });
  } catch (err) {
    skipReason = `BUNQUEUE_TINYFS="${dir}" not writable: ${err instanceof Error ? err.message : String(err)}`;
    return;
  }
  tiny = { dir, kind: 'tinyfs-env' };
}

if (process.platform === 'darwin') {
  detectMacRamDisk();
} else if (process.platform === 'linux') {
  detectLinuxTinyFs();
} else {
  skipReason = `unsupported platform ${process.platform}`;
}

const capable = tiny !== null;
if (!capable) {
  console.log(`[repro-disk-full] SKIP: ${skipReason}`);
} else {
  console.log(`[repro-disk-full] using tiny fs at ${tiny?.dir} (${tiny?.kind})`);
}

// ---------------------------------------------------------------------------
// Tiny-volume helpers
// ---------------------------------------------------------------------------

/** Filler files currently occupying the tiny volume (freed to "fix" the disk). */
let fillers: string[] = [];

/**
 * Fill the volume to (near) capacity with progressively smaller chunks:
 * 1 MB → 64 KB → 4 KB, each size until the first ENOSPC. Leaves <4 KB free.
 */
function fillVolume(dir: string): void {
  let n = 0;
  for (const size of [1 << 20, 1 << 16, 1 << 12]) {
    const chunk = Buffer.alloc(size, 0xab);
    // Safety cap: a 16 MB volume needs ≤16 1MB chunks; 300 total is generous.
    for (let i = 0; i < 300; i++) {
      const path = join(dir, `filler-${String(n).padStart(4, '0')}.bin`);
      try {
        writeFileSync(path, chunk);
        fillers.push(path);
        n++;
      } catch {
        // ENOSPC (possibly a partial file left behind — remove it, it holds space
        // we can't account for; the smaller chunk sizes take over from here).
        rmSync(path, { force: true });
        break;
      }
    }
  }
}

/** Delete all filler files — the operator "freeing space". */
function freeFillers(): void {
  for (const f of fillers) {
    rmSync(f, { force: true });
  }
  fillers = [];
}

/** Remove everything this suite may have left on the volume (db, wal, fillers). */
function cleanVolume(): void {
  if (!tiny) return;
  freeFillers();
  for (const entry of readdirSync(tiny.dir)) {
    if (entry.startsWith('bunq-') || entry.startsWith('filler-')) {
      rmSync(join(tiny.dir, entry), { force: true });
    }
  }
}

/** Detach the macOS ram disk. Never throws; retries once (server fds may lag). */
function detachRamDisk(): void {
  if (!tiny?.device) return;
  for (let attempt = 0; attempt < 2; attempt++) {
    const r = Bun.spawnSync(['hdiutil', 'detach', tiny.device, '-force'], { stdin: 'ignore' });
    if (r.exitCode === 0) {
      console.log(`[repro-disk-full] detached ${tiny.device}`);
      return;
    }
    // Brief synchronous settle before the retry.
    Bun.spawnSync(['sleep', '0.5']);
  }
  console.error(`[repro-disk-full] WARNING: failed to detach ${tiny.device}`);
}

// ---------------------------------------------------------------------------
// Server harness (pattern from test/repro-chaos-crash-recovery.test.ts)
// ---------------------------------------------------------------------------

type Subprocess = ReturnType<typeof Bun.spawn>;

interface Server {
  proc: Subprocess;
  port: number;
  db: string;
}

let active: Server | null = null;
let activeClient: TcpClient | null = null;
let activeManager: QueueManager | null = null;

function randomPort(): number {
  return 20000 + Math.floor(Math.random() * 20000);
}

/** A port confirmed free right now (bind+close), to avoid cross-test collisions. */
function getFreePort(): number {
  for (let i = 0; i < 50; i++) {
    const port = randomPort();
    try {
      const l = Bun.listen({
        hostname: '127.0.0.1',
        port,
        socket: {
          data() {
            // Probe listener: no payload is expected.
          },
        },
      });
      l.stop();
      return port;
    } catch {
      /* taken — try another */
    }
  }
  throw new Error('no free port found');
}

// 60s: CI runners cold-start the real server binary much slower than a dev machine
async function waitPort(port: number, timeoutMs = 60000): Promise<void> {
  const t0 = Date.now();
  while (Date.now() - t0 < timeoutMs) {
    try {
      const s = await Bun.connect({
        hostname: '127.0.0.1',
        port,
        socket: {
          data() {
            // Readiness probe: receiving data is irrelevant.
          },
        },
      });
      s.end();
      return;
    } catch {
      await Bun.sleep(100);
    }
  }
  throw new Error(`server not ready on :${port}`);
}

/** Spawn the real server against a given db + port. Waits until it accepts TCP. */
async function spawnServer(db: string, port: number): Promise<Server> {
  const proc = Bun.spawn([process.execPath, 'run', 'src/main.ts'], {
    cwd: process.cwd(),
    env: {
      ...process.env,
      BUNQUEUE_EMBEDDED: '',
      TCP_PORT: String(port),
      HTTP_PORT: String(port + 1),
      BUNQUEUE_DATA_PATH: db,
      LOG_LEVEL: 'error',
    },
    stdout: 'ignore',
    stderr: 'ignore',
  });
  await waitPort(port);
  const s: Server = { proc, port, db };
  active = s;
  return s;
}

/** Fresh connected client to the current server. */
async function connect(port: number): Promise<TcpClient> {
  if (activeClient) {
    try {
      activeClient.close();
    } catch {
      /* ignore */
    }
  }
  const c = new TcpClient({
    host: '127.0.0.1',
    port,
    autoReconnect: false,
    pingInterval: 0,
    commandTimeout: 10000,
    connectTimeout: 5000,
  });
  await c.connect();
  activeClient = c;
  return c;
}

async function stopServer(): Promise<void> {
  if (activeClient) {
    try {
      activeClient.close();
    } catch {
      /* ignore */
    }
    activeClient = null;
  }
  if (active) {
    try {
      active.proc.kill(9);
      await active.proc.exited;
    } catch {
      /* ignore */
    }
    active = null;
  }
}

afterEach(async () => {
  await stopServer();
  if (activeManager) {
    activeManager.shutdown();
    activeManager = null;
  }
  cleanVolume();
}, 30000);

// GUARANTEED teardown: the ram disk must NEVER stay mounted, even when a test
// (or afterEach) failed. Kill the server first so no fd pins the volume.
afterAll(async () => {
  try {
    await stopServer();
    cleanVolume();
  } finally {
    detachRamDisk();
  }
}, 30000);

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe('DISK-FULL — real ENOSPC/SQLITE_FULL against the real server', () => {
  it.skipIf(!capable)(
    'DF1: durable pushes fail loudly at the wall, server survives, acknowledged jobs remain, recovery after freeing space',
    async () => {
      const dir = (tiny as TinyFs).dir;
      cleanVolume();
      const db = join(dir, 'bunq-df1.db');
      const port = getFreePort();
      const QUEUE = 'df1';

      const s = await spawnServer(db, port);
      const c = await connect(port);

      // Durable pushes acknowledged ok while there is still space — these are
      // the jobs whose acceptance the server must stand behind at the wall.
      const okIds: string[] = [];
      for (let i = 0; i < 5; i++) {
        const res = await c.send({
          cmd: 'PUSH',
          queue: QUEUE,
          data: { i, pre: true },
          durable: true,
        });
        expect(res.ok).toBe(true);
        okIds.push(String(res.id));
      }

      // Brick the volume (leaves <4 KB free).
      fillVolume(dir);
      expect(fillers.length).toBeGreaterThan(0);

      // Push durable jobs (64 KB payloads) until the wall. Any that still fit
      // are acknowledged ok and join the "must survive" set; the rest MUST be
      // rejected with an explicit error, never a fake id.
      const payload = 'x'.repeat(64 * 1024);
      let failures = 0;
      let lastError = '';
      for (let i = 0; i < 200 && failures < 3; i++) {
        const res = await c.send({
          cmd: 'PUSH',
          queue: QUEUE,
          data: { i, payload },
          durable: true,
        });
        if (res.ok) {
          okIds.push(String(res.id));
        } else {
          failures++;
          // (2) an error surfaced to the client — no silent accept-then-lose
          expect(typeof res.error).toBe('string');
          expect(String(res.error).length).toBeGreaterThan(0);
          expect(res.id).toBeUndefined();
          lastError = String(res.error);
        }
      }
      // The wall was really hit (repeatedly).
      expect(failures).toBeGreaterThanOrEqual(3);
      console.log(
        `[DF1] wall hit: ${okIds.length} pushes acknowledged ok, ${failures} rejected, error="${lastError}"`
      );

      // A rejected durable write is not accepted in any form: it must not be
      // visible or executable from the in-memory queue after SQLite rejected
      // it. This also makes a client retry unambiguous.
      const countsAfterRejection = await c.send({ cmd: 'GetJobCounts', queue: QUEUE });
      expect((countsAfterRejection.counts as { waiting: number }).waiting).toBe(okIds.length);

      // (1) the server did NOT crash — process alive AND responsive to Ping on
      // a FRESH connection (not just the already-open socket).
      expect(s.proc.exitCode).toBeNull();
      const fresh = new TcpClient({
        host: '127.0.0.1',
        port,
        autoReconnect: false,
        pingInterval: 0,
        commandTimeout: 5000,
        connectTimeout: 5000,
      });
      await fresh.connect();
      try {
        const pong = await fresh.send({ cmd: 'Ping' });
        expect(pong.ok).toBe(true);
      } finally {
        try {
          fresh.close();
        } catch {
          /* ignore */
        }
      }

      // (3) every acknowledged push is still queryable — nothing that got an
      // ok:true evaporated when the disk filled.
      for (const id of okIds) {
        const st = await c.send({ cmd: 'GetState', queue: QUEUE, id });
        expect(String(st.state)).not.toBe('unknown');
      }

      // (4) free space (operator deletes the filler) → durable pushes succeed
      // again without a restart. Allow a few attempts for the FS to settle.
      freeFillers();
      let recovered = false;
      for (let i = 0; i < 20 && !recovered; i++) {
        const res = await c.send({ cmd: 'PUSH', queue: QUEUE, data: { post: i }, durable: true });
        if (res.ok) {
          recovered = true;
          okIds.push(String(res.id));
        } else {
          await Bun.sleep(250);
        }
      }
      expect(recovered).toBe(true);

      // And the recovered server still accounts for every acknowledged job.
      for (const id of okIds) {
        const st = await c.send({ cmd: 'GetState', queue: QUEUE, id });
        expect(String(st.state)).not.toBe('unknown');
      }
    },
    120000
  );

  it.skipIf(!capable)(
    'DF3: embedded durable rejections never remain visible or executable in memory',
    async () => {
      const dir = (tiny as TinyFs).dir;
      cleanVolume();
      const queue = 'df3-embedded';
      const manager = new QueueManager({ dataPath: join(dir, 'bunq-df3.db') });
      activeManager = manager;

      const acceptedIds: string[] = [];
      for (let i = 0; i < 5; i++) {
        const job = await manager.push(queue, { data: { i, pre: true }, durable: true });
        acceptedIds.push(job.id);
      }

      fillVolume(dir);
      expect(fillers.length).toBeGreaterThan(0);

      const payload = 'z'.repeat(64 * 1024);
      let failures = 0;
      for (let i = 0; i < 3; i++) {
        try {
          const job = await manager.push(queue, { data: { i, payload }, durable: true });
          acceptedIds.push(job.id);
        } catch (error) {
          expect(error).toBeInstanceOf(Error);
          failures++;
        }
      }

      expect(failures).toBe(3);
      expect(manager.getQueueJobCounts(queue).waiting).toBe(acceptedIds.length);

      freeFillers();
    },
    120000
  );

  it.skipIf(!capable)(
    'DF4: a rejected durable TCP batch remains absent from memory',
    async () => {
      const dir = (tiny as TinyFs).dir;
      cleanVolume();
      const port = getFreePort();
      const tcpQueue = 'df4-tcp';
      await spawnServer(join(dir, 'bunq-df4-tcp.db'), port);
      const client = await connect(port);
      fillVolume(dir);

      const payload = 'b'.repeat(64 * 1024);
      const tcpResult = await client.send({
        cmd: 'PUSHB',
        queue: tcpQueue,
        jobs: Array.from({ length: 3 }, (_, i) => ({ data: { i, payload }, durable: true })),
      });
      expect(tcpResult.ok).toBe(false);
      const tcpCounts = await client.send({ cmd: 'GetJobCounts', queue: tcpQueue });
      expect((tcpCounts.counts as { waiting: number }).waiting).toBe(0);
    },
    120000
  );

  it.skipIf(!capable)(
    'DF5: a rejected durable embedded batch remains absent from memory',
    async () => {
      const dir = (tiny as TinyFs).dir;
      cleanVolume();
      const embeddedQueue = 'df5-embedded';
      const manager = new QueueManager({ dataPath: join(dir, 'bunq-df5-embedded.db') });
      activeManager = manager;
      fillVolume(dir);

      const payload = 'e'.repeat(64 * 1024);
      await expect(
        manager.pushBatch(
          embeddedQueue,
          Array.from({ length: 3 }, (_, i) => ({ data: { i, payload }, durable: true }))
        )
      ).rejects.toThrow();
      expect(manager.getQueueJobCounts(embeddedQueue).waiting).toBe(0);

      freeFillers();
    },
    120000
  );

  it.skipIf(!capable)(
    'DF6: rejected TCP reuse preserves the previous DLQ generation across restart',
    async () => {
      const dir = (tiny as TinyFs).dir;
      cleanVolume();
      const db = join(dir, 'bunq-df6.db');
      const port = getFreePort();
      const queue = 'df6-tcp-dlq';
      const customId = 'df6-terminal-id';
      await spawnServer(db, port);
      let client = await connect(port);

      const first = await client.send({
        cmd: 'PUSH',
        queue,
        jobId: customId,
        data: { generation: 1 },
        durable: true,
      });
      const pull = await client.send({ cmd: 'PULL', queue, owner: 'df6-worker' });
      expect(
        await client.send({
          cmd: 'FAIL',
          id: String(first.id),
          token: pull.token as string,
          error: 'terminal generation',
          unrecoverable: true,
        })
      ).toMatchObject({ ok: true });

      fillVolume(dir);
      const rejected = await client.send({
        cmd: 'PUSH',
        queue,
        jobId: customId,
        data: { generation: 2 },
        durable: true,
      });
      expect(rejected.ok).toBe(false);
      const failedCounts = await client.send({ cmd: 'GetJobCounts', queue });
      expect((failedCounts.counts as { failed: number; waiting: number }).failed).toBe(1);
      expect((failedCounts.counts as { failed: number; waiting: number }).waiting).toBe(0);
      const oldDlq = await client.send({ cmd: 'Dlq', queue, count: 10 });
      expect(
        (oldDlq.jobs as Array<{ id: string; data: unknown }>).find((job) => job.id === customId)
          ?.data
      ).toEqual({ generation: 1 });

      freeFillers();
      await stopServer();
      await spawnServer(db, port);
      client = await connect(port);
      const recoveredDlq = await client.send({ cmd: 'Dlq', queue, count: 10 });
      expect(
        (recoveredDlq.jobs as Array<{ id: string; data: unknown }>).find(
          (job) => job.id === customId
        )?.data
      ).toEqual({ generation: 1 });

      const replacement = await client.send({
        cmd: 'PUSH',
        queue,
        jobId: customId,
        data: { generation: 2 },
        durable: true,
      });
      expect(replacement.ok).toBe(true);
      const replacedCounts = await client.send({ cmd: 'GetJobCounts', queue });
      expect((replacedCounts.counts as { failed: number; waiting: number }).failed).toBe(0);
      expect((replacedCounts.counts as { failed: number; waiting: number }).waiting).toBe(1);
    },
    120000
  );

  it.skipIf(!capable)(
    'DF7: rejected embedded reuse preserves the previous DLQ generation across restart',
    async () => {
      const dir = (tiny as TinyFs).dir;
      cleanVolume();
      const db = join(dir, 'bunq-df7.db');
      const queue = 'df7-embedded-dlq';
      const customId = 'df7-terminal-id';
      let manager = new QueueManager({ dataPath: db });
      activeManager = manager;
      const first = await manager.push(queue, {
        customId,
        data: { generation: 1 },
        durable: true,
      });
      expect(await manager.discard(first.id)).toBe(true);

      fillVolume(dir);
      await expect(
        manager.push(queue, { customId, data: { generation: 2 }, durable: true })
      ).rejects.toThrow();
      expect(manager.getQueueJobCounts(queue).failed).toBe(1);
      expect(manager.getQueueJobCounts(queue).waiting).toBe(0);
      expect(manager.getDlq(queue)[0]?.data).toEqual({ generation: 1 });

      freeFillers();
      manager.shutdown();
      activeManager = null;
      manager = new QueueManager({ dataPath: db });
      activeManager = manager;
      expect(manager.getDlq(queue)[0]?.data).toEqual({ generation: 1 });

      const replacement = await manager.push(queue, {
        customId,
        data: { generation: 2 },
        durable: true,
      });
      expect(replacement.data).toEqual({ generation: 2 });
      expect(manager.getQueueJobCounts(queue).failed).toBe(0);
      expect(manager.getQueueJobCounts(queue).waiting).toBe(1);
    },
    120000
  );

  it.skipIf(!capable)(
    'DF8: rejected TCP reuse preserves completed data and result across restart',
    async () => {
      const dir = (tiny as TinyFs).dir;
      cleanVolume();
      const db = join(dir, 'bunq-df8.db');
      const port = getFreePort();
      const queue = 'df8-tcp-completed';
      const customId = 'df8-completed-id';
      await spawnServer(db, port);
      let client = await connect(port);

      await client.send({
        cmd: 'PUSH',
        queue,
        jobId: customId,
        data: { generation: 1 },
        durable: true,
      });
      const pull = await client.send({ cmd: 'PULL', queue, owner: 'df8-worker' });
      expect(
        await client.send({
          cmd: 'ACK',
          id: customId,
          token: pull.token as string,
          result: { generation: 1, result: true },
        })
      ).toMatchObject({ ok: true });

      fillVolume(dir);
      const rejected = await client.send({
        cmd: 'PUSH',
        queue,
        jobId: customId,
        data: { generation: 2 },
        durable: true,
      });
      expect(rejected.ok).toBe(false);
      expect((await client.send({ cmd: 'GetState', queue, id: customId })).state).toBe('completed');
      expect((await client.send({ cmd: 'GetResult', id: customId })).result).toEqual({
        generation: 1,
        result: true,
      });

      freeFillers();
      await stopServer();
      await spawnServer(db, port);
      client = await connect(port);
      expect((await client.send({ cmd: 'GetState', queue, id: customId })).state).toBe('completed');
      expect((await client.send({ cmd: 'GetResult', id: customId })).result).toEqual({
        generation: 1,
        result: true,
      });

      const replacement = await client.send({
        cmd: 'PUSH',
        queue,
        jobId: customId,
        data: { generation: 2 },
        durable: true,
      });
      expect(replacement.ok).toBe(true);
      expect((await client.send({ cmd: 'GetState', queue, id: customId })).state).toBe('waiting');
      expect((await client.send({ cmd: 'GetResult', id: customId })).result).toBeUndefined();
    },
    120000
  );

  it.skipIf(!capable)(
    'DF9: rejected embedded reuse preserves completed data and result across restart',
    async () => {
      const dir = (tiny as TinyFs).dir;
      cleanVolume();
      const db = join(dir, 'bunq-df9.db');
      const queue = 'df9-embedded-completed';
      const customId = 'df9-completed-id';
      let manager = new QueueManager({ dataPath: db });
      activeManager = manager;
      await manager.push(queue, { customId, data: { generation: 1 }, durable: true });
      const pulled = await manager.pull(queue);
      expect(pulled?.id).toBe(customId);
      await manager.ack(customId, { generation: 1, result: true });

      fillVolume(dir);
      await expect(
        manager.push(queue, { customId, data: { generation: 2 }, durable: true })
      ).rejects.toThrow();
      expect(await manager.getJobState(customId)).toBe('completed');
      expect(manager.getResult(customId)).toEqual({ generation: 1, result: true });
      expect((await manager.getJob(customId))?.data).toEqual({ generation: 1 });

      freeFillers();
      manager.shutdown();
      activeManager = null;
      manager = new QueueManager({ dataPath: db });
      activeManager = manager;
      expect(await manager.getJobState(customId)).toBe('completed');
      expect(manager.getResult(customId)).toEqual({ generation: 1, result: true });

      const replacement = await manager.push(queue, {
        customId,
        data: { generation: 2 },
        durable: true,
      });
      expect(replacement.data).toEqual({ generation: 2 });
      expect(await manager.getJobState(customId)).toBe('waiting');
      expect(manager.getResult(customId)).toBeUndefined();
    },
    120000
  );

  it.skipIf(!capable)(
    'DF2: buffered pushes ack ok while the disk is full, but StorageStatus/health surface diskFull — and clear after space is freed',
    async () => {
      const dir = (tiny as TinyFs).dir;
      cleanVolume(); // reclaim DF1's db + wal before spawning on the tiny volume
      const db = join(dir, 'bunq-df2.db');
      const port = getFreePort();
      const QUEUE = 'df2';

      await spawnServer(db, port);
      const c = await connect(port);

      // Baseline: healthy storage.
      const st0 = await c.send({ cmd: 'StorageStatus' });
      expect(st0.ok).toBe(true);
      expect((st0.data as { diskFull: boolean }).diskFull).toBe(false);

      // Brick the volume.
      fillVolume(dir);
      expect(fillers.length).toBeGreaterThan(0);

      // Write-buffered (non-durable) pushes: the server acks ok immediately —
      // the documented 10ms-window trade-off. The point of this test is that
      // the resulting persistence failure is NOT hidden forever.
      const payload = 'y'.repeat(64 * 1024);
      for (let i = 0; i < 40; i++) {
        const res = await c.send({ cmd: 'PUSH', queue: QUEUE, data: { i, payload } });
        expect(res.ok).toBe(true); // pretend-success window — by design
      }

      // The WriteBuffer flush hits SQLITE_FULL → StorageStatus.diskFull flips
      // to true (sqlite.ts onError → setDiskFull). Keep nudging fresh flushes
      // while polling; generous deadline for flush timers + backoff.
      let flipped = false;
      const deadline = Date.now() + 20000;
      while (Date.now() < deadline) {
        const st = await c.send({ cmd: 'StorageStatus' });
        if (st.ok && (st.data as { diskFull: boolean }).diskFull === true) {
          flipped = true;
          break;
        }
        await c.send({ cmd: 'PUSH', queue: QUEUE, data: { nudge: Date.now(), payload } });
        await Bun.sleep(200);
      }
      expect(flipped).toBe(true);

      // GET /health mirrors the degradation.
      const healthRes = await fetch(`http://127.0.0.1:${port + 1}/health`);
      const health = (await healthRes.json()) as {
        ok: boolean;
        status: string;
        storage?: { diskFull: boolean; error: string | null };
      };
      expect(health.status).toBe('degraded');
      expect(health.ok).toBe(false);
      expect(health.storage?.diskFull).toBe(true);
      expect(typeof health.storage?.error).toBe('string');
      console.log(`[DF2] surfaced diskFull, /health error="${health.storage?.error}"`);

      // Free space → the next successful durable write clears the flag
      // (safeWrite success path). Retry the durable push briefly, then poll.
      freeFillers();
      let durableOk = false;
      for (let i = 0; i < 20 && !durableOk; i++) {
        const res = await c.send({ cmd: 'PUSH', queue: QUEUE, data: { clear: i }, durable: true });
        if (res.ok) durableOk = true;
        else await Bun.sleep(250);
      }
      expect(durableOk).toBe(true);

      let cleared = false;
      const deadline2 = Date.now() + 15000;
      while (Date.now() < deadline2) {
        const st = await c.send({ cmd: 'StorageStatus' });
        if (st.ok && (st.data as { diskFull: boolean }).diskFull === false) {
          cleared = true;
          break;
        }
        await Bun.sleep(200);
      }
      expect(cleared).toBe(true);

      // /health is healthy again.
      const health2 = (await (await fetch(`http://127.0.0.1:${port + 1}/health`)).json()) as {
        ok: boolean;
        status: string;
      };
      expect(health2.status).toBe('healthy');
      expect(health2.ok).toBe(true);
    },
    120000
  );
});
