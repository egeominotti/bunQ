#!/usr/bin/env bun
/**
 * Test Missing TCP Commands: PULLB, ACKB, Clean, Ping
 */

import { Queue } from '../../src/client';
import { pack, unpack } from 'msgpackr';
import { FrameParser } from '../../src/infrastructure/server/protocol';
import type { Socket } from 'bun';

const QUEUE_NAME = 'tcp-test-missing-cmds';
const TCP_PORT = parseInt(process.env.TCP_PORT ?? '16789');

interface SocketData {
  frameParser: FrameParser;
  resolve: ((value: Record<string, unknown>) => void) | null;
  reject: ((error: Error) => void) | null;
}

async function createTcpClient(): Promise<{
  send: (cmd: Record<string, unknown>) => Promise<Record<string, unknown>>;
  close: () => void;
}> {
  return new Promise((resolve, reject) => {
    const sd: SocketData = { frameParser: new FrameParser(), resolve: null, reject: null };
    let socket: Socket<unknown> | null = null;
    void Bun.connect({
      hostname: 'localhost',
      port: TCP_PORT,
      socket: {
        data(_s: Socket<unknown>, data: Buffer) {
          for (const frame of sd.frameParser.addData(new Uint8Array(data))) {
            if (sd.resolve) {
              try { sd.resolve(unpack(frame) as Record<string, unknown>); }
              catch { sd.reject?.(new Error('Invalid response')); }
              sd.resolve = null; sd.reject = null;
            }
          }
        },
        open(sock: Socket<unknown>) {
          socket = sock;
          resolve({
            send: (cmd: Record<string, unknown>) => new Promise((res, rej) => {
              sd.resolve = res; sd.reject = rej;
              sock.write(FrameParser.frame(pack(cmd)));
              setTimeout(() => { if (sd.resolve === res) { sd.resolve = null; sd.reject = null; rej(new Error('Timeout')); } }, 10000);
            }),
            close: () => sock.end(),
          });
        },
        error(_s: Socket<unknown>, e: Error) { reject(new Error(`Connection error: ${e.message}`)); },
        connectError(_s: Socket<unknown>, e: Error) { reject(new Error(`Connect failed: ${e.message}`)); },
      },
    });
    setTimeout(() => { if (!socket) reject(new Error('Connection timeout')); }, 5000);
  });
}

function pass(msg: string, p: { v: number }) { console.log(`   PASS ${msg}`); p.v++; }
function fail(msg: string, f: { v: number }, detail?: string) {
  console.log(`   FAIL ${msg}`); if (detail) console.log(`   ${detail}`); f.v++;
}

async function main() {
  console.log('=== Test Missing Commands (TCP) ===\n');
  const queue = new Queue<{ value: number }>(QUEUE_NAME, { connection: { port: TCP_PORT } });
  let tcp: Awaited<ReturnType<typeof createTcpClient>>;
  const p = { v: 0 }, f = { v: 0 };

  try { tcp = await createTcpClient(); } catch (e) {
    console.error(`Failed to connect: ${e}`); process.exit(1);
  }

  queue.obliterate(); await Bun.sleep(100);

  // Test 1: Ping
  console.log('1. Testing PING...');
  try {
    const r = await tcp.send({ cmd: 'Ping' });
    const d = r.data as Record<string, unknown> | undefined;
    if (r.ok === true && d?.pong === true && typeof d.time === 'number' && d.time > 0)
      pass(`pong=true, time=${d.time}`, p);
    else fail('Invalid response', f, JSON.stringify(r));
  } catch (e) { fail(`${e}`, f); }

  // Test 2: Ping time freshness
  console.log('\n2. Testing PING (time freshness)...');
  try {
    const before = Date.now();
    const r = await tcp.send({ cmd: 'Ping' });
    const after = Date.now();
    const d = r.data as Record<string, unknown> | undefined;
    if (r.ok && d && (d.time as number) >= before && (d.time as number) <= after)
      pass(`Fresh (within ${after - before}ms)`, p);
    else fail('Time not in window', f, `before=${before}, time=${d?.time}, after=${after}`);
  } catch (e) { fail(`${e}`, f); }

  // Test 3: PULLB empty queue
  console.log('\n3. Testing PULLB (empty queue)...');
  try {
    queue.obliterate(); await Bun.sleep(100);
    const r = await tcp.send({ cmd: 'PULLB', queue: QUEUE_NAME, count: 5 });
    const jobs = r.jobs as unknown[];
    if (r.ok && Array.isArray(jobs) && jobs.length === 0)
      pass('0 jobs from empty queue', p);
    else fail(`Expected 0, got ${jobs?.length}`, f);
  } catch (e) { fail(`${e}`, f); }

  // Test 4: PULLB with jobs (partial pull)
  console.log('\n4. Testing PULLB (partial pull)...');
  try {
    queue.obliterate(); await Bun.sleep(100);
    await queue.addBulk(Array.from({ length: 5 }, (_, i) => ({ name: `pb-${i}`, data: { value: i } })));
    await Bun.sleep(100);
    const r = await tcp.send({ cmd: 'PULLB', queue: QUEUE_NAME, count: 3 });
    const jobs = r.jobs as unknown[];
    if (r.ok && Array.isArray(jobs) && jobs.length === 3)
      pass(`Pulled 3 of 5`, p);
    else fail(`Expected 3, got ${jobs?.length}`, f);
  } catch (e) { fail(`${e}`, f); }

  // Test 5: PULLB request more than available
  console.log('\n5. Testing PULLB (more than available)...');
  try {
    const r = await tcp.send({ cmd: 'PULLB', queue: QUEUE_NAME, count: 10 });
    const jobs = r.jobs as unknown[];
    if (r.ok && Array.isArray(jobs) && jobs.length === 2)
      pass(`Pulled 2 (requested 10)`, p);
    else fail(`Expected 2, got ${jobs?.length}`, f);
  } catch (e) { fail(`${e}`, f); }

  // Test 6: ACKB batch ack
  console.log('\n6. Testing ACKB (batch ack)...');
  try {
    queue.obliterate(); await Bun.sleep(100);
    await queue.addBulk([
      { name: 'a1', data: { value: 10 } }, { name: 'a2', data: { value: 20 } },
      { name: 'a3', data: { value: 30 } },
    ]);
    await Bun.sleep(100);
    const pr = await tcp.send({ cmd: 'PULLB', queue: QUEUE_NAME, count: 3 });
    const pulled = pr.jobs as Array<{ id: string }>;
    if (!Array.isArray(pulled) || pulled.length !== 3) { fail('Could not pull 3 jobs', f); }
    else {
      const ids = pulled.map(j => String(j.id));
      const ar = await tcp.send({ cmd: 'ACKB', ids });
      if (ar.ok === true) pass(`Acked ${ids.length} jobs`, p);
      else fail('ACKB not ok', f, JSON.stringify(ar));
    }
  } catch (e) { fail(`${e}`, f); }

  // Test 7: ACKB with results
  console.log('\n7. Testing ACKB (with results)...');
  try {
    queue.obliterate(); await Bun.sleep(100);
    await queue.addBulk([{ name: 'ar1', data: { value: 100 } }, { name: 'ar2', data: { value: 200 } }]);
    await Bun.sleep(100);
    const pr = await tcp.send({ cmd: 'PULLB', queue: QUEUE_NAME, count: 2 });
    const pulled = pr.jobs as Array<{ id: string }>;
    if (!Array.isArray(pulled) || pulled.length !== 2) { fail('Could not pull 2 jobs', f); }
    else {
      const ids = pulled.map(j => String(j.id));
      const ar = await tcp.send({ cmd: 'ACKB', ids, results: [{ code: 1 }, { code: 2 }] });
      if (ar.ok === true) pass(`Acked ${ids.length} with results`, p);
      else fail('ACKB with results not ok', f, JSON.stringify(ar));
    }
  } catch (e) { fail(`${e}`, f); }

  // Test 8: Clean completed jobs
  console.log('\n8. Testing CLEAN (completed jobs)...');
  try {
    queue.obliterate(); await Bun.sleep(100);
    await queue.addBulk([
      { name: 'c1', data: { value: 1 } }, { name: 'c2', data: { value: 2 } },
      { name: 'c3', data: { value: 3 } },
    ]);
    await Bun.sleep(100);
    const pr = await tcp.send({ cmd: 'PULLB', queue: QUEUE_NAME, count: 3 });
    const pulled = pr.jobs as Array<{ id: string }>;
    if (Array.isArray(pulled) && pulled.length > 0) {
      await tcp.send({ cmd: 'ACKB', ids: pulled.map(j => String(j.id)) });
      await Bun.sleep(100);
    }
    const cr = await tcp.send({ cmd: 'Clean', queue: QUEUE_NAME, grace: 0, state: 'completed' });
    if (cr.ok === true && typeof cr.count === 'number')
      pass(`Cleaned ${cr.count} completed jobs`, p);
    else fail('Clean response invalid', f, JSON.stringify(cr));
  } catch (e) { fail(`${e}`, f); }

  // Test 9: Clean with limit on empty queue
  console.log('\n9. Testing CLEAN (with limit, empty queue)...');
  try {
    queue.obliterate(); await Bun.sleep(100);
    const cr = await tcp.send({ cmd: 'Clean', queue: QUEUE_NAME, grace: 0, limit: 5 });
    if (cr.ok === true && typeof cr.count === 'number')
      pass(`Clean with limit returned count=${cr.count}`, p);
    else fail('Clean with limit invalid', f, JSON.stringify(cr));
  } catch (e) { fail(`${e}`, f); }

  // Cleanup
  queue.obliterate();
  queue.close();
  tcp.close();

  console.log('\n=== Summary ===');
  console.log(`Passed: ${p.v}`);
  console.log(`Failed: ${f.v}`);
  process.exit(f.v > 0 ? 1 : 0);
}

main().catch(console.error);
