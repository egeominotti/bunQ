#!/usr/bin/env bun
/**
 * Test Additional TCP Commands: GetProgress, Count, JobHeartbeat
 */

import { Queue } from '../../src/client';
import { pack, unpack } from 'msgpackr';
import { FrameParser } from '../../src/infrastructure/server/protocol';
import type { Socket } from 'bun';

const QUEUE_NAME = 'tcp-test-additional-cmds';
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
  console.log('=== Test Additional Commands (TCP) ===\n');
  const queue = new Queue<{ value: number }>(QUEUE_NAME, { connection: { port: TCP_PORT } });
  let tcp: Awaited<ReturnType<typeof createTcpClient>>;
  const p = { v: 0 }, f = { v: 0 };

  try { tcp = await createTcpClient(); } catch (e) {
    console.error(`Failed to connect: ${e}`); process.exit(1);
  }

  queue.obliterate(); await Bun.sleep(100);

  // Test 1: GetProgress on non-existent job
  console.log('1. Testing GetProgress (non-existent job)...');
  try {
    const r = await tcp.send({ cmd: 'GetProgress', id: 'nonexistent-999' });
    if (r.ok === false || r.error)
      pass('Returns error for non-existent job', p);
    else fail('Expected error for non-existent job', f, JSON.stringify(r));
  } catch (e) { fail(`${e}`, f); }

  // Test 2: GetProgress after updating progress on an active job
  console.log('\n2. Testing GetProgress (active job with progress)...');
  try {
    queue.obliterate(); await Bun.sleep(100);

    // Push a job via TCP
    const pushR = await tcp.send({
      cmd: 'PUSH', queue: QUEUE_NAME, name: 'progress-test', data: { value: 42 },
    });
    const jobId = pushR.id as string;
    if (!jobId) { fail('Could not push job', f); }
    else {
      // Pull the job to make it active
      const pullR = await tcp.send({ cmd: 'PULL', queue: QUEUE_NAME });
      const pulledJob = pullR.job as Record<string, unknown> | undefined;
      if (!pulledJob) { fail('Could not pull job', f); }
      else {
        const activeId = String(pulledJob.id);
        // Update progress via TCP
        const progR = await tcp.send({ cmd: 'Progress', id: activeId, progress: 75, message: 'three-quarters' });
        if (progR.ok !== true) { fail('Progress update failed', f, JSON.stringify(progR)); }
        else {
          // Query progress via GetProgress
          const getR = await tcp.send({ cmd: 'GetProgress', id: activeId });
          if (getR.ok === true && (getR.progress as number) === 75 && getR.message === 'three-quarters')
            pass(`progress=75, message="three-quarters"`, p);
          else fail('GetProgress returned unexpected data', f, JSON.stringify(getR));
        }
        // Clean up: ack the job
        await tcp.send({ cmd: 'ACK', id: activeId });
      }
    }
  } catch (e) { fail(`${e}`, f); }

  // Test 3: GetProgress returns updated value after second update
  console.log('\n3. Testing GetProgress (progress overwrite)...');
  try {
    queue.obliterate(); await Bun.sleep(100);

    await tcp.send({ cmd: 'PUSH', queue: QUEUE_NAME, name: 'prog-overwrite', data: { value: 1 } });
    const pullR = await tcp.send({ cmd: 'PULL', queue: QUEUE_NAME });
    const pulledJob = pullR.job as Record<string, unknown> | undefined;
    if (!pulledJob) { fail('Could not pull job', f); }
    else {
      const id = String(pulledJob.id);
      await tcp.send({ cmd: 'Progress', id, progress: 25 });
      await tcp.send({ cmd: 'Progress', id, progress: 90, message: 'almost done' });

      const getR = await tcp.send({ cmd: 'GetProgress', id });
      if (getR.ok === true && (getR.progress as number) === 90 && getR.message === 'almost done')
        pass('Progress overwritten to 90', p);
      else fail('GetProgress did not reflect latest update', f, JSON.stringify(getR));

      await tcp.send({ cmd: 'ACK', id });
    }
  } catch (e) { fail(`${e}`, f); }

  // Test 4: Count on empty queue
  console.log('\n4. Testing Count (empty queue)...');
  try {
    queue.obliterate(); await Bun.sleep(100);

    const r = await tcp.send({ cmd: 'Count', queue: QUEUE_NAME });
    if (r.ok === true && (r.count as number) === 0)
      pass('count=0 for empty queue', p);
    else fail('Expected count=0', f, JSON.stringify(r));
  } catch (e) { fail(`${e}`, f); }

  // Test 5: Count after pushing jobs
  console.log('\n5. Testing Count (after pushing jobs)...');
  try {
    queue.obliterate(); await Bun.sleep(100);

    await tcp.send({ cmd: 'PUSH', queue: QUEUE_NAME, name: 'cnt-1', data: { value: 1 } });
    await tcp.send({ cmd: 'PUSH', queue: QUEUE_NAME, name: 'cnt-2', data: { value: 2 } });
    await tcp.send({ cmd: 'PUSH', queue: QUEUE_NAME, name: 'cnt-3', data: { value: 3 } });
    await Bun.sleep(50);

    const r = await tcp.send({ cmd: 'Count', queue: QUEUE_NAME });
    if (r.ok === true && (r.count as number) === 3)
      pass('count=3 after pushing 3 jobs', p);
    else fail(`Expected count=3, got ${r.count}`, f, JSON.stringify(r));
  } catch (e) { fail(`${e}`, f); }

  // Test 6: Count reflects active jobs (pulled but not acked)
  console.log('\n6. Testing Count (includes active jobs)...');
  try {
    // Pull one job from the 3 we pushed in test 5
    const pullR = await tcp.send({ cmd: 'PULL', queue: QUEUE_NAME });
    const pulledJob = pullR.job as Record<string, unknown> | undefined;
    if (!pulledJob) { fail('Could not pull job', f); }
    else {
      const r = await tcp.send({ cmd: 'Count', queue: QUEUE_NAME });
      // Count should include waiting + active jobs (still 3, or 2 waiting depending on impl)
      if (r.ok === true && typeof r.count === 'number')
        pass(`count=${r.count} after pulling 1 of 3`, p);
      else fail('Count response invalid', f, JSON.stringify(r));

      // Ack the pulled job
      await tcp.send({ cmd: 'ACK', id: String(pulledJob.id) });
    }
  } catch (e) { fail(`${e}`, f); }

  // Test 7: GetJobCounts returns per-state breakdown
  console.log('\n7. Testing GetJobCounts (per-state breakdown)...');
  try {
    queue.obliterate(); await Bun.sleep(100);

    // Push 3 waiting jobs
    await tcp.send({ cmd: 'PUSH', queue: QUEUE_NAME, name: 'jc-1', data: { value: 1 } });
    await tcp.send({ cmd: 'PUSH', queue: QUEUE_NAME, name: 'jc-2', data: { value: 2 } });
    await tcp.send({ cmd: 'PUSH', queue: QUEUE_NAME, name: 'jc-3', data: { value: 3 } });
    await Bun.sleep(50);

    // Pull one to make it active
    const pullR = await tcp.send({ cmd: 'PULL', queue: QUEUE_NAME });
    const pulledJob = pullR.job as Record<string, unknown> | undefined;

    const r = await tcp.send({ cmd: 'GetJobCounts', queue: QUEUE_NAME });
    const counts = r.counts as Record<string, number> | undefined;
    if (r.ok === true && counts && typeof counts.waiting === 'number' && typeof counts.active === 'number') {
      pass(`waiting=${counts.waiting}, active=${counts.active}, delayed=${counts.delayed}`, p);
    } else {
      fail('GetJobCounts response invalid', f, JSON.stringify(r));
    }

    // Clean up active job
    if (pulledJob) await tcp.send({ cmd: 'ACK', id: String(pulledJob.id) });
  } catch (e) { fail(`${e}`, f); }

  // Test 8: JobHeartbeat on non-existent job
  console.log('\n8. Testing JobHeartbeat (non-existent job)...');
  try {
    const r = await tcp.send({ cmd: 'JobHeartbeat', id: 'nonexistent-hb-999' });
    if (r.ok === false || r.error)
      pass('Returns error for non-existent job', p);
    else fail('Expected error for non-existent job', f, JSON.stringify(r));
  } catch (e) { fail(`${e}`, f); }

  // Test 9: JobHeartbeat on active job
  console.log('\n9. Testing JobHeartbeat (active job)...');
  try {
    queue.obliterate(); await Bun.sleep(100);

    await tcp.send({ cmd: 'PUSH', queue: QUEUE_NAME, name: 'hb-test', data: { value: 99 } });
    const pullR = await tcp.send({ cmd: 'PULL', queue: QUEUE_NAME });
    const pulledJob = pullR.job as Record<string, unknown> | undefined;
    if (!pulledJob) { fail('Could not pull job', f); }
    else {
      const id = String(pulledJob.id);
      const r = await tcp.send({ cmd: 'JobHeartbeat', id });
      const d = r.data as Record<string, unknown> | undefined;
      if (r.ok === true && d?.ok === true)
        pass(`Heartbeat accepted for active job ${id}`, p);
      else fail('JobHeartbeat not accepted', f, JSON.stringify(r));

      await tcp.send({ cmd: 'ACK', id });
    }
  } catch (e) { fail(`${e}`, f); }

  // Test 10: JobHeartbeat on waiting (non-active) job returns error
  console.log('\n10. Testing JobHeartbeat (waiting job - should fail)...');
  try {
    queue.obliterate(); await Bun.sleep(100);

    const pushR = await tcp.send({
      cmd: 'PUSH', queue: QUEUE_NAME, name: 'hb-waiting', data: { value: 50 },
    });
    const jobId = pushR.id as string;
    if (!jobId) { fail('Could not push job', f); }
    else {
      // Don't pull - job is in waiting state
      const r = await tcp.send({ cmd: 'JobHeartbeat', id: jobId });
      if (r.ok === false || r.error)
        pass('Returns error for waiting (non-active) job', p);
      else fail('Expected error for non-active job', f, JSON.stringify(r));
    }
  } catch (e) { fail(`${e}`, f); }

  // Test 11: Multiple JobHeartbeats on same active job
  console.log('\n11. Testing JobHeartbeat (multiple heartbeats)...');
  try {
    queue.obliterate(); await Bun.sleep(100);

    await tcp.send({ cmd: 'PUSH', queue: QUEUE_NAME, name: 'hb-multi', data: { value: 77 } });
    const pullR = await tcp.send({ cmd: 'PULL', queue: QUEUE_NAME });
    const pulledJob = pullR.job as Record<string, unknown> | undefined;
    if (!pulledJob) { fail('Could not pull job', f); }
    else {
      const id = String(pulledJob.id);
      const r1 = await tcp.send({ cmd: 'JobHeartbeat', id });
      const r2 = await tcp.send({ cmd: 'JobHeartbeat', id });
      const r3 = await tcp.send({ cmd: 'JobHeartbeat', id });
      const [d1, d2, d3] = [r1, r2, r3].map(r => r.data as Record<string, unknown> | undefined);
      if (r1.ok && d1?.ok && r2.ok && d2?.ok && r3.ok && d3?.ok)
        pass('3 consecutive heartbeats accepted', p);
      else fail('One or more heartbeats rejected', f,
        `r1=${JSON.stringify(r1)} r2=${JSON.stringify(r2)} r3=${JSON.stringify(r3)}`);

      await tcp.send({ cmd: 'ACK', id });
    }
  } catch (e) { fail(`${e}`, f); }

  queue.obliterate(); queue.close(); tcp.close();
  console.log('\n=== Summary ===');
  console.log(`Passed: ${p.v}`);
  console.log(`Failed: ${f.v}`);
  process.exit(f.v > 0 ? 1 : 0);
}

main().catch(console.error);
