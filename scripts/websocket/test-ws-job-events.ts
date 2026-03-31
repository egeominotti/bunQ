#!/usr/bin/env bun
/**
 * Test WebSocket Job Lifecycle Events — all 14 job event types
 */

const HTTP_PORT = parseInt(process.env.HTTP_PORT ?? '26790');
const WS_URL = `ws://localhost:${HTTP_PORT}/ws`;
const API = `http://localhost:${HTTP_PORT}`;
const Q = 'ws-job-events';

let passed = 0;
let failed = 0;

function ok(condition: boolean, msg: string): void {
  if (condition) { console.log(`   ✅ ${msg}`); passed++; }
  else { console.log(`   ❌ ${msg}`); failed++; }
}

async function connectWs(): Promise<WebSocket> {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(WS_URL);
    const t = setTimeout(() => reject(new Error('WS connect timeout')), 5000);
    ws.addEventListener('open', () => { clearTimeout(t); resolve(ws); });
    ws.addEventListener('error', (e) => { clearTimeout(t); reject(e); });
  });
}

async function waitEvent(ws: WebSocket, eventName: string, timeoutMs = 5000): Promise<Record<string, unknown>> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      ws.removeEventListener('message', handler);
      reject(new Error(`Timeout waiting for ${eventName}`));
    }, timeoutMs);
    function handler(ev: MessageEvent) {
      try {
        const parsed = JSON.parse(String(ev.data));
        if (parsed.event === eventName) {
          clearTimeout(timer);
          ws.removeEventListener('message', handler);
          resolve(parsed);
        }
      } catch { /* */ }
    }
    ws.addEventListener('message', handler);
  });
}

async function api(method: string, path: string, body?: unknown): Promise<Record<string, unknown>> {
  const res = await fetch(`${API}${path}`, {
    method,
    headers: body ? { 'Content-Type': 'application/json' } : undefined,
    body: body ? JSON.stringify(body) : undefined,
  });
  return res.json() as Promise<Record<string, unknown>>;
}

async function pushJob(data: unknown = { x: 1 }, opts: Record<string, unknown> = {}): Promise<string> {
  const res = await api('POST', `/queues/${Q}/jobs`, { data, ...opts });
  return res.id as string;
}

async function pullJob(): Promise<string | null> {
  const res = await api('GET', `/queues/${Q}/jobs`);
  if (!res.ok) return null;
  const job = res.job as Record<string, unknown> | undefined;
  return (job?.id as string) ?? null;
}

async function cleanup(): Promise<void> {
  await api('POST', `/queues/${Q}/obliterate`);
  await Bun.sleep(100);
}

async function main() {
  console.log('=== Test WebSocket Job Lifecycle Events ===\n');

  let ws: WebSocket;
  try {
    ws = await connectWs();
  } catch (e) {
    ok(false, `Connect failed: ${e}`);
    printSummary();
    return;
  }
  ws.send(JSON.stringify({ cmd: 'Subscribe', events: ['*'] }));
  await Bun.sleep(100);

  // 1. job:pushed
  console.log('1. job:pushed');
  try {
    await cleanup();
    const ep = waitEvent(ws, 'job:pushed');
    await pushJob();
    const ev = await ep;
    ok((ev.data as Record<string, unknown>).queue === Q, `queue=${(ev.data as Record<string, unknown>).queue}`);
    ok(typeof (ev.data as Record<string, unknown>).jobId === 'string', 'has jobId');
    ok(typeof ev.ts === 'number', 'has timestamp');
  } catch (e) { ok(false, `${e}`); }

  // 2. job:active
  console.log('\n2. job:active');
  try {
    await cleanup();
    await pushJob();
    const ep = waitEvent(ws, 'job:active');
    await pullJob();
    const ev = await ep;
    ok((ev.data as Record<string, unknown>).queue === Q, `queue=${(ev.data as Record<string, unknown>).queue}`);
  } catch (e) { ok(false, `${e}`); }

  // 3. job:completed
  console.log('\n3. job:completed');
  try {
    await cleanup();
    const id = await pushJob();
    await pullJob();
    const ep = waitEvent(ws, 'job:completed');
    await api('POST', `/jobs/${id}/ack`, { result: { done: true } });
    const ev = await ep;
    ok((ev.data as Record<string, unknown>).queue === Q, `queue=${(ev.data as Record<string, unknown>).queue}`);
  } catch (e) { ok(false, `${e}`); }

  // 4. job:failed
  console.log('\n4. job:failed');
  try {
    await cleanup();
    const id = await pushJob({ x: 1 }, { maxAttempts: 1 });
    await pullJob();
    const ep = waitEvent(ws, 'job:failed');
    await api('POST', `/jobs/${id}/fail`, { error: 'test failure' });
    const ev = await ep;
    ok((ev.data as Record<string, unknown>).queue === Q, `queue=${(ev.data as Record<string, unknown>).queue}`);
    ok(typeof (ev.data as Record<string, unknown>).error === 'string', 'has error');
  } catch (e) { ok(false, `${e}`); }

  // 5. job:removed
  console.log('\n5. job:removed');
  try {
    await cleanup();
    const id = await pushJob();
    const ep = waitEvent(ws, 'job:removed');
    await api('DELETE', `/jobs/${id}`);
    const ev = await ep;
    ok((ev.data as Record<string, unknown>).jobId === id, `jobId=${(ev.data as Record<string, unknown>).jobId}`);
  } catch (e) { ok(false, `${e}`); }

  // 6. job:promoted
  console.log('\n6. job:promoted');
  try {
    await cleanup();
    const id = await pushJob({ x: 1 }, { delay: 60000 });
    const ep = waitEvent(ws, 'job:promoted');
    await api('POST', `/jobs/${id}/promote`);
    const ev = await ep;
    ok(typeof (ev.data as Record<string, unknown>).jobId === 'string', 'has jobId');
  } catch (e) { ok(false, `${e}`); }

  // 7. job:progress
  console.log('\n7. job:progress');
  try {
    await cleanup();
    const id = await pushJob();
    await pullJob();
    const ep = waitEvent(ws, 'job:progress');
    await api('POST', `/jobs/${id}/progress`, { progress: 50 });
    const ev = await ep;
    ok((ev.data as Record<string, unknown>).progress !== undefined, `progress=${(ev.data as Record<string, unknown>).progress}`);
  } catch (e) { ok(false, `${e}`); }

  // 8. job:discarded
  console.log('\n8. job:discarded');
  try {
    await cleanup();
    const id = await pushJob();
    const ep = waitEvent(ws, 'job:discarded');
    await api('POST', `/jobs/${id}/discard`);
    const ev = await ep;
    ok(typeof (ev.data as Record<string, unknown>).jobId === 'string', 'has jobId');
  } catch (e) { ok(false, `${e}`); }

  // 9. job:priority-changed
  console.log('\n9. job:priority-changed');
  try {
    await cleanup();
    const id = await pushJob();
    const ep = waitEvent(ws, 'job:priority-changed');
    await api('PUT', `/jobs/${id}/priority`, { priority: 99 });
    const ev = await ep;
    ok(typeof (ev.data as Record<string, unknown>).jobId === 'string', 'has jobId');
  } catch (e) { ok(false, `${e}`); }

  // 10. job:data-updated
  console.log('\n10. job:data-updated');
  try {
    await cleanup();
    const id = await pushJob();
    const ep = waitEvent(ws, 'job:data-updated');
    await api('PUT', `/jobs/${id}/data`, { data: { updated: true } });
    const ev = await ep;
    ok(typeof (ev.data as Record<string, unknown>).jobId === 'string', 'has jobId');
  } catch (e) { ok(false, `${e}`); }

  // 11. job:delay-changed
  console.log('\n11. job:delay-changed');
  try {
    await cleanup();
    const id = await pushJob();
    await pullJob();
    const ep = waitEvent(ws, 'job:delay-changed');
    await api('PUT', `/jobs/${id}/delay`, { delay: 5000 });
    const ev = await ep;
    ok(typeof (ev.data as Record<string, unknown>).jobId === 'string', 'has jobId');
  } catch (e) { ok(false, `${e}`); }

  // 12. queue:counts auto-emitted on state change
  console.log('\n12. queue:counts (auto-emitted)');
  try {
    await cleanup();
    const ep = waitEvent(ws, 'queue:counts');
    await pushJob();
    const ev = await ep;
    ok((ev.data as Record<string, unknown>).queue === Q, `queue=${(ev.data as Record<string, unknown>).queue}`);
    ok(typeof (ev.data as Record<string, unknown>).waiting === 'number', 'has waiting count');
  } catch (e) { ok(false, `${e}`); }

  ws.close();
  await cleanup();
  printSummary();
}

function printSummary() {
  console.log(`\n=== Summary ===`);
  console.log(`Passed: ${passed}, Failed: ${failed}, Total: ${passed + failed}`);
  if (failed > 0) process.exit(1);
}

main().catch((err) => { console.error('Fatal:', err); process.exit(1); });
