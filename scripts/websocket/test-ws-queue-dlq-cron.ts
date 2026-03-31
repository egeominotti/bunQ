#!/usr/bin/env bun
/**
 * Test WebSocket Queue, DLQ, and Cron events
 */

const HTTP_PORT = parseInt(process.env.HTTP_PORT ?? '26790');
const WS_URL = `ws://localhost:${HTTP_PORT}/ws`;
const API = `http://localhost:${HTTP_PORT}`;
const Q = 'ws-queue-dlq-cron';

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

async function cleanup(): Promise<void> {
  await api('POST', `/queues/${Q}/obliterate`);
  await Bun.sleep(100);
}

async function main() {
  console.log('=== Test WebSocket Queue, DLQ, Cron Events ===\n');

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

  // ── Queue Events ──

  // 1. queue:paused
  console.log('1. queue:paused');
  try {
    await cleanup();
    await api('POST', `/queues/${Q}/jobs`, { data: { x: 1 } });
    const ep = waitEvent(ws, 'queue:paused');
    await api('POST', `/queues/${Q}/pause`);
    const ev = await ep;
    ok((ev.data as Record<string, unknown>).queue === Q, `queue=${(ev.data as Record<string, unknown>).queue}`);
  } catch (e) { ok(false, `${e}`); }

  // 2. queue:resumed
  console.log('\n2. queue:resumed');
  try {
    const ep = waitEvent(ws, 'queue:resumed');
    await api('POST', `/queues/${Q}/resume`);
    const ev = await ep;
    ok((ev.data as Record<string, unknown>).queue === Q, `queue=${(ev.data as Record<string, unknown>).queue}`);
  } catch (e) { ok(false, `${e}`); }

  // 3. queue:drained
  console.log('\n3. queue:drained');
  try {
    await cleanup();
    await api('POST', `/queues/${Q}/jobs`, { data: { x: 1 } });
    const ep = waitEvent(ws, 'queue:drained');
    await api('POST', `/queues/${Q}/drain`);
    const ev = await ep;
    ok((ev.data as Record<string, unknown>).queue === Q, `queue=${(ev.data as Record<string, unknown>).queue}`);
  } catch (e) { ok(false, `${e}`); }

  // 4. queue:cleaned
  console.log('\n4. queue:cleaned');
  try {
    await cleanup();
    await api('POST', `/queues/${Q}/jobs`, { data: { x: 1 } });
    await api('POST', `/queues/${Q}/jobs`, { data: { x: 2 } });
    await Bun.sleep(100);
    const ep = waitEvent(ws, 'queue:cleaned');
    await api('POST', `/queues/${Q}/clean`, { grace: 0, state: 'waiting' });
    const ev = await ep;
    ok((ev.data as Record<string, unknown>).queue === Q, `queue=${(ev.data as Record<string, unknown>).queue}`);
  } catch (e) { ok(false, `${e}`); }

  // 5. queue:obliterated
  console.log('\n5. queue:obliterated');
  try {
    await api('POST', `/queues/${Q}/jobs`, { data: { x: 1 } });
    const ep = waitEvent(ws, 'queue:obliterated');
    await api('POST', `/queues/${Q}/obliterate`);
    const ev = await ep;
    ok((ev.data as Record<string, unknown>).queue === Q, `queue=${(ev.data as Record<string, unknown>).queue}`);
  } catch (e) { ok(false, `${e}`); }

  // ── DLQ Events ──
  console.log('\n── DLQ Events ──');

  // 6. dlq:added
  console.log('\n6. dlq:added');
  try {
    await cleanup();
    const res = await api('POST', `/queues/${Q}/jobs`, { data: { x: 1 }, maxAttempts: 1 });
    const id = res.id as string;
    // Pull to make active
    await api('GET', `/queues/${Q}/jobs`);
    const ep = waitEvent(ws, 'dlq:added');
    await api('POST', `/jobs/${id}/fail`, { error: 'dlq test' });
    const ev = await ep;
    ok((ev.data as Record<string, unknown>).queue === Q, `queue=${(ev.data as Record<string, unknown>).queue}`);
  } catch (e) { ok(false, `${e}`); }

  // 7. dlq:retry-all
  console.log('\n7. dlq:retry-all');
  try {
    await Bun.sleep(200);
    const ep = waitEvent(ws, 'dlq:retry-all');
    await api('POST', `/queues/${Q}/dlq/retry`);
    const ev = await ep;
    ok((ev.data as Record<string, unknown>).queue === Q, `queue=${(ev.data as Record<string, unknown>).queue}`);
  } catch (e) { ok(false, `${e}`); }

  // 8. dlq:purged
  console.log('\n8. dlq:purged');
  try {
    // Create another DLQ entry
    await cleanup();
    const res = await api('POST', `/queues/${Q}/jobs`, { data: { x: 1 }, maxAttempts: 1 });
    const id = res.id as string;
    await api('GET', `/queues/${Q}/jobs`);
    await api('POST', `/jobs/${id}/fail`, { error: 'purge test' });
    await Bun.sleep(200);
    const ep = waitEvent(ws, 'dlq:purged');
    await api('POST', `/queues/${Q}/dlq/purge`);
    const ev = await ep;
    ok((ev.data as Record<string, unknown>).queue === Q, `queue=${(ev.data as Record<string, unknown>).queue}`);
  } catch (e) { ok(false, `${e}`); }

  // ── Cron Events ──
  console.log('\n── Cron Events ──');

  // 9. cron:created
  console.log('\n9. cron:created');
  try {
    const ep = waitEvent(ws, 'cron:created');
    await api('POST', '/crons', { name: 'ws-cron-test', queue: Q, data: { t: 1 }, repeatEvery: 60000 });
    const ev = await ep;
    ok((ev.data as Record<string, unknown>).name === 'ws-cron-test', `name=${(ev.data as Record<string, unknown>).name}`);
  } catch (e) { ok(false, `${e}`); }

  // 10. cron:deleted
  console.log('\n10. cron:deleted');
  try {
    const ep = waitEvent(ws, 'cron:deleted');
    await api('DELETE', '/crons/ws-cron-test');
    const ev = await ep;
    ok((ev.data as Record<string, unknown>).name === 'ws-cron-test', `name=${(ev.data as Record<string, unknown>).name}`);
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
