#!/usr/bin/env bun
/**
 * Test WebSocket Worker, Rate Limit, Concurrency, Webhook, and Config events
 */

const HTTP_PORT = parseInt(process.env.HTTP_PORT ?? '26790');
const WS_URL = `ws://localhost:${HTTP_PORT}/ws`;
const API = `http://localhost:${HTTP_PORT}`;
const Q = 'ws-workers-rate';

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
  console.log('=== Test WebSocket Workers, Rate Limit, Webhooks, Config ===\n');

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

  // ── Worker Events ──
  console.log('── Worker Events ──');

  // 1. worker:connected
  console.log('\n1. worker:connected');
  try {
    const ep = waitEvent(ws, 'worker:connected');
    await api('POST', '/workers', { name: 'ws-test-worker', queues: [Q] });
    const ev = await ep;
    ok(typeof (ev.data as Record<string, unknown>).workerId === 'string', `workerId=${(ev.data as Record<string, unknown>).workerId}`);
  } catch (e) { ok(false, `${e}`); }

  // 2. worker:disconnected
  console.log('\n2. worker:disconnected');
  try {
    const res = await api('GET', '/workers');
    const data = res.data as { workers: { id: string }[] } | undefined;
    const wId = data?.workers?.[0]?.id;
    if (wId) {
      const ep = waitEvent(ws, 'worker:disconnected');
      await api('DELETE', `/workers/${wId}`);
      const ev = await ep;
      ok(typeof (ev.data as Record<string, unknown>).workerId === 'string', `workerId=${(ev.data as Record<string, unknown>).workerId}`);
    } else {
      ok(false, 'No worker found to disconnect');
    }
  } catch (e) { ok(false, `${e}`); }

  // ── Rate Limit & Concurrency ──
  console.log('\n── Rate Limit & Concurrency ──');

  // 3. ratelimit:set
  console.log('\n3. ratelimit:set');
  try {
    const ep = waitEvent(ws, 'ratelimit:set');
    await api('PUT', `/queues/${Q}/rate-limit`, { limit: 100 });
    const ev = await ep;
    ok((ev.data as Record<string, unknown>).queue === Q, `queue=${(ev.data as Record<string, unknown>).queue}`);
  } catch (e) { ok(false, `${e}`); }

  // 4. ratelimit:cleared
  console.log('\n4. ratelimit:cleared');
  try {
    const ep = waitEvent(ws, 'ratelimit:cleared');
    await api('DELETE', `/queues/${Q}/rate-limit`);
    const ev = await ep;
    ok((ev.data as Record<string, unknown>).queue === Q, `queue=${(ev.data as Record<string, unknown>).queue}`);
  } catch (e) { ok(false, `${e}`); }

  // 5. concurrency:set
  console.log('\n5. concurrency:set');
  try {
    const ep = waitEvent(ws, 'concurrency:set');
    await api('PUT', `/queues/${Q}/concurrency`, { concurrency: 5 });
    const ev = await ep;
    ok((ev.data as Record<string, unknown>).queue === Q, `queue=${(ev.data as Record<string, unknown>).queue}`);
  } catch (e) { ok(false, `${e}`); }

  // 6. concurrency:cleared
  console.log('\n6. concurrency:cleared');
  try {
    const ep = waitEvent(ws, 'concurrency:cleared');
    await api('DELETE', `/queues/${Q}/concurrency`);
    const ev = await ep;
    ok((ev.data as Record<string, unknown>).queue === Q, `queue=${(ev.data as Record<string, unknown>).queue}`);
  } catch (e) { ok(false, `${e}`); }

  // ── Webhook Events ──
  console.log('\n── Webhook Events ──');

  // 7. webhook:added
  console.log('\n7. webhook:added');
  let webhookId = '';
  try {
    const ep = waitEvent(ws, 'webhook:added');
    const res = await api('POST', '/webhooks', { url: 'https://example.com/hook', events: ['job.completed'] });
    const data = res.data as { webhookId: string } | undefined;
    webhookId = data?.webhookId ?? '';
    const ev = await ep;
    ok(typeof (ev.data as Record<string, unknown>).id === 'string', `id=${(ev.data as Record<string, unknown>).id}`);
  } catch (e) { ok(false, `${e}`); }

  // 8. webhook:removed
  console.log('\n8. webhook:removed');
  try {
    if (webhookId) {
      const ep = waitEvent(ws, 'webhook:removed');
      await api('DELETE', `/webhooks/${webhookId}`);
      const ev = await ep;
      ok(typeof (ev.data as Record<string, unknown>).id === 'string', `id=${(ev.data as Record<string, unknown>).id}`);
    } else {
      ok(false, 'No webhook to remove');
    }
  } catch (e) { ok(false, `${e}`); }

  // ── Config Events ──
  console.log('\n── Config Events ──');

  // 9. config:stall-changed
  console.log('\n9. config:stall-changed');
  try {
    const ep = waitEvent(ws, 'config:stall-changed');
    await api('PUT', `/queues/${Q}/stall-config`, { config: { enabled: true, stallInterval: 20000 } });
    const ev = await ep;
    ok((ev.data as Record<string, unknown>).queue === Q, `queue=${(ev.data as Record<string, unknown>).queue}`);
  } catch (e) { ok(false, `${e}`); }

  // 10. config:dlq-changed
  console.log('\n10. config:dlq-changed');
  try {
    const ep = waitEvent(ws, 'config:dlq-changed');
    await api('PUT', `/queues/${Q}/dlq-config`, { config: { autoRetry: true } });
    const ev = await ep;
    ok((ev.data as Record<string, unknown>).queue === Q, `queue=${(ev.data as Record<string, unknown>).queue}`);
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
