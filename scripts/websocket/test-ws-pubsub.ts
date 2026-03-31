#!/usr/bin/env bun
/**
 * Test WebSocket Pub/Sub — Subscribe, unsubscribe, wildcards, queue filtering
 */

const HTTP_PORT = parseInt(process.env.HTTP_PORT ?? '26790');
const WS_URL = `ws://localhost:${HTTP_PORT}/ws`;
const API = `http://localhost:${HTTP_PORT}`;
const Q = 'ws-pubsub-test';

let passed = 0;
let failed = 0;

function ok(condition: boolean, msg: string): void {
  if (condition) { console.log(`   ✅ ${msg}`); passed++; }
  else { console.log(`   ❌ ${msg}`); failed++; }
}

async function connectWs(url = WS_URL): Promise<WebSocket> {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(url);
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
  console.log('=== Test WebSocket Pub/Sub ===\n');

  let ws: WebSocket;
  try {
    ws = await connectWs();
  } catch (e) {
    ok(false, `Connect failed: ${e}`);
    printSummary();
    return;
  }

  // 1. Subscribe to specific events
  console.log('1. Subscribe to job:pushed');
  try {
    ws.send(JSON.stringify({ cmd: 'Subscribe', events: ['job:pushed'] }));
    const resp = await waitForResponse(ws, 3000);
    ok(resp.ok === true, 'Subscribe acknowledged');
    ok(Array.isArray(resp.subscribed), `Subscribed: ${JSON.stringify(resp.subscribed)}`);
  } catch (e) { ok(false, `Subscribe failed: ${e}`); }

  // 2. Receive subscribed event
  console.log('\n2. Receive job:pushed event');
  try {
    await cleanup();
    const ep = waitEvent(ws, 'job:pushed');
    await api('POST', `/queues/${Q}/jobs`, { data: { x: 1 } });
    const ev = await ep;
    ok(ev.event === 'job:pushed', `event=${ev.event}`);
    ok((ev.data as Record<string, unknown>).queue === Q, `queue=${(ev.data as Record<string, unknown>).queue}`);
  } catch (e) { ok(false, `Receive event failed: ${e}`); }

  // 3. Don't receive unsubscribed events
  console.log('\n3. No events after unsubscribe');
  try {
    ws.send(JSON.stringify({ cmd: 'Unsubscribe', events: [] }));
    await Bun.sleep(200);
    let received = false;
    const h = (ev: MessageEvent) => {
      try {
        const p = JSON.parse(String(ev.data));
        if (p.event) received = true;
      } catch { /* */ }
    };
    ws.addEventListener('message', h);
    await api('POST', `/queues/${Q}/jobs`, { data: { x: 2 } });
    await Bun.sleep(500);
    ws.removeEventListener('message', h);
    ok(!received, 'No events after unsubscribe');
  } catch (e) { ok(false, `Unsubscribe test failed: ${e}`); }

  // 4. Wildcard job:*
  console.log('\n4. Wildcard job:* subscription');
  try {
    ws.send(JSON.stringify({ cmd: 'Subscribe', events: ['job:*'] }));
    await Bun.sleep(100);
    await cleanup();
    const ep = waitEvent(ws, 'job:pushed');
    await api('POST', `/queues/${Q}/jobs`, { data: { x: 3 } });
    await ep;
    ok(true, 'Wildcard job:* received job:pushed');
  } catch (e) { ok(false, `Wildcard failed: ${e}`); }

  // 5. Catch-all * wildcard
  console.log('\n5. Catch-all * wildcard');
  try {
    ws.send(JSON.stringify({ cmd: 'Subscribe', events: ['*'] }));
    await Bun.sleep(100);
    await cleanup();
    const ep = waitEvent(ws, 'job:pushed');
    await api('POST', `/queues/${Q}/jobs`, { data: { x: 4 } });
    await ep;
    ok(true, 'Catch-all * received job:pushed');
  } catch (e) { ok(false, `Catch-all failed: ${e}`); }

  // 6. Multiple event subscription
  console.log('\n6. Multiple event subscription');
  try {
    ws.send(JSON.stringify({ cmd: 'Unsubscribe', events: [] }));
    await Bun.sleep(100);
    ws.send(JSON.stringify({ cmd: 'Subscribe', events: ['job:pushed', 'queue:paused'] }));
    await Bun.sleep(100);
    await cleanup();

    const ep = waitEvent(ws, 'job:pushed');
    await api('POST', `/queues/${Q}/jobs`, { data: { x: 5 } });
    await ep;

    const ep2 = waitEvent(ws, 'queue:paused');
    await api('POST', `/queues/${Q}/pause`);
    await ep2;
    await api('POST', `/queues/${Q}/resume`);

    ok(true, 'Received both job:pushed and queue:paused');
  } catch (e) { ok(false, `Multi-sub failed: ${e}`); }

  // 7. Queue-filtered WebSocket
  console.log('\n7. Queue-filtered WebSocket');
  try {
    const wsFiltered = await connectWs(`ws://localhost:${HTTP_PORT}/ws/queues/${Q}`);
    wsFiltered.send(JSON.stringify({ cmd: 'Subscribe', events: ['job:*'] }));
    await Bun.sleep(100);

    // Push to filtered queue — should receive
    const ep = waitEvent(wsFiltered, 'job:pushed');
    await api('POST', `/queues/${Q}/jobs`, { data: { x: 6 } });
    await ep;
    ok(true, 'Received event for filtered queue');

    // Push to different queue — should NOT receive
    let gotOther = false;
    const h = (ev: MessageEvent) => {
      try {
        const p = JSON.parse(String(ev.data));
        if (p.event === 'job:pushed' && (p.data as Record<string, unknown>)?.queue === 'other-queue') {
          gotOther = true;
        }
      } catch { /* */ }
    };
    wsFiltered.addEventListener('message', h);
    await api('POST', '/queues/other-queue/jobs', { data: { x: 7 } });
    await Bun.sleep(500);
    wsFiltered.removeEventListener('message', h);
    ok(!gotOther, 'Did NOT receive event for other queue');

    wsFiltered.close();
    await api('POST', '/queues/other-queue/obliterate');
  } catch (e) { ok(false, `Queue filter failed: ${e}`); }

  // 8. reqId echo
  console.log('\n8. reqId is echoed back');
  try {
    ws.send(JSON.stringify({ cmd: 'Subscribe', events: ['job:*'], reqId: 'test-123' }));
    const resp = await waitForResponse(ws, 3000);
    ok(resp.reqId === 'test-123', `reqId=${resp.reqId}`);
  } catch (e) { ok(false, `reqId failed: ${e}`); }

  ws.close();
  await cleanup();
  printSummary();
}

async function waitForResponse(ws: WebSocket, timeoutMs: number): Promise<Record<string, unknown>> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      ws.removeEventListener('message', handler);
      reject(new Error('Response timeout'));
    }, timeoutMs);
    function handler(ev: MessageEvent) {
      try {
        const parsed = JSON.parse(String(ev.data));
        if (parsed.ok !== undefined) {
          clearTimeout(timer);
          ws.removeEventListener('message', handler);
          resolve(parsed);
        }
      } catch { /* */ }
    }
    ws.addEventListener('message', handler);
  });
}

function printSummary() {
  console.log(`\n=== Summary ===`);
  console.log(`Passed: ${passed}, Failed: ${failed}, Total: ${passed + failed}`);
  if (failed > 0) process.exit(1);
}

main().catch((err) => { console.error('Fatal:', err); process.exit(1); });
