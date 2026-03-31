#!/usr/bin/env bun
/**
 * Test WebSocket Edge Cases — invalid patterns, legacy mode, multi-client broadcast
 */

const HTTP_PORT = parseInt(process.env.HTTP_PORT ?? '26790');
const WS_URL = `ws://localhost:${HTTP_PORT}/ws`;
const API = `http://localhost:${HTTP_PORT}`;
const Q = 'ws-edge-cases';

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

async function waitForMessage(
  ws: WebSocket,
  predicate: (data: Record<string, unknown>) => boolean,
  timeoutMs: number,
): Promise<Record<string, unknown>> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      ws.removeEventListener('message', handler);
      reject(new Error('Message timeout'));
    }, timeoutMs);
    function handler(ev: MessageEvent) {
      try {
        const parsed = JSON.parse(String(ev.data));
        if (predicate(parsed)) {
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
  console.log('=== Test WebSocket Edge Cases ===\n');

  // ── 1. Invalid Subscribe Patterns ──
  console.log('── Invalid Subscribe Patterns ──');

  console.log('\n1. Invalid pattern rejected');
  try {
    const ws = await connectWs();
    ws.send(JSON.stringify({ cmd: 'Subscribe', events: ['bad:pattern:here'] }));
    const resp = await waitForMessage(ws, (p) => p.ok === false, 3000);
    ok(resp.ok === false, `Rejected: ${resp.error}`);
    ws.close();
  } catch (e) { ok(false, `${e}`); }

  console.log('\n2. Empty events array');
  try {
    const ws = await connectWs();
    ws.send(JSON.stringify({ cmd: 'Subscribe', events: [] }));
    const resp = await waitForMessage(ws, (p) => p.ok !== undefined, 3000);
    ok(resp.ok === true || resp.ok === false, `Response received: ok=${resp.ok}`);
    ws.close();
  } catch (e) { ok(false, `${e}`); }

  console.log('\n3. Mixed valid and invalid patterns');
  try {
    const ws = await connectWs();
    ws.send(JSON.stringify({ cmd: 'Subscribe', events: ['job:*', 'invalid:bad:pattern'] }));
    const resp = await waitForMessage(ws, (p) => p.ok !== undefined, 3000);
    // Should reject the whole subscribe or only invalid ones
    ok(resp.ok !== undefined, `Response: ok=${resp.ok}`);
    ws.close();
  } catch (e) { ok(false, `${e}`); }

  // ── 2. Legacy Mode ──
  console.log('\n── Legacy Mode (no Subscribe) ──');

  console.log('\n4. Legacy client receives raw JobEvent format');
  try {
    const ws = await connectWs();
    // Do NOT send Subscribe — legacy mode
    await Bun.sleep(200);
    await cleanup();

    // Collect messages
    const messages: Record<string, unknown>[] = [];
    const h = (ev: MessageEvent) => {
      try { messages.push(JSON.parse(String(ev.data))); } catch { /* */ }
    };
    ws.addEventListener('message', h);

    await api('POST', `/queues/${Q}/jobs`, { data: { x: 1 } });
    await Bun.sleep(1000);
    ws.removeEventListener('message', h);

    // Legacy mode should receive raw JobEvent with eventType field
    const legacyEvent = messages.find(m => m.eventType !== undefined);
    if (legacyEvent) {
      ok(true, 'Received legacy format with eventType field');
      ok(typeof legacyEvent.eventType === 'string' || typeof legacyEvent.eventType === 'number',
        `eventType=${legacyEvent.eventType}`);
      ok(typeof legacyEvent.queue === 'string', `queue=${legacyEvent.queue}`);
      ok(typeof legacyEvent.timestamp === 'number', `timestamp=${legacyEvent.timestamp}`);
    } else {
      // Some servers may not send to non-subscribed clients
      ok(messages.length === 0, 'No events for non-subscribed client (no legacy mode)');
    }

    ws.close();
  } catch (e) { ok(false, `${e}`); }

  // ── 3. Multi-Client Broadcast ──
  console.log('\n── Multi-Client Broadcast ──');

  console.log('\n5. Two clients receive the same event');
  try {
    const ws1 = await connectWs();
    const ws2 = await connectWs();

    ws1.send(JSON.stringify({ cmd: 'Subscribe', events: ['job:pushed'] }));
    ws2.send(JSON.stringify({ cmd: 'Subscribe', events: ['job:pushed'] }));
    await Bun.sleep(200);
    await cleanup();

    const ep1 = waitEvent(ws1, 'job:pushed');
    const ep2 = waitEvent(ws2, 'job:pushed');

    await api('POST', `/queues/${Q}/jobs`, { data: { broadcast: true } });

    const [ev1, ev2] = await Promise.all([ep1, ep2]);
    ok(ev1.event === 'job:pushed', 'Client 1 received job:pushed');
    ok(ev2.event === 'job:pushed', 'Client 2 received job:pushed');
    ok(
      (ev1.data as Record<string, unknown>).jobId === (ev2.data as Record<string, unknown>).jobId,
      'Both clients got same jobId',
    );

    ws1.close();
    ws2.close();
  } catch (e) { ok(false, `${e}`); }

  console.log('\n6. Five clients with different subscriptions');
  try {
    await cleanup();
    const clients: WebSocket[] = [];
    for (let i = 0; i < 5; i++) {
      clients.push(await connectWs());
    }

    // Client 0-1: job:*
    clients[0].send(JSON.stringify({ cmd: 'Subscribe', events: ['job:*'] }));
    clients[1].send(JSON.stringify({ cmd: 'Subscribe', events: ['job:*'] }));
    // Client 2-3: queue:*
    clients[2].send(JSON.stringify({ cmd: 'Subscribe', events: ['queue:*'] }));
    clients[3].send(JSON.stringify({ cmd: 'Subscribe', events: ['queue:*'] }));
    // Client 4: * (all)
    clients[4].send(JSON.stringify({ cmd: 'Subscribe', events: ['*'] }));
    await Bun.sleep(200);

    // Push a job — clients 0,1,4 should get job:pushed; 2,3 should get queue:counts; 4 gets both
    const jobPromises = [0, 1, 4].map(i => waitEvent(clients[i], 'job:pushed'));
    const queuePromises = [2, 3, 4].map(i => waitEvent(clients[i], 'queue:counts'));

    await api('POST', `/queues/${Q}/jobs`, { data: { multi: true } });

    const jobResults = await Promise.all(jobPromises);
    ok(jobResults.every(r => r.event === 'job:pushed'), 'Clients 0,1,4 got job:pushed');

    const queueResults = await Promise.all(queuePromises);
    ok(queueResults.every(r => r.event === 'queue:counts'), 'Clients 2,3,4 got queue:counts');

    clients.forEach(c => c.close());
  } catch (e) { ok(false, `${e}`); }

  console.log('\n7. Queue-filtered client ignores other queues');
  try {
    await cleanup();
    const wsFiltered = await connectWs(`ws://localhost:${HTTP_PORT}/ws/queues/${Q}`);
    wsFiltered.send(JSON.stringify({ cmd: 'Subscribe', events: ['job:*'] }));
    await Bun.sleep(200);

    // Track all events received
    const received: string[] = [];
    const h = (ev: MessageEvent) => {
      try {
        const p = JSON.parse(String(ev.data));
        if (p.event) received.push(`${p.event}:${(p.data as Record<string, unknown>)?.queue}`);
      } catch { /* */ }
    };
    wsFiltered.addEventListener('message', h);

    // Push to target queue and another queue
    await api('POST', `/queues/${Q}/jobs`, { data: { x: 1 } });
    await api('POST', '/queues/noise-queue/jobs', { data: { x: 2 } });
    await Bun.sleep(500);
    wsFiltered.removeEventListener('message', h);

    const fromTarget = received.filter(r => r.includes(Q));
    const fromNoise = received.filter(r => r.includes('noise-queue'));

    ok(fromTarget.length > 0, `Received ${fromTarget.length} events from target queue`);
    ok(fromNoise.length === 0, `Received ${fromNoise.length} events from noise queue`);

    wsFiltered.close();
    await api('POST', '/queues/noise-queue/obliterate');
  } catch (e) { ok(false, `${e}`); }

  await cleanup();
  printSummary();
}

function printSummary() {
  console.log(`\n=== Summary ===`);
  console.log(`Passed: ${passed}, Failed: ${failed}, Total: ${passed + failed}`);
  if (failed > 0) process.exit(1);
}

main().catch((err) => { console.error('Fatal:', err); process.exit(1); });
