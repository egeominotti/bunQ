#!/usr/bin/env bun
/**
 * Test WebSocket System Periodic Events — stats:snapshot, health:status, storage:status
 */

const HTTP_PORT = parseInt(process.env.HTTP_PORT ?? '26790');
const WS_URL = `ws://localhost:${HTTP_PORT}/ws`;

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

async function waitEvent(ws: WebSocket, eventName: string, timeoutMs = 15000): Promise<Record<string, unknown>> {
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

async function main() {
  console.log('=== Test WebSocket System Periodic Events ===\n');

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

  // 1. stats:snapshot (every ~5s)
  console.log('1. stats:snapshot (waiting up to 7s)');
  try {
    const ev = await waitEvent(ws, 'stats:snapshot', 8000);
    const data = ev.data as Record<string, unknown>;
    ok(typeof data.waiting === 'number', `waiting=${data.waiting}`);
    ok(typeof data.totalPushed === 'number', `totalPushed=${data.totalPushed}`);
    ok(data.queues !== undefined, 'has queues breakdown');
  } catch (e) { ok(false, `${e}`); }

  // 2. health:status (every ~10s)
  console.log('\n2. health:status (waiting up to 12s)');
  try {
    const ev = await waitEvent(ws, 'health:status', 13000);
    const data = ev.data as Record<string, unknown>;
    ok(typeof data.ok === 'boolean', `ok=${data.ok}`);
    ok(typeof data.uptime === 'number', `uptime=${data.uptime}`);
    ok(data.memory !== undefined, 'has memory info');
  } catch (e) { ok(false, `${e}`); }

  // 3. storage:status (every ~30s) — use shorter timeout, may not fire in CI
  console.log('\n3. storage:status (waiting up to 35s)');
  try {
    const ev = await waitEvent(ws, 'storage:status', 36000);
    const data = ev.data as Record<string, unknown>;
    ok(data.collections !== undefined, 'has collections info');
  } catch (e) { ok(false, `${e}`); }

  ws.close();
  printSummary();
}

function printSummary() {
  console.log(`\n=== Summary ===`);
  console.log(`Passed: ${passed}, Failed: ${failed}, Total: ${passed + failed}`);
  if (failed > 0) process.exit(1);
}

main().catch((err) => { console.error('Fatal:', err); process.exit(1); });
