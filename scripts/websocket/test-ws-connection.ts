#!/usr/bin/env bun
/**
 * Test WebSocket Connection — connect, ping, close, max clients
 */

const HTTP_PORT = parseInt(process.env.HTTP_PORT ?? '26790');
const WS_URL = `ws://localhost:${HTTP_PORT}/ws`;

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

async function main() {
  console.log('=== Test WebSocket Connection ===\n');

  // 1. Basic connection
  console.log('1. Connect to /ws');
  let ws: WebSocket;
  try {
    ws = await connectWs();
    ok(ws.readyState === WebSocket.OPEN, 'WebSocket connected');
  } catch (e) {
    ok(false, `Connect failed: ${e}`);
    printSummary();
    return;
  }

  // 2. Ping/Pong
  console.log('\n2. Ping command');
  try {
    ws.send(JSON.stringify({ cmd: 'Ping' }));
    const resp = await waitForMessage(ws, (p) => p.ok === true, 3000);
    ok(resp.ok === true, 'Ping response ok');
  } catch (e) { ok(false, `Ping failed: ${e}`); }

  // 3. Invalid command
  console.log('\n3. Invalid command');
  try {
    ws.send(JSON.stringify({ cmd: 'InvalidCmd' }));
    const resp = await waitForMessage(ws, (p) => p.ok === false, 3000);
    ok(resp.ok === false, `Rejected invalid cmd: ${resp.error}`);
  } catch (e) { ok(false, `Invalid cmd test failed: ${e}`); }

  // 4. Malformed JSON
  console.log('\n4. Malformed JSON');
  try {
    ws.send('not valid json{{{');
    const resp = await waitForMessage(ws, (p) => p.ok === false, 3000);
    ok(resp.ok === false, 'Rejected malformed JSON');
  } catch (e) { ok(false, `Malformed JSON test failed: ${e}`); }

  // 5. Close connection
  console.log('\n5. Close connection');
  ws.close();
  await Bun.sleep(200);
  ok(ws.readyState === WebSocket.CLOSED || ws.readyState === WebSocket.CLOSING, 'WS closed');

  // 6. Reconnect after close
  console.log('\n6. Reconnect after close');
  try {
    const ws2 = await connectWs();
    ok(ws2.readyState === WebSocket.OPEN, 'Reconnected successfully');
    ws2.close();
  } catch (e) { ok(false, `Reconnect failed: ${e}`); }

  // 7. Queue-filtered connection
  console.log('\n7. Queue-filtered /ws/queues/test-queue');
  try {
    const wsQ = await connectWs(`ws://localhost:${HTTP_PORT}/ws/queues/test-queue`);
    ok(wsQ.readyState === WebSocket.OPEN, 'Queue-filtered WS connected');
    wsQ.close();
  } catch (e) { ok(false, `Queue-filtered connect failed: ${e}`); }

  // 8. Multiple concurrent connections
  console.log('\n8. Multiple concurrent connections');
  try {
    const connections: WebSocket[] = [];
    for (let i = 0; i < 10; i++) {
      connections.push(await connectWs());
    }
    ok(connections.every(c => c.readyState === WebSocket.OPEN), '10 concurrent connections open');
    connections.forEach(c => c.close());
    await Bun.sleep(100);
  } catch (e) { ok(false, `Concurrent connections failed: ${e}`); }

  printSummary();
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
      } catch { /* not JSON */ }
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
