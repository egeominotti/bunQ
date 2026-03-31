#!/usr/bin/env bun
/**
 * Test WebSocket Pub/Sub — All Dashboard Events
 *
 * Tests every event category: job lifecycle, queue, DLQ, cron, worker,
 * rate-limit, concurrency, webhook, system health, config changes.
 * Uses HTTP API to trigger events that SDK can't reach directly.
 */

const HTTP_PORT = parseInt(process.env.HTTP_PORT ?? '16790');
const WS_URL = `ws://localhost:${HTTP_PORT}/ws`;
const API = `http://localhost:${HTTP_PORT}`;
const Q = 'ws-test-events';

let passed = 0;
let failed = 0;

function ok(condition: boolean, msg: string): void {
  if (condition) {
    console.log(`   ✅ ${msg}`);
    passed++;
  } else {
    console.log(`   ❌ ${msg}`);
    failed++;
  }
}

// ── Helpers ─────────────────────────────────────────────

type WsEvent = { event: string; ts: number; data: Record<string, unknown> };

async function waitEvent(ws: WebSocket, eventName: string, timeoutMs = 5000): Promise<WsEvent> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      ws.removeEventListener('message', handler);
      reject(new Error(`Timeout waiting for ${eventName}`));
    }, timeoutMs);
    function handler(ev: MessageEvent) {
      try {
        const parsed = JSON.parse(String(ev.data)) as WsEvent;
        if (parsed.event === eventName) {
          clearTimeout(timer);
          ws.removeEventListener('message', handler);
          resolve(parsed);
        }
      } catch {
        /* not JSON */
      }
    }
    ws.addEventListener('message', handler);
  });
}

async function connectWs(): Promise<WebSocket> {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(WS_URL);
    const t = setTimeout(() => reject(new Error('WS connect timeout')), 5000);
    ws.addEventListener('open', () => { clearTimeout(t); resolve(ws); });
    ws.addEventListener('error', (e) => { clearTimeout(t); reject(e); });
  });
}

async function subscribe(ws: WebSocket, events: string[]): Promise<void> {
  ws.send(JSON.stringify({ cmd: 'Subscribe', events }));
  await Bun.sleep(100);
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

// ── Tests ───────────────────────────────────────────────

async function main() {
  console.log('=== Test WebSocket Pub/Sub — All Events ===\n');

  let ws: WebSocket;
  try {
    ws = await connectWs();
    ok(ws.readyState === WebSocket.OPEN, 'WebSocket connected');
  } catch (e) {
    console.log(`   ❌ WS connect failed: ${e}`);
    failed++;
    printSummary();
    return;
  }

  await subscribe(ws, ['*']);

  // ── 1. Job Lifecycle ─────────────────────────────────
  console.log('\n── Job Lifecycle Events ──');

  // 1.1 job:pushed
  console.log('\n1. job:pushed');
  try {
    await cleanup();
    const ep = waitEvent(ws, 'job:pushed');
    await pushJob();
    const ev = await ep;
    ok(ev.data.queue === Q, `queue=${ev.data.queue}`);
    ok(typeof ev.data.jobId === 'string', `jobId=${ev.data.jobId}`);
    ok(typeof ev.ts === 'number', `ts=${ev.ts}`);
  } catch (e) { console.log(`   ❌ ${e}`); failed++; }

  // 1.2 job:active
  console.log('\n2. job:active');
  try {
    await cleanup();
    await pushJob();
    const ep = waitEvent(ws, 'job:active');
    await pullJob();
    const ev = await ep;
    ok(ev.data.queue === Q, `queue=${ev.data.queue}`);
  } catch (e) { console.log(`   ❌ ${e}`); failed++; }

  // 1.3 job:completed
  console.log('\n3. job:completed');
  try {
    await cleanup();
    const id = await pushJob();
    await pullJob(); // make active
    const ep = waitEvent(ws, 'job:completed');
    await api('POST', `/jobs/${id}/ack`, { result: { done: true } });
    const ev = await ep;
    ok(ev.data.queue === Q, `queue=${ev.data.queue}`);
  } catch (e) { console.log(`   ❌ ${e}`); failed++; }

  // 1.4 job:failed
  console.log('\n4. job:failed');
  try {
    await cleanup();
    const id = await pushJob({ x: 1 }, { maxAttempts: 1 });
    await pullJob();
    const ep = waitEvent(ws, 'job:failed');
    await api('POST', `/jobs/${id}/fail`, { error: 'test failure' });
    const ev = await ep;
    ok(ev.data.queue === Q, `queue=${ev.data.queue}`);
    ok(typeof ev.data.error === 'string', `error=${ev.data.error}`);
  } catch (e) { console.log(`   ❌ ${e}`); failed++; }

  // 1.5 job:removed
  console.log('\n5. job:removed');
  try {
    await cleanup();
    const id = await pushJob();
    const ep = waitEvent(ws, 'job:removed');
    await api('DELETE', `/jobs/${id}`);
    const ev = await ep;
    ok(ev.data.jobId === id, `jobId=${ev.data.jobId}`);
  } catch (e) { console.log(`   ❌ ${e}`); failed++; }

  // 1.6 job:promoted
  console.log('\n6. job:promoted');
  try {
    await cleanup();
    const id = await pushJob({ x: 1 }, { delay: 60000 });
    const ep = waitEvent(ws, 'job:promoted');
    await api('POST', `/jobs/${id}/promote`);
    const ev = await ep;
    ok(typeof ev.data.jobId === 'string', `jobId present`);
  } catch (e) { console.log(`   ❌ ${e}`); failed++; }

  // 1.7 job:progress
  console.log('\n7. job:progress');
  try {
    await cleanup();
    const id = await pushJob();
    await pullJob();
    const ep = waitEvent(ws, 'job:progress');
    await api('POST', `/jobs/${id}/progress`, { progress: 50 });
    const ev = await ep;
    ok(ev.data.progress !== undefined, `progress=${ev.data.progress}`);
  } catch (e) { console.log(`   ❌ ${e}`); failed++; }

  // 1.8 job:discarded
  console.log('\n8. job:discarded');
  try {
    await cleanup();
    const id = await pushJob();
    const ep = waitEvent(ws, 'job:discarded');
    await api('POST', `/jobs/${id}/discard`);
    const ev = await ep;
    ok(typeof ev.data.jobId === 'string', `jobId present`);
  } catch (e) { console.log(`   ❌ ${e}`); failed++; }

  // 1.9 job:priority-changed
  console.log('\n9. job:priority-changed');
  try {
    await cleanup();
    const id = await pushJob();
    const ep = waitEvent(ws, 'job:priority-changed');
    await api('PUT', `/jobs/${id}/priority`, { priority: 99 });
    const ev = await ep;
    ok(typeof ev.data.jobId === 'string', `jobId present`);
  } catch (e) { console.log(`   ❌ ${e}`); failed++; }

  // 1.10 job:data-updated
  console.log('\n10. job:data-updated');
  try {
    await cleanup();
    const id = await pushJob();
    const ep = waitEvent(ws, 'job:data-updated');
    await api('PUT', `/jobs/${id}/data`, { data: { updated: true } });
    const ev = await ep;
    ok(typeof ev.data.jobId === 'string', `jobId present`);
  } catch (e) { console.log(`   ❌ ${e}`); failed++; }

  // 1.11 job:delay-changed
  console.log('\n11. job:delay-changed');
  try {
    await cleanup();
    const id = await pushJob();
    await pullJob(); // make active
    const ep = waitEvent(ws, 'job:delay-changed');
    await api('PUT', `/jobs/${id}/delay`, { delay: 5000 });
    const ev = await ep;
    ok(typeof ev.data.jobId === 'string', `jobId present`);
  } catch (e) { console.log(`   ❌ ${e}`); failed++; }

  // 1.12 queue:counts (auto-emitted on state change)
  console.log('\n12. queue:counts');
  try {
    await cleanup();
    const ep = waitEvent(ws, 'queue:counts');
    await pushJob();
    const ev = await ep;
    ok(ev.data.queue === Q, `queue=${ev.data.queue}`);
    ok(typeof ev.data.waiting === 'number', `waiting=${ev.data.waiting}`);
  } catch (e) { console.log(`   ❌ ${e}`); failed++; }

  // ── 2. Queue Events ──────────────────────────────────
  console.log('\n── Queue Events ──');

  // 2.1 queue:paused
  console.log('\n13. queue:paused');
  try {
    await cleanup();
    await pushJob();
    const ep = waitEvent(ws, 'queue:paused');
    await api('POST', `/queues/${Q}/pause`);
    const ev = await ep;
    ok(ev.data.queue === Q, `queue=${ev.data.queue}`);
  } catch (e) { console.log(`   ❌ ${e}`); failed++; }

  // 2.2 queue:resumed
  console.log('\n14. queue:resumed');
  try {
    const ep = waitEvent(ws, 'queue:resumed');
    await api('POST', `/queues/${Q}/resume`);
    const ev = await ep;
    ok(ev.data.queue === Q, `queue=${ev.data.queue}`);
  } catch (e) { console.log(`   ❌ ${e}`); failed++; }

  // 2.3 queue:drained
  console.log('\n15. queue:drained');
  try {
    await cleanup();
    await pushJob();
    const ep = waitEvent(ws, 'queue:drained');
    await api('POST', `/queues/${Q}/drain`);
    const ev = await ep;
    ok(ev.data.queue === Q, `queue=${ev.data.queue}`);
  } catch (e) { console.log(`   ❌ ${e}`); failed++; }

  // 2.4 queue:cleaned (clean waiting jobs — completed don't persist in memory-only mode)
  console.log('\n16. queue:cleaned');
  try {
    await cleanup();
    // Push multiple waiting jobs, then clean them
    await pushJob(); await pushJob(); await pushJob();
    await Bun.sleep(100);
    const ep = waitEvent(ws, 'queue:cleaned');
    await api('POST', `/queues/${Q}/clean`, { grace: 0, state: 'waiting' });
    const ev = await ep;
    ok(ev.data.queue === Q, `queue=${ev.data.queue}`);
  } catch (e) { console.log(`   ❌ ${e}`); failed++; }

  // 2.5 queue:obliterated
  console.log('\n17. queue:obliterated');
  try {
    await pushJob();
    const ep = waitEvent(ws, 'queue:obliterated');
    await api('POST', `/queues/${Q}/obliterate`);
    const ev = await ep;
    ok(ev.data.queue === Q, `queue=${ev.data.queue}`);
  } catch (e) { console.log(`   ❌ ${e}`); failed++; }

  // ── 3. DLQ Events ────────────────────────────────────
  console.log('\n── DLQ Events ──');

  // 3.1 dlq:added
  console.log('\n18. dlq:added');
  try {
    await cleanup();
    // Push with maxAttempts=1, pull, fail → goes to DLQ
    const id = await pushJob({ x: 1 }, { maxAttempts: 1 });
    await pullJob();
    const ep = waitEvent(ws, 'dlq:added');
    await api('POST', `/jobs/${id}/fail`, { error: 'dlq test' });
    const ev = await ep;
    ok(ev.data.queue === Q, `queue=${ev.data.queue}`);
  } catch (e) { console.log(`   ❌ ${e}`); failed++; }

  // 3.2 dlq:retried (retry-all emits dlq:retry-all when count > 0)
  console.log('\n19. dlq:retry-all');
  try {
    // Create fresh DLQ entry to retry
    await cleanup();
    const rid = await pushJob({ x: 1 }, { maxAttempts: 1 });
    await pullJob();
    await api('POST', `/jobs/${rid}/fail`, { error: 'retry test' });
    await Bun.sleep(200);
    const ep = waitEvent(ws, 'dlq:retry-all');
    await api('POST', `/queues/${Q}/dlq/retry`);
    const ev = await ep;
    ok(ev.data.queue === Q, `queue=${ev.data.queue}`);
  } catch (e) { console.log(`   ❌ ${e}`); failed++; }

  // 3.3 dlq:purged
  console.log('\n20. dlq:purged');
  try {
    // Create another DLQ entry
    await cleanup();
    const id2 = await pushJob({ x: 1 }, { maxAttempts: 1 });
    await pullJob();
    await api('POST', `/jobs/${id2}/fail`, { error: 'purge test' });
    await Bun.sleep(200);
    const ep = waitEvent(ws, 'dlq:purged');
    await api('POST', `/queues/${Q}/dlq/purge`);
    const ev = await ep;
    ok(ev.data.queue === Q, `queue=${ev.data.queue}`);
  } catch (e) { console.log(`   ❌ ${e}`); failed++; }

  // ── 4. Cron Events ───────────────────────────────────
  console.log('\n── Cron Events ──');

  // 4.1 cron:created
  console.log('\n21. cron:created');
  try {
    const ep = waitEvent(ws, 'cron:created');
    await api('POST', '/crons', { name: 'ws-cron', queue: Q, data: { t: 1 }, repeatEvery: 60000 });
    const ev = await ep;
    ok(ev.data.name === 'ws-cron', `name=${ev.data.name}`);
  } catch (e) { console.log(`   ❌ ${e}`); failed++; }

  // 4.2 cron:deleted
  console.log('\n22. cron:deleted');
  try {
    const ep = waitEvent(ws, 'cron:deleted');
    await api('DELETE', '/crons/ws-cron');
    const ev = await ep;
    ok(ev.data.name === 'ws-cron', `name=${ev.data.name}`);
  } catch (e) { console.log(`   ❌ ${e}`); failed++; }

  // ── 5. Worker Events ─────────────────────────────────
  console.log('\n── Worker Events ──');

  // 5.1 worker:connected
  console.log('\n23. worker:connected');
  try {
    const ep = waitEvent(ws, 'worker:connected');
    await api('POST', '/workers', { name: 'ws-worker', queues: [Q] });
    const ev = await ep;
    ok(typeof ev.data.workerId === 'string', `workerId=${ev.data.workerId}`);
  } catch (e) { console.log(`   ❌ ${e}`); failed++; }

  // 5.2 worker:disconnected
  console.log('\n24. worker:disconnected');
  try {
    const res = await api('GET', '/workers');
    const data = res.data as { workers: { id: string }[] } | undefined;
    const wId = data?.workers?.[0]?.id;
    if (wId) {
      const ep = waitEvent(ws, 'worker:disconnected');
      await api('DELETE', `/workers/${wId}`);
      const ev = await ep;
      ok(typeof ev.data.workerId === 'string', `workerId=${ev.data.workerId}`);
    } else {
      ok(false, 'No worker found');
    }
  } catch (e) { console.log(`   ❌ ${e}`); failed++; }

  // ── 6. Rate Limit & Concurrency ──────────────────────
  console.log('\n── Rate Limit & Concurrency ──');

  console.log('\n25. ratelimit:set');
  try {
    const ep = waitEvent(ws, 'ratelimit:set');
    await api('PUT', `/queues/${Q}/rate-limit`, { limit: 100 });
    const ev = await ep;
    ok(ev.data.queue === Q, `queue=${ev.data.queue}`);
  } catch (e) { console.log(`   ❌ ${e}`); failed++; }

  console.log('\n26. ratelimit:cleared');
  try {
    const ep = waitEvent(ws, 'ratelimit:cleared');
    await api('DELETE', `/queues/${Q}/rate-limit`);
    const ev = await ep;
    ok(ev.data.queue === Q, `queue=${ev.data.queue}`);
  } catch (e) { console.log(`   ❌ ${e}`); failed++; }

  console.log('\n27. concurrency:set');
  try {
    const ep = waitEvent(ws, 'concurrency:set');
    await api('PUT', `/queues/${Q}/concurrency`, { concurrency: 5 });
    const ev = await ep;
    ok(ev.data.queue === Q, `queue=${ev.data.queue}`);
  } catch (e) { console.log(`   ❌ ${e}`); failed++; }

  console.log('\n28. concurrency:cleared');
  try {
    const ep = waitEvent(ws, 'concurrency:cleared');
    await api('DELETE', `/queues/${Q}/concurrency`);
    const ev = await ep;
    ok(ev.data.queue === Q, `queue=${ev.data.queue}`);
  } catch (e) { console.log(`   ❌ ${e}`); failed++; }

  // ── 7. Webhook Events ────────────────────────────────
  console.log('\n── Webhook Events ──');

  console.log('\n29. webhook:added');
  let webhookId = '';
  try {
    const ep = waitEvent(ws, 'webhook:added');
    const res = await api('POST', '/webhooks', { url: 'https://example.com/hook', events: ['job.completed'] });
    const data = res.data as { webhookId: string } | undefined;
    webhookId = data?.webhookId ?? '';
    const ev = await ep;
    ok(typeof ev.data.id === 'string', `id=${ev.data.id}`);
  } catch (e) { console.log(`   ❌ ${e}`); failed++; }

  console.log('\n30. webhook:removed');
  try {
    if (webhookId) {
      const ep = waitEvent(ws, 'webhook:removed');
      await api('DELETE', `/webhooks/${webhookId}`);
      const ev = await ep;
      ok(typeof ev.data.id === 'string', `id=${ev.data.id}`);
    } else {
      ok(false, 'No webhook to remove');
    }
  } catch (e) { console.log(`   ❌ ${e}`); failed++; }

  // ── 8. Config Changes ────────────────────────────────
  console.log('\n── Config Events ──');

  console.log('\n31. config:stall-changed');
  try {
    const ep = waitEvent(ws, 'config:stall-changed');
    await api('PUT', `/queues/${Q}/stall-config`, { config: { enabled: true, stallInterval: 20000 } });
    const ev = await ep;
    ok(ev.data.queue === Q, `queue=${ev.data.queue}`);
  } catch (e) { console.log(`   ❌ ${e}`); failed++; }

  console.log('\n32. config:dlq-changed');
  try {
    const ep = waitEvent(ws, 'config:dlq-changed');
    await api('PUT', `/queues/${Q}/dlq-config`, { config: { autoRetry: true } });
    const ev = await ep;
    ok(ev.data.queue === Q, `queue=${ev.data.queue}`);
  } catch (e) { console.log(`   ❌ ${e}`); failed++; }

  // ── 9. System Periodic ───────────────────────────────
  console.log('\n── System Periodic Events ──');

  console.log('\n33. stats:snapshot (≤6s)');
  try {
    const ev = await waitEvent(ws, 'stats:snapshot', 7000);
    ok(typeof ev.data.waiting === 'number', `waiting=${ev.data.waiting}`);
    ok(typeof ev.data.totalPushed === 'number', `totalPushed=${ev.data.totalPushed}`);
    ok(ev.data.queues !== undefined, 'has queues breakdown');
  } catch (e) { console.log(`   ❌ ${e}`); failed++; }

  console.log('\n34. health:status (≤11s)');
  try {
    const ev = await waitEvent(ws, 'health:status', 12000);
    ok(typeof ev.data.ok === 'boolean', `ok=${ev.data.ok}`);
    ok(typeof ev.data.uptime === 'number', `uptime=${ev.data.uptime}`);
  } catch (e) { console.log(`   ❌ ${e}`); failed++; }

  // ── 10. Protocol Tests ───────────────────────────────
  console.log('\n── Protocol Tests ──');

  console.log('\n35. Unsubscribe all → no events');
  try {
    ws.send(JSON.stringify({ cmd: 'Unsubscribe', events: [] }));
    await Bun.sleep(200);
    let received = false;
    const h = (ev: MessageEvent) => {
      try { const p = JSON.parse(String(ev.data)) as { event?: string }; if (p.event) received = true; } catch { /* */ }
    };
    ws.addEventListener('message', h);
    await pushJob();
    await Bun.sleep(500);
    ws.removeEventListener('message', h);
    ok(!received, 'No events after unsubscribe all');
  } catch (e) { console.log(`   ❌ ${e}`); failed++; }

  console.log('\n36. Wildcard job:*');
  try {
    await subscribe(ws, ['job:*']);
    const ep = waitEvent(ws, 'job:pushed');
    await pushJob();
    await ep;
    ok(true, 'Wildcard received job:pushed');
  } catch (e) { console.log(`   ❌ ${e}`); failed++; }

  console.log('\n37. Invalid pattern rejected');
  try {
    const rp = new Promise<Record<string, unknown>>((resolve) => {
      function h(ev: MessageEvent) {
        try { const p = JSON.parse(String(ev.data)) as Record<string, unknown>; if (p.ok === false) { ws.removeEventListener('message', h); resolve(p); } } catch { /* */ }
      }
      ws.addEventListener('message', h);
    });
    ws.send(JSON.stringify({ cmd: 'Subscribe', events: ['bad:pattern:here'] }));
    const resp = await rp;
    ok(resp.ok === false, `Rejected: ${resp.error}`);
  } catch (e) { console.log(`   ❌ ${e}`); failed++; }

  console.log('\n38. Ping over WebSocket');
  try {
    ws.send(JSON.stringify({ cmd: 'Ping' }));
    const pong = await new Promise<Record<string, unknown>>((resolve, reject) => {
      const t = setTimeout(() => reject(new Error('Ping timeout')), 3000);
      function h(ev: MessageEvent) {
        try { const p = JSON.parse(String(ev.data)) as Record<string, unknown>; if (p.ok === true) { clearTimeout(t); ws.removeEventListener('message', h); resolve(p); } } catch { /* */ }
      }
      ws.addEventListener('message', h);
    });
    ok(pong.ok === true, 'Ping ok');
  } catch (e) { console.log(`   ❌ ${e}`); failed++; }

  console.log('\n39. WebSocket close');
  ws.close();
  await Bun.sleep(200);
  ok(ws.readyState === WebSocket.CLOSED || ws.readyState === WebSocket.CLOSING, 'WS closed');

  await cleanup();
  printSummary();
}

function printSummary() {
  console.log(`\n${'─'.repeat(50)}`);
  console.log(`Results: ${passed} passed, ${failed} failed, ${passed + failed} total`);
  if (failed > 0) process.exit(1);
}

main().catch((err) => { console.error('Fatal error:', err); process.exit(1); });
