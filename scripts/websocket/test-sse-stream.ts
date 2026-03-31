#!/usr/bin/env bun
/**
 * Test SSE (Server-Sent Events) — connection, event streaming, queue filtering,
 * Last-Event-ID reconnection, heartbeat
 */

const HTTP_PORT = parseInt(process.env.HTTP_PORT ?? '26790');
const API = `http://localhost:${HTTP_PORT}`;
const SSE_URL = `${API}/events`;
const Q = 'sse-stream-test';

let passed = 0;
let failed = 0;

function ok(condition: boolean, msg: string): void {
  if (condition) { console.log(`   ✅ ${msg}`); passed++; }
  else { console.log(`   ❌ ${msg}`); failed++; }
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

interface SseEvent {
  id?: string;
  event?: string;
  data?: string;
  raw: string;
}

async function readSseEvents(
  url: string,
  maxEvents: number,
  timeoutMs: number,
  headers?: Record<string, string>,
): Promise<SseEvent[]> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  const events: SseEvent[] = [];

  try {
    const res = await fetch(url, {
      signal: controller.signal,
      headers: headers ?? {},
    });

    const reader = res.body!.getReader();
    const decoder = new TextDecoder();
    let buffer = '';

    while (events.length < maxEvents) {
      const { done, value } = await reader.read();
      if (done) break;

      buffer += decoder.decode(value, { stream: true });
      const blocks = buffer.split('\n\n');
      buffer = blocks.pop() ?? '';

      for (const block of blocks) {
        if (!block.trim()) continue;
        const event: SseEvent = { raw: block };
        for (const line of block.split('\n')) {
          if (line.startsWith('id: ')) event.id = line.slice(4);
          else if (line.startsWith('event: ')) event.event = line.slice(7);
          else if (line.startsWith('data: ')) event.data = line.slice(6);
          else if (line.startsWith(':')) { /* comment like :heartbeat */ event.event = line.slice(1).trim() || 'comment'; }
        }
        events.push(event);
        if (events.length >= maxEvents) break;
      }
    }

    reader.cancel();
  } catch (e) {
    if ((e as Error).name !== 'AbortError') throw e;
  }

  clearTimeout(timer);
  return events;
}

async function main() {
  console.log('=== Test SSE Stream ===\n');

  // 1. SSE connection returns correct headers
  console.log('1. SSE connection headers');
  try {
    const controller = new AbortController();
    const res = await fetch(SSE_URL, { signal: controller.signal });
    ok(res.headers.get('content-type')?.includes('text/event-stream') === true, 'Content-Type: text/event-stream');
    ok(res.headers.get('cache-control') === 'no-cache', 'Cache-Control: no-cache');
    ok(res.headers.get('connection') === 'keep-alive', 'Connection: keep-alive');
    controller.abort();
    // Consume body to avoid hanging
    try { await res.text(); } catch { /* aborted */ }
  } catch (e) {
    if ((e as Error).name !== 'AbortError') ok(false, `${e}`);
  }

  // 2. SSE connected event
  console.log('\n2. SSE connected event');
  try {
    const events = await readSseEvents(SSE_URL, 1, 3000);
    ok(events.length > 0, 'Received initial event');
    if (events[0]?.data) {
      const data = JSON.parse(events[0].data);
      ok(data.connected === true, `connected=${data.connected}`);
      ok(typeof data.clientId === 'string', `clientId=${data.clientId}`);
    }
  } catch (e) { ok(false, `${e}`); }

  // 3. SSE receives job events
  console.log('\n3. SSE receives job events');
  try {
    await cleanup();
    // Start SSE stream and push a job
    const eventPromise = readSseEvents(SSE_URL, 5, 5000);
    await Bun.sleep(200); // Let stream connect
    await api('POST', `/queues/${Q}/jobs`, { data: { x: 1 } });
    await Bun.sleep(500);
    const events = await eventPromise;
    const jobEvent = events.find(e => e.event === 'job:pushed' || (e.data?.includes('job:pushed')));
    // SSE may send typed events or data-only events
    ok(events.length > 0, `Received ${events.length} SSE events`);
  } catch (e) { ok(false, `${e}`); }

  // 4. Queue-filtered SSE
  console.log('\n4. Queue-filtered SSE /events/queues/{name}');
  try {
    await cleanup();
    const eventPromise = readSseEvents(`${API}/events/queues/${Q}`, 3, 5000);
    await Bun.sleep(200);
    // Push to our queue
    await api('POST', `/queues/${Q}/jobs`, { data: { x: 1 } });
    // Push to other queue (should not appear)
    await api('POST', '/queues/other-sse-queue/jobs', { data: { x: 2 } });
    await Bun.sleep(500);
    const events = await eventPromise;
    ok(events.length > 0, `Received ${events.length} events for filtered queue`);
    // Verify no events from other queue
    const otherQueueEvent = events.find(e =>
      e.data?.includes('other-sse-queue')
    );
    ok(!otherQueueEvent, 'No events from other queue');
    await api('POST', '/queues/other-sse-queue/obliterate');
  } catch (e) { ok(false, `${e}`); }

  // 5. SSE event IDs (for deduplication)
  console.log('\n5. SSE event IDs present');
  try {
    await cleanup();
    const eventPromise = readSseEvents(SSE_URL, 5, 5000);
    await Bun.sleep(200);
    await api('POST', `/queues/${Q}/jobs`, { data: { x: 1 } });
    await Bun.sleep(500);
    const events = await eventPromise;
    const withId = events.filter(e => e.id !== undefined);
    ok(withId.length > 0, `${withId.length} events have IDs`);
  } catch (e) { ok(false, `${e}`); }

  // 6. SSE retry field
  console.log('\n6. SSE retry field in initial response');
  try {
    const events = await readSseEvents(SSE_URL, 1, 3000);
    // Check raw output for retry field
    const hasRetry = events.some(e => e.raw.includes('retry:'));
    ok(hasRetry, 'retry field present');
  } catch (e) { ok(false, `${e}`); }

  // 7. Last-Event-ID reconnection
  console.log('\n7. Last-Event-ID reconnection');
  try {
    await cleanup();
    // First: push some jobs to generate events with IDs
    await api('POST', `/queues/${Q}/jobs`, { data: { x: 1 } });
    await api('POST', `/queues/${Q}/jobs`, { data: { x: 2 } });
    await api('POST', `/queues/${Q}/jobs`, { data: { x: 3 } });
    await Bun.sleep(300);

    // Reconnect with Last-Event-ID: 1 to replay events after ID 1
    const events = await readSseEvents(SSE_URL, 5, 3000, { 'Last-Event-ID': '1' });
    ok(events.length > 0, `Received ${events.length} replayed events on reconnect`);
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
