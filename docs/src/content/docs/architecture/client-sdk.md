---
title: "bunqueue Client SDK Architecture: Connection Pooling & Worker Modes"
description: "bunqueue Client SDK internals: TCP connection pooling, embedded vs server mode, worker heartbeats, ACK batching, and auto-batching."
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og-image.png
---

<div class="bq-wrap bq-hero">
  <span class="bq-eyebrow">architecture · client sdk</span>
  <h1 class="bq-hero-h1 bq-bench-h1">Thin client, <em>smart server.</em></h1>
  <p class="bq-hero-sub">The client layer provides the interface for applications to interact with bunqueue. It supports both embedded (in-process) and TCP (server) modes.</p>
</div>

## Module Structure

```
src/client/
├── queue/          # Job submission (Queue class)
├── worker/         # Job processing (Worker class)
├── tcp/            # Network communication
├── flow.ts         # Job dependencies (FlowProducer)
└── queueGroup.ts   # Namespace isolation
```

## Dual-Mode Architecture

<div class="bq-diag">
  <div class="bq-diag-head"><b>Dual-mode architecture</b><span>one API, two transports</span></div>
  <div class="bq-diag-layer bq-diag-accent">Application <i>Queue.add(), Worker.process()</i></div>
  <div class="bq-diag-arrow">↓</div>
  <div class="bq-diag-row">
    <div class="bq-diag-cell">Embedded mode <i>direct calls to QueueManager</i></div>
    <div class="bq-diag-cell">TCP mode <i>TcpPool → Server, msgpack protocol</i></div>
  </div>
</div>

| Mode | Throughput | Use Case |
|------|------------|----------|
| Embedded | ~100k jobs/sec | Single process |
| TCP | Network limited | Distributed workers |

## Job Submission Flow

<div class="bq-diag">
  <div class="bq-diag-head"><b>Job submission</b><span>Queue.add(name, data, options)</span></div>
  <div class="bq-diag-layer">Merge options with defaults</div>
  <div class="bq-diag-arrow">↓</div>
  <div class="bq-diag-layer">Mode check</div>
  <div class="bq-diag-arrow">↓</div>
  <div class="bq-diag-row">
    <div class="bq-diag-cell">Embedded <i>direct manager.push()</i></div>
    <div class="bq-diag-cell bq-diag-accent">TCP <i>tcpPool.send({ cmd: 'PUSH', queue, data, priority, delay, ...options })</i></div>
  </div>
  <div class="bq-diag-arrow">↓</div>
  <div class="bq-diag-layer">Return Job with methods</div>
</div>

## Worker Processing Flow

<div class="bq-diag">
  <div class="bq-diag-head"><b>Worker processing loop</b><span>Worker(queue, processor, options)</span></div>
  <div class="bq-diag-layer">Start heartbeat timer <i>default: 10s interval</i></div>
  <div class="bq-diag-arrow">↓</div>
  <div class="bq-diag-layer">Poll loop <i>respects concurrency limit</i></div>
  <div class="bq-diag-arrow">↓</div>
  <div class="bq-diag-layer">Pull batch from server <i>PULLB command</i></div>
  <div class="bq-diag-arrow">↓</div>
  <div class="bq-diag-group">
    <span class="bq-diag-group-label">for each job</span>
    <div class="bq-diag-row">
      <div class="bq-diag-cell">1. Mark active</div>
      <div class="bq-diag-cell bq-diag-accent">2. Execute processor(job)</div>
      <div class="bq-diag-cell">3. On success: ACK batch</div>
      <div class="bq-diag-cell">4. On failure: FAIL</div>
    </div>
  </div>
  <p class="bq-diag-note">After each batch the worker returns to the poll loop.</p>
</div>

## Connection Pool Architecture

<div class="bq-diag">
  <div class="bq-diag-head"><b>TcpConnectionPool</b><span>4 connections per pool, default</span></div>
  <div class="bq-diag-row">
    <div class="bq-diag-cell">Client 1 <i>socket, parser, health</i></div>
    <div class="bq-diag-cell">Client 2 <i>socket, parser, health</i></div>
    <div class="bq-diag-cell">Client 3 <i>socket, parser, health</i></div>
    <div class="bq-diag-cell">Client 4 <i>socket, parser, health</i></div>
  </div>
  <div class="bq-diag-layer bq-diag-accent">Round-robin selection, health tracking, auto-reconnect, shared pool management</div>
</div>

**Key Features:**
- 4 connections per pool (default)
- Load-aware client selection
- Automatic reconnection with exponential backoff
- Shared pools across Queue/Worker instances

## Heartbeat & Stall Detection

<div class="bq-diag">
  <div class="bq-diag-head"><b>Heartbeat and stall detection</b><span>worker to server</span></div>
  <div class="bq-diag-group">
    <span class="bq-diag-group-label">worker</span>
    <div class="bq-diag-layer">heartbeatTimer <i>every 10s sends JobHeartbeatB { ids, tokens } to the server</i></div>
    <div class="bq-diag-row">
      <div class="bq-diag-cell">pulledJobIds <i>all pulled jobs get heartbeat</i></div>
      <div class="bq-diag-cell">activeJobIds <i>jobs being processed</i></div>
      <div class="bq-diag-cell">jobTokens <i>lock tokens for verification</i></div>
    </div>
  </div>
  <div class="bq-diag-arrow">↓</div>
  <div class="bq-diag-group">
    <span class="bq-diag-group-label">server</span>
    <div class="bq-diag-layer bq-diag-accent">No heartbeat for stallInterval (30s): 1. mark job as stalled, 2. increment stallCount, 3. after maxStalls (3) move to DLQ</div>
  </div>
</div>

## ACK Batching Flow

<div class="bq-diag">
  <div class="bq-diag-head"><b>ACK batching</b><span>job completes</span></div>
  <div class="bq-diag-layer">AckBatcher.queue(id, result)</div>
  <div class="bq-diag-arrow">↓</div>
  <div class="bq-diag-layer">Buffer pending ACKs <i>max 10 or 50ms timeout</i></div>
  <div class="bq-diag-arrow">↓</div>
  <div class="bq-diag-row">
    <div class="bq-diag-cell">Batch full</div>
    <div class="bq-diag-cell">Timer fires</div>
  </div>
  <div class="bq-diag-arrow">↓</div>
  <div class="bq-diag-layer bq-diag-accent">Send ACKB { ids, results, tokens }</div>
</div>

**Benefits:**
- Reduces network round-trips
- Batches lock verification
- Handles retry on failure

## FlowProducer (Dependencies)

<div class="bq-diag">
  <div class="bq-diag-head"><b>Dependency chain</b><span>addChain([A, B, C])</span></div>
  <div class="bq-diag-flow">
    <div class="bq-diag-cell">A <i>no dependencies, queued</i></div>
    <div class="bq-diag-arrow">→</div>
    <div class="bq-diag-cell">B <i>dependsOn: [A]</i></div>
    <div class="bq-diag-arrow">→</div>
    <div class="bq-diag-cell">C <i>dependsOn: [B]</i></div>
  </div>
  <div class="bq-diag-arrow">↓</div>
  <div class="bq-diag-layer bq-diag-accent">Server tracks in waitingDeps until dependencies complete</div>
</div>

## Graceful Shutdown

<div class="bq-diag">
  <div class="bq-diag-head"><b>Graceful shutdown</b><span>worker.close()</span></div>
  <div class="bq-diag-layer">1. Stop poll loop</div>
  <div class="bq-diag-arrow">↓</div>
  <div class="bq-diag-layer">2. Stop heartbeat</div>
  <div class="bq-diag-arrow">↓</div>
  <div class="bq-diag-layer bq-diag-accent">3. Wait active jobs finish</div>
  <div class="bq-diag-arrow">↓</div>
  <div class="bq-diag-layer">4. Flush pending ACKs</div>
  <div class="bq-diag-arrow">↓</div>
  <div class="bq-diag-layer">5. Wait in-flight flushes</div>
  <div class="bq-diag-arrow">↓</div>
  <div class="bq-diag-layer">6. Close TCP connections</div>
</div>
