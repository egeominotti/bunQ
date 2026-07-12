---
title: "CPU-Intensive Workers over TCP in Bunqueue"
description: Run CPU-heavy Bun jobs over TCP without dropping connections. Configure ping intervals, command timeouts, and yield patterns for reliability.
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/guide/cpu-intensive-workers.png
---

<div class="bq-wrap bq-hero">
  <span class="bq-eyebrow">guide · cpu-intensive-workers</span>
  <h1 class="bq-hero-h1 bq-bench-h1">CPU-intensive workers, off the <em>event loop.</em></h1>
  <p class="bq-hero-sub">Synchronous CPU-heavy work over TCP blocks the event loop, drops the connection, and requeues every in-flight job. This page explains why it happens and how to configure workers to avoid it.</p>
</div>

## The Problem

The TCP client sends periodic ping health checks (default: every 30s). Under heavy CPU load, the event loop can't process these pings in time. After 3 consecutive failures (~90s), the client triggers a forced reconnect, which closes the socket. When the server detects the socket close, it calls `releaseClientJobs()` and **requeues all processing jobs**. The worker then fails to ACK completed jobs with:

```
Error: Job not found or not in processing state
```

## Connection Options

Disable the ping health check and increase the command timeout:

```typescript
const worker = new Worker('heavy-queue', processor, {
  concurrency: 3,
  connection: {
    port: 6789,
    pingInterval: 0,        // Disable ping health check
    commandTimeout: 60000,  // Increase command timeout to 60s
  },
  useLocks: false,          // Avoid lock expiration under load
  heartbeatInterval: 0,     // Disable heartbeat
});
```

:::caution[Apply the same options to Queue]
Each `Worker` opens its own connection pool from its `connection` options, but `Queue` instances with the default `poolSize` and no token **share** a pool keyed by host, port, pool size, token, and TLS config. The **first** Queue created fixes tuning options like `pingInterval` and `commandTimeout` for every later Queue that maps to the same pool. Pass the same connection config everywhere so all connections get the tuned settings:

```typescript
const tcpOpts = { port: 6789, pingInterval: 0, commandTimeout: 60000 };

const queue  = new Queue('heavy', { connection: tcpOpts });
const worker = new Worker('heavy', processor, { connection: tcpOpts });
```
:::

## Non-Blocking CPU Work

Even with pings disabled, long synchronous CPU work blocks heartbeats, lock renewals, and TCP responses. Break up CPU-heavy loops with periodic yields:

```typescript
// Bad: blocks event loop for entire duration
function findNthPrime(n: number): number {
  let count = 0, candidate = 1;
  while (count < n) {
    candidate++;
    if (isPrime(candidate)) count++;
  }
  return candidate;
}

// Good: yields every 500 iterations
async function findNthPrime(n: number): Promise<number> {
  let count = 0, candidate = 1, ops = 0;
  while (count < n) {
    candidate++;
    if (isPrime(candidate)) count++;
    if (++ops % 500 === 0) await Bun.sleep(0);
  }
  return candidate;
}
```

`await Bun.sleep(0)` yields to the event loop for one tick, allowing timers, TCP I/O, and heartbeats to fire.

## Default Timeouts Reference

| Setting | Default | Effect under CPU load |
|---------|---------|----------------------|
| `pingInterval` | 30000ms | 3 consecutive failures → forced reconnect (~90s) |
| `commandTimeout` | 30000ms | Long-running commands timeout (3 consecutive timeouts also force a reconnect) |
| `lockDuration` | 30000ms | Job lock expires if heartbeats cannot renew it in time |
| `stallInterval` | 30000ms | Job marked stalled if no heartbeat |

## Alternative: SandboxedWorker

:::caution[Experimental, Bun Workers are not yet stable]
`SandboxedWorker` relies on [Bun Workers](https://bun.sh/docs/runtime/workers), which are **experimental**. Known issues include memory growth and thread duplication across Bun versions. For production, prefer the yield-based approach above or use the standard `Worker`. See [Worker vs SandboxedWorker](/guide/worker/#worker-vs-sandboxedworker) for details.
:::

For truly CPU-bound work where yielding is not practical, [`SandboxedWorker`](/guide/worker/#sandboxedworker) runs each job in an isolated Bun Worker thread, so the main event loop is never blocked:

```typescript
import { SandboxedWorker } from 'bunqueue/client';

const worker = new SandboxedWorker('heavy-queue', {
  processor: './heavy-processor.ts',
  concurrency: 4,
  connection: { port: 6789 },
});

await worker.start();
```

:::tip[Related Guides]
- [Worker API](/guide/worker/) - Full worker configuration options
- [Stall Detection & Recovery](/guide/stall-detection/) - Handle stalled workers
- [Monitoring & Prometheus Metrics](/guide/monitoring/) - Monitor CPU-heavy workloads
:::
