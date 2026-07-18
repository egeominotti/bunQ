---
title: "CPU-Intensive Workers: Keep TCP Connections Alive"
description: "Heavy CPU jobs can freeze Bun's event loop, drop the TCP connection, and requeue in-flight jobs. Here is the config fix, the yield pattern, and SandboxedWorker."
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/guide/cpu-intensive-workers.png
---

<div class="bq-wrap bq-hero">
  <span class="bq-eyebrow">guide · cpu-intensive-workers</span>
  <h1 class="bq-hero-h1 bq-bench-h1">CPU-intensive workers, off the <em>event loop.</em></h1>
  <p class="bq-hero-sub">Heavy number crunching in a TCP worker can freeze the connection, drop it, and requeue every in-flight job. This page shows the fix.</p>
</div>

If your jobs do heavy synchronous computation (image processing, crypto, parsing large files) and your workers connect over TCP, read this page. It explains a failure mode you will otherwise meet in production, and three ways to avoid it.

:::note[Runtime]
The code on this page uses the Bun `bunqueue` package, and `SandboxedWorker` is built on Bun Workers, so it is Bun-only. The underlying failure mode, a blocked event loop starving connection health checks, applies to any single-threaded runtime; in other languages, prefer moving CPU-heavy work off the worker's main thread.
:::

## The quick fix

Relax the connection health checks so CPU bursts do not look like a dead connection:

```typescript
const worker = new Worker('heavy-queue', processor, {
  concurrency: 3,
  connection: {
    port: 6789,
    pingInterval: 0,        // disable the ping health check
    commandTimeout: 60000,  // allow slow replies (default 30s)
  },
  useLocks: false,          // no job lock to expire under load
  heartbeatInterval: 0,     // no heartbeat to miss
});
```

`useLocks` and `heartbeatInterval` are part of stall detection, the safety net that reclaims jobs from crashed workers. Disabling them means a genuinely crashed worker's jobs wait longer to be recovered, so only do this for queues where jobs are CPU-heavy and workers are supervised.

## Why this happens

Bun runs your code on a single **event loop**, the one thread that handles all timers and network I/O. A long synchronous computation blocks it completely, so nothing else runs:

1. The TCP client pings the server every 30 seconds as a health check. Blocked loop, no pings.
2. After 3 missed pings (about 90 seconds), the client assumes the connection is dead and reconnects, closing the socket.
3. The server sees the socket close and **requeues all jobs that worker was processing**.
4. Your worker finishes its computation and tries to report completion, and gets:

```
Error: Job not found or not in processing state
```

The job then runs again on another worker. The config above removes steps 1 and 2.

:::caution[Apply the same connection options to your Queue instances]
Each `Worker` opens its own connection pool, but `Queue` instances with the default pool size and no auth token **share** a pool keyed by host, port, pool size, token, and TLS config. The first `Queue` created fixes `pingInterval` and `commandTimeout` for every later `Queue` on the same pool. Pass one shared config everywhere:

```typescript
const tcpOpts = { port: 6789, pingInterval: 0, commandTimeout: 60000 };

const queue  = new Queue('heavy', { connection: tcpOpts });
const worker = new Worker('heavy', processor, { connection: tcpOpts });
```
:::

## Better: yield inside CPU loops

Even with pings disabled, a blocked event loop stalls progress updates, lock renewals, and every other queue on the same process. If you can, break the work up:

```typescript
// Bad: blocks the event loop for the whole run
function findNthPrime(n: number): number {
  let count = 0, candidate = 1;
  while (count < n) {
    candidate++;
    if (isPrime(candidate)) count++;
  }
  return candidate;
}

// Good: hands control back to the event loop every 500 iterations
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

`await Bun.sleep(0)` pauses for one tick, letting timers, TCP traffic, and heartbeats fire. With regular yields you can keep the default health checks on.

## Alternative: SandboxedWorker

For work that cannot yield, `SandboxedWorker` runs each job in a separate Bun Worker thread, so the main event loop never blocks:

```typescript
import { SandboxedWorker } from 'bunqueue/client';

const worker = new SandboxedWorker('heavy-queue', {
  processor: './heavy-processor.ts', // file with a default-exported async function
  concurrency: 4,
  connection: { port: 6789 },
});

await worker.start();
```

:::caution[Experimental]
`SandboxedWorker` relies on [Bun Workers](https://bun.sh/docs/runtime/workers), which are experimental. Known issues include memory growth and thread duplication across Bun versions. Prefer the yield pattern for production. See [Worker vs SandboxedWorker](/guide/worker/#worker-vs-sandboxedworker).
:::

## Timeout reference

What each default does to a CPU-bound worker:

| Setting | Default | Effect under CPU load |
|---------|---------|----------------------|
| `pingInterval` | 30000ms | 3 missed pings force a reconnect (~90s) |
| `commandTimeout` | 30000ms | Slow replies time out; 3 in a row also force a reconnect |
| `lockDuration` | 30000ms | The job's ownership lease expires if heartbeats cannot renew it |
| `stallInterval` | 30000ms | Job is marked stalled after this long without a heartbeat |

## Gotchas

- **Embedded mode is not immune.** There is no TCP connection to drop, but a blocked event loop still freezes timers, progress updates, and any HTTP server in the same process. Yield anyway.
- **Do not disable pings globally.** Tune only the heavy queue's connections; keep defaults for normal queues so dead connections are still detected quickly.

:::tip[Related]
- [Worker API](/guide/worker/) - All worker options
- [Stall Detection](/guide/stall-detection/) - How stalled jobs are recovered
- [Monitoring](/guide/monitoring/) - Watch CPU-heavy workloads in production
:::
