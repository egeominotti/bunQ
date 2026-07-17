---
title: "Stall Detection: Auto-Recover Unresponsive Jobs"
description: "bunqueue stall detection auto-recovers stuck jobs. It is on by default; tune heartbeat intervals, max stall thresholds, and grace periods for long jobs."
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/guide/stall-detection.png
---

<div class="bq-wrap bq-hero">
  <span class="bq-eyebrow">guide · stall-detection</span>
  <h1 class="bq-hero-h1 bq-bench-h1">Stuck jobs <em>come back.</em></h1>
  <p class="bq-hero-sub">If a worker crashes or hangs mid-job, the job is not lost. bunqueue notices the silence, retries the job, and parks repeat offenders in the dead letter queue.</p>

  <div class="bq-proof">
    <span><b>30s</b> without a heartbeat marks a job stalled</span>
    <span><b>3</b> stalls before a job moves to the DLQ</span>
    <span><b>5s</b> grace period after a job starts</span>
  </div>
</div>

While a worker processes a job it sends periodic **heartbeats**, small "I'm still alive" signals. If heartbeats stop (crashed process, hung code, dead network), the job is **stalled**: bunqueue re-queues it for another worker, and after too many stalls moves it to the [dead letter queue](/guide/dlq/) (DLQ), the holding area for jobs that keep failing.

**Stall detection is on by default with sensible defaults.** You only need this page if your jobs run longer than 30 seconds, or you want to tune the thresholds.

## Configuration

```typescript
import { Queue } from 'bunqueue/client';

const queue = new Queue('my-queue', { embedded: true });

queue.setStallConfig({
  enabled: true,         // on by default
  stallInterval: 30000,  // stalled after 30s without a heartbeat
  maxStalls: 3,          // move to DLQ after 3 stalls
  gracePeriod: 5000,     // no stall checks in the first 5s of a job
});
```

| Option | Default | Description |
|--------|---------|-------------|
| `enabled` | `true` | Enable/disable stall detection |
| `stallInterval` | `30000` | Time (ms) without a heartbeat before a job is stalled |
| `maxStalls` | `3` | Max stalls before moving to DLQ |
| `gracePeriod` | `5000` | Initial grace period (ms) after a job starts |

On the worker side, heartbeats are automatic:

```typescript
const worker = new Worker('queue', processor, {
  heartbeatInterval: 10000, // heartbeat every 10 seconds (default)
});
```

Keep `heartbeatInterval` well below `stallInterval`, otherwise healthy jobs get flagged as stalled.

With SQLite persistence enabled, a custom stall policy and every job's
cumulative stall count survive process restarts. A crash consumes one
`attempts` slot and one `stallCount` slot; reaching either `maxAttempts` or
`maxStalls` is terminal and moves the job to the DLQ. Repeated crashes therefore
cannot reset either retry budget.

## Long-running jobs

Jobs longer than 30 seconds need a wider stall window, or they will be re-queued while still running:

```typescript
// Video processing may take hours
const videoQueue = new Queue('video-processing', { embedded: true });

videoQueue.setStallConfig({
  stallInterval: 300000,  // 5 minutes
  maxStalls: 2,
  gracePeriod: 60000,
});

const worker = new Worker('video-processing', async (job) => {
  for (const chunk of video.chunks) {
    await processChunk(chunk);
    await job.updateProgress(chunk.progress); // also counts as a heartbeat
  }
}, { embedded: true, heartbeatInterval: 30000 });
```

Two things reset the stall timer: the worker's automatic heartbeat (every `heartbeatInterval` ms) and any `job.updateProgress()` call. For long jobs without natural progress points, the automatic heartbeat is enough.

## What happens when a job stalls

1. **Retry**: the job goes back to waiting with its stall count incremented, and waiting workers are notified immediately, so it is picked up without delay.
2. **DLQ**: once the stall count exceeds `maxStalls`, the job moves to the dead letter queue with reason `stalled`.

## Listening for stalls

In embedded mode (queue and listener in the same process), use `QueueEvents`:

```typescript
import { QueueEvents } from 'bunqueue/client';

const events = new QueueEvents('my-queue');
events.on('stalled', ({ jobId }) => {
  console.log(`Job ${jobId} stalled`);
});
```

In TCP mode, use the Worker's `stalled` event or subscribe via [webhooks](/guide/webhooks/).

## Monitoring

```typescript
const stats = queue.getDlqStats();
console.log('Stalled jobs in DLQ:', stats.byReason.stalled);

const stalledJobs = queue.getDlq({ reason: 'stalled' });
```

## SandboxedWorker

:::caution[Experimental]
`SandboxedWorker` depends on experimental Bun Workers. For production, use the standard `Worker`. See [Worker vs SandboxedWorker](/guide/worker/#worker-vs-sandboxedworker).
:::

`SandboxedWorker` also sends heartbeats automatically in both modes; in embedded mode `heartbeatInterval` defaults to `5000` ms. If its jobs run longer than `stallInterval`, either raise `stallInterval`, call `progress()` periodically, or disable stall detection with `queue.setStallConfig({ enabled: false })`.

:::tip[Related Guides]
- [Dead Letter Queue](/guide/dlq/) - Where stalled jobs end up after max stalls
- [Worker API](/guide/worker/) - Configure heartbeat intervals
- [CPU-Intensive Workers](/guide/cpu-intensive-workers/) - Prevent stalls in CPU-heavy workloads
:::
