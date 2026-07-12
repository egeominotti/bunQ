---
title: "Stall Detection: Recover Unresponsive Jobs Automatically in Bun"
description: "Configure bunqueue stall detection to auto-recover stuck jobs. Set heartbeat intervals, max stall thresholds, and grace periods for Workers."
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/guide/stall-detection.png
---

<div class="bq-wrap bq-hero">
  <span class="bq-eyebrow">guide · stall-detection</span>
  <h1 class="bq-hero-h1 bq-bench-h1">Stuck jobs <em>come back.</em></h1>
  <p class="bq-hero-sub">Stall detection automatically identifies jobs that stop sending heartbeats during processing and recovers them, retrying first and moving repeat offenders to the DLQ.</p>

  <div class="bq-proof">
    <span><b>30s</b> without a heartbeat marks a job stalled</span>
    <span><b>3</b> stalls before a job moves to the DLQ</span>
    <span><b>5s</b> grace period after a job starts</span>
  </div>
</div>

## How It Works

1. Workers send periodic heartbeats while processing jobs
2. The queue manager checks for jobs without recent heartbeats
3. Stalled jobs are either retried or moved to the DLQ

## Configuration

```typescript
import { Queue } from 'bunqueue/client';

const queue = new Queue('my-queue', { embedded: true });

queue.setStallConfig({
  enabled: true,         // Enable stall detection (default: true)
  stallInterval: 30000,  // Job is stalled after 30s without heartbeat
  maxStalls: 3,          // Move to DLQ after 3 stalls
  gracePeriod: 5000,     // Grace period after job starts
});
```

### Options

| Option | Default | Description |
|--------|---------|-------------|
| `enabled` | `true` | Enable/disable stall detection |
| `stallInterval` | `30000` | Time (ms) without heartbeat before job is stalled |
| `maxStalls` | `3` | Max stalls before moving to DLQ |
| `gracePeriod` | `5000` | Initial grace period after job starts |

## Worker Heartbeats

Workers automatically send heartbeats:

```typescript
const worker = new Worker('queue', processor, {
  embedded: true,
  heartbeatInterval: 10000, // Heartbeat every 10 seconds
});
```

The `heartbeatInterval` should be less than `stallInterval` to avoid false positives.

## Stall Actions

When a job stalls, one of these actions is taken:

1. **Retry** - Job is re-queued with incremented stall count
2. **Move to DLQ** - Job exceeds `maxStalls` and is moved to Dead Letter Queue

When a job is retried after a stall or lock expiry, its internal counters (queued count, shard stats) are updated correctly and waiting workers are notified immediately. This means requeued jobs are picked up without delay.

## Events

`QueueEvents` subscribes to the in-process event stream, so it works in embedded mode (same process as the queue):

```typescript
import { QueueEvents } from 'bunqueue/client';

const events = new QueueEvents('my-queue');

events.on('stalled', ({ jobId }) => {
  console.log(`Job ${jobId} stalled`);
});
```

In TCP mode, use the Worker's `stalled` event or subscribe via [webhooks](/guide/webhooks/).

## Example: Long-Running Jobs

For jobs that take a long time, increase the stall interval:

```typescript
// Queue for video processing (may take hours)
const videoQueue = new Queue('video-processing', { embedded: true });

videoQueue.setStallConfig({
  stallInterval: 300000,  // 5 minutes
  maxStalls: 2,
  gracePeriod: 60000,     // 1 minute grace
});

// Worker with frequent heartbeats
const worker = new Worker('video-processing', async (job) => {
  for (const chunk of video.chunks) {
    await processChunk(chunk);
    // updateProgress() also sends a heartbeat to reset the stall timer
    await job.updateProgress(chunk.progress);
  }
}, {
  embedded: true,
  heartbeatInterval: 30000, // Automatic heartbeat every 30 seconds
});
```

:::tip[Heartbeat Methods]
Both methods reset the stall detection timer:
- `job.updateProgress()` - Use when you have progress to report
- Worker's automatic heartbeat - Runs every `heartbeatInterval` ms in the background

For long-running jobs without natural progress points, rely on `heartbeatInterval`.
:::

## Monitoring

Check stall-related stats:

```typescript
const stats = queue.getDlqStats();
console.log('Stalled jobs in DLQ:', stats.byReason.stalled);
```

Filter DLQ by stalled reason:

```typescript
const stalledJobs = queue.getDlq({ reason: 'stalled' });
```

## SandboxedWorker

:::caution[Experimental]
`SandboxedWorker` depends on experimental Bun Workers. For production, use the standard `Worker`. See [Worker vs SandboxedWorker](/guide/worker/#worker-vs-sandboxedworker).
:::

SandboxedWorker automatically sends heartbeats in both embedded and TCP mode. In embedded mode, `heartbeatInterval` defaults to `5000ms`, keeping `lastHeartbeat` fresh so long-running jobs are not falsely detected as stalled.

```typescript
const worker = new SandboxedWorker('heavy-jobs', {
  processor: './processor.ts',
  timeout: 0,              // Disable worker-level timeout for long jobs
  heartbeatInterval: 5000, // Default in embedded mode (keeps stall detection happy)
});
```

:::tip[Long-running SandboxedWorker jobs]
If your jobs run longer than the default `stallInterval` (30s), you have three options:
1. **Increase `stallInterval`**, `queue.setStallConfig({ stallInterval: 300000 })` (5 minutes)
2. **Call `progress()` periodically**, Each call refreshes `lastHeartbeat`
3. **Disable stall detection**, `queue.setStallConfig({ enabled: false })`
:::

:::tip[Related Guides]
- [Dead Letter Queue](/guide/dlq/) - Where stalled jobs end up after max retries
- [Worker API](/guide/worker/) - Configure heartbeat intervals
- [CPU-Intensive Workers](/guide/cpu-intensive-workers/) - Prevent stalls in CPU-heavy workloads
:::
