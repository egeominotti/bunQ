---
title: "Dead Letter Queue: What Happens to Failed Jobs"
description: "bunqueue keeps failed jobs in a Dead Letter Queue with full error history. Inspect, retry manually or automatically, and expire old entries."
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/guide/dlq.png
---

<div class="bq-wrap bq-hero">
  <span class="bq-eyebrow">guide · dlq</span>
  <h1 class="bq-hero-h1 bq-bench-h1">Failed jobs, kept in the <em>DLQ.</em></h1>
  <p class="bq-hero-sub">When a job runs out of retries, bunqueue does not throw it away. It lands in the Dead Letter Queue with its error and full attempt history, so you can see what went wrong and retry it.</p>
</div>

The Dead Letter Queue (DLQ) is a holding area for jobs that failed permanently, for example after exhausting all their retry attempts. Nothing is lost: each entry keeps the original job, the error message, and a record of every attempt.

## Quick Start

```typescript
import { Queue } from 'bunqueue/client';

const queue = new Queue('emails', { embedded: true });

// See what failed and why
const entries = queue.getDlq();
for (const entry of entries) {
  console.log(entry.job.id, entry.reason, entry.error);
}

// Put everything back in the queue for another try
queue.retryDlq();
```

From the CLI, against a running server:

```bash
bunqueue dlq list emails
bunqueue dlq retry emails
bunqueue dlq purge emails
```

:::note[Embedded vs TCP]
The synchronous query API on this page (`getDlq()`, `getDlqStats()`, `retryDlqByFilter()`) reads in-process state and returns data only in **embedded mode**. In TCP mode these getters return empty results, while `setDlqConfig()`, `retryDlq()`, and `purgeDlq()` send fire-and-forget commands to the server. To inspect a remote server's DLQ, use the CLI or the dashboard.
:::

## Common Tasks

### Filter entries

```typescript
queue.getDlq({ reason: 'timeout' });                       // by failure reason
queue.getDlq({ olderThan: Date.now() - 86400000 });        // older than 24h
queue.getDlq({ newerThan: Date.now() - 3600000 });         // last hour
queue.getDlq({ retriable: true });                         // still eligible for auto-retry
queue.getDlq({ limit: 10, offset: 20 });                   // pagination
```

### Retry selectively

```typescript
queue.retryDlq();                                          // retry everything
queue.retryDlq('job-123');                                 // retry one job
queue.retryDlqByFilter({ reason: 'timeout' });             // retry by filter
```

### Check DLQ health

```typescript
const stats = queue.getDlqStats();
console.log(stats.total);         // total entries
console.log(stats.byReason);      // { explicit_fail: 5, timeout: 2, ... }
console.log(stats.pendingRetry);  // entries waiting for auto-retry
```

A simple alert loop:

```typescript
setInterval(() => {
  const stats = queue.getDlqStats();
  if (stats.total > 100) alertOps('High DLQ count', stats);
}, 30000);
```

### Purge

```typescript
const purged = queue.purgeDlq();  // permanently deletes all entries
```

## Automatic Retry

With `autoRetry` enabled, bunqueue re-queues DLQ entries on its own, spacing retries out with exponential backoff (each retry waits twice as long as the previous one):

```typescript
queue.setDlqConfig({
  autoRetry: true,
  autoRetryInterval: 60000,  // base delay: 1 minute
  maxAutoRetries: 3,
});
// 1st retry: 1 min after entering the DLQ
// 2nd retry: 1 min after the 1st  (60s x 2^0)
// 3rd retry: 2 min after the 2nd  (60s x 2^1)
// After that the entry stays in the DLQ permanently
```

A background task checks for due retries every minute and re-queues jobs with a reset attempt count.

## Configuration

```typescript
queue.setDlqConfig({
  autoRetry: true,
  autoRetryInterval: 3600000,
  maxAutoRetries: 3,
  maxAge: 604800000,   // purge entries after 7 days (null = never)
  maxEntries: 10000,
});
```

| Option | Default | Description |
|--------|---------|-------------|
| `autoRetry` | `false` | Enable automatic retry |
| `autoRetryInterval` | `3600000` | Base delay between auto-retries (1 hour) |
| `maxAutoRetries` | `3` | Maximum auto-retry attempts |
| `maxAge` | `604800000` | Auto-purge age (7 days, `null` = never) |
| `maxEntries` | `10000` | Maximum DLQ entries per queue |

## Reference

### Why jobs end up in the DLQ

| Reason | Description |
|--------|-------------|
| `explicit_fail` | The processor threw an error |
| `max_attempts_exceeded` | Job exceeded its retry attempts |
| `timeout` | Job timed out during processing |
| `stalled` | Job stopped sending heartbeats (worker likely crashed) |
| `ttl_expired` | Job expired before it was processed |
| `worker_lost` | Worker disconnected during processing |
| `unknown` | Fallback for unclassified failures |

### Entry structure

```typescript
interface DlqEntry<T> {
  job: Job<T>;                    // The failed job
  enteredAt: number;              // When first moved to DLQ
  reason: FailureReason;          // Why it failed
  error: string | null;           // Error message
  attempts: AttemptRecord[];      // Full attempt history
  retryCount: number;             // Times retried from DLQ
  lastRetryAt: number | null;     // Last DLQ retry time
  nextRetryAt: number | null;     // Next scheduled auto-retry
  expiresAt: number | null;       // When entry expires
}
```

Each `AttemptRecord` carries the attempt number, start and failure timestamps, failure reason, error message, and duration in ms.

:::tip[Related Guides]
- [Stall Detection & Recovery](/guide/stall-detection/) - Stalled jobs are sent to the DLQ
- [Worker API](/guide/worker/) - Configure retry behavior
- [Monitoring & Prometheus Metrics](/guide/monitoring/) - Alert on DLQ size
:::
