---
title: "Dead Letter Queue: Handle Failed Jobs & Auto-Retry in bunqueue"
description: "Configure bunqueue's Dead Letter Queue: auto-retry with backoff, expiration policies, filter by failure reason, and monitor DLQ stats."
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/client-sdk.png
---

<div class="bq-wrap bq-hero">
  <span class="bq-eyebrow">guide · dlq</span>
  <h1 class="bq-hero-h1 bq-bench-h1">Failures are kept, <em>never lost.</em></h1>
  <p class="bq-hero-sub">The Dead Letter Queue stores failed jobs with full metadata for debugging and recovery. Inspect why a job died, retry it automatically or by hand, and expire old entries on a policy.</p>

  <div class="bq-proof">
    <span><b>7</b> tracked failure reasons</span>
    <span><b>7 days</b> default retention before purge</span>
    <span><b>10,000</b> max entries per queue</span>
  </div>
</div>

:::tip[Using AI agents?]
AI agents connected via MCP can inspect DLQ entries, retry failed jobs, and purge the queue via natural language using `bunqueue_get_dlq`, `bunqueue_retry_dlq`, and `bunqueue_purge_dlq` tools. See [MCP Server](/guide/mcp/).
:::

## Why Jobs End Up in DLQ

| Reason | Description |
|--------|-------------|
| `explicit_fail` | Job explicitly failed via error throw |
| `max_attempts_exceeded` | Job exceeded retry attempts |
| `timeout` | Job timed out during processing |
| `stalled` | Job stalled (no heartbeat) |
| `ttl_expired` | Job TTL expired before processing |
| `worker_lost` | Worker disconnected during processing |
| `unknown` | Fallback for unclassified failures |

## Configuration

```typescript
import { Queue } from 'bunqueue/client';

const queue = new Queue('my-queue', { embedded: true });

queue.setDlqConfig({
  autoRetry: true,              // Enable automatic retry from DLQ
  autoRetryInterval: 3600000,   // Retry every hour
  maxAutoRetries: 3,            // Max 3 auto-retries
  maxAge: 604800000,            // Purge after 7 days (null = never)
  maxEntries: 10000,            // Max entries per queue
});
```

### Options

| Option | Default | Description |
|--------|---------|-------------|
| `autoRetry` | `false` | Enable automatic retry |
| `autoRetryInterval` | `3600000` | Time between auto-retries (1 hour) |
| `maxAutoRetries` | `3` | Maximum auto-retry attempts |
| `maxAge` | `604800000` | Auto-purge age (7 days, null = never) |
| `maxEntries` | `10000` | Maximum DLQ entries per queue |

:::note[Embedded vs TCP]
The synchronous query API on this page (`getDlq()`, `getDlqStats()`, `retryDlqByFilter()`) reads in-process state and returns data only in **embedded mode**. In TCP mode these getters return empty results, while `setDlqConfig()`, `retryDlq()`, and `purgeDlq()` send fire-and-forget commands to the server. To inspect or manage the DLQ of a remote server, use the CLI (`bunqueue dlq list|retry|purge <queue>`) or the dashboard.
:::

## Viewing DLQ Entries

```typescript
// Get all DLQ entries
const entries = queue.getDlq();

entries.forEach(entry => {
  console.log('Job ID:', entry.job.id);
  console.log('Reason:', entry.reason);
  console.log('Error:', entry.error);
  console.log('Entered DLQ:', new Date(entry.enteredAt));
  console.log('Attempts:', entry.attempts.length);
  console.log('Retry count:', entry.retryCount);
  console.log('Next retry:', entry.nextRetryAt);
  console.log('Expires:', entry.expiresAt);
});
```

## Filtering

```typescript
// Filter by reason
const stalledJobs = queue.getDlq({ reason: 'stalled' });
const timeoutJobs = queue.getDlq({ reason: 'timeout' });

// Filter by age
const oldJobs = queue.getDlq({
  olderThan: Date.now() - 86400000  // Older than 24 hours
});

const recentJobs = queue.getDlq({
  newerThan: Date.now() - 3600000   // Last hour
});

// Filter retriable entries
const retriable = queue.getDlq({ retriable: true });

// Pagination
const page = queue.getDlq({ limit: 10, offset: 20 });
```

## Statistics

```typescript
const stats = queue.getDlqStats();

console.log('Total entries:', stats.total);
console.log('By reason:', stats.byReason);
// { explicit_fail: 5, timeout: 2, stalled: 1, ... }

console.log('Pending retry:', stats.pendingRetry);
console.log('Expired:', stats.expired);
console.log('Oldest entry:', new Date(stats.oldestEntry));
console.log('Newest entry:', new Date(stats.newestEntry));
```

## Retrying Jobs

```typescript
// Retry all jobs
const count = queue.retryDlq();

// Retry specific job
queue.retryDlq('job-123');

// Retry by filter
queue.retryDlqByFilter({ reason: 'timeout' });
queue.retryDlqByFilter({ olderThan: Date.now() - 86400000 });
```

## Purging

```typescript
// Purge all DLQ entries
const purged = queue.purgeDlq();
console.log(`Purged ${purged} entries`);
```

## DLQ Entry Structure

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

interface AttemptRecord {
  attempt: number;       // Attempt number (1-based)
  startedAt: number;     // When attempt started
  failedAt: number;      // When attempt failed
  reason: FailureReason; // Failure reason
  error: string | null;  // Error message
  duration: number;      // Attempt duration (ms)
}
```

## Auto-Retry Behavior

When `autoRetry` is enabled:

1. Failed jobs are added to DLQ with `nextRetryAt` set
2. Background task checks for due retries every minute
3. Jobs are re-queued with reset attempt count
4. Uses exponential backoff: `interval * 2^(retryCount-1)`
5. After `maxAutoRetries`, job stays in DLQ permanently

```typescript
queue.setDlqConfig({
  autoRetry: true,
  autoRetryInterval: 60000,  // Base: 1 minute
  maxAutoRetries: 3,
});

// Retry schedule (backoff multiplier is 2^(retryCount-1) at scheduling time):
// 1st retry: 1 minute after entering the DLQ
// 2nd retry: 1 minute after the 1st retry (60s × 2^0)
// 3rd retry: 2 minutes after the 2nd retry (60s × 2^1)
// After that: no more auto-retries
```

## Example: Monitoring Dashboard

```typescript
// Poll DLQ stats every 30 seconds
setInterval(() => {
  const stats = queue.getDlqStats();

  // Alert if too many failures
  if (stats.total > 100) {
    alertOps('High DLQ count', stats);
  }

  // Check for stall issues
  if (stats.byReason.stalled > 10) {
    alertOps('Many stalled jobs - check workers', stats);
  }
}, 30000);
```

:::tip[Related Guides]
- [Stall Detection & Recovery](/guide/stall-detection/) - Stalled jobs are sent to the DLQ
- [Worker API](/guide/worker/) - Configure retry behavior
- [Monitoring & Prometheus Metrics](/guide/monitoring/) - Monitor DLQ metrics
:::
