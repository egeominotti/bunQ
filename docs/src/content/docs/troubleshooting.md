---
title: "Troubleshooting bunqueue: Common Issues & Fixes"
description: "Fix common bunqueue problems: SQLite database locks, memory leaks, connection timeouts, job processing failures, and embedded mode issues."
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/getting-started.png
---

<div class="bq-wrap bq-hero">
  <span class="bq-eyebrow">reference · troubleshooting</span>
  <h1 class="bq-hero-h1 bq-bench-h1">When something looks <em>wrong.</em></h1>
  <p class="bq-hero-sub">Symptoms, causes and fixes for the issues people actually hit: SQLite locks, embedded mode misconfiguration, stuck jobs, half-open connections and backup failures.</p>
</div>

## Installation Issues

### "bunqueue is Bun-only and requires the Bun runtime"

Running under Node.js throws:

> bunqueue is Bun-only and requires the Bun runtime (https://bun.sh). Node.js is not supported: install Bun and run your program with `bun`.

bunqueue only works with Bun (v1.3.9+), not Node.js, run your program with `bun`, not `node`.

```bash
# Check if Bun is installed
bun --version

# Install Bun if needed
curl -fsSL https://bun.sh/install | bash
```

### Permission errors on install

```bash
# Try with sudo (not recommended)
sudo bun add bunqueue

# Better: fix npm permissions
mkdir ~/.bun
chown -R $(whoami) ~/.bun
```

## Database Issues

### "SQLITE_BUSY: database is locked"

Multiple processes trying to write simultaneously.

**Solutions:**
1. Use WAL mode (default in bunqueue)
2. Ensure only one server instance per database file
3. Use server mode for multi-process access

```bash
# Check for multiple processes
lsof ./data/queue.db

# Kill stale processes
pkill -f bunqueue
```

### "SQLITE_CORRUPT: database disk image is malformed"

Database corruption, usually from crash during write.

**Solutions:**
1. Restore from S3 backup
2. Delete and recreate database (data loss)

```bash
# Restore from backup
bunqueue backup list
bunqueue backup restore <key> --force

# Or recreate (loses data)
rm ./data/queue.db*
bunqueue start
```

### Database file keeps growing

SQLite doesn't automatically reclaim space.

```bash
# Vacuum the database (run when server is stopped)
sqlite3 ./data/queue.db "VACUUM;"

# Enable auto-vacuum (before creating database)
sqlite3 ./data/queue.db "PRAGMA auto_vacuum = INCREMENTAL;"
```

## Embedded Mode Issues

### "Command timeout" error

```
error: Command timeout
      queue: "my-queue",
      context: "pull"
```

This error means your Worker is trying to connect to a TCP server instead of using embedded mode.

**Solution:** Add `embedded: true` to **both** Queue and Worker:

```typescript
// WRONG - Worker defaults to TCP mode
const queue = new Queue('tasks', { embedded: true });
const worker = new Worker('tasks', processor); // Missing embedded: true!

// CORRECT - Both have embedded: true
const queue = new Queue('tasks', { embedded: true });
const worker = new Worker('tasks', processor, { embedded: true });
```

### SQLite database not created

The database is only created when a data path is configured.

**Solution (embedded mode):**

```typescript
import { Queue, Worker } from 'bunqueue/client';

// Pass dataPath directly
const queue = new Queue('tasks', { embedded: true, dataPath: './data/bunqueue.db' });
const worker = new Worker('tasks', processor, { embedded: true, dataPath: './data/bunqueue.db' });
```

**Solution (server mode):** Use a [configuration file](/guide/configuration/) or set `DATA_PATH`:

```bash
DATA_PATH=./data/bunqueue.db bunqueue start
```

:::note
Without `dataPath` or `DATA_PATH`, bunqueue runs in-memory (no persistence across restarts).
:::

### Jobs not persisted across restarts

If you're using multiple files, the Worker's `autorun: true` (default) can cause the QueueManager to initialize before `DATA_PATH` is set.

**Solution:** Use `autorun: false` and start the worker manually:

```typescript
// queue.ts
const worker = new Worker('tasks', processor, {
  embedded: true,
  autorun: false,  // Don't start automatically
});

export function startWorker() {
  worker.run();
}
```

```typescript
// main.ts
process.env.DATA_PATH = './data/bunqueue.db';

import { startWorker } from './queue';
startWorker();  // Start after DATA_PATH is set
```

## Job Processing Issues

### Jobs stuck in "active" state

Worker crashed while processing.

**Solutions:**
1. Enable stall detection
2. Restart workers

```typescript
queue.setStallConfig({
  enabled: true,
  stallInterval: 30000,
  maxStalls: 3,
});
```

### Jobs not being processed

**Check these:**
1. Is the queue paused?
2. Is there a worker for this queue?
3. Is rate limiting blocking jobs?

```typescript
// Check if paused
const isPaused = queue.isPaused();

// Check counts
const counts = queue.getJobCounts();
console.log(counts);

// Check rate limit
const config = queue.getGlobalRateLimit();
```

### Jobs failing immediately

Check the error in failed event:

```typescript
worker.on('failed', (job, error) => {
  console.error('Job failed:', error);
  console.error('Job data:', job.data);
  console.error('Attempts:', job.attemptsMade);
});
```

### Memory usage keeps growing

**Possible causes:**
1. Jobs not being removed after completion
2. Too many jobs in DLQ
3. Memory leak in processor

```typescript
// Enable removeOnComplete
await queue.add('task', data, {
  removeOnComplete: true,
});

// Purge old DLQ entries
queue.purgeDlq();

// Check for leaks in processor
worker.on('completed', () => {
  console.log('Memory:', process.memoryUsage().heapUsed);
});
```

## Connection Issues

### "Connection refused" to server

Server not running or wrong port.

```bash
# Check if server is running
ps aux | grep bunqueue

# Check listening ports
lsof -i :6789
lsof -i :6790

# Start server
bunqueue start
```

### TCP connection drops

Network issues or server overload.

```typescript
// Add reconnection logic
let client = createClient();

client.on('error', async () => {
  await sleep(1000);
  client = createClient();
});
```

### Worker stalls on a half-open connection (throughput drops to 0)

A worker's TCP socket can go **half-open**, the peer vanishes with no FIN/RST (host
suspended/hibernated, NAT or load-balancer silently dropping an idle connection). Writes
still succeed and no `close` event fires, so the symptom is: every command rejects with
`Command timeout`, `consecutiveErrors` climbs, jobs pile up in `waiting` with `active=0`,
and throughput sits at 0.

bunqueue detects this and reconnects automatically via two signals: the health-check ping
(`maxPingFailures`) **and** consecutive command timeouts (`maxCommandTimeouts`, default 3).
With default timings (`pingInterval`/`commandTimeout` = 30s) recovery takes up to ~120s.
For faster recovery, tighten the detection cadence:

```typescript
const worker = new Worker('q', handler, {
  connection: {
    host, port,
    pingInterval: 10_000,    // health-check every 10s (0 disables)
    commandTimeout: 5_000,   // fail a command after 5s
    maxCommandTimeouts: 3,   // 3 consecutive timeouts → reconnect (0 disables)
  },
});
```

This recovers in ~tens of seconds and works even with the ping disabled. If a *fresh*
connection also can't be established (e.g. the server is genuinely unreachable, a firewall
dropping inbound SYNs, not just an idle drop), no client can reconnect until connectivity
returns; auto-reconnect with infinite attempts resumes on its own once it does.

### Authentication failures

```bash
# Check token is set
echo $AUTH_TOKENS

# Test with curl
curl -H "Authorization: Bearer your-token" \
  http://localhost:6790/health
```

## Performance Issues

### Slow job processing

**Optimize:**
1. Increase worker concurrency
2. Use batch operations
3. Check database I/O

```typescript
// Increase concurrency
const worker = new Worker('queue', processor, {
  concurrency: 20,
});

// Use bulk add
await queue.addBulk([...jobs]);

// Use bulk ack
await queue.ackBatch([...jobIds]);
```

### High latency on pull

**Check:**
1. Index on queue table
2. Too many delayed jobs
3. Database on slow disk

```sql
-- Check indexes exist
.indices jobs

-- Check delayed jobs count
SELECT COUNT(*) FROM jobs WHERE state = 'delayed';
```

### Server CPU at 100%

Too many connections or jobs.

```bash
# Check connection count
bunqueue stats

# Reduce polling frequency
# Add backoff in clients
```

## Backup Issues

### S3 backup failing

```bash
# Check credentials
echo $S3_ACCESS_KEY_ID
echo $S3_BUCKET

# Test connectivity
aws s3 ls s3://$S3_BUCKET/

# Check logs
bunqueue backup status
```

### Restore failing

```bash
# List available backups
bunqueue backup list

# Force restore (overwrites existing)
bunqueue backup restore <key> --force
```

## Sandboxed Worker Issues

:::danger[SandboxedWorker is experimental]
`SandboxedWorker` depends on [Bun Workers](https://bun.sh/docs/runtime/workers), which are **experimental**. Known issues include memory growth, thread duplication, and segfaults across Bun versions.

**For production, use the standard `Worker` instead**, it provides the same API (events, concurrency, heartbeats, retries) without any experimental dependencies. See [Worker vs SandboxedWorker](/guide/worker/#worker-vs-sandboxedworker).
:::

### Segmentation fault when terminating workers

If you experience crashes (segfaults) when using `SandboxedWorker`, especially during worker timeout or error handling, this is a **known Bun bug**.

**Symptoms:**
- `Segmentation fault at address 0xE8`
- `Worker has been terminated` errors
- Crashes during `worker.terminate()` calls
- Unexpected memory growth or thread duplication ([#52](https://github.com/egeominotti/bunqueue/issues/52))

**Solution:** Switch to the standard `Worker` for production workloads:

```typescript
// ❌ SandboxedWorker — experimental, may crash
const worker = new SandboxedWorker('queue', {
  processor: './processor.ts',
  concurrency: 4,
});

// ✅ Worker — stable, production-ready, same functionality
const worker = new Worker('queue', async (job) => {
  // same logic from your processor.ts
  return result;
}, { embedded: true, concurrency: 4 });
```

**If you must use SandboxedWorker:**
- Pin your Bun version, behavior varies across releases
- Use graceful shutdown (`await worker.stop()`) instead of force termination
- Use longer timeout values to avoid frequent terminations
- Monitor memory usage closely

These issues will be resolved when Bun stabilizes their Worker API.

## Common Error Messages

| Error | Cause | Solution |
|-------|-------|----------|
| `Command timeout` | Worker missing `embedded: true` | Add `embedded: true` to Worker options |
| `SQLITE_BUSY` | Database locked | Use single writer |
| `SQLITE_FULL` | Disk full | Free disk space |
| `ECONNREFUSED` | Server not running | Start server or use embedded mode |
| `ETIMEDOUT` | Network issue | Check connectivity |
| `Job not found` | Already completed/removed | Check job lifecycle |
| `Segmentation fault` | Bun Worker termination bug | Use graceful shutdown, see above |

## Debug Mode

Enable verbose logging:

```bash
# Server mode
LOG_FORMAT=json DEBUG=bunqueue:* bunqueue start

# Check logs
tail -f /var/log/bunqueue.log
```

## Getting Help

If these solutions don't help:

1. Check [GitHub Issues](https://github.com/egeominotti/bunqueue/issues)
2. Search [Discussions](https://github.com/egeominotti/bunqueue/discussions)
3. Open a new issue with:
   - bunqueue version
   - Bun version
   - OS and hardware
   - Error message and stack trace
   - Minimal reproduction code

:::tip[Related Guides]
- [Monitoring & Prometheus Metrics](/guide/monitoring/) - Set up monitoring to prevent issues
- [FAQ](/faq/) - Frequently asked questions
- [Stall Detection & Recovery](/guide/stall-detection/) - Debug stalled jobs
:::
