---
title: "bunqueue SQLite Persistence: WAL Mode, Write Buffering & S3 Backups"
description: "bunqueue persistence layer: SQLite WAL mode config, write buffering, read-through cache, S3 backup flows, and durability guarantees."
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og-image.png
---

<div class="bq-wrap bq-hero">
  <span class="bq-eyebrow">architecture · persistence</span>
  <h1 class="bq-hero-h1 bq-bench-h1">WAL mode and <em>write buffers.</em></h1>
  <p class="bq-hero-sub">bunqueue uses SQLite with WAL mode for persistence, optimized for high-throughput job processing: batched writes, crash recovery, and S3 backups.</p>
</div>

## SQLite Configuration

<div class="bq-diag">
  <div class="bq-diag-head"><b>SQLite pragmas</b><span>set at startup</span></div>
  <div class="bq-diag-row">
    <div class="bq-diag-cell bq-diag-accent"><code>journal_mode = WAL</code> <i>Write-Ahead Logging</i></div>
    <div class="bq-diag-cell"><code>synchronous = NORMAL</code> <i>balanced safety/performance</i></div>
    <div class="bq-diag-cell"><code>cache_size = -64000</code> <i>64MB in-memory cache</i></div>
  </div>
  <div class="bq-diag-row">
    <div class="bq-diag-cell"><code>temp_store = MEMORY</code> <i>in-memory temp tables</i></div>
    <div class="bq-diag-cell"><code>mmap_size = 268435456</code> <i>256MB memory-mapped I/O</i></div>
    <div class="bq-diag-cell"><code>page_size = 4096</code> <i>4KB pages</i></div>
  </div>
</div>

## Write Buffer Architecture

<div class="bq-diag">
  <div class="bq-diag-head"><b>Write buffer</b><span>job arrives</span></div>
  <div class="bq-diag-layer">Add to buffer <i>max 100 jobs</i></div>
  <div class="bq-diag-arrow">↓</div>
  <div class="bq-diag-row">
    <div class="bq-diag-cell">Buffer full <i>100 jobs</i></div>
    <div class="bq-diag-cell">Timer fires <i>10ms</i></div>
  </div>
  <div class="bq-diag-arrow">↓</div>
  <div class="bq-diag-layer bq-diag-accent">Batch insert <i>INSERT INTO jobs VALUES (...), (...), (...)</i></div>
  <p class="bq-diag-note">Prepared statement cache (1-100 rows), single transaction, 50-100x faster than individual inserts.</p>
</div>

## Buffered vs Durable Mode

<div class="bq-diag">
  <div class="bq-diag-head"><b>Write modes</b><span>per-job choice</span></div>
  <div class="bq-diag-row">
    <div class="bq-diag-cell bq-diag-accent">Buffered, default <i>~100k jobs/sec, up to 10ms loss, batched writes. Use for: emails, notifications, analytics</i></div>
    <div class="bq-diag-cell">Durable, opt-in per job <i>~10k jobs/sec, no data loss, immediate write. Use for: payments, critical events, financial transactions</i></div>
  </div>
  <p class="bq-diag-note">Usage: <code>queue.add('job', data, { durable: true })</code></p>
</div>

## Database Schema

<div class="bq-diag">
  <div class="bq-diag-head"><b>Tables</b><span>SQLite schema</span></div>
  <div class="bq-diag-group">
    <span class="bq-diag-group-label">jobs, 28 columns</span>
    <div class="bq-diag-row">
      <div class="bq-diag-cell bq-diag-accent">id <i>TEXT PRIMARY KEY, UUIDv7</i></div>
      <div class="bq-diag-cell">queue <i>TEXT</i></div>
      <div class="bq-diag-cell">data <i>BLOB, MessagePack</i></div>
      <div class="bq-diag-cell">priority <i>INTEGER</i></div>
    </div>
    <div class="bq-diag-row">
      <div class="bq-diag-cell">state <i>TEXT</i></div>
      <div class="bq-diag-cell">run_at <i>INTEGER</i></div>
      <div class="bq-diag-cell">attempts <i>INTEGER</i></div>
      <div class="bq-diag-cell">... <i>21 more fields</i></div>
    </div>
  </div>
  <div class="bq-diag-group">
    <span class="bq-diag-group-label">indexes</span>
    <div class="bq-diag-row">
      <div class="bq-diag-cell">(queue, state) <i>PULL queries</i></div>
      <div class="bq-diag-cell">(run_at) <i>delayed job scheduler</i></div>
      <div class="bq-diag-cell">(queue, unique_key) <i>deduplication</i></div>
      <div class="bq-diag-cell">(custom_id) <i>idempotency</i></div>
    </div>
    <div class="bq-diag-row">
      <div class="bq-diag-cell">(parent_id) <i>parent job lookup</i></div>
      <div class="bq-diag-cell">(state, started_at) <i>stall detection</i></div>
      <div class="bq-diag-cell">(group_id) <i>group operations</i></div>
      <div class="bq-diag-cell">(queue, state, priority, run_at) <i>priority pull</i></div>
    </div>
  </div>
  <div class="bq-diag-row">
    <div class="bq-diag-cell">job_results <i>job_id TEXT PRIMARY KEY, result BLOB MessagePack, completed_at INTEGER</i></div>
    <div class="bq-diag-cell">dlq <i>id INTEGER PRIMARY KEY AUTOINCREMENT, job_id TEXT, queue TEXT, entry BLOB full DlqEntry MessagePack, entered_at INTEGER</i></div>
    <div class="bq-diag-cell">cron_jobs <i>name TEXT PRIMARY KEY, queue TEXT, schedule TEXT, next_run INTEGER, executions INTEGER, timezone TEXT</i></div>
  </div>
</div>

## Crash Recovery Flow

<div class="bq-diag">
  <div class="bq-diag-head"><b>Startup recovery</b><span>crash recovery on boot</span></div>
  <div class="bq-diag-layer">1. Load pending jobs <i>state waiting/delayed: push each to its shard queue, update jobIndex</i></div>
  <div class="bq-diag-arrow">↓</div>
  <div class="bq-diag-layer bq-diag-accent">2. Load active jobs <i>reset each to waiting, worker may have died, push back to queue</i></div>
  <div class="bq-diag-arrow">↓</div>
  <div class="bq-diag-layer">3. Load DLQ entries <i>restore to in-memory DLQ shards</i></div>
  <div class="bq-diag-arrow">↓</div>
  <div class="bq-diag-layer">4. Load cron jobs <i>populate cron scheduler heap</i></div>
</div>

## S3 Backup Flow

<div class="bq-diag">
  <div class="bq-diag-head"><b>S3 backup</b><span>scheduled: every 6 hours, configurable</span></div>
  <div class="bq-diag-group">
    <span class="bq-diag-group-label">backup</span>
    <div class="bq-diag-row">
      <div class="bq-diag-cell">1. Check database file exists</div>
      <div class="bq-diag-cell">2. Generate key <i>backups/bunqueue-{timestamp}.db</i></div>
      <div class="bq-diag-cell">3. Read file as ArrayBuffer</div>
      <div class="bq-diag-cell">4. Calculate SHA256 checksum</div>
    </div>
    <div class="bq-diag-row">
      <div class="bq-diag-cell bq-diag-accent">5. Upload to S3 <i>application/x-sqlite3</i></div>
      <div class="bq-diag-cell">6. Upload metadata.json</div>
      <div class="bq-diag-cell">7. Cleanup old backups <i>keep N most recent</i></div>
    </div>
  </div>
  <div class="bq-diag-group">
    <span class="bq-diag-group-label">restore</span>
    <div class="bq-diag-row">
      <div class="bq-diag-cell">1. Download backup file from S3</div>
      <div class="bq-diag-cell">2. Load metadata.json</div>
      <div class="bq-diag-cell">3. Verify SHA256 checksum</div>
      <div class="bq-diag-cell">4. Write to database path</div>
      <div class="bq-diag-cell">5. Restart server to load</div>
    </div>
  </div>
  <p class="bq-diag-note">Supports AWS S3, Cloudflare R2, MinIO, DigitalOcean.</p>
</div>

## Flush on Failure

<div class="bq-diag">
  <div class="bq-diag-head"><b>Error recovery</b><span>flush() fails</span></div>
  <div class="bq-diag-layer bq-diag-accent">Re-add jobs to buffer <i>this.buffer = jobs.concat(this.buffer)</i></div>
  <div class="bq-diag-arrow">↓</div>
  <div class="bq-diag-layer">Jobs not lost, will retry on next flush</div>
  <div class="bq-diag-group">
    <span class="bq-diag-group-label">on shutdown</span>
    <div class="bq-diag-layer">Final flush of remaining buffer, wait for completion before exit</div>
  </div>
</div>

## Serialization

<div class="bq-diag">
  <div class="bq-diag-head"><b>MessagePack</b><span>why MessagePack instead of JSON?</span></div>
  <div class="bq-diag-row">
    <div class="bq-diag-cell bq-diag-accent">2-3x faster encoding/decoding</div>
    <div class="bq-diag-cell">Smaller payload size</div>
    <div class="bq-diag-cell">Binary data support</div>
  </div>
  <div class="bq-diag-group">
    <span class="bq-diag-group-label">used for</span>
    <div class="bq-diag-row">
      <div class="bq-diag-cell">Job data BLOB</div>
      <div class="bq-diag-cell">DLQ entry BLOB</div>
      <div class="bq-diag-cell">TCP protocol payloads</div>
      <div class="bq-diag-cell">Job results storage</div>
    </div>
  </div>
</div>
