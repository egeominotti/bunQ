---
title: 'SQLite Persistence: WAL, Write Buffer, S3 Backups'
description: 'bunqueue persistence layer: SQLite WAL mode config, write buffering, read-through cache, S3 backup flows, and durability guarantees.'
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/architecture/persistence.png
---

<div class="bq-wrap bq-hero">
  <span class="bq-eyebrow">architecture · persistence</span>
  <h1 class="bq-hero-h1 bq-bench-h1">WAL mode and <em>write buffers.</em></h1>
  <p class="bq-hero-sub">bunqueue uses SQLite with WAL mode for persistence, optimized for high-throughput job processing: batched writes, crash recovery, and S3 backups.</p>
</div>

This page is intentionally specific to the memory/SQLite engine. Standalone
servers configured with PostgreSQL bypass `SqliteStorage` and `WriteBuffer`;
they use Bun's native `SQL` pool and database-authoritative transactions. See
[Storage backends](/guide/databases/) for that architecture and its backup,
durability, and failover boundaries.

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
    <div class="bq-diag-cell"><code>busy_timeout = 5000</code> <i>wait up to 5s on lock contention</i></div>
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
    <div class="bq-diag-cell bq-diag-accent">Buffered, default <i>up to 10 ms loss; 186,384 jobs/s median for the published public on-disk Embedded addBulk workload</i></div>
    <div class="bq-diag-cell">Durable, opt-in per job <i>immediate write; 60,835 ops/s median for the published sequential Embedded add workload</i></div>
  </div>
  <p class="bq-diag-note">Usage: <code>queue.add('job', data, { durable: true })</code></p>
</div>

The two medians above describe different workloads. They are capacity evidence,
not a direct buffered-versus-durable ratio; see [Engineering
Benchmarks](/guide/benchmarks/) for distributions and TCP results.

## Database Schema

<div class="bq-diag">
  <div class="bq-diag-head"><b>Tables</b><span>SQLite schema</span></div>
  <div class="bq-diag-group">
    <span class="bq-diag-group-label">jobs, 30 columns</span>
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
      <div class="bq-diag-cell">... <i>23 more fields</i></div>
    </div>
  </div>
  <div class="bq-diag-group">
    <span class="bq-diag-group-label">indexes</span>
    <div class="bq-diag-row">
      <div class="bq-diag-cell">(queue, state) <i>PULL queries</i></div>
      <div class="bq-diag-cell">(queue, created_at, id) <i>stable unfiltered pagination</i></div>
      <div class="bq-diag-cell">(queue, state, created_at, id) <i>stable state pagination</i></div>
      <div class="bq-diag-cell">(run_at) <i>delayed job scheduler</i></div>
      <div class="bq-diag-cell">(queue, unique_key) <i>deduplication</i></div>
      <div class="bq-diag-cell">(custom_id) <i>idempotency</i></div>
    </div>
    <div class="bq-diag-row">
      <div class="bq-diag-cell">(parent_id) <i>parent job lookup</i></div>
      <div class="bq-diag-cell">(state, started_at) <i>stall detection</i></div>
      <div class="bq-diag-cell">(group_id) <i>group operations</i></div>
      <div class="bq-diag-cell">(queue, state, priority, run_at) <i>priority pull</i></div>
      <div class="bq-diag-cell">(completed_at DESC) <i>completed-job recovery ordering</i></div>
    </div>
  </div>
  <div class="bq-diag-row">
    <div class="bq-diag-cell">job_results <i>job_id TEXT PRIMARY KEY, result BLOB MessagePack, completed_at INTEGER</i></div>
    <div class="bq-diag-cell">dlq <i>id INTEGER PRIMARY KEY AUTOINCREMENT, job_id TEXT, queue TEXT, entry BLOB full DlqEntry MessagePack, entered_at INTEGER</i></div>
    <div class="bq-diag-cell">cron_jobs <i>name TEXT PRIMARY KEY, queue TEXT, data BLOB, schedule TEXT, repeat_every INTEGER, priority INTEGER, next_run INTEGER, executions INTEGER, max_limit INTEGER, timezone TEXT, unique_key TEXT, dedup BLOB, skip_missed_on_restart INTEGER, skip_if_no_worker INTEGER, prevent_overlap INTEGER, job_options BLOB</i></div>
    <div class="bq-diag-cell">queue_state <i>name TEXT PRIMARY KEY, paused/rate/concurrency fields plus stall_enabled, stall_interval, max_stalls, stall_grace_period; persists queue controls and custom stall policy across restarts</i></div>
  </div>
</div>

## Crash Recovery Flow

<div class="bq-diag">
  <div class="bq-diag-head"><b>Startup recovery</b><span>crash recovery on boot, batches of 10,000 rows</span></div>
  <div class="bq-diag-layer bq-diag-accent">1. Recover active jobs <i>restore custom stall policy first, then read repeated 10k batches from offset zero; each interrupted job persists stallCount++ and attempts++. Below both maxStalls and maxAttempts it is requeued with backoff; reaching either bound persists exactly one DLQ entry. Cron-spawned preventOverlap jobs are dropped, the scheduler recreates them</i></div>
  <div class="bq-diag-arrow">↓</div>
  <div class="bq-diag-layer">2. Load pending jobs <i>state waiting/delayed: jobs with unmet dependencies go to waitingDeps, the rest to their shard queue; jobIndex, customId and uniqueKey mappings restored</i></div>
  <div class="bq-diag-arrow">↓</div>
  <div class="bq-diag-layer">3. Load DLQ entries <i>restore to in-memory DLQ shards, populate jobIndex</i></div>
  <div class="bq-diag-arrow">↓</div>
  <div class="bq-diag-layer">4. Restore queue state <i>paused flag, rate limit and concurrency limit per queue; the stall policy snapshot was already applied before active recovery</i></div>
  <div class="bq-diag-arrow">↓</div>
  <div class="bq-diag-layer">5. Load completed jobs <i>up to the 50k in-memory cap, for clean() and stats</i></div>
  <div class="bq-diag-arrow">↓</div>
  <div class="bq-diag-layer">6. Load cron jobs <i>populate cron scheduler heap; past next_run is recalculated forward when skipMissedOnRestart is set</i></div>
</div>

## S3 Backup Flow

<div class="bq-diag">
  <div class="bq-diag-head"><b>S3 backup</b><span>scheduled: every 6 hours, configurable</span></div>
  <div class="bq-diag-group">
    <span class="bq-diag-group-label">backup</span>
    <div class="bq-diag-row">
      <div class="bq-diag-cell">1. Flush pending WriteBuffer <i>abort if any write remains</i></div>
      <div class="bq-diag-cell">2. SQLite VACUUM INTO <i>WAL-safe standalone snapshot</i></div>
      <div class="bq-diag-cell">3. PRAGMA integrity_check</div>
      <div class="bq-diag-cell">4. Gzip + SHA256 <i>uncompressed bytes</i></div>
    </div>
    <div class="bq-diag-row">
      <div class="bq-diag-cell">5. Upload metadata sidecar <i>{unique-key}.meta.json</i></div>
      <div class="bq-diag-cell bq-diag-accent">6. Publish gzip payload <i>commit point</i></div>
      <div class="bq-diag-cell">7. Cleanup old backups <i>keep N most recent</i></div>
    </div>
  </div>
  <div class="bq-diag-group">
    <span class="bq-diag-group-label">restore</span>
    <div class="bq-diag-row">
      <div class="bq-diag-cell">1. Download backup file from S3</div>
      <div class="bq-diag-cell">2. Load metadata sidecar</div>
      <div class="bq-diag-cell">3. Decompress, verify size + SHA256</div>
      <div class="bq-diag-cell">4. Validate temp SQLite candidate</div>
      <div class="bq-diag-cell">5. Quarantine WAL/SHM, atomically rename</div>
    </div>
  </div>
  <p class="bq-diag-note">Stop the server before restore. Current compressed backups require metadata; legacy no-metadata restores are uncompressed only.</p>
</div>

## Flush on Failure

<div class="bq-diag">
  <div class="bq-diag-head"><b>Error recovery</b><span>flush() fails</span></div>
  <div class="bq-diag-layer bq-diag-accent">Re-buffer failed jobs <i>double-buffered swap: the failed flush buffer is merged back, nothing is dropped</i></div>
  <div class="bq-diag-arrow">↓</div>
  <div class="bq-diag-layer">Retry with exponential backoff <i>100ms initial, 30s max, up to 10 retries; regular flushing pauses during backoff</i></div>
  <div class="bq-diag-arrow">↓</div>
  <div class="bq-diag-layer">After max retries <i>critical-error callback fires with the affected jobs, so they can be surfaced instead of silently lost</i></div>
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

:::tip[Related]

- [Architecture Overview](/architecture/) - Full component map
- [Data Structures](/architecture/data-structures/) - In-memory structures backed by this store
- [TCP Protocol](/architecture/tcp-protocol/) - MessagePack payloads shared with the wire format
- [S3 Backup](/guide/backup/) - Backing up the SQLite file this layer owns
  :::
