---
title: "bunqueue SQLite Persistence: WAL Mode, Write Buffering & S3 Backups"
description: "bunqueue persistence layer: SQLite WAL mode config, write buffering, read-through cache, S3 backup flows, and durability guarantees."
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og-image.png
---

# Persistence Layer

bunqueue uses SQLite with WAL mode for persistence, optimized for high-throughput job processing.

## SQLite Configuration

```
┌─────────────────────────────────────────────────────────────┐
│                  SQLITE PRAGMAS                              │
│                                                              │
│  journal_mode = WAL     │ Write-Ahead Logging              │
│  synchronous = NORMAL   │ Balanced safety/performance      │
│  cache_size = -64000    │ 64MB in-memory cache             │
│  temp_store = MEMORY    │ In-memory temp tables            │
│  mmap_size = 268435456  │ 256MB memory-mapped I/O          │
│  page_size = 4096       │ 4KB pages                        │
└─────────────────────────────────────────────────────────────┘
```

## Write Buffer Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    WRITE BUFFER                              │
│                                                              │
│  Job arrives                                                │
│       │                                                      │
│       ▼                                                      │
│  ┌─────────────────┐                                        │
│  │ Add to buffer   │                                        │
│  │ (max 100 jobs)  │                                        │
│  └────────┬────────┘                                        │
│           │                                                  │
│     ┌─────┴─────────────┐                                   │
│     │                   │                                   │
│     ▼                   ▼                                   │
│ Buffer full (100)   Timer fires (10ms)                     │
│     │                   │                                   │
│     └─────────┬─────────┘                                   │
│               │                                              │
│               ▼                                              │
│  ┌─────────────────────────────────────────┐               │
│  │        BATCH INSERT                      │               │
│  │                                          │               │
│  │  INSERT INTO jobs VALUES                 │               │
│  │    (...), (...), (...), ...             │               │
│  │                                          │               │
│  │  • Prepared statement cache (1-100 rows) │               │
│  │  • Single transaction                    │               │
│  │  • 50-100x faster than individual        │               │
│  └─────────────────────────────────────────┘               │
└─────────────────────────────────────────────────────────────┘
```

## Buffered vs Durable Mode

```
┌─────────────────────────────────────────────────────────────┐
│                  WRITE MODES                                 │
│                                                              │
│  ┌────────────────────┬───────────────────────────────────┐│
│  │  BUFFERED (default)│  DURABLE (opt-in per job)         ││
│  ├────────────────────┼───────────────────────────────────┤│
│  │  ~100k jobs/sec    │  ~10k jobs/sec                    ││
│  │  Up to 10ms loss   │  No data loss                     ││
│  │  Batched writes    │  Immediate write                  ││
│  │                    │                                    ││
│  │  Use for:          │  Use for:                         ││
│  │  • Emails          │  • Payments                       ││
│  │  • Notifications   │  • Critical events                ││
│  │  • Analytics       │  • Financial transactions         ││
│  └────────────────────┴───────────────────────────────────┘│
│                                                              │
│  Usage:                                                     │
│  queue.add('job', data, { durable: true })                 │
└─────────────────────────────────────────────────────────────┘
```

## Database Schema

```
┌─────────────────────────────────────────────────────────────┐
│                    TABLES                                    │
│                                                              │
│  jobs (28 columns)                                          │
│  ├─ id: TEXT PRIMARY KEY (UUIDv7)                          │
│  ├─ queue: TEXT                                             │
│  ├─ data: BLOB (MessagePack)                               │
│  ├─ priority: INTEGER                                       │
│  ├─ state: TEXT                                            │
│  ├─ run_at: INTEGER                                        │
│  ├─ attempts: INTEGER                                       │
│  └─ ... (21 more fields)                                   │
│                                                              │
│  Indexes:                                                   │
│  ├─ (queue, state) ──► PULL queries                        │
│  ├─ (run_at) ──► Delayed job scheduler                     │
│  ├─ (queue, unique_key) ──► Deduplication                  │
│  ├─ (custom_id) ──► Idempotency                            │
│  ├─ (parent_id) ──► Parent job lookup                      │
│  ├─ (state, started_at) ──► Stall detection                │
│  ├─ (group_id) ──► Group operations                        │
│  └─ (queue, state, priority, run_at) ──► Priority pull     │
│                                                              │
│  job_results                                                │
│  ├─ job_id: TEXT PRIMARY KEY                               │
│  ├─ result: BLOB (MessagePack)                             │
│  └─ completed_at: INTEGER                                  │
│                                                              │
│  dlq                                                        │
│  ├─ id: INTEGER PRIMARY KEY AUTOINCREMENT                  │
│  ├─ job_id: TEXT                                           │
│  ├─ queue: TEXT                                            │
│  ├─ entry: BLOB (full DlqEntry, MessagePack)               │
│  └─ entered_at: INTEGER                                    │
│                                                              │
│  cron_jobs                                                  │
│  ├─ name: TEXT PRIMARY KEY                                 │
│  ├─ queue: TEXT                                            │
│  ├─ schedule: TEXT                                         │
│  ├─ next_run: INTEGER                                      │
│  ├─ executions: INTEGER                                    │
│  └─ timezone: TEXT                                         │
└─────────────────────────────────────────────────────────────┘
```

## Crash Recovery Flow

```
┌─────────────────────────────────────────────────────────────┐
│                  STARTUP RECOVERY                            │
│                                                              │
│  1. Load pending jobs (state = waiting/delayed)            │
│     └─ For each job:                                       │
│        ├─ Push to appropriate shard queue                  │
│        └─ Update jobIndex                                  │
│                                                              │
│  2. Load active jobs (state = active)                      │
│     └─ For each job:                                       │
│        ├─ Reset to waiting (worker may have died)          │
│        └─ Push back to queue                               │
│                                                              │
│  3. Load DLQ entries                                        │
│     └─ Restore to in-memory DLQ shards                     │
│                                                              │
│  4. Load cron jobs                                          │
│     └─ Populate cron scheduler heap                        │
└─────────────────────────────────────────────────────────────┘
```

## S3 Backup Flow

```
┌─────────────────────────────────────────────────────────────┐
│                  S3 BACKUP                                   │
│                                                              │
│  Scheduled: every 6 hours (configurable)                    │
│                                                              │
│  BACKUP:                                                    │
│  ┌────────────────────────────────────────────────────────┐│
│  │ 1. Check database file exists                          ││
│  │ 2. Generate key: backups/bunqueue-{timestamp}.db       ││
│  │ 3. Read file as ArrayBuffer                            ││
│  │ 4. Calculate SHA256 checksum                           ││
│  │ 5. Upload to S3 (application/x-sqlite3)               ││
│  │ 6. Upload metadata.json                                ││
│  │ 7. Cleanup old backups (keep N most recent)            ││
│  └────────────────────────────────────────────────────────┘│
│                                                              │
│  RESTORE:                                                   │
│  ┌────────────────────────────────────────────────────────┐│
│  │ 1. Download backup file from S3                        ││
│  │ 2. Load metadata.json                                  ││
│  │ 3. Verify SHA256 checksum                              ││
│  │ 4. Write to database path                              ││
│  │ 5. Restart server to load                              ││
│  └────────────────────────────────────────────────────────┘│
│                                                              │
│  Supports: AWS S3, Cloudflare R2, MinIO, DigitalOcean      │
└─────────────────────────────────────────────────────────────┘
```

## Flush on Failure

```
┌─────────────────────────────────────────────────────────────┐
│                  ERROR RECOVERY                              │
│                                                              │
│  flush() fails                                              │
│       │                                                      │
│       ▼                                                      │
│  ┌─────────────────────────────────────────┐               │
│  │ Re-add jobs to buffer                    │               │
│  │ this.buffer = jobs.concat(this.buffer)  │               │
│  │                                          │               │
│  │ Jobs not lost, will retry on next flush │               │
│  └─────────────────────────────────────────┘               │
│                                                              │
│  On shutdown:                                               │
│  ┌─────────────────────────────────────────┐               │
│  │ Final flush of remaining buffer         │               │
│  │ Wait for completion before exit         │               │
│  └─────────────────────────────────────────┘               │
└─────────────────────────────────────────────────────────────┘
```

## Serialization

```
┌─────────────────────────────────────────────────────────────┐
│                  MESSAGEPACK                                 │
│                                                              │
│  Why MessagePack instead of JSON?                           │
│  • 2-3x faster encoding/decoding                           │
│  • Smaller payload size                                     │
│  • Binary data support                                      │
│                                                              │
│  Used for:                                                  │
│  • Job data BLOB                                            │
│  • DLQ entry BLOB                                           │
│  • TCP protocol payloads                                    │
│  • Job results storage                                      │
└─────────────────────────────────────────────────────────────┘
```
