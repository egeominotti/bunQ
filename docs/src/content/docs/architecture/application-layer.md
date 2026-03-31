---
title: "bunqueue Application Layer: Operations, Stall Detection & DLQ Management"
description: "bunqueue application layer: PUSH/PULL/ACK operations, stall detection, dependency resolution, DLQ management, and job lifecycle flows."
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og-image.png
---

# Application Layer Architecture

The application layer orchestrates all queue operations, coordinating between the client layer and domain layer.

## Module Structure

```
src/application/
├── queueManager.ts        # Central orchestrator
├── operations/            # PUSH, PULL, ACK, Query
├── backgroundTasks.ts     # Task orchestration
├── cleanupTasks.ts        # Memory cleanup, orphan removal
├── clientTracking.ts      # Client connection tracking
├── contextFactory.ts      # Context creation helpers
├── dependencyProcessor.ts # Dependency resolution
├── dlqManager.ts          # Dead letter queue
├── eventsManager.ts       # Event pub/sub
├── jobLogsManager.ts      # Job logs management
├── lockManager.ts         # Lock management
├── lockOperations.ts      # Lock acquire/release ops
├── metricsExporter.ts     # Prometheus metrics export
├── stallDetection.ts      # Stall detection
├── statsManager.ts        # Queue statistics
├── types.ts               # Shared type definitions
├── webhookManager.ts      # Webhook notifications
└── workerManager.ts       # Worker tracking
```

## QueueManager Orchestration

```
┌─────────────────────────────────────────────────────────────┐
│                    QUEUE MANAGER                             │
│                                                              │
│  ┌────────────────────────────────────────────────────────┐ │
│  │                     STATE                               │ │
│  │                                                         │ │
│  │  shards[N] ◄──► shardLocks[N]    (N = auto-detected)  │ │
│  │  processingShards[N] ◄──► processingLocks[N]          │ │
│  │                                                         │ │
│  │  jobIndex: Map<id, location>                           │ │
│  │  completedJobs: BoundedSet (50k)                       │ │
│  │  jobResults: LRU (5k)                                  │ │
│  │  customIdMap: LRU (50k)                                │ │
│  └────────────────────────────────────────────────────────┘ │
│                                                              │
│  ┌────────────────────────────────────────────────────────┐ │
│  │                   OPERATIONS                            │ │
│  │                                                         │ │
│  │  push() ──► operations/push.ts                         │ │
│  │  pull() ──► operations/pull.ts                         │ │
│  │  ack()  ──► operations/ack.ts                          │ │
│  │  query  ──► operations/queryOperations.ts              │ │
│  └────────────────────────────────────────────────────────┘ │
│                                                              │
│  ┌────────────────────────────────────────────────────────┐ │
│  │                   MANAGERS                              │ │
│  │                                                         │ │
│  │  DLQManager │ EventsManager │ WorkerManager            │ │
│  │  WebhookManager │ StatsManager │ JobLogsManager        │ │
│  └────────────────────────────────────────────────────────┘ │
│                                                              │
│  ┌────────────────────────────────────────────────────────┐ │
│  │               BACKGROUND TASKS                          │ │
│  │                                                         │ │
│  │  cleanup │ stall │ dependency │ dlq │ cron             │ │
│  └────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

## PUSH Operation Flow

```
push(queue, input)
         │
         ▼
┌─────────────────────────────────────────────────────────────┐
│  1. Generate UUIDv7 ID                                      │
│  2. Check customId idempotency (customIdMap)               │
│     └─ If exists: return existing job                       │
└─────────────┬───────────────────────────────────────────────┘
              │
              ▼
┌─────────────────────────────────────────────────────────────┐
│  3. Acquire shard write lock                                │
│     shardIdx = fnv1aHash(queue) & SHARD_MASK               │
└─────────────┬───────────────────────────────────────────────┘
              │
              ▼
┌─────────────────────────────────────────────────────────────┐
│  4. Check unique key deduplication                          │
│     │                                                        │
│     ├─ Key available? ──► Register key, continue            │
│     └─ Key exists?                                          │
│        ├─ strategy: replace ──► Remove old, insert new     │
│        ├─ strategy: extend ──► Reset TTL, return existing  │
│        └─ default ──► Return existing                       │
└─────────────┬───────────────────────────────────────────────┘
              │
              ▼
┌─────────────────────────────────────────────────────────────┐
│  5. Check dependencies                                      │
│     │                                                        │
│     ├─ All satisfied? ──► Push to queue                    │
│     └─ Not satisfied? ──► Add to waitingDeps               │
│                           Register in dependencyIndex       │
└─────────────┬───────────────────────────────────────────────┘
              │
              ▼
┌─────────────────────────────────────────────────────────────┐
│  6. Update jobIndex                                         │
│  7. Persist to SQLite (buffered or durable)                │
│  8. Notify waiters (wake long poll)                        │
│  9. Broadcast 'pushed' event                                │
└─────────────────────────────────────────────────────────────┘
```

## PULL Operation Flow

```
pull(queue, timeoutMs)
         │
         ▼
┌─────────────────────────────────────────────────────────────┐
│  LOOP:                                                      │
│                                                              │
│  1. Acquire shard read lock                                 │
│  2. Check queue paused? ──► return null                    │
│  3. Check rate limit ──► return null if exceeded           │
│  4. Check concurrency ──► return null if at limit          │
└─────────────┬───────────────────────────────────────────────┘
              │
              ▼
┌─────────────────────────────────────────────────────────────┐
│  5. Pop from priority queue                                 │
│     │                                                        │
│     ├─ TTL expired? ──► Skip, try next                     │
│     ├─ Not ready (delayed)? ──► Skip, try next             │
│     ├─ FIFO group active? ──► Skip, try next               │
│     └─ Valid job ──► Continue                               │
└─────────────┬───────────────────────────────────────────────┘
              │
     ┌────────┴────────┐
     │                 │
  Job found         No job
     │                 │
     ▼                 ▼
┌─────────────┐   ┌─────────────────────────┐
│ Move to     │   │ Wait for notification   │
│ processing  │   │ (event-based, timeout)  │
│ shard       │   └──────────┬──────────────┘
└──────┬──────┘              │
       │                     └───► Retry loop
       ▼
┌─────────────────────────────────────────────────────────────┐
│  6. Create lock token (if useLocks enabled)                │
│  7. Update jobIndex to 'processing'                        │
│  8. Mark active in SQLite                                  │
│  9. Broadcast 'pulled' event                               │
│  10. Return job with token                                 │
└─────────────────────────────────────────────────────────────┘
```

## ACK Operation Flow

```
ack(jobId, result, token)
         │
         ▼
┌─────────────────────────────────────────────────────────────┐
│  1. Verify lock token (if provided)                        │
│     └─ Mismatch? ──► Error: token invalid                  │
└─────────────┬───────────────────────────────────────────────┘
              │
              ▼
┌─────────────────────────────────────────────────────────────┐
│  2. Remove from processing shard                            │
│     procIdx = fnv1aHash(jobId) & SHARD_MASK                │
└─────────────┬───────────────────────────────────────────────┘
              │
              ▼
┌─────────────────────────────────────────────────────────────┐
│  3. Release shard resources                                 │
│     ├─ Release unique key                                   │
│     ├─ Release FIFO group                                   │
│     └─ Release concurrency slot                             │
└─────────────┬───────────────────────────────────────────────┘
              │
              ▼
┌─────────────────────────────────────────────────────────────┐
│  4. Finalize                                                │
│     ├─ Store result in jobResults (LRU)                    │
│     ├─ Store result in SQLite                              │
│     ├─ Update jobIndex to 'completed'                      │
│     └─ Add to completedJobs (signals deps)                 │
└─────────────┬───────────────────────────────────────────────┘
              │
              ▼
┌─────────────────────────────────────────────────────────────┐
│  5. Add to pendingDepChecks (wake dependents)              │
│  6. Broadcast 'completed' event                            │
│  7. Trigger webhooks                                        │
└─────────────────────────────────────────────────────────────┘
```

## Background Tasks

```
┌─────────────────────────────────────────────────────────────┐
│                  BACKGROUND TASK SCHEDULER                   │
│                                                              │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │ Cleanup      │  │ Stall Check  │  │ Dependency   │      │
│  │ every 10s    │  │ every 5s     │  │ every 100ms  │      │
│  └──────────────┘  └──────────────┘  └──────────────┘      │
│                                                              │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │ DLQ Maint    │  │ Lock Expire  │  │ Cron         │      │
│  │ every 60s    │  │ every 5s     │  │ every 1s     │      │
│  └──────────────┘  └──────────────┘  └──────────────┘      │
│                                                              │
│  Circuit breaker: After 5 consecutive failures,             │
│  log CRITICAL warning but continue retrying                 │
└─────────────────────────────────────────────────────────────┘
```

### Stall Detection (Two-Phase)

```
┌─────────────────────────────────────────────────────────────┐
│                    STALL CHECK (every 5s)                    │
│                                                              │
│  PHASE 1: Process previous candidates                       │
│  ┌────────────────────────────────────────────────────────┐ │
│  │ For each job in stalledCandidates:                     │ │
│  │   ├─ Still in processing?                              │ │
│  │   ├─ Get stall config                                  │ │
│  │   └─ If confirmed stalled:                             │ │
│  │      ├─ stallCount < maxStalls ──► Increment + retry   │ │
│  │      └─ stallCount >= maxStalls ──► Move to DLQ        │ │
│  └────────────────────────────────────────────────────────┘ │
│                                                              │
│  PHASE 2: Mark new candidates                               │
│  ┌────────────────────────────────────────────────────────┐ │
│  │ For each job in processingShards:                      │ │
│  │   ├─ No heartbeat for > stallInterval (30s)?          │ │
│  │   └─ Add to stalledCandidates (check next tick)       │ │
│  └────────────────────────────────────────────────────────┘ │
│                                                              │
│  Why two-phase? Prevents false positives from               │
│  transient delays (GC pause, network hiccup)               │
└─────────────────────────────────────────────────────────────┘
```

### Dependency Resolution

```
┌─────────────────────────────────────────────────────────────┐
│              DEPENDENCY PROCESSOR (every 100ms)              │
│                                                              │
│  1. Collect completedIds from pendingDepChecks             │
│     (Set of jobs that completed since last check)          │
│                                                              │
│  2. For each completedId:                                   │
│     └─ Look up dependencyIndex[completedId]                │
│        └─ Returns: Set<jobIds waiting for this job>        │
│                                                              │
│  3. Group by shard for efficient locking                   │
│                                                              │
│  4. For each waiting job:                                   │
│     └─ Check: ALL dependencies completed?                  │
│        └─ completedJobs.has(depId) for all deps           │
│                                                              │
│  5. If all satisfied:                                       │
│     ├─ Remove from waitingDeps                             │
│     ├─ Unregister from dependencyIndex                     │
│     └─ Push to active queue                                │
└─────────────────────────────────────────────────────────────┘
```

### Cleanup Tasks

```
┌─────────────────────────────────────────────────────────────┐
│                  CLEANUP (every 10s)                         │
│                                                              │
│  1. Refresh delayed counts in each shard                   │
│                                                              │
│  2. Compact priority queues                                 │
│     └─ If stale ratio > 20%: rebuild heap                  │
│                                                              │
│  3. Clean orphaned processing entries                       │
│     └─ Jobs stuck > 30min with no heartbeat               │
│                                                              │
│  4. Clean stale waiting dependencies                        │
│     └─ Waiting > 1 hour                                    │
│                                                              │
│  5. Clean expired unique keys                               │
│                                                              │
│  6. Clean orphaned job index entries                        │
│                                                              │
│  7. Remove empty queues                                     │
└─────────────────────────────────────────────────────────────┘
```

## Event Broadcasting

```
┌─────────────────────────────────────────────────────────────┐
│                  EVENTS MANAGER                              │
│                                                              │
│  Event occurs (completed, failed, progress, stalled)        │
│         │                                                    │
│         ▼                                                    │
│  broadcast(event)                                           │
│         │                                                    │
│         ├──► Notify all subscribers (Set-based, O(1) add)  │
│         ├──► Trigger matching webhooks                      │
│         └──► Wake completion waiters                        │
│                                                              │
│  Event-based waiting (no polling):                          │
│  waitForJobCompletion(jobId, timeout)                       │
│    └─ Resolved when 'completed' event for jobId            │
└─────────────────────────────────────────────────────────────┘
```
