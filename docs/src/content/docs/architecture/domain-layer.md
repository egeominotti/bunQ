---
title: "bunqueue Domain Layer: Sharding, Priority Queues & Job State Machine"
description: "bunqueue domain layer internals: auto-scaled sharding, 4-ary priority queues, job state machine, DLQ flow, and rate limiting logic."
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og-image.png
---

# Domain Layer Architecture

The domain layer contains the pure business logic of bunqueue. No external dependencies, just core algorithms and data structures.

## Module Structure

```
src/domain/
├── types/           # Type definitions
└── queue/           # Core queue logic
    ├── shard.ts             # Shard container
    ├── priorityQueue.ts     # 4-ary indexed heap
    ├── dlqShard.ts          # Dead letter queue
    ├── uniqueKeyManager.ts  # Deduplication
    ├── limiterManager.ts    # Rate/concurrency
    ├── dependencyTracker.ts # Job dependencies
    └── temporalManager.ts   # Delayed jobs
```

## Sharding Architecture

Jobs are distributed across N shards (auto-detected from CPU cores) for parallelism:

```
┌─────────────────────────────────────────────────────────────┐
│                    QUEUE MANAGER                             │
│                                                              │
│  ┌──────────────────────────────────────────────────────┐   │
│  │         N INDEPENDENT SHARDS (auto-detected)          │   │
│  │         Power of 2, based on CPU cores, max 64        │   │
│  │                                                       │   │
│  │  queueName ──► fnv1aHash() ──► & SHARD_MASK ──► idx  │   │
│  │                                                       │   │
│  │  ┌────────┬────────┬────────┬─────────┬────────┐     │   │
│  │  │Shard 0 │Shard 1 │Shard 2 │  ...    │Shard N │     │   │
│  │  │        │        │        │         │        │     │   │
│  │  │ queues │ queues │ queues │         │ queues │     │   │
│  │  │ unique │ unique │ unique │         │ unique │     │   │
│  │  │ dlq    │ dlq    │ dlq    │         │ dlq    │     │   │
│  │  │ limits │ limits │ limits │         │ limits │     │   │
│  │  └────────┴────────┴────────┴─────────┴────────┘     │   │
│  └──────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

### Shard Composition

Each shard is a composition of managers:

```
┌─────────────────────────────────────────┐
│                SHARD                     │
│                                         │
│  queues: Map<string, PriorityQueue>     │
│                                         │
│  ┌─────────────────────────────────┐    │
│  │ UniqueKeyManager                │    │
│  │ • Deduplication with TTL        │    │
│  └─────────────────────────────────┘    │
│  ┌─────────────────────────────────┐    │
│  │ DlqShard                        │    │
│  │ • Failed job storage            │    │
│  └─────────────────────────────────┘    │
│  ┌─────────────────────────────────┐    │
│  │ LimiterManager                  │    │
│  │ • Rate & concurrency control    │    │
│  └─────────────────────────────────┘    │
│  ┌─────────────────────────────────┐    │
│  │ DependencyTracker               │    │
│  │ • waitingDeps + dependencyIndex │    │
│  └─────────────────────────────────┘    │
│  ┌─────────────────────────────────┐    │
│  │ TemporalManager                 │    │
│  │ • Delayed jobs (MinHeap)        │    │
│  └─────────────────────────────────┘    │
│                                         │
│  stats: { queued, delayed, dlq }        │
│  activeGroups: Map (FIFO groups)        │
│  waiters: Array (long poll support)     │
└─────────────────────────────────────────┘
```

## Priority Queue Flow

4-ary indexed heap with lazy deletion:

```
                    PUSH
                      │
                      ▼
┌─────────────────────────────────────────────────────────────┐
│  1. Generate generation number                              │
│  2. Add to index: Map<jobId, {job, generation}>            │
│  3. Push to heap: {jobId, priority, runAt, generation}     │
│  4. bubbleUp (O(log₄ n))                                   │
└─────────────────────────────────────────────────────────────┘

                    POP
                      │
                      ▼
┌─────────────────────────────────────────────────────────────┐
│  Loop:                                                      │
│    1. Peek heap top                                         │
│    2. Check index for matching generation                   │
│    3. If generation mismatch (stale): removeTop, continue   │
│    4. If match: removeTop, delete from index, return job    │
│  (O(log₄ n) amortized)                                     │
└─────────────────────────────────────────────────────────────┘

                   REMOVE (by jobId)
                      │
                      ▼
┌─────────────────────────────────────────────────────────────┐
│  1. Delete from index (O(1))                                │
│  2. Heap entry becomes "stale" (skipped on pop)            │
│  3. Compact heap when stale ratio > 20%                     │
└─────────────────────────────────────────────────────────────┘
```

## Job State Machine

```
                    ┌─────────────┐
                    │   WAITING   │◄────────── retry
                    └──────┬──────┘              │
                           │                     │
              delay > 0    │    delay = 0        │
              ┌────────────┼────────────┐        │
              ▼            │            ▼        │
       ┌─────────────┐     │     ┌─────────────┐ │
       │  DELAYED    │─────┼────►│  (ready)    │ │
       └─────────────┘     │     └─────────────┘ │
              │            │            │        │
              │  runAt     │            │        │
              │  reached   │            │        │
              └────────────┼────────────┘        │
                           │                     │
                           ▼                     │
                    ┌─────────────┐              │
                    │   ACTIVE    │──────────────┘
                    └──────┬──────┘         fail
                           │             (retryable)
              ┌────────────┼────────────┐
              │            │            │
           success      fail         timeout
              │       (max retries)     │
              ▼            ▼            ▼
       ┌─────────────┐ ┌─────────────┐
       │ COMPLETED   │ │    DLQ      │
       └─────────────┘ └─────────────┘
```

## Dependency Resolution Flow

```
┌─────────────────────────────────────────────────────────────┐
│                  JOB WITH DEPENDENCIES                       │
│                                                              │
│  Job B: dependsOn: [A]                                      │
│                                                              │
│  1. Push B ──► Check: is A completed?                       │
│     │                                                        │
│     ├─ NO ──► Add B to waitingDeps                          │
│     │         Register B in dependencyIndex[A]              │
│     │                                                        │
│     └─ YES ─► Push B to active queue                        │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│                  WHEN A COMPLETES                            │
│                                                              │
│  1. Add A.id to pendingDepChecks                            │
│  2. Background task (every 100ms):                          │
│     │                                                        │
│     ▼                                                        │
│  3. For each completedId:                                   │
│     └─ Get dependencyIndex[completedId] ──► Set<jobIds>     │
│                                                              │
│  4. For each waiting job:                                   │
│     └─ Check: all deps in completedJobs?                    │
│        │                                                     │
│        └─ YES ──► Move from waitingDeps to queue            │
└─────────────────────────────────────────────────────────────┘
```

**Reverse Index:**
```
dependencyIndex: Map<JobId, Set<JobId>>
  A ──► {B, C}  (B and C wait for A)
  D ──► {E}     (E waits for D)
```

## DLQ (Dead Letter Queue) Flow

```
Job fails with attempts >= maxAttempts
         │
         ▼
┌─────────────────────────────────────────────────────────────┐
│                  MOVE TO DLQ                                 │
│                                                              │
│  DlqEntry:                                                  │
│  ├─ job: original job                                       │
│  ├─ reason: explicit_fail | max_attempts_exceeded | timeout │
│  │          | stalled | ttl_expired | worker_lost | unknown │
│  ├─ error: error message                                    │
│  ├─ attempts: full history [{attempt, error, duration}]     │
│  ├─ enteredAt: timestamp                                    │
│  ├─ nextRetryAt: if autoRetry enabled                       │
│  └─ expiresAt: 7 days default                               │
└─────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────┐
│               DLQ MAINTENANCE (every 60s)                    │
│                                                              │
│  1. Auto-retry eligible entries                             │
│     └─ nextRetryAt <= now && retryCount < maxAutoRetries    │
│                                                              │
│  2. Purge expired entries                                   │
│     └─ expiresAt <= now                                     │
│                                                              │
│  3. Enforce maxEntries per queue (10k default)              │
│     └─ FIFO eviction when full                              │
└─────────────────────────────────────────────────────────────┘
```

## Rate & Concurrency Limiting

```
┌─────────────────────────────────────────────────────────────┐
│                    PULL REQUEST                              │
│                                                              │
│  1. Check rate limit (token bucket)                         │
│     │                                                        │
│     ├─ tokens available? ──► consume 1, proceed             │
│     └─ no tokens? ──► return null                           │
│                                                              │
│  2. Check concurrency limit                                 │
│     │                                                        │
│     ├─ active < limit? ──► increment, proceed               │
│     └─ at limit? ──► return null                            │
│                                                              │
│  3. Pop from priority queue                                 │
│                                                              │
└─────────────────────────────────────────────────────────────┘

Token Bucket:
  ┌─────────────────────────────────────────┐
  │ capacity: N tokens                       │
  │ refillRate: N tokens/sec                 │
  │                                          │
  │ tryAcquire():                           │
  │   1. Refill based on elapsed time        │
  │   2. If tokens >= 1: consume, return true│
  │   3. Else: return false                  │
  └─────────────────────────────────────────┘
```

## FIFO Groups

Ensures only one job per group processes at a time:

```
Job with groupId: "user-123"
         │
         ▼
┌─────────────────────────────────────────────────────────────┐
│  PULL:                                                       │
│  1. Pop job from queue                                      │
│  2. Check: is groupId in activeGroups?                      │
│     │                                                        │
│     ├─ YES ──► Skip this job, try next                      │
│     └─ NO ──► Add to activeGroups, return job               │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│  ACK/FAIL:                                                   │
│  1. Remove groupId from activeGroups                        │
│  2. Next job in same group can now be pulled                │
└─────────────────────────────────────────────────────────────┘
```
