---
title: "bunqueue Data Structures: MinHeap, Skip List, LRU Cache & Hashing"
description: "Data structures powering bunqueue: 4-ary MinHeap, skip lists, LRU cache, FNV-1a hashing, and read-write locks with complexities."
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og-image.png
---

# Data Structures

bunqueue uses specialized data structures optimized for job queue operations.

## Overview

| Structure | Use Case | Complexity |
|-----------|----------|------------|
| 4-ary MinHeap | Priority queue, cron scheduling | O(log₄ n) |
| Skip List | Temporal indexing, delayed jobs | O(log n) |
| LRU Cache | Job results, custom IDs | O(1) |
| Hash (FNV-1a) | Sharding, distribution | O(n) |

## 4-ary MinHeap

Used for priority queues and cron scheduling.

```
┌─────────────────────────────────────────────────────────────┐
│                  WHY 4-ARY VS BINARY?                        │
│                                                              │
│  Binary Heap:                                               │
│  • Height: log₂(n) = 16 levels for 65k items               │
│  • 2 children per node                                      │
│  • More memory indirections                                 │
│                                                              │
│  4-ary Heap:                                                │
│  • Height: log₄(n) = 8 levels for 65k items                │
│  • 4 children per node                                      │
│  • Children fit in cache line (64 bytes)                   │
│  • Fewer cache misses                                       │
│                                                              │
│  Trade-off: 4 comparisons per level vs 2                   │
│  Win: Better cache locality outweighs extra comparisons    │
└─────────────────────────────────────────────────────────────┘
```

### Heap with Lazy Deletion

```
┌─────────────────────────────────────────────────────────────┐
│                  GENERATION TRACKING                         │
│                                                              │
│  Each entry has a generation number:                        │
│  { jobId, priority, runAt, generation: 42n }               │
│                                                              │
│  Index maps jobId → { job, generation }                    │
│                                                              │
│  REMOVE:                                                    │
│  └─ Delete from index (O(1))                               │
│  └─ Heap entry becomes "stale"                             │
│                                                              │
│  POP:                                                       │
│  └─ Loop: peek, check generation match                     │
│     └─ Mismatch? Skip (stale entry)                        │
│     └─ Match? Return job                                   │
│                                                              │
│  COMPACT (when stale ratio > 20%):                         │
│  └─ Filter valid entries                                   │
│  └─ Rebuild heap: O(n)                                     │
└─────────────────────────────────────────────────────────────┘
```

## Skip List

Used for temporal indexing and efficient range queries.

```
┌─────────────────────────────────────────────────────────────┐
│                  SKIP LIST STRUCTURE                         │
│                                                              │
│  Level 3:  ─────────────────────► [50] ─────────────────►   │
│  Level 2:  ─────► [25] ─────────► [50] ─────────► [75] ─►   │
│  Level 1:  ─► [10] ─► [25] ─► [30] ─► [50] ─► [60] ─► [75]  │
│  Level 0:  ─► [10] ─► [25] ─► [30] ─► [50] ─► [60] ─► [75]  │
│                                                              │
│  Properties:                                                │
│  • Probabilistic level assignment (p=0.5)                  │
│  • Expected height: O(log n)                               │
│  • Simpler than balanced trees                             │
│  • Good cache locality (sequential links)                  │
└─────────────────────────────────────────────────────────────┘
```

### Range Queries

```
┌─────────────────────────────────────────────────────────────┐
│                  RANGE QUERY                                 │
│                                                              │
│  getOldJobs(threshold, limit):                              │
│                                                              │
│  1. Navigate to leftmost element: O(log n)                 │
│  2. Walk forward at level 0: O(k)                          │
│  3. Collect while createdAt < threshold                    │
│                                                              │
│  Total: O(log n + k) where k = results                     │
│                                                              │
│  Use case: Find jobs older than X for cleanup              │
└─────────────────────────────────────────────────────────────┘
```

## LRU Cache

Used for job results, custom ID mapping, and logs.

```
┌─────────────────────────────────────────────────────────────┐
│                  DOUBLY-LINKED LRU                           │
│                                                              │
│  Structure:                                                 │
│  ┌─────────────────────────────────────────────────────┐   │
│  │ Map<Key, Node> + Doubly-Linked List                 │   │
│  │                                                      │   │
│  │ HEAD (most recent) ◄──────────────────► TAIL (LRU)  │   │
│  │   [A] ◄──► [B] ◄──► [C] ◄──► [D]                   │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                              │
│  GET(key):                                                  │
│  └─ Find in map: O(1)                                      │
│  └─ Move to head: O(1) pointer updates                     │
│                                                              │
│  SET(key, value):                                          │
│  └─ If at capacity: remove tail (evict LRU)               │
│  └─ Add new node at head                                   │
│                                                              │
│  All operations: O(1)                                       │
└─────────────────────────────────────────────────────────────┘
```

### Memory Bounds

```
┌─────────────────────────────────────────────────────────────┐
│                  BOUNDED COLLECTIONS                         │
│                                                              │
│  Collection        │ Max Size │ Eviction                   │
│  ──────────────────┼──────────┼───────────────────────────│
│  completedJobs     │ 50,000   │ FIFO batch (10%)          │
│  jobResults        │ 5,000    │ LRU                       │
│  jobLogs           │ 10,000   │ LRU                       │
│  customIdMap       │ 50,000   │ LRU                       │
│  DLQ per queue     │ 10,000   │ FIFO                      │
│                                                              │
│  BoundedSet (FIFO):                                        │
│  └─ No recency tracking (faster)                           │
│  └─ Batch eviction: remove 10% when full                  │
│  └─ Amortized cost across many operations                  │
└─────────────────────────────────────────────────────────────┘
```

## Hash Function (FNV-1a)

Used for sharding and distribution.

```
┌─────────────────────────────────────────────────────────────┐
│                  FNV-1a HASH                                 │
│                                                              │
│  Algorithm:                                                 │
│  hash = FNV_OFFSET (0x811c9dc5)                            │
│  for each byte:                                             │
│    hash = hash XOR byte                                    │
│    hash = hash * FNV_PRIME (0x01000193)                    │
│  return hash as unsigned 32-bit                            │
│                                                              │
│  Properties:                                                │
│  • Fast: ~10-15 CPU cycles per character                  │
│  • Good distribution                                        │
│  • Deterministic                                            │
│  • Non-cryptographic (speed over security)                 │
└─────────────────────────────────────────────────────────────┘
```

### Sharding

```
┌─────────────────────────────────────────────────────────────┐
│                  SHARD SELECTION                             │
│                                                              │
│  shardIndex = fnv1aHash(queueName) & SHARD_MASK            │
│                                                              │
│  SHARD_COUNT = auto-detected from CPU cores (power of 2)   │
│  SHARD_MASK = SHARD_COUNT - 1                              │
│                                                              │
│  Examples:                                                  │
│  • 4 cores  → SHARD_COUNT=4,  SHARD_MASK=0x03 (binary: 11) │
│  • 10 cores → SHARD_COUNT=16, SHARD_MASK=0x0f (binary: 1111)│
│  • 20 cores → SHARD_COUNT=32, SHARD_MASK=0x1f (binary: 11111)│
│  • 64+ cores → SHARD_COUNT=64 (capped)                      │
│                                                              │
│  Why bitwise AND?                                          │
│  • 3-5x faster than modulo                                 │
│  • Requires power-of-2 shard count                         │
│  • hash & SHARD_MASK equivalent to hash % SHARD_COUNT      │
└─────────────────────────────────────────────────────────────┘
```

## Lock Structures

### RWLock (Read-Write Lock)

```
┌─────────────────────────────────────────────────────────────┐
│                  READ-WRITE LOCK                             │
│                                                              │
│  Allows:                                                    │
│  • Multiple concurrent readers                              │
│  • Single exclusive writer                                  │
│  • Writer priority (prevents starvation)                   │
│                                                              │
│  Fast Path (uncontested write):                            │
│  if (!writer && readers === 0) {                           │
│    writer = true;                                          │
│    return guard;  // Synchronous, no Promise               │
│  }                                                          │
│                                                              │
│  Timeout Cancellation:                                      │
│  └─ Mark entry as cancelled (O(1))                         │
│  └─ Skip cancelled entries on release                      │
│  └─ No O(n) array splice                                   │
└─────────────────────────────────────────────────────────────┘
```

## Complexity Summary

| Operation | Structure | Time |
|-----------|-----------|------|
| Push job | 4-ary heap | O(log₄ n) |
| Pop job | 4-ary heap | O(log₄ n) |
| Find job | Index map | O(1) |
| Remove job | Lazy deletion | O(1) |
| Get result | LRU map | O(1) |
| Shard lookup | Hash + AND | O(len) |
| Range query | Skip list | O(log n + k) |
| Lock acquire | RWLock | O(1) uncontested |
