---
title: "bunqueue Architecture: Sharded Job Queue System Design for Bun"
description: "Architecture overview of bunqueue: auto-scaling shards, TCP protocol, SQLite WAL persistence, and background task scheduling internals."
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og-image.png
---

# Architecture Overview

bunqueue is a high-performance job queue built for Bun with SQLite persistence. This section covers the internal architecture, data flows, and design decisions.

## System Overview

```
                            ┌─────────────────────────────────────┐
                            │            CLIENT LAYER              │
                            │  Queue.add() ──────► Worker.process()│
                            │       │                    ▲         │
                            │       ▼                    │         │
                            │   TcpPool ◄── msgpack ──► TcpPool   │
                            └───────┬────────────────────┬─────────┘
                                    │ TCP :6789          │
                            ┌───────▼────────────────────▼─────────┐
                            │            SERVER LAYER              │
                            │                                      │
                            │  ┌────────────────────────────────┐  │
                            │  │         QueueManager           │  │
                            │  │                                │  │
                            │  │  ┌──────────────────────────┐  │  │
                            │  │  │   N Shards (auto-detect) │  │  │
                            │  │  │  ┌──────┬──────┬──────┐  │  │  │
                            │  │  │  │Shard0│Shard1│ ...N │  │  │  │
                            │  │  │  └──────┴──────┴──────┘  │  │  │
                            │  │  └──────────────────────────┘  │  │
                            │  │                                │  │
                            │  │  jobIndex │ completedJobs      │  │
                            │  │  customIdMap │ jobResults      │  │
                            │  └────────────────────────────────┘  │
                            │                  │                   │
                            │  ┌───────────────▼───────────────┐   │
                            │  │      PERSISTENCE LAYER        │   │
                            │  │  WriteBuffer ──► SQLite (WAL) │   │
                            │  └───────────────────────────────┘   │
                            │                                      │
                            │  ┌────────────────────────────────┐  │
                            │  │     BACKGROUND TASKS           │  │
                            │  │  Scheduler │ Stall Detection   │  │
                            │  │  DLQ Maint │ Cleanup           │  │
                            │  └────────────────────────────────┘  │
                            └──────────────────────────────────────┘
```

## Layered Architecture

| Layer | Purpose | Key Components |
|-------|---------|----------------|
| **Client** | SDK for applications | Queue, Worker, FlowProducer, TcpPool |
| **Server** | Request handling | TcpServer, HttpServer, Handlers |
| **Application** | Orchestration | QueueManager, Operations, Managers |
| **Domain** | Business logic | Shard, PriorityQueue, DLQ |
| **Infrastructure** | External systems | SQLite, S3 Backup, Scheduler |
| **Shared** | Utilities | Hash, Lock, LRU, MinHeap |

## Architecture Sections

| Section | Description |
|---------|-------------|
| [Client SDK](/architecture/client-sdk/) | TCP connection, job submission, worker processing |
| [Domain Layer](/architecture/domain-layer/) | Sharding, priority queues, DLQ logic |
| [Application Layer](/architecture/application-layer/) | Operations flow, background tasks |
| [Persistence](/architecture/persistence/) | SQLite configuration, write buffering, servers |
| [Data Structures](/architecture/data-structures/) | Core algorithms and complexities |
| [TCP Protocol](/architecture/tcp-protocol/) | Wire format and commands |

## Key Design Decisions

### Dynamic Shard Architecture

Jobs are distributed across N independent shards (auto-detected from CPU cores) using FNV-1a hash:

```
SHARD_COUNT = calculateShardCount()  // Power of 2, based on CPU cores, max 64
SHARD_MASK = SHARD_COUNT - 1
shardIndex = fnv1aHash(queueName) & SHARD_MASK

// Examples: 4 cores → 4 shards, 10 cores → 16 shards, 64+ cores → 64 shards
```

**Benefits:**
- Auto-scales with hardware (power of 2, max 64)
- Parallel operations on different queues
- Reduced lock contention
- Bitwise AND faster than modulo

### 4-ary Priority Queue

Each shard contains a 4-ary heap instead of binary:

- Better cache locality (children fit in cache line)
- Fewer tree levels (8 vs 16 for 65k items)
- O(log₄ n) operations

### Write Buffer

Jobs batch before SQLite write:

```
┌─────────┐   10ms or    ┌───────────────────┐
│ Buffer  │ ──────────►  │ Multi-row INSERT  │
│ (100)   │   100 jobs   │ ~100k jobs/sec    │
└─────────┘              └───────────────────┘
```

- **Buffered**: ~100k jobs/sec, up to 10ms loss risk
- **Durable**: ~10k jobs/sec, immediate persistence

### Lazy Deletion

Heap entries use generation tracking:

```
Remove: Delete from index (O(1)), mark heap entry stale
Pop: Skip entries where generation != current
Compact: Rebuild when >20% stale
```

## Lock Hierarchy

Acquire in order to prevent deadlocks:

```
1. jobIndex (read-only)
2. completedJobs (check before lock)
3. shardLocks[N]
4. processingLocks[N]
```

## Memory Bounds

| Collection | Limit | Eviction |
|------------|-------|----------|
| completedJobs | 50,000 | FIFO batch |
| jobResults | 5,000 | LRU |
| jobLogs | 10,000 | LRU |
| customIdMap | 50,000 | LRU |
| DLQ per queue | 10,000 | FIFO |

## Performance Summary

| Operation | Complexity |
|-----------|-----------|
| PUSH | O(log₄ n) |
| PULL | O(log₄ n) |
| ACK | O(1) |
| ACK batch | O(shards) |
| Job lookup | O(1) |
| Stats | O(1) |

:::tip[Related]
- [Client SDK Architecture](/architecture/client-sdk/) - Queue, Worker, and connection pool
- [TCP Protocol Architecture](/architecture/tcp-protocol/) - Binary protocol and commands
- [SQLite Persistence Layer](/architecture/persistence/) - Write buffer and WAL mode
- [Core Data Structures](/architecture/data-structures/) - Skip lists, heaps, and LRU caches
:::
