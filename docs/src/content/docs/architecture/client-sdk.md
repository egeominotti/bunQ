---
title: "bunqueue Client SDK Architecture: Connection Pooling & Worker Modes"
description: "bunqueue Client SDK internals: TCP connection pooling, embedded vs server mode, worker heartbeats, ACK batching, and auto-batching."
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og-image.png
---

# Client SDK Architecture

The client layer provides the interface for applications to interact with bunqueue. It supports both **embedded** (in-process) and **TCP** (server) modes.

## Module Structure

```
src/client/
├── queue/          # Job submission (Queue class)
├── worker/         # Job processing (Worker class)
├── tcp/            # Network communication
├── flow.ts         # Job dependencies (FlowProducer)
└── queueGroup.ts   # Namespace isolation
```

## Dual-Mode Architecture

```
                    ┌─────────────────────────────────────┐
                    │           APPLICATION               │
                    │                                     │
                    │  Queue.add()      Worker.process()  │
                    └────────┬─────────────────┬──────────┘
                             │                 │
              ┌──────────────┴─────────────────┴──────────────┐
              │                                               │
    ┌─────────▼─────────┐                     ┌───────────────▼───────┐
    │   EMBEDDED MODE   │                     │      TCP MODE         │
    │                   │                     │                       │
    │  Direct calls to  │                     │  TcpPool ──► Server   │
    │  QueueManager     │                     │  (msgpack protocol)   │
    └───────────────────┘                     └───────────────────────┘
```

| Mode | Throughput | Use Case |
|------|------------|----------|
| Embedded | ~100k jobs/sec | Single process |
| TCP | Network limited | Distributed workers |

## Job Submission Flow

```
Queue.add(name, data, options)
         │
         ▼
┌─────────────────────────────┐
│ Merge options with defaults │
└─────────────┬───────────────┘
              │
              ▼
    ┌─────────────────────┐
    │ Mode check          │
    └─────────┬───────────┘
              │
     ┌────────┴────────┐
     ▼                 ▼
┌─────────┐       ┌─────────────────────┐
│EMBEDDED │       │       TCP           │
│         │       │                     │
│ Direct  │       │ tcpPool.send({      │
│ manager │       │   cmd: 'PUSH',      │
│ .push() │       │   queue, data,      │
│         │       │   priority, delay,  │
└────┬────┘       │   ...options        │
     │            │ })                  │
     │            └──────────┬──────────┘
     │                       │
     └───────────┬───────────┘
                 ▼
         ┌──────────────┐
         │ Return Job   │
         │ with methods │
         └──────────────┘
```

## Worker Processing Flow

```
Worker(queue, processor, options)
         │
         ▼
┌─────────────────────────────┐
│ Start heartbeat timer       │
│ (default: 10s interval)     │
└─────────────┬───────────────┘
              │
              ▼
┌─────────────────────────────┐
│ Poll loop (respects         │◄──────────────┐
│ concurrency limit)          │               │
└─────────────┬───────────────┘               │
              │                               │
              ▼                               │
┌─────────────────────────────┐               │
│ Pull batch from server      │               │
│ (PULLB command)             │               │
└─────────────┬───────────────┘               │
              │                               │
              ▼                               │
┌─────────────────────────────┐               │
│ For each job:               │               │
│ 1. Mark active              │               │
│ 2. Execute processor(job)   │               │
│ 3. On success: ACK batch    │               │
│ 4. On failure: FAIL         │               │
└─────────────┬───────────────┘               │
              │                               │
              └───────────────────────────────┘
```

## Connection Pool Architecture

```
┌─────────────────────────────────────────────────────┐
│                  TcpConnectionPool                   │
│                                                     │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌────────┐ │
│  │ Client 1 │ │ Client 2 │ │ Client 3 │ │Client 4│ │
│  │          │ │          │ │          │ │        │ │
│  │ socket   │ │ socket   │ │ socket   │ │ socket │ │
│  │ parser   │ │ parser   │ │ parser   │ │ parser │ │
│  │ health   │ │ health   │ │ health   │ │ health │ │
│  └──────────┘ └──────────┘ └──────────┘ └────────┘ │
│                                                     │
│  Round-robin selection │ Health tracking            │
│  Auto-reconnect        │ Shared pool management     │
└─────────────────────────────────────────────────────┘
```

**Key Features:**
- 4 connections per pool (default)
- Load-aware client selection
- Automatic reconnection with exponential backoff
- Shared pools across Queue/Worker instances

## Heartbeat & Stall Detection

```
┌─────────────────────────────────────────────────────┐
│                    WORKER                            │
│                                                      │
│  heartbeatTimer ─────► every 10s ─────► Server      │
│       │                    │                         │
│       │            ┌───────▼───────┐                │
│       │            │ JobHeartbeatB │                │
│       │            │ { ids, tokens}│                │
│       │            └───────────────┘                │
│       │                                             │
│  pulledJobIds ◄─── All pulled jobs get heartbeat   │
│  activeJobIds ◄─── Jobs being processed            │
│  jobTokens    ◄─── Lock tokens for verification    │
└─────────────────────────────────────────────────────┘

                         │
                         ▼
┌─────────────────────────────────────────────────────┐
│                    SERVER                            │
│                                                      │
│  If no heartbeat for stallInterval (30s):           │
│  1. Mark job as stalled                             │
│  2. Increment stallCount                            │
│  3. After maxStalls (3): move to DLQ               │
└─────────────────────────────────────────────────────┘
```

## ACK Batching Flow

```
Job completes
     │
     ▼
┌─────────────────────────────┐
│ AckBatcher.queue(id, result)│
└─────────────┬───────────────┘
              │
              ▼
┌─────────────────────────────┐
│ Buffer pending ACKs         │
│ (max 10 or 50ms timeout)    │
└─────────────┬───────────────┘
              │
     ┌────────┴────────┐
     │                 │
     ▼                 ▼
┌─────────┐       ┌─────────┐
│ Batch   │       │ Timer   │
│ full    │       │ fires   │
└────┬────┘       └────┬────┘
     │                 │
     └────────┬────────┘
              ▼
┌─────────────────────────────┐
│ Send ACKB { ids, results,   │
│             tokens }        │
└─────────────────────────────┘
```

**Benefits:**
- Reduces network round-trips
- Batches lock verification
- Handles retry on failure

## FlowProducer (Dependencies)

```
Chain: A ──► B ──► C
       │         │
       └─dependsOn

addChain([A, B, C])
         │
         ▼
┌─────────────────────────────┐
│ A: no dependencies ──► queue│
│ B: dependsOn: [A]           │
│ C: dependsOn: [B]           │
└─────────────────────────────┘
         │
         ▼
Server tracks in waitingDeps
until dependencies complete
```

## Graceful Shutdown

```
worker.close()
     │
     ▼
┌─────────────────────────────┐
│ 1. Stop poll loop           │
│ 2. Stop heartbeat           │
│ 3. Wait active jobs finish  │
│ 4. Flush pending ACKs       │
│ 5. Wait in-flight flushes   │
│ 6. Close TCP connections    │
└─────────────────────────────┘
```
