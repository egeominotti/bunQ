---
title: "Storage: SQLite by Design, No Postgres Needed"
description: "bunqueue persists jobs in a single local SQLite file, no Postgres, MySQL, or Redis. Why, and how to deploy on serverless and ephemeral platforms anyway."
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/guide/databases.png
---

<div class="bq-wrap bq-hero">
  <span class="bq-eyebrow">guide · storage</span>
  <h1 class="bq-hero-h1 bq-bench-h1">SQLite by default, tuned for <em>queues.</em></h1>
  <p class="bq-hero-sub">bunqueue stores jobs in a single local SQLite file. No Postgres, no MySQL, no Redis, and none planned. This page explains why, and what to do on platforms without a durable disk.</p>
</div>

This page answers two questions evaluators ask: "can I point bunqueue at my Postgres?" (no, by design) and "how do I keep jobs safe on a platform where the disk disappears on restart?" (three patterns, below).

## The short answer

bunqueue needs **zero external infrastructure**. Persistence is one SQLite file, and the only runtime dependencies are `croner` and `msgpackr`. If your host has a disk that survives restarts, you are done:

```bash
# Server mode: point the data path at durable storage
BUNQUEUE_DATA_PATH=/data/bunq.db bunqueue start
```

```typescript
// Embedded mode (queue runs inside your app's process)
const queue = new Queue('jobs', { embedded: true, dataPath: '/data/jobs.db' });
```

The rest of this page covers hosts where the disk does not survive restarts.

## Deploying without a durable local disk

### Option 1: Mount a persistent volume (simplest)

Most container platforms can attach a durable disk. Point the data path at it and SQLite behaves normally across restarts.

| Platform | Durable storage |
| --- | --- |
| Fly.io | Fly Volumes |
| Railway | Volumes |
| Render | Persistent Disks |
| Docker / Compose | A named volume mounted at the data directory |
| Kubernetes | A PersistentVolumeClaim |

:::note
bunqueue runs SQLite in WAL mode (write-ahead logging, a mode that allows concurrent reads and writes). WAL creates `-wal` and `-shm` sidecar files next to the `.db` file, so mount the whole data directory on the durable volume, not just the database file.
:::

### Option 2: Store-and-forward from ephemeral instances

If instances scale to zero and no disk can be attached, run bunqueue embedded on the instance and forward jobs to one central, durable bunqueue server:

```typescript
const local = new Queue('ingest', { embedded: true, dataPath: '/tmp/spool.db' });

const forwarder = local.forward({
  to: { host: 'central.internal', port: 6789, tls: true },
  queue: 'ingest', // optional remote queue name
});
```

If the central server is unreachable, jobs stay local and retry; permanent failures land in the local DLQ (dead letter queue, the holding area for jobs that exhausted their retries). Nothing is lost while offline. Full walkthrough: [IoT & Edge](/guide/iot-edge/).

### Option 3: One central server, stateless workers

Run a single bunqueue server on a host with a durable disk. Producers and workers connect over TCP and hold no state themselves:

```typescript
const queue = new Queue('jobs', { connection: { host: 'queue.internal', port: 6789 } });
const worker = new Worker('jobs', processor, {
  connection: { host: 'queue.internal', port: 6789 },
});
```

Clients exist for Node.js, Deno, Python, and Cloudflare Workers, see [SDKs](/guide/sdks/).

### Choosing

| Your situation | Use |
| --- | --- |
| Container with an attachable disk | Persistent volume (Option 1) |
| Scale-to-zero instances, want local durability first | Store-and-forward (Option 2) |
| Many stateless workers, one durable host | Central TCP server (Option 3) |
| The queue must live inside a shared Postgres | A Postgres-native queue (below) |

## Why not Postgres or MySQL?

Two engineering reasons, beyond the zero-infrastructure philosophy:

1. **Synchronous storage is load-bearing.** The storage layer is built on `bun:sqlite`, which is synchronous. Bun's Postgres and MySQL client (`bun:sql`) is async only, and making storage async would push `await` through the hot push, pull, and ack paths, adding latency exactly where bunqueue is tuned to be fast.

2. **A database swap would not buy horizontal scaling.** bunqueue is a single-process engine: in-memory queues are the source of truth at runtime, and SQLite is the durability log behind them. There is no distributed locking, so two bunqueue processes sharing one Postgres would double-run jobs. Real multi-instance elasticity would be a different product, not a storage adapter.

If a shared SQL database as the queue is a hard requirement, use a Postgres-native queue such as [pg-boss](https://github.com/timgit/pg-boss) or [graphile-worker](https://github.com/graphile/worker). bunqueue optimizes for a different point: one fast process with nothing else to operate.

## Will Postgres support be added?

Not planned. It could be reconsidered as an optional durability adapter if strong, specific demand emerges, but as explained above it would not add clustering by itself. Share requirements on [discussion #105](https://github.com/egeominotti/bunqueue/discussions/105).

## See also

- [Deployment Guide](/guide/deployment/) - Docker, systemd, and PM2
- [IoT & Edge](/guide/iot-edge/) - Store-and-forward in depth
- [Environment Variables](/guide/env-vars/) - `BUNQUEUE_DATA_PATH` and friends
- [Comparison](/guide/comparison/) - bunqueue vs Redis-backed queues
- [FAQ](/faq/)
