---
title: "Postgres, MySQL & Storage Backends"
description: "Does bunqueue support Postgres or MySQL? Why bunqueue is SQLite-only by design, and how to run it on serverless or ephemeral-filesystem platforms using a persistent volume or store-and-forward, no database required."
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/server-mode.png
---

**Short answer:** bunqueue is SQLite-only by design. There is no Postgres or MySQL backend, and adding one via Bun's `bun:sql` client is not on the roadmap. This page explains why, and shows how to run bunqueue on serverless or ephemeral-filesystem platforms without a database.

## Why bunqueue is SQLite-only

The defining constraint of bunqueue is **zero external runtime infrastructure**: no Redis, no broker, no companion database. Persistence is a single local SQLite file in WAL mode, and the only runtime dependencies are `croner` and `msgpackr`. That is the whole point of the project, and it is why install is trivial and there is no version skew between a queue and a separate store. See [Why bunqueue: SQLite Over Redis](/blog/why-bunqueue/).

Even setting the design philosophy aside, swapping in `bun:sql` (Bun's Postgres/MySQL client) would be a large, invasive change for two concrete reasons.

### 1. Async versus sync

bunqueue's storage layer, write buffer, and startup recovery are all built on `bun:sqlite`, which is **synchronous**. `bun:sql` is **async-only**. Making the storage layer async cascades `await` through the hot `push` / `pull` / `ack` path and adds latency to the core operations that bunqueue is tuned to keep fast.

### 2. A database swap would not unlock scaling

bunqueue is a **single-process** engine: the in-memory sharded priority queues are the source of truth at runtime, and SQLite is a write-behind durability log, not a shared coordination point. There is no distributed locking and no cross-instance coordination. Two bunqueue processes pointed at the same Postgres database would double-execute jobs and race on state transitions.

So a Postgres backend would cost a lot and, on its own, would not deliver the multi-instance or serverless scaling that most people are actually asking it for. That elasticity would need a much deeper rearchitecture (distributed locks, `SKIP LOCKED` polling, removing the in-memory caches), which is a different product.

## Running on serverless or ephemeral filesystems

The real reason people ask for Postgres is usually a serverless container whose filesystem is ephemeral, so a SQLite file does not survive a restart. You have three good options today, and none of them need a database.

### Option 1: Mount a persistent volume (simplest)

Most "serverless container" platforms let you attach a durable disk. Point bunqueue's data path at it and SQLite behaves normally across restarts.

| Platform | Durable storage |
| --- | --- |
| Fly.io | Fly Volumes |
| Railway | Volumes |
| Render | Persistent Disks |
| Docker / Compose | A named volume mounted at the data directory |
| Kubernetes | A PersistentVolumeClaim |

```bash
# Server mode
BUNQUEUE_DATA_PATH=/data/bunq.db bunqueue start
```

```typescript
// Embedded mode
const queue = new Queue('jobs', { embedded: true, dataPath: '/data/jobs.db' });
```

:::note
Keep the mount path stable and on a single filesystem. SQLite WAL mode writes `-wal` and `-shm` sidecar files next to the database, so the whole data directory must live on the durable volume, not just the `.db` file.
:::

### Option 2: Store-and-forward from the ephemeral instance

If your instances are truly ephemeral (scale-to-zero, no disk you can attach), run bunqueue embedded locally and forward jobs to a central, durable bunqueue server. The ephemeral instance holds nothing long-term, and the central node owns persistence.

```typescript
const local = new Queue('ingest', { embedded: true, dataPath: '/tmp/spool.db' });

const forwarder = local.forward({
  to: { host: 'central.internal', port: 6789, tls: true },
  queue: 'ingest', // optional remote name
});
```

Remote failures fall back to local retry and DLQ, so nothing is lost, and a deterministic remote `jobId` dedupes re-forwards within the server's custom-id retention window. This is the cleanest fit for scale-to-zero edge or serverless workers. Full walkthrough in the store-and-forward section of the [IoT & Edge guide](/guide/iot-edge/).

### Option 3: Central TCP server, stateless workers

Run one bunqueue server on a host or container that has a durable disk, and connect stateless producers and workers to it over TCP (optionally TLS). Your serverless functions stay stateless, and only the single server persists.

```typescript
const queue = new Queue('jobs', { connection: { host: 'queue.internal', port: 6789 } });
const worker = new Worker('jobs', processor, {
  connection: { host: 'queue.internal', port: 6789 },
});
```

## Choosing an option

| Your situation | Use |
| --- | --- |
| Container with an attachable disk | Persistent volume (Option 1) |
| Scale-to-zero or no disk, want local durability first | Store-and-forward (Option 2) |
| Many stateless workers, one durable host | Central TCP server (Option 3) |
| You genuinely need a shared SQL database as the queue itself | A Postgres-native queue |

That last row is deliberate. If a shared Postgres or MySQL queue is a hard requirement, for example you already operate Postgres and want the queue to live inside it, or you need many writers against one SQL store, then a Postgres-native queue such as [pg-boss](https://github.com/timgit/pg-boss) or [graphile-worker](https://github.com/graphile/worker) is the right tool. bunqueue optimizes for a different point: one fast process with zero external infrastructure.

## Will bunqueue add Postgres or MySQL?

Not planned. The SQLite-only, zero-infrastructure design is a core value of the project. If there is strong, specific demand for Postgres as a durability target, it could be reconsidered as an optional adapter, but it is a large change and, as explained above, would not by itself add clustering. You can weigh in on [discussion #105](https://github.com/egeominotti/bunqueue/discussions/105).

## See also

- [Deployment Guide](/guide/deployment/), Docker, systemd, and PM2
- [IoT & Edge](/guide/iot-edge/), the store-and-forward pattern in depth
- [Environment Variables](/guide/env-vars/), including `BUNQUEUE_DATA_PATH`
- [Why bunqueue: SQLite Over Redis](/blog/why-bunqueue/)
- [FAQ](/faq/)
