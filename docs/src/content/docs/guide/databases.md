---
title: "Postgres, MySQL & Storage Backends"
description: "Why bunqueue is SQLite-only by design, what that means for Postgres and MySQL users, and how to run it on serverless and edge platforms."
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/server-mode.png
---

<div class="bq-wrap bq-hero">
  <span class="bq-eyebrow">guide · storage backends</span>
  <h1 class="bq-hero-h1 bq-bench-h1">SQLite by default, tuned for <em>queues.</em></h1>
  <p class="bq-hero-sub">bunqueue is SQLite only by design: no Postgres or MySQL backend, and none on the roadmap. This page explains the engineering reasons, and shows how to run on serverless or ephemeral filesystem platforms without an external database.</p>

  <div class="bq-proof">
    <span><b>0</b> external databases required</span>
    <span><b>2</b> runtime dependencies</span>
    <span><b>3</b> patterns for ephemeral platforms</span>
    <span><b>1</b> local WAL-mode file</span>
  </div>
</div>

## Why bunqueue is SQLite-only

The defining constraint of bunqueue is **zero external runtime infrastructure**: no Redis, no broker, no companion database. Persistence is a single local SQLite file in WAL mode, and the only runtime dependencies are `croner` and `msgpackr`. This constraint is the core value of the project: installation stays trivial, operations stay simple, and there is no version skew between the queue and a separate store. The rationale is covered in depth in [Why bunqueue: SQLite Over Redis](/blog/why-bunqueue/).

Independently of the design philosophy, adopting `bun:sql` (Bun's Postgres and MySQL client) would be a large, invasive change for two concrete engineering reasons.

### 1. Synchronous storage is load bearing

The storage layer, the write buffer, and startup recovery are built on `bun:sqlite`, which is **synchronous**. `bun:sql` is **async only**. Making the storage layer asynchronous cascades `await` through the hot `push`, `pull` and `ack` path and adds latency to exactly the operations bunqueue is tuned to keep fast.

### 2. A database swap would not deliver horizontal scaling

bunqueue is a **single process** engine: the in memory sharded priority queues are the source of truth at runtime, and SQLite is a write behind durability log, not a shared coordination point. There is no distributed locking and no cross instance coordination. Two bunqueue processes pointed at the same Postgres database would double execute jobs and race on state transitions.

A Postgres backend would therefore carry a high cost and, on its own, would not deliver the multi instance or serverless elasticity that usually motivates the request. That elasticity requires a much deeper rearchitecture, distributed locks, `SKIP LOCKED` polling, removal of the in memory caches, which is a different product.

## Running on serverless or ephemeral filesystems

The underlying requirement behind most Postgres requests is a serverless container whose filesystem is ephemeral, where a SQLite file does not survive a restart. Three supported patterns address this today, and none of them requires a database.

### Option 1: Mount a persistent volume

Most serverless container platforms can attach a durable disk. Point the bunqueue data path at it and SQLite behaves normally across restarts. This is the simplest option.

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

When instances are truly ephemeral, scale to zero with no attachable disk, run bunqueue embedded locally and forward jobs to a central, durable bunqueue server. The ephemeral instance holds nothing long term, the central node owns persistence.

```typescript
const local = new Queue('ingest', { embedded: true, dataPath: '/tmp/spool.db' });

const forwarder = local.forward({
  to: { host: 'central.internal', port: 6789, tls: true },
  queue: 'ingest', // optional remote name
});
```

Remote failures fall back to local retry and the DLQ, nothing is lost, and a deterministic remote `jobId` deduplicates re forwards within the server's custom id retention window. This is the recommended fit for scale to zero edge or serverless workers. The full walkthrough is in the [IoT & Edge guide](/guide/iot-edge/).

### Option 3: Central TCP server, stateless workers

Run one bunqueue server on a host or container with a durable disk, and connect stateless producers and workers to it over TCP, optionally with TLS. Serverless functions stay stateless, only the single server persists. The [client SDKs](/guide/sdks/) connect from Node.js, Deno, Python and Cloudflare Workers.

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

The last row is deliberate. When a shared Postgres or MySQL queue is a hard requirement, for example the organization already operates Postgres and the queue must live inside it, or many writers must target one SQL store, a Postgres native queue such as [pg-boss](https://github.com/timgit/pg-boss) or [graphile-worker](https://github.com/graphile/worker) is the correct tool. bunqueue optimizes for a different point: one fast process with zero external infrastructure.

## Will bunqueue add Postgres or MySQL?

Not planned. The SQLite only, zero infrastructure design is a core value of the project. Should strong, specific demand emerge for Postgres as a durability target, it could be reconsidered as an optional adapter; it remains a large change and, as explained above, would not by itself add clustering. Requirements and use cases are welcome on [discussion #105](https://github.com/egeominotti/bunqueue/discussions/105).

## See also

- [Deployment Guide](/guide/deployment/), Docker, systemd, and PM2
- [IoT & Edge](/guide/iot-edge/), the store-and-forward pattern in depth
- [Environment Variables](/guide/env-vars/), including `BUNQUEUE_DATA_PATH`
- [Why bunqueue: SQLite Over Redis](/blog/why-bunqueue/)
- [FAQ](/faq/)
