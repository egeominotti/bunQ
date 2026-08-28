---
title: "bunqueue Glossary: Job Queue Terms in Plain Words"
description: "Plain-language definitions of every bunqueue concept: job, queue, worker, DLQ, backoff, embedded mode, stall detection, and more. Each term links to its guide."
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/guide/glossary.png
---

<div class="bq-wrap bq-hero">
  <span class="bq-eyebrow">reference · glossary</span>
  <h1 class="bq-hero-h1 bq-bench-h1">Every job queue term, <em>defined.</em></h1>
  <p class="bq-hero-sub">Short, plain-words definitions. Each term links to the guide that covers it in full. If a word in the docs is unfamiliar, it is explained here.</p>
</div>

A **job queue** is a to-do list for your app: you add tasks now, and they run later, in order, with retries if they fail. This page defines the words bunqueue uses, grouped by topic.

## The basics

### Job

One unit of work: a name, a JSON payload, and options such as priority or delay. A job moves through states (`waiting`, `active`, `completed`, `failed`, `delayed`) until it finishes or runs out of retries. See the [Queue API](/guide/queue/).

### Queue

A named list that holds jobs of one kind, for example `emails`. You add jobs to a queue, workers take them out. Each queue can be paused, drained, or rate limited on its own. See the [Queue API](/guide/queue/).

### Worker

A loop that pulls jobs from a queue and runs your function on each one. You choose how many jobs it runs in parallel. See the [Worker API](/guide/worker/).

### Producer

Any code that adds jobs. Often just your HTTP handler calling `queue.add()`. See the [Queue API](/guide/queue/).

### Embedded mode vs server mode

**Embedded mode** runs the whole queue inside your app's process, in memory by
default or backed by a local SQLite file when `dataPath` is set; there is no
server to run. **Server mode** runs bunqueue as a standalone server that many
apps and workers connect to over TCP, using memory/SQLite for one broker or
PostgreSQL for a broker fleet. See [Server Mode](/guide/server/) and the
[Introduction](/guide/introduction/).

### Ack

The confirmation a worker sends when a job is done. The `Worker` class acks for you automatically when your function returns. See the [Worker API](/guide/worker/).

### Simple mode

The `Bunqueue` class, a Queue and a Worker bundled into one object, with named routes and middleware. The fastest way to start. See [Simple Mode](/guide/simple-mode/).

## When things fail

### Retry

Running a failed job again. bunqueue retries up to `attempts` times (default 3) before giving up. See the [Worker API](/guide/worker/).

### Backoff

The waiting time between retries. Each retry waits longer than the last, which stops a struggling service from being hammered. See the [Dead Letter Queue guide](/guide/dlq/).

### DLQ (Dead Letter Queue)

The place where jobs go after all retries fail, with their error and stack trace kept so you can inspect and retry them by hand or on a schedule. See the [Dead Letter Queue guide](/guide/dlq/).

### Stall detection

The safety net for crashed workers. A working worker sends heartbeats; if they stop, the job is taken back and re-queued so another worker can run it. See [Stall Detection](/guide/stall-detection/).

### Heartbeat

A small "still alive" signal a worker sends while a job runs. Missed heartbeats trigger stall detection. See [Stall Detection](/guide/stall-detection/).

### Lock (lease)

Temporary, fenced ownership of a job given to the worker that pulled it. While
the lease is valid, only its holder may commit an outcome. If it expires, the
job can be handed out again; handlers must therefore tolerate at-least-once
execution when a stalled original is still alive. See the [Worker API](/guide/worker/).

### Durable write

A job option (`durable: true`) that makes SQLite write the job immediately
instead of using its 10ms write buffer. It trades some SQLite throughput for no
buffer-loss window. PostgreSQL admissions are already transactional, so the
flag does not change server-side durability there. See the [Queue API](/guide/queue/).

## Timing and ordering

### Priority

A number on a job; higher numbers run first within the same queue. See the [Queue API](/guide/queue/).

### Delayed job

A job that waits a set time before it becomes runnable. It sits in the `delayed` state, then moves to `waiting`. See the [Queue API](/guide/queue/).

### Cron

A schedule that adds jobs on a recurring basis, from cron expressions like
`0 9 * * *` or plain intervals, with timezone support. Schedules survive
restarts when SQLite or PostgreSQL persistence is configured; memory-only
schedules do not. See [Cron Jobs](/guide/cron/).

### Promote

Moving a delayed job to `waiting` right now, ahead of its schedule. See the [Queue API](/guide/queue/).

### Concurrency

How many jobs one worker runs at the same time. A separate queue-level cap can limit active jobs across all workers. See the [Worker API](/guide/worker/) and [Rate Limiting](/guide/rate-limiting/).

### Rate limiting

Capping how many jobs run per time window, to protect a downstream service like an email API from overload. See [Rate Limiting](/guide/rate-limiting/).

### Deduplication and idempotency

Giving a job a custom `jobId` so adding it twice does nothing the second time. This makes `add()` safe to call more than once for the same logical task. See the [Queue API](/guide/queue/).

## Composing jobs

### Flow

Parent-child job dependencies: children run first, the parent runs only after all children complete. Built with `FlowProducer`. See the [Flow Producer guide](/guide/flow/).

### Workflow and saga compensation

The Workflow Engine runs multi-step processes with branching, parallel steps, loops, and waits for human approval. **Saga compensation** means each step can register an undo function, and on failure the completed steps are undone in reverse order. See the [Workflow Engine guide](/guide/workflow/).

### Queue group

Several queues managed as one unit, useful for tenant-per-queue setups. See [Queue Group](/guide/queue-group/).

### Webhook

An HTTP call bunqueue makes to your URL when queue events happen, so other systems can react. See [Webhooks](/guide/webhooks/).

## Control operations

### Pause and resume

Pausing stops workers from receiving new jobs from a queue; jobs already running finish normally. Resume turns delivery back on. See the [Queue API](/guide/queue/).

### Drain and obliterate

**Drain** removes waiting and delayed jobs but lets active ones finish. **Obliterate** deletes the queue and everything in it. See the [Queue API](/guide/queue/).

## Under the hood

### Sharding

In memory/SQLite mode, bunqueue splits queue state across independent in-memory
slices (one per CPU core group) so operations on different queues do not wait on
one lock. It is automatic. PostgreSQL mode orders and locks authoritative
database rows instead of using those delivery shards. See
[Benchmarks](/guide/benchmarks/).

### WAL (Write-Ahead Logging)

The SQLite mode bunqueue uses, which lets reads and writes happen at the same time. It creates `-wal` and `-shm` files next to the database file. See [Storage](/guide/databases/).

### MessagePack

The compact binary format used on the TCP wire, smaller and faster to parse than JSON. See the [TCP Protocol](/api/tcp/).

### Store-and-forward

An edge pattern: a small embedded queue stores jobs on a persistent local volume,
then forwards them to a central server when it can reach it. A network outage
does not drop jobs while the local process and volume survive; use SQLite
`durable: true` when even its 10ms hard-crash window is unacceptable. See
[IoT & Edge](/guide/iot-edge/).

:::tip[Related]
- [Introduction](/guide/introduction/) - What bunqueue is and when to use it
- [Quickstart](/guide/quickstart/) - Running in five minutes
- [FAQ](/faq/) - Common questions answered
:::
