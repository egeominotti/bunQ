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

**Embedded mode** runs the whole queue inside your app's process, backed by a local SQLite file, no server to run. **Server mode** runs bunqueue as a standalone server that many apps and workers connect to over TCP. See [Server Mode](/guide/server/) and the [Introduction](/guide/introduction/).

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

Temporary ownership of a job, given to the worker that pulled it, so two workers never run the same job. If the worker dies, the lock expires and the job can be handed out again. See the [Worker API](/guide/worker/).

### Durable write

A job option (`durable: true`) that writes the job to disk immediately instead of through the 10ms write buffer. Slower, but zero data loss even if the process crashes in that window. See the [Queue API](/guide/queue/).

## Timing and ordering

### Priority

A number on a job; higher numbers run first within the same queue. See the [Queue API](/guide/queue/).

### Delayed job

A job that waits a set time before it becomes runnable. It sits in the `delayed` state, then moves to `waiting`. See the [Queue API](/guide/queue/).

### Cron

A schedule that adds jobs on a recurring basis, from cron expressions like `0 9 * * *` or plain intervals, with timezone support. Schedules survive restarts. See [Cron Jobs](/guide/cron/).

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

Splitting the queue's in-memory state across independent slices (one per CPU core group) so operations on different jobs do not wait on one lock. You never configure this; it is automatic. See [Benchmarks](/guide/benchmarks/).

### WAL (Write-Ahead Logging)

The SQLite mode bunqueue uses, which lets reads and writes happen at the same time. It creates `-wal` and `-shm` files next to the database file. See [Storage](/guide/databases/).

### MessagePack

The compact binary format used on the TCP wire, smaller and faster to parse than JSON. See the [TCP Protocol](/api/tcp/).

### Store-and-forward

An edge pattern: a small embedded queue on the device stores jobs locally, then forwards them to a central server when it can reach it. Nothing is lost while offline. See [IoT & Edge](/guide/iot-edge/).

:::tip[Related]
- [Introduction](/guide/introduction/) - What bunqueue is and when to use it
- [Quickstart](/guide/quickstart/) - Running in five minutes
- [FAQ](/faq/) - Common questions answered
:::
