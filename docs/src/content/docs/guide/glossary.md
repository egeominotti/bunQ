---
title: "bunqueue Glossary: Job Queue Terms & Concepts Explained"
description: "Plain-language definitions of every bunqueue concept: job, queue, worker, DLQ, backoff, sharding, stall detection, saga compensation, and more."
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/getting-started.png
---

<div class="bq-wrap bq-hero">
  <span class="bq-eyebrow">reference · glossary</span>
  <h1 class="bq-hero-h1 bq-bench-h1">Every job queue term, <em>defined.</em></h1>
  <p class="bq-hero-sub">One concise definition per concept, each linking to the guide that covers it in full. If a word in the docs is unfamiliar, it is explained here.</p>
</div>

A **job queue** stores units of work (jobs) so that producers can hand off tasks and workers can process them later, with retries, ordering, and persistence. This page defines the vocabulary bunqueue uses. Every term links to its full guide.

## Job

A single unit of work: a name, a JSON payload, and options (priority, delay, attempts, timeout). A job moves through states, `waiting`, `active`, `completed`, `failed`, `delayed`, until it finishes or exhausts its retries. See the [Queue API](/guide/queue/).

## Queue

A named channel that holds jobs of one logical kind. Producers add jobs to a queue; workers pull from it. Each queue can be paused, drained, or rate limited independently. See the [Queue API](/guide/queue/).

## Worker

A process that pulls jobs from a queue and runs a processor function, with a configurable concurrency, heartbeats for stall detection, and lock-based ownership so no two workers run the same job. See the [Worker API](/guide/worker/).

## Producer

Any code that adds jobs. In embedded mode the producer and worker share a process; in server mode producers connect over TCP. See [Server Mode](/guide/server/).

## Embedded mode vs server mode

**Embedded mode** runs the queue in your app's process against a local SQLite file, with no network hop. **Server mode** runs bunqueue as a standalone TCP and HTTP server that many producers and workers connect to. See [Server Mode](/guide/server/) and the [Introduction](/guide/introduction/).

## DLQ (Dead Letter Queue)

The Dead Letter Queue holds jobs that failed permanently, after exhausting all retry attempts, with their error, stack trace, and metadata preserved so you can inspect, retry, or expire them on a policy. See the [Dead Letter Queue guide](/guide/dlq/).

## Retry

Re-running a failed job. bunqueue retries up to `attempts` times before the job moves to the DLQ. See the [Worker API](/guide/worker/).

## Backoff

The delay between retry attempts. bunqueue uses exponential backoff by default, so each failure waits longer than the last, which spreads load away from a struggling dependency. See the [Dead Letter Queue guide](/guide/dlq/).

## Stall detection

The mechanism that recovers jobs from a worker that stopped responding. A worker sends heartbeats while processing; if it misses them past a grace period, the job is marked stalled and re-queued, or sent to the DLQ after too many stalls. See [Stall Detection](/guide/stall-detection/).

## Heartbeat

A periodic signal a worker sends while a job runs, proving it is still alive. Missed heartbeats trigger stall detection. See [Stall Detection](/guide/stall-detection/).

## Sharding

Splitting a queue's in-memory structures across N independent shards (chosen as a power of two from the CPU core count) so operations on different jobs do not contend on one lock. See the [Architecture overview](/architecture/) and [Domain Layer](/architecture/domain-layer/).

## Priority

A number attached to a job; higher-priority jobs are pulled before lower ones within the same queue. Implemented with a 4-ary MinHeap. See the [Queue API](/guide/queue/) and [Data Structures](/architecture/data-structures/).

## Concurrency

The number of jobs a single worker runs in parallel. A separate queue-level concurrency cap limits total active jobs across all workers. See [Rate Limiting](/guide/rate-limiting/).

## Rate limiting

Capping how many jobs run per time window (or how many are active at once) to protect downstream services from overload. See [Rate Limiting](/guide/rate-limiting/).

## Cron and scheduler

A scheduler that adds jobs on a recurring basis, from cron expressions or plain intervals, with IANA timezone support, persisted in SQLite so schedules survive a restart. See [Cron Jobs](/guide/cron/).

## Delayed job

A job that becomes eligible to run only after a set delay. Delayed jobs sit in the `delayed` state until their run time arrives, then move to `waiting`. See the [Queue API](/guide/queue/).

## Deduplication and idempotency

Giving a job a custom `jobId` (or a `deduplication` window) so that re-adding the same id while it is unfinished is a no-op, making `add()` safe to call more than once. See the [Queue API](/guide/queue/).

## Durable write

A job option that flushes the job straight to SQLite instead of the 10ms write buffer, trading throughput for zero data-loss on a crash. See the [Queue API](/guide/queue/).

## Flow and FlowProducer

FlowProducer creates parent-child job dependencies: children run first, and the parent runs only after all children complete. Used for pipelines and fan-out/fan-in. See the [Flow Producer guide](/guide/flow/).

## Workflow and saga compensation

The Workflow Engine orchestrates multi-step processes with branching, parallel steps, loops, and human-in-the-loop signals. **Saga compensation** runs registered rollback steps in reverse order when a later step fails, undoing partial work. See the [Workflow Engine guide](/guide/workflow/).

## Lock (lease)

Temporary ownership of a job granted to the worker that pulled it, so no other worker processes it. If the lock expires (for example the worker died), the job can be re-leased. See the [Worker API](/guide/worker/).

## Promote

Moving a delayed or DLQ job to `waiting` immediately, ahead of its schedule. See the [Queue API](/guide/queue/).

## Drain and obliterate

**Drain** removes waiting and delayed jobs from a queue while leaving active ones to finish. **Obliterate** removes the queue and every job in it. See the [Queue API](/guide/queue/).

## WAL (Write-Ahead Logging)

The SQLite journaling mode bunqueue runs in, which lets readers and a writer work concurrently and improves write throughput. See [Persistence](/architecture/persistence/).

## MessagePack

The compact binary serialization format used on the TCP wire protocol, faster and smaller than JSON. See the [TCP Protocol](/api/tcp/).

## Store-and-forward

An edge pattern where a local embedded queue drains its jobs to a central remote server, with local retry and DLQ if the remote is unreachable, so nothing is lost offline. See [IoT & Edge](/guide/iot-edge/).

:::tip[Related]
- [Introduction](/guide/introduction/) - What bunqueue is and why SQLite over Redis
- [FAQ](/faq/) - Common questions answered
- [Architecture Overview](/architecture/) - How the internals fit together
- [Queue API](/guide/queue/) - The API most of these terms live in
:::
