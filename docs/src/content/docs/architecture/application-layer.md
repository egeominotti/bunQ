---
title: 'Application Layer: Operations, Stalls & DLQ'
description: 'bunqueue application layer: PUSH/PULL/ACK operations, stall detection, dependency resolution, DLQ management, and job lifecycle flows.'
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/architecture/application-layer.png
---

<div class="bq-wrap bq-hero">
  <span class="bq-eyebrow">architecture · application layer</span>
  <h1 class="bq-hero-h1 bq-bench-h1">Use cases in the <em>application layer.</em></h1>
  <p class="bq-hero-sub">The application layer orchestrates all queue operations, coordinating between the client layer and domain layer: PUSH, PULL and ACK flows, stall detection, dependency resolution, and background tasks.</p>
</div>

The server selects one application manager at startup. `QueueManager` owns the
synchronous memory/SQLite path shown in the diagrams below;
`PostgresQueueManager` exposes the same handler-facing operations but commits
state through database transactions and refreshes a bounded compatibility
projection. See [Storage backends](/guide/databases/) for the multi-broker path.

## Module Structure

```
src/application/
├── queueManager.ts        # Central orchestrator
├── postgresQueueManager.ts # PostgreSQL manager facade
├── postgres-queue-manager/ # Transactional operations and local projection
├── operations/            # PUSH, PULL, ACK, Query
├── backgroundTasks.ts     # Task orchestration
├── cleanupTasks.ts        # Memory cleanup, orphan removal
├── clientTracking.ts      # Client connection tracking
├── contextFactory.ts      # Context creation helpers
├── dependencyProcessor.ts # Dependency resolution
├── dlqManager.ts          # Dead letter queue
├── eventsManager.ts       # Event pub/sub
├── jobLogsManager.ts      # Job logs management
├── latencyTracker.ts      # Operation latency percentiles
├── lockManager.ts         # Lock management
├── lockOperations.ts      # Lock acquire/release ops
├── metricsExporter.ts     # Prometheus metrics export
├── monitoringChecks.ts    # Periodic health checks
├── stallDetection.ts      # Stall detection
├── statsManager.ts        # Queue statistics
├── taskErrorTracking.ts   # Background task circuit breaker
├── throughputTracker.ts   # Push/pull/ack rate tracking
├── types.ts               # Shared type definitions
├── webhookManager.ts      # Webhook notifications
└── workerManager.ts       # Worker tracking
```

`PostgresQueueManager` replaces the base delivery and lifecycle operations at
the same handler boundary. Admissions and terminal transitions commit job
state, ownership, results, and durable events together; pulls claim ordered
rows with `FOR UPDATE SKIP LOCKED`; ACK/FAIL validates database-clock leases and
broker-session tokens. A bounded local projection serves compatibility reads
and is repaired from the durable outbox plus polling. The complete transaction,
lease, and replay model is documented in the repository reference
`docs/features/postgres-multibroker.md`, the [architecture overview](/architecture/),
and the user-facing [storage guide](/guide/databases/).

## QueueManager Orchestration

<div class="bq-diag">
  <div class="bq-diag-head"><b>QueueManager</b><span>memory / SQLite orchestrator</span></div>
  <div class="bq-diag-group">
    <span class="bq-diag-group-label">state</span>
    <div class="bq-diag-row">
      <div class="bq-diag-cell bq-diag-accent">shards[N] <i>paired with shardLocks[N], N auto-detected</i></div>
      <div class="bq-diag-cell">processingShards[N] <i>paired with processingLocks[N]</i></div>
    </div>
    <div class="bq-diag-row">
      <div class="bq-diag-cell">jobIndex <i>Map&lt;id, location&gt;</i></div>
      <div class="bq-diag-cell">completedJobs <i>BoundedSet, 50k</i></div>
      <div class="bq-diag-cell">jobResults <i>LRU, 10k</i></div>
      <div class="bq-diag-cell">customIdMap <i>LRU, 50k</i></div>
    </div>
  </div>
  <div class="bq-diag-group">
    <span class="bq-diag-group-label">operations</span>
    <div class="bq-diag-row">
      <div class="bq-diag-cell">push() <i>operations/push.ts</i></div>
      <div class="bq-diag-cell">pull() <i>operations/pull.ts</i></div>
      <div class="bq-diag-cell">ack() <i>operations/ack.ts</i></div>
      <div class="bq-diag-cell">query <i>operations/queryOperations.ts</i></div>
    </div>
  </div>
  <div class="bq-diag-group">
    <span class="bq-diag-group-label">managers</span>
    <div class="bq-diag-row">
      <div class="bq-diag-cell">DLQManager</div>
      <div class="bq-diag-cell">EventsManager</div>
      <div class="bq-diag-cell">WorkerManager</div>
    </div>
    <div class="bq-diag-row">
      <div class="bq-diag-cell">WebhookManager</div>
      <div class="bq-diag-cell">StatsManager</div>
      <div class="bq-diag-cell">JobLogsManager</div>
    </div>
  </div>
  <div class="bq-diag-group">
    <span class="bq-diag-group-label">background tasks</span>
    <div class="bq-diag-row">
      <div class="bq-diag-cell">cleanup</div>
      <div class="bq-diag-cell">stall</div>
      <div class="bq-diag-cell">dependency</div>
      <div class="bq-diag-cell">dlq</div>
      <div class="bq-diag-cell">cron</div>
    </div>
  </div>
</div>

## PUSH Operation Flow

<div class="bq-diag">
  <div class="bq-diag-head"><b>PUSH flow</b><span>memory / SQLite push(queue, input)</span></div>
  <div class="bq-diag-layer">1. Generate UUIDv7 ID, 2. check customId idempotency <i>customIdMap, if exists return existing job</i></div>
  <div class="bq-diag-arrow">↓</div>
  <div class="bq-diag-layer">3. Acquire shard write lock <i>shardIdx = fnv1a(queue) &amp; SHARD_MASK</i></div>
  <div class="bq-diag-arrow">↓</div>
  <div class="bq-diag-group">
    <span class="bq-diag-group-label">4. check unique key deduplication</span>
    <div class="bq-diag-row">
      <div class="bq-diag-cell">Key available <i>register key, continue</i></div>
      <div class="bq-diag-cell">Key exists, strategy replace <i>remove old, insert new</i></div>
      <div class="bq-diag-cell">Key exists, strategy extend <i>reset TTL, return existing</i></div>
      <div class="bq-diag-cell">Key exists, default <i>return existing</i></div>
    </div>
  </div>
  <div class="bq-diag-arrow">↓</div>
  <div class="bq-diag-group">
    <span class="bq-diag-group-label">5. check dependencies</span>
    <div class="bq-diag-row">
      <div class="bq-diag-cell">All satisfied <i>push to queue</i></div>
      <div class="bq-diag-cell">Not satisfied <i>add to waitingDeps, register in dependencyIndex</i></div>
    </div>
  </div>
  <div class="bq-diag-arrow">↓</div>
  <div class="bq-diag-layer bq-diag-accent">6. Update jobIndex, 7. persist to configured SQLite <i>buffered or durable; no-op in memory mode</i>, 8. notify waiters <i>wake long poll</i>, 9. broadcast 'pushed' event</div>
</div>

## PULL Operation Flow

<div class="bq-diag">
  <div class="bq-diag-head"><b>PULL flow</b><span>pull(queue, timeoutMs), runs as a loop</span></div>
  <div class="bq-diag-layer">1. Acquire shard write lock, 2. queue paused, return null, 3. rate limit exceeded, return null, 4. concurrency at limit, return null</div>
  <div class="bq-diag-arrow">↓</div>
  <div class="bq-diag-group">
    <span class="bq-diag-group-label">5. dequeue loop, inspect priority-ordered candidates</span>
    <div class="bq-diag-row">
      <div class="bq-diag-cell">TTL expired <i>drop, try next</i></div>
      <div class="bq-diag-cell">Not ready, delayed <i>temporarily park, track earliest runAt, inspect next candidate</i></div>
      <div class="bq-diag-cell">FIFO group active <i>temporarily park, inspect work from another eligible group</i></div>
      <div class="bq-diag-cell">Valid job <i>pop, continue</i></div>
    </div>
    <p class="bq-diag-note">Parked jobs remain logically queued: counters, temporal indexes and jobIndex do not change. They are restored to the heap before the shard lock is released, so a blocked root cannot hide unrelated ready work.</p>
  </div>
  <div class="bq-diag-arrow">↓</div>
  <div class="bq-diag-row">
    <div class="bq-diag-cell bq-diag-accent">Job found <i>move to processing shard</i></div>
    <div class="bq-diag-cell">No job <i>wait for notification, event-based with timeout, then retry loop</i></div>
  </div>
  <div class="bq-diag-arrow">↓</div>
  <div class="bq-diag-layer">6. Create lock token <i>if useLocks enabled</i>, 7. update jobIndex to 'processing', 8. mark active in SQLite, 9. broadcast 'pulled' event, 10. return job with token</div>
</div>

## ACK Operation Flow

<div class="bq-diag">
  <div class="bq-diag-head"><b>ACK flow</b><span>ack(jobId, result, token)</span></div>
  <div class="bq-diag-layer">1. Verify lock token <i>if provided, on mismatch error: token invalid</i></div>
  <div class="bq-diag-arrow">↓</div>
  <div class="bq-diag-layer">2. Remove from processing shard <i>procIdx = fnv1a(jobId) &amp; SHARD_MASK</i></div>
  <div class="bq-diag-arrow">↓</div>
  <div class="bq-diag-group">
    <span class="bq-diag-group-label">3. release shard resources</span>
    <div class="bq-diag-row">
      <div class="bq-diag-cell">Release unique key</div>
      <div class="bq-diag-cell">Release FIFO group</div>
      <div class="bq-diag-cell">Release concurrency slot</div>
    </div>
  </div>
  <div class="bq-diag-arrow">↓</div>
  <div class="bq-diag-group">
    <span class="bq-diag-group-label">4. finalize</span>
    <div class="bq-diag-row">
      <div class="bq-diag-cell">Store result in jobResults <i>LRU</i></div>
      <div class="bq-diag-cell">Store result in SQLite</div>
      <div class="bq-diag-cell">Update jobIndex to 'completed'</div>
      <div class="bq-diag-cell bq-diag-accent">Add to completedJobs <i>signals deps</i></div>
    </div>
  </div>
  <div class="bq-diag-arrow">↓</div>
  <div class="bq-diag-layer">5. Add to pendingDepChecks <i>wake dependents</i>, 6. broadcast 'completed' event, 7. trigger webhooks</div>
</div>

## Background Tasks

<div class="bq-diag">
  <div class="bq-diag-head"><b>Background task scheduler</b></div>
  <div class="bq-diag-row">
    <div class="bq-diag-cell">Cleanup <i>every 10s</i></div>
    <div class="bq-diag-cell bq-diag-accent">Stall check <i>every 5s</i></div>
    <div class="bq-diag-cell">Dependency <i>event-driven, 30s safety fallback</i></div>
  </div>
  <div class="bq-diag-row">
    <div class="bq-diag-cell">DLQ maintenance <i>every 60s</i></div>
    <div class="bq-diag-cell">Lock expire <i>every 5s</i></div>
    <div class="bq-diag-cell">Cron <i>precise setTimeout, 60s safety fallback</i></div>
  </div>
  <p class="bq-diag-note">Processing timeouts use one next-deadline timer keyed by each active job's <code>startedAt + timeout</code>. Far-future timers are safely chunked at the runtime ceiling; failed timeout transitions are logged and retried.</p>
</div>

### Stall Detection (Two-Phase)

<div class="bq-diag">
  <div class="bq-diag-head"><b>Stall check</b><span>every 5s, two-phase</span></div>
  <div class="bq-diag-group">
    <span class="bq-diag-group-label">phase 1, process previous candidates</span>
    <div class="bq-diag-layer">For each job in stalledCandidates: still in processing? get stall config</div>
    <div class="bq-diag-arrow">↓ if confirmed stalled</div>
    <div class="bq-diag-row">
      <div class="bq-diag-cell">stallCount &lt; maxStalls <i>increment + retry</i></div>
      <div class="bq-diag-cell bq-diag-accent">stallCount &gt;= maxStalls <i>move to DLQ</i></div>
    </div>
  </div>
  <div class="bq-diag-group">
    <span class="bq-diag-group-label">phase 2, mark new candidates</span>
    <div class="bq-diag-layer">For each job in processingShards: no heartbeat for &gt; stallInterval <i>30s</i>, add to stalledCandidates <i>checked next tick</i></div>
  </div>
  <p class="bq-diag-note">Why two-phase? It prevents false positives from transient delays, like a GC pause or a network hiccup.</p>
</div>

### Dependency Resolution

<div class="bq-diag">
  <div class="bq-diag-head"><b>Dependency processor</b><span>event-driven, microtask-coalesced</span></div>
  <div class="bq-diag-layer">0. On job completion, a flush is scheduled on the next microtask <i>completions from the same tick are coalesced; a 30s interval is only a safety fallback for missed events</i></div>
  <div class="bq-diag-arrow">↓</div>
  <div class="bq-diag-layer">1. Collect completedIds from pendingDepChecks <i>set of jobs that completed since last flush</i></div>
  <div class="bq-diag-arrow">↓</div>
  <div class="bq-diag-layer">2. For each completedId, look up dependencyIndex[completedId] <i>returns the Set of jobIds waiting for this job</i></div>
  <div class="bq-diag-arrow">↓</div>
  <div class="bq-diag-layer">3. Group by shard <i>for efficient locking</i></div>
  <div class="bq-diag-arrow">↓</div>
  <div class="bq-diag-layer">4. For each waiting job, check ALL dependencies completed <i>completedJobs.has(depId) for all deps</i></div>
  <div class="bq-diag-arrow">↓</div>
  <div class="bq-diag-layer bq-diag-accent">5. If all satisfied: remove from waitingDeps, unregister from dependencyIndex, push to active queue</div>
</div>

### Cleanup Tasks

<div class="bq-diag">
  <div class="bq-diag-head"><b>Cleanup</b><span>every 10s</span></div>
  <div class="bq-diag-layer">1. Refresh delayed counts in each shard</div>
  <div class="bq-diag-layer bq-diag-accent">2. Compact priority queues <i>if stale ratio &gt; 20%, rebuild heap</i></div>
  <div class="bq-diag-layer">3. Clean orphaned processing entries <i>jobs stuck &gt; 30min with no heartbeat</i></div>
  <div class="bq-diag-layer">4. Clean stale waiting dependencies <i>waiting &gt; 1 hour</i></div>
  <div class="bq-diag-layer">5. Clean expired unique keys</div>
  <div class="bq-diag-layer">6. Clean orphaned job index entries</div>
  <div class="bq-diag-layer">7. Remove empty queues</div>
</div>

## Event Broadcasting

<div class="bq-diag">
  <div class="bq-diag-head"><b>Events manager</b></div>
  <div class="bq-diag-layer">Event occurs <i>completed, failed, progress, stalled</i></div>
  <div class="bq-diag-arrow">↓</div>
  <div class="bq-diag-layer bq-diag-accent">broadcast(event)</div>
  <div class="bq-diag-arrow">↓</div>
  <div class="bq-diag-row">
    <div class="bq-diag-cell">Notify all subscribers <i>Set-based, O(1) add</i></div>
    <div class="bq-diag-cell">Trigger matching webhooks</div>
    <div class="bq-diag-cell">Wake completion waiters</div>
  </div>
  <p class="bq-diag-note">Event-based waiting, no polling: waitForJobCompletion(jobId, timeout) resolves when the 'completed' event for jobId arrives.</p>
</div>

:::tip[Related]

- [Architecture Overview](/architecture/) - Full component map
- [Domain Layer](/architecture/domain-layer/) - Shards and the state machine these operations mutate
- [TCP Protocol](/architecture/tcp-protocol/) - How operations arrive over the wire
- [Persistence](/architecture/persistence/) - Where these operations are durably recorded
  :::
