---
title: "Domain Layer: Sharding, Priority Queues, Job States"
description: "bunqueue domain layer internals: auto-scaled sharding, 4-ary priority queues, job state machine, DLQ flow, and rate limiting logic."
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og-image.png
---

<div class="bq-wrap bq-hero">
  <span class="bq-eyebrow">architecture · domain layer</span>
  <h1 class="bq-hero-h1 bq-bench-h1">Pure logic, no <em>I/O.</em></h1>
  <p class="bq-hero-sub">The domain layer contains the pure business logic of bunqueue. No external dependencies, just core algorithms and data structures.</p>
</div>

## Module Structure

```
src/domain/
├── types/           # Type definitions
└── queue/           # Core queue logic
    ├── shard.ts             # Shard container
    ├── priorityQueue.ts     # 4-ary indexed heap
    ├── dlqShard.ts          # Dead letter queue
    ├── uniqueKeyManager.ts  # Deduplication
    ├── limiterManager.ts    # Rate/concurrency
    ├── dependencyTracker.ts # Job dependencies
    ├── temporalManager.ts   # Temporal index + delayed jobs
    ├── waiterManager.ts     # Long-poll waiters
    └── shardCounters.ts     # O(1) per-queue stats
```

## Sharding Architecture

Jobs are distributed across N shards (auto-detected from CPU cores) for parallelism:

<div class="bq-diag">
  <div class="bq-diag-head"><b>QueueManager</b><span>N independent shards, auto-detected</span></div>
  <div class="bq-diag-flow">
    <div class="bq-diag-cell">queueName</div>
    <div class="bq-diag-arrow">→</div>
    <div class="bq-diag-cell">fnv1a()</div>
    <div class="bq-diag-arrow">→</div>
    <div class="bq-diag-cell">&amp; SHARD_MASK</div>
    <div class="bq-diag-arrow">→</div>
    <div class="bq-diag-cell">idx</div>
  </div>
  <div class="bq-diag-arrow">↓</div>
  <div class="bq-diag-row">
    <div class="bq-diag-cell bq-diag-accent">Shard 0 <i>queues, unique, dlq, limits</i></div>
    <div class="bq-diag-cell">Shard 1 <i>queues, unique, dlq, limits</i></div>
    <div class="bq-diag-cell">Shard 2 <i>queues, unique, dlq, limits</i></div>
    <div class="bq-diag-cell">Shard N <i>queues, unique, dlq, limits</i></div>
  </div>
  <p class="bq-diag-note">Shard count is a power of 2, based on CPU cores, max 64.</p>
</div>

### Shard Composition

Each shard is a composition of managers:

<div class="bq-diag">
  <div class="bq-diag-head"><b>Shard</b><span>composition of managers</span></div>
  <div class="bq-diag-layer bq-diag-accent">queues <i>Map&lt;string, PriorityQueue&gt;</i></div>
  <div class="bq-diag-row">
    <div class="bq-diag-cell">UniqueKeyManager <i>deduplication with TTL</i></div>
    <div class="bq-diag-cell">DlqShard <i>failed job storage</i></div>
    <div class="bq-diag-cell">LimiterManager <i>rate and concurrency control</i></div>
  </div>
  <div class="bq-diag-row">
    <div class="bq-diag-cell">DependencyTracker <i>waitingDeps + dependencyIndex</i></div>
    <div class="bq-diag-cell">TemporalManager <i>delayed jobs, MinHeap</i></div>
  </div>
  <div class="bq-diag-row">
    <div class="bq-diag-cell">stats <i>queued, delayed, dlq</i></div>
    <div class="bq-diag-cell">activeGroups <i>Map, FIFO groups</i></div>
    <div class="bq-diag-cell">waiters <i>Array, long poll support</i></div>
  </div>
</div>

## Priority Queue Flow

4-ary indexed heap with lazy deletion:

<div class="bq-diag">
  <div class="bq-diag-head"><b>PriorityQueue</b><span>4-ary indexed heap with lazy deletion</span></div>
  <div class="bq-diag-group">
    <span class="bq-diag-group-label">PUSH</span>
    <div class="bq-diag-layer">1. Generate generation number, 2. add to index <i>Map&lt;jobId, {job, generation}&gt;</i>, 3. push to heap <i>{jobId, priority, runAt, generation}</i>, 4. bubbleUp <i>O(log₄ n)</i></div>
  </div>
  <div class="bq-diag-group">
    <span class="bq-diag-group-label">POP</span>
    <div class="bq-diag-layer bq-diag-accent">Loop: 1. peek heap top, 2. check index for matching generation, 3. if generation mismatch, stale entry: removeTop, continue, 4. if match: removeTop, delete from index, return job <i>O(log₄ n) amortized</i></div>
  </div>
  <div class="bq-diag-group">
    <span class="bq-diag-group-label">REMOVE, by jobId</span>
    <div class="bq-diag-layer">1. Delete from index <i>O(1)</i>, 2. heap entry becomes stale <i>skipped on pop</i>, 3. compact heap when stale ratio &gt; 20%</div>
  </div>
</div>

## Job State Machine

<div class="bq-diag">
  <div class="bq-diag-head"><b>Job state machine</b></div>
  <div class="bq-diag-layer">WAITING <i>re-entered when a retryable fail triggers retry</i></div>
  <div class="bq-diag-arrow">↓</div>
  <div class="bq-diag-row">
    <div class="bq-diag-cell">DELAYED <i>delay &gt; 0, becomes ready when runAt is reached</i></div>
    <div class="bq-diag-cell">ready <i>delay = 0</i></div>
  </div>
  <div class="bq-diag-arrow">↓</div>
  <div class="bq-diag-layer bq-diag-accent">ACTIVE <i>on retryable fail, back to WAITING</i></div>
  <div class="bq-diag-arrow">↓</div>
  <div class="bq-diag-row">
    <div class="bq-diag-cell">COMPLETED <i>success</i></div>
    <div class="bq-diag-cell">DLQ <i>fail at max retries, or timeout</i></div>
  </div>
</div>

## Dependency Resolution Flow

<div class="bq-diag">
  <div class="bq-diag-head"><b>Dependency resolution</b><span>Job B, dependsOn: [A]</span></div>
  <div class="bq-diag-group">
    <span class="bq-diag-group-label">push B, job with dependencies</span>
    <div class="bq-diag-layer">1. Push B, check: is A completed?</div>
    <div class="bq-diag-arrow">↓</div>
    <div class="bq-diag-row">
      <div class="bq-diag-cell">NO <i>add B to waitingDeps, register B in dependencyIndex[A]</i></div>
      <div class="bq-diag-cell">YES <i>push B to active queue</i></div>
    </div>
  </div>
  <div class="bq-diag-group">
    <span class="bq-diag-group-label">when A completes</span>
    <div class="bq-diag-layer">1. Add A.id to pendingDepChecks</div>
    <div class="bq-diag-arrow">↓</div>
    <div class="bq-diag-layer">2. Event-driven flush <i>scheduled on the next microtask, coalescing completions from the same tick; a 30s interval acts as safety fallback only</i></div>
    <div class="bq-diag-arrow">↓</div>
    <div class="bq-diag-layer">3. For each completedId, get dependencyIndex[completedId] <i>Set&lt;jobIds&gt;</i></div>
    <div class="bq-diag-arrow">↓</div>
    <div class="bq-diag-layer bq-diag-accent">4. For each waiting job, check all deps in completedJobs, if YES move from waitingDeps to queue</div>
  </div>
</div>

**Reverse Index:**

<div class="bq-diag">
  <div class="bq-diag-head"><b>Reverse index</b><span><code>dependencyIndex: Map&lt;JobId, Set&lt;JobId&gt;&gt;</code></span></div>
  <div class="bq-diag-flow">
    <div class="bq-diag-cell">A</div>
    <div class="bq-diag-arrow">→</div>
    <div class="bq-diag-cell bq-diag-accent">{B, C} <i>B and C wait for A</i></div>
  </div>
  <div class="bq-diag-flow">
    <div class="bq-diag-cell">D</div>
    <div class="bq-diag-arrow">→</div>
    <div class="bq-diag-cell">{E} <i>E waits for D</i></div>
  </div>
</div>

## DLQ (Dead Letter Queue) Flow

<div class="bq-diag">
  <div class="bq-diag-head"><b>Move to DLQ</b><span>job fails with attempts &gt;= maxAttempts</span></div>
  <div class="bq-diag-group">
    <span class="bq-diag-group-label">DlqEntry</span>
    <div class="bq-diag-row">
      <div class="bq-diag-cell">job <i>original job</i></div>
      <div class="bq-diag-cell bq-diag-accent">reason <i>explicit_fail, max_attempts_exceeded, timeout, stalled, ttl_expired, worker_lost, unknown</i></div>
      <div class="bq-diag-cell">error <i>error message</i></div>
    </div>
    <div class="bq-diag-row">
      <div class="bq-diag-cell">attempts <i>full history: attempt, error, duration</i></div>
      <div class="bq-diag-cell">enteredAt <i>timestamp</i></div>
      <div class="bq-diag-cell">nextRetryAt <i>if autoRetry enabled</i></div>
      <div class="bq-diag-cell">expiresAt <i>7 days default</i></div>
    </div>
  </div>
  <div class="bq-diag-arrow">↓</div>
  <div class="bq-diag-group">
    <span class="bq-diag-group-label">DLQ maintenance, every 60s</span>
    <div class="bq-diag-row">
      <div class="bq-diag-cell">1. Auto-retry eligible entries <i>nextRetryAt &lt;= now &amp;&amp; retryCount &lt; maxAutoRetries</i></div>
      <div class="bq-diag-cell">2. Purge expired entries <i>expiresAt &lt;= now</i></div>
      <div class="bq-diag-cell">3. Enforce maxEntries per queue <i>10k default, FIFO eviction when full</i></div>
    </div>
  </div>
</div>

## Rate & Concurrency Limiting

<div class="bq-diag">
  <div class="bq-diag-head"><b>Pull request</b><span>rate and concurrency limiting</span></div>
  <div class="bq-diag-group">
    <span class="bq-diag-group-label">1. check rate limit, token bucket</span>
    <div class="bq-diag-row">
      <div class="bq-diag-cell">Tokens available <i>consume 1, proceed</i></div>
      <div class="bq-diag-cell">No tokens <i>return null</i></div>
    </div>
  </div>
  <div class="bq-diag-arrow">↓</div>
  <div class="bq-diag-group">
    <span class="bq-diag-group-label">2. check concurrency limit</span>
    <div class="bq-diag-row">
      <div class="bq-diag-cell">active &lt; limit <i>increment, proceed</i></div>
      <div class="bq-diag-cell">At limit <i>return null</i></div>
    </div>
  </div>
  <div class="bq-diag-arrow">↓</div>
  <div class="bq-diag-layer bq-diag-accent">3. Pop from priority queue</div>
  <div class="bq-diag-group">
    <span class="bq-diag-group-label">token bucket</span>
    <div class="bq-diag-row">
      <div class="bq-diag-cell">capacity <i>N tokens</i></div>
      <div class="bq-diag-cell">refillRate <i>N tokens/sec</i></div>
      <div class="bq-diag-cell">tryAcquire() <i>1. refill based on elapsed time, 2. if tokens &gt;= 1 consume and return true, 3. else return false</i></div>
    </div>
  </div>
</div>

## FIFO Groups

Ensures only one job per group processes at a time:

<div class="bq-diag">
  <div class="bq-diag-head"><b>FIFO groups</b><span>job with groupId: "user-123"</span></div>
  <div class="bq-diag-group">
    <span class="bq-diag-group-label">PULL</span>
    <div class="bq-diag-layer">1. Peek job at queue head, 2. check: is groupId in activeGroups?</div>
    <div class="bq-diag-arrow">↓</div>
    <div class="bq-diag-row">
      <div class="bq-diag-cell">YES <i>job stays at the head, this pull returns no job (preserves strict per-group order)</i></div>
      <div class="bq-diag-cell bq-diag-accent">NO <i>pop, add to activeGroups, return job</i></div>
    </div>
  </div>
  <div class="bq-diag-group">
    <span class="bq-diag-group-label">ACK / FAIL</span>
    <div class="bq-diag-layer">1. Remove groupId from activeGroups, 2. next job in the same group can now be pulled</div>
  </div>
</div>
