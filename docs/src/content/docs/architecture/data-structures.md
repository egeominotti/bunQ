---
title: "Data Structures: MinHeap, Skip List, LRU, Hashing"
description: "Data structures powering bunqueue: 4-ary MinHeap, skip lists, LRU cache, FNV-1a hashing, and read-write locks with complexities."
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og-image.png
---

<div class="bq-wrap bq-hero">
  <span class="bq-eyebrow">architecture · data structures</span>
  <h1 class="bq-hero-h1 bq-bench-h1">Heaps, skip lists, <em>LRUs.</em></h1>
  <p class="bq-hero-sub">bunqueue uses specialized data structures optimized for job queue operations: a 4-ary MinHeap, skip lists, LRU caches, FNV-1a hashing, and read-write locks.</p>
</div>

## Overview

| Structure | Use Case | Complexity |
|-----------|----------|------------|
| 4-ary MinHeap | Priority queue, cron scheduling | O(log₄ n) |
| Skip List | Temporal indexing, delayed jobs | O(log n) |
| LRU Cache | Job results, custom IDs | O(1) |
| Hash (FNV-1a) | Sharding, distribution | O(n) |

## 4-ary MinHeap

Used for priority queues and cron scheduling.

<div class="bq-diag">
  <div class="bq-diag-head"><b>Why 4-ary vs binary?</b><span>cache locality</span></div>
  <div class="bq-diag-row">
    <div class="bq-diag-cell">Binary heap <i>height log₂(n) = 16 levels for 65k items, 2 children per node, more memory indirections</i></div>
    <div class="bq-diag-cell bq-diag-accent">4-ary heap <i>height log₄(n) = 8 levels for 65k items, 4 children per node, children fit in cache line (64 bytes), fewer cache misses</i></div>
  </div>
  <p class="bq-diag-note">Trade-off: 4 comparisons per level vs 2. Win: better cache locality outweighs extra comparisons.</p>
</div>

### Heap with Lazy Deletion

<div class="bq-diag">
  <div class="bq-diag-head"><b>Generation tracking</b><span>lazy deletion</span></div>
  <div class="bq-diag-layer">Each entry has a generation number <i>{ jobId, priority, runAt, generation: 42n }</i></div>
  <div class="bq-diag-layer">Index maps jobId → { job, generation }</div>
  <div class="bq-diag-row">
    <div class="bq-diag-cell bq-diag-accent">REMOVE <i>delete from index O(1), heap entry becomes stale</i></div>
    <div class="bq-diag-cell">POP <i>loop: peek, check generation match, mismatch: skip stale entry, match: return job</i></div>
    <div class="bq-diag-cell">COMPACT, stale ratio &gt; 20% <i>filter valid entries, rebuild heap O(n)</i></div>
  </div>
</div>

## Skip List

Used for temporal indexing and efficient range queries.

<div class="bq-diag">
  <div class="bq-diag-head"><b>Skip list structure</b><span>sorted list with express lanes</span></div>
  <div class="bq-diag-group">
    <span class="bq-diag-group-label">level 3</span>
    <div class="bq-diag-flow">
      <div class="bq-diag-arrow">→</div>
      <div class="bq-diag-cell bq-diag-accent">50</div>
      <div class="bq-diag-arrow">→</div>
    </div>
  </div>
  <div class="bq-diag-group">
    <span class="bq-diag-group-label">level 2</span>
    <div class="bq-diag-flow">
      <div class="bq-diag-arrow">→</div>
      <div class="bq-diag-cell">25</div>
      <div class="bq-diag-arrow">→</div>
      <div class="bq-diag-cell">50</div>
      <div class="bq-diag-arrow">→</div>
      <div class="bq-diag-cell">75</div>
      <div class="bq-diag-arrow">→</div>
    </div>
  </div>
  <div class="bq-diag-group">
    <span class="bq-diag-group-label">level 1</span>
    <div class="bq-diag-flow">
      <div class="bq-diag-arrow">→</div>
      <div class="bq-diag-cell">10</div>
      <div class="bq-diag-arrow">→</div>
      <div class="bq-diag-cell">25</div>
      <div class="bq-diag-arrow">→</div>
      <div class="bq-diag-cell">30</div>
      <div class="bq-diag-arrow">→</div>
      <div class="bq-diag-cell">50</div>
      <div class="bq-diag-arrow">→</div>
      <div class="bq-diag-cell">60</div>
      <div class="bq-diag-arrow">→</div>
      <div class="bq-diag-cell">75</div>
    </div>
  </div>
  <div class="bq-diag-group">
    <span class="bq-diag-group-label">level 0</span>
    <div class="bq-diag-flow">
      <div class="bq-diag-arrow">→</div>
      <div class="bq-diag-cell">10</div>
      <div class="bq-diag-arrow">→</div>
      <div class="bq-diag-cell">25</div>
      <div class="bq-diag-arrow">→</div>
      <div class="bq-diag-cell">30</div>
      <div class="bq-diag-arrow">→</div>
      <div class="bq-diag-cell">50</div>
      <div class="bq-diag-arrow">→</div>
      <div class="bq-diag-cell">60</div>
      <div class="bq-diag-arrow">→</div>
      <div class="bq-diag-cell">75</div>
    </div>
  </div>
  <p class="bq-diag-note">Properties: probabilistic level assignment (p=0.5), expected height O(log n), simpler than balanced trees, good cache locality (sequential links).</p>
</div>

### Range Queries

<div class="bq-diag">
  <div class="bq-diag-head"><b>Range query</b><span>getOldJobs(threshold, limit)</span></div>
  <div class="bq-diag-flow">
    <div class="bq-diag-cell">1. Navigate to leftmost element <i>O(log n)</i></div>
    <div class="bq-diag-arrow">→</div>
    <div class="bq-diag-cell">2. Walk forward at level 0 <i>O(k)</i></div>
    <div class="bq-diag-arrow">→</div>
    <div class="bq-diag-cell bq-diag-accent">3. Collect while createdAt &lt; threshold</div>
  </div>
  <p class="bq-diag-note">Total: O(log n + k) where k = results. Use case: find jobs older than X for cleanup.</p>
</div>

## LRU Cache

Used for job results, custom ID mapping, and logs.

<div class="bq-diag">
  <div class="bq-diag-head"><b>Doubly-linked LRU</b><span>Map&lt;Key, Node&gt; plus doubly-linked list</span></div>
  <div class="bq-diag-flow">
    <div class="bq-diag-cell bq-diag-accent">A <i>HEAD, most recent</i></div>
    <div class="bq-diag-arrow">↔</div>
    <div class="bq-diag-cell">B</div>
    <div class="bq-diag-arrow">↔</div>
    <div class="bq-diag-cell">C</div>
    <div class="bq-diag-arrow">↔</div>
    <div class="bq-diag-cell">D <i>TAIL, LRU</i></div>
  </div>
  <div class="bq-diag-row">
    <div class="bq-diag-cell">GET(key) <i>find in map O(1), move to head, O(1) pointer updates</i></div>
    <div class="bq-diag-cell">SET(key, value) <i>if at capacity remove tail, evict LRU, add new node at head</i></div>
  </div>
  <p class="bq-diag-note">All operations: O(1).</p>
</div>

### Memory Bounds

<div class="bq-diag">
  <div class="bq-diag-head"><b>Bounded collections</b><span>max size, eviction</span></div>
  <div class="bq-diag-row">
    <div class="bq-diag-cell bq-diag-accent">completedJobs <i>50,000, FIFO batch (10%)</i></div>
    <div class="bq-diag-cell">jobResults <i>5,000, LRU</i></div>
    <div class="bq-diag-cell">jobLogs <i>10,000, LRU</i></div>
    <div class="bq-diag-cell">customIdMap <i>50,000, LRU</i></div>
    <div class="bq-diag-cell">DLQ per queue <i>10,000, FIFO</i></div>
  </div>
  <p class="bq-diag-note">BoundedSet (FIFO): no recency tracking (faster), batch eviction removes 10% when full, amortized cost across many operations.</p>
</div>

## Hash Function (FNV-1a)

Used for sharding and distribution.

<div class="bq-diag">
  <div class="bq-diag-head"><b>FNV-1a hash</b><span>algorithm</span></div>
  <div class="bq-diag-flow">
    <div class="bq-diag-cell">hash = FNV_OFFSET <i>0x811c9dc5</i></div>
    <div class="bq-diag-arrow">→</div>
    <div class="bq-diag-cell bq-diag-accent">for each byte: hash = hash XOR byte, hash = hash * FNV_PRIME <i>0x01000193</i></div>
    <div class="bq-diag-arrow">→</div>
    <div class="bq-diag-cell">return hash <i>unsigned 32-bit</i></div>
  </div>
  <div class="bq-diag-row">
    <div class="bq-diag-cell">Fast <i>~10-15 CPU cycles per character</i></div>
    <div class="bq-diag-cell">Good distribution</div>
    <div class="bq-diag-cell">Deterministic</div>
    <div class="bq-diag-cell">Non-cryptographic <i>speed over security</i></div>
  </div>
</div>

### Sharding

<div class="bq-diag">
  <div class="bq-diag-head"><b>Shard selection</b><span><code>shardIndex = fnv1aHash(queueName) &amp; SHARD_MASK</code></span></div>
  <div class="bq-diag-layer bq-diag-accent">SHARD_COUNT auto-detected from CPU cores, power of 2, SHARD_MASK = SHARD_COUNT - 1</div>
  <div class="bq-diag-row">
    <div class="bq-diag-cell">4 cores <i>SHARD_COUNT=4, SHARD_MASK=0x03, binary 11</i></div>
    <div class="bq-diag-cell">10 cores <i>SHARD_COUNT=16, SHARD_MASK=0x0f, binary 1111</i></div>
    <div class="bq-diag-cell">20 cores <i>SHARD_COUNT=32, SHARD_MASK=0x1f, binary 11111</i></div>
    <div class="bq-diag-cell">64+ cores <i>SHARD_COUNT=64, capped</i></div>
  </div>
  <p class="bq-diag-note">Why bitwise AND? 3-5x faster than modulo, requires power-of-2 shard count, <code>hash &amp; SHARD_MASK</code> is equivalent to <code>hash % SHARD_COUNT</code>.</p>
</div>

## Lock Structures

### RWLock (Read-Write Lock)

<div class="bq-diag">
  <div class="bq-diag-head"><b>Read-write lock</b><span>RWLock</span></div>
  <div class="bq-diag-row">
    <div class="bq-diag-cell">Multiple concurrent readers</div>
    <div class="bq-diag-cell">Single exclusive writer</div>
    <div class="bq-diag-cell">Writer priority <i>prevents starvation</i></div>
  </div>
  <div class="bq-diag-layer bq-diag-accent">Fast path, uncontested write <i><code>if (!writer &amp;&amp; readers === 0) { writer = true; return guard; }</code>, synchronous, no Promise</i></div>
  <div class="bq-diag-group">
    <span class="bq-diag-group-label">timeout cancellation</span>
    <div class="bq-diag-layer">Mark entry as cancelled O(1), skip cancelled entries on release, no O(n) array splice</div>
  </div>
</div>

## Complexity Summary

| Operation | Structure | Time |
|-----------|-----------|------|
| Push job | 4-ary heap | O(log₄ n) |
| Pop job | 4-ary heap | O(log₄ n) |
| Find job | Index map | O(1) |
| Remove job | Lazy deletion | O(1) |
| Get result | LRU map | O(1) |
| Shard lookup | Hash + AND | O(len) |
| Range query | Skip list | O(log n + k) |
| Lock acquire | RWLock | O(1) uncontested |
