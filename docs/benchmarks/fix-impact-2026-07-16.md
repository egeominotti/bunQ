# Core Fix Impact Benchmark — 2026-07-16

This report compares the last committed revision before the fixes
(`c8b9a8f966a9`) with the complete candidate worktree. It covers SQLite
recovery, job listing, FIFO-group scheduling, statistics, temporal indexing,
waiter notification, and delayed-heap retention.

## Method

- Host: Apple M1 Max, 10 cores, 32 GiB RAM; macOS 26.5.2 (25F84).
- Runtime: Bun 1.3.14, arm64.
- Isolation: the baseline ran from a detached Git worktree; the candidate ran
  from the main worktree. Both used the same `node_modules` and harness.
- Order: baseline → candidate → candidate → baseline, using a fresh Bun process
  for each full run to reduce warm-cache and thermal-order bias.
- Aggregation: samples from both processes were pooled; the table reports the
  median and nearest-rank p95. Setup is excluded except for startup recovery and
  delayed-heap churn, where setup is the operation under test.
- Correctness is measured with each timing. An incorrect baseline result is not
  treated as a performance win.
- Memory retention uses exact internal live/heap entry counts, not RSS. RSS is
  allocator- and GC-dependent and would obscure this regression.

Reproduce either side with:

```bash
bun bench/fix-impact.ts \
  --label=after --revision=worktree --source-root="$PWD" \
  --profile=full --output=/tmp/bunqueue-after.json
```

The full profile is defined in `bench/fix-impact/types.ts`; the runner also
accepts `--profile=smoke` for fast harness validation.

## Results

Times are milliseconds. `×` is baseline median divided by candidate median.

| Operation and workload | Before median / p95 | After median / p95 | Change | Correctness |
| --- | ---: | ---: | ---: | --- |
| Recovery, 10,001 active jobs (6 samples) | 967.947 / 1001.384 | 1187.993 / 1250.521 | +22.7% | Before left 1 active and doubled the queued counter; after exact |
| In-memory descending page, 50k jobs (42) | 1.058 / 2.078 | 3.612 / 5.113 | not comparable | Before returned indexes 100…1; after 49,999…49,900 |
| SQLite prioritized page, 20k normal + 5k priority (42) | 2.214 / 2.545 | 2.127 / 2.320 | not comparable | Before returned 0/100; after 100/100 priority jobs |
| SQLite unfiltered page at offset 20k (42) | 11.109 / 11.684 | 0.628 / 0.688 | **17.69×** | Exact on both; temporary sort removed after |
| Pull past 5k jobs from an active group (14) | 0.007 / 0.028 | 1.672 / 4.394 | not comparable | Before returned `null`; after returned eligible `B1` |
| `getStats`, 200 queues / 50k jobs (30) | 1.457 / 1.847 | 1.986 / 3.017 | **+36.3%** | Exact on both |
| `getQueuesSummary`, 200 queues / 50k jobs (30) | 503.575 / 522.408 | 4.883 / 6.167 | **103.13×** | Before omitted 15k prioritized jobs; after exact |
| Sparse queue temporal lookup behind 500k unrelated jobs (62) | 6.227896 / 9.467917 | 0.000417 / 0.005958 | **~14,935×** | Exact on both |
| Temporal removal by ID behind 500k unrelated jobs (30) | 5.568917 / 6.593417 | 0.001792 / 0.135667 | **~3,108×** | Exact on both |
| Notify 10k active waiters (10) | 446.255 / 487.839 | 0.807 / 1.316 | **552.98×** | Both drain; only after coalesces surplus notification debt |
| 100k immediate delayed add/remove pairs (10) | 17.261 / 18.838 | 18.694 / 20.987 | +8.3% | Retained heap entries: 100,000 → 0 |

## Correctness evidence

- Recovery now produces 0 active rows, 10,001 pending/ready/indexed jobs, and a
  queued counter of 10,001. The baseline produced 1 / 10,000 / 10,000 / 20,000.
- SQLite deep-page `EXPLAIN QUERY PLAN` changed from
  `idx_jobs_queue_state` plus `USE TEMP B-TREE FOR ORDER BY` to the covering
  `idx_jobs_queue_created` index.
- FIFO-group pull returns `B1` in every sample instead of stopping at the blocked
  `A2` head.
- Queue summary now carries `prioritized` as a separate field. This fixes both
  the in-process return type and the bare `GET /queues/summary` JSON contract.
- A surplus batch notification now coalesces to one retry hint: the second wait
  observes its timeout instead of consuming accumulated false credits.
- With zero live delayed jobs, the delayed heap is empty after every 100k-pair
  churn sample.

## Engineering interpretation

The SQLite indexes, one-pass per-queue aggregation, queue-local temporal index,
and cursor-based waiter structure deliver large wins without weakening the
measured invariants. The summary improvement is especially important for
dashboards because it removes the prior queues × global-jobs scan.

Three paths remain performance work:

1. `getStats()` still classifies every queued job from `runAt` and priority. Its
   O(n) semantics are correct, but this candidate is 36% slower in this workload.
   Live per-state counters, with exact updates on every transition and delay
   maturation, remain the structural optimization.
2. Correct in-memory descending pagination currently collects and sorts the
   entire matching set. The old fast result was wrong, but an ordered per-state
   index or bounded top-k selection could recover sublinear first-page latency.
3. FIFO-group pull now scans/reinserts blocked entries. At 5,000 blocked jobs it
   costs about 1.7 ms median. A global heap containing only eligible group heads
   would preserve correctness with better worst-case behavior.

Recovery is 23% slower while recovering one additional job and eliminating a
double transition, so its raw latency is not an apples-to-apples regression.
Keyset recovery remains worth evaluating for larger databases. Delayed churn is
8% slower, an acceptable measured cost for eliminating unbounded stale memory;
the compaction threshold should still be watched under mixed live/stale loads.

## Regression coverage

The benchmark is performance evidence, not a replacement for deterministic
tests. The corresponding correctness suites are:

- `test/repro-sqlite-recovery-offset.test.ts`
- `test/repro-getjobs-order-pagination.test.ts`
- `test/repro-group-head-of-line.test.ts`
- `test/repro-queues-summary-prioritized.test.ts`
- `test/repro-temporal-onsquared.test.ts`
- `test/repro-waiter-notification-debt.test.ts`
- `test/temporalManager.test.ts`
- `test/sqlite-indexes.test.ts`
- `test/queue-counts-scheduler.test.ts`
- `test/statsManager.test.ts`

Performance assertions are limited to regressions with orders-of-magnitude
separation; exact latency ratios remain in the benchmark to avoid flaky CI.
