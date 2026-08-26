# PostgreSQL 18 Multi-Broker Performance Analysis — 2026-08-26

## Scope and conclusion

This engineering campaign investigates PostgreSQL 18.6 bottlenecks with one
database and up to four independent bunqueue broker processes. It is separate
from the clean-tree PostgreSQL 15–18 compatibility matrix: the measurements in
this report intentionally use an uncommitted performance candidate so that
before/after behavior can be evaluated before commit.

The strongest actionable result is batch amortization. With four brokers,
16 fixed consumers, pool size 4, and the production 250 ms polling interval,
increasing the command batch from 100 to 250 raised the lifecycle median from
7,478 to 8,362 jobs/s (+11.8%). The mean CI95 intervals did not overlap. Median
database commits fell from 1,076 to 627 per 10,000-job sample (-41.7%). A batch
of 500 did not improve on 250 in the preliminary sweep.

The code candidate primarily improves tail behavior rather than headline
throughput at batch 100. Against the clean control, median lifecycle throughput
changed by +0.5% with overlapping confidence intervals, while `ACKB` p95 fell
from 165.8 to 108.9 ms, `PUSHB` p95 fell from 37.5 to 34.3 ms, and lifecycle CV
fell from 5.27% to 3.26%. `PULLB` p95 regressed from 83.8 to 100.7 ms, so the
candidate is not described as an unconditional latency improvement.

PostgreSQL `work_mem=8MB` was inconclusive against the 4 MB default, and 32 MB
was slower despite eliminating temporary-file spill. Pool sizes above 4 did not
improve this fixed 16-consumer workload. The recommended measured configuration
for the four-broker topology is therefore batch 250, pool size 4 per broker,
polling interval 250 ms, and the default 4 MB `work_mem`, subject to validation
on the production host and payload mix.

## Environment and evidence class

| Field             | Value                                                      |
| ----------------- | ---------------------------------------------------------- |
| Date              | 2026-08-26, Europe/Rome                                    |
| Base commit       | `fd29bbcf6b780cdcc49a8dfc84f4e52c1db20f42`                 |
| Runtime           | Bun 1.4.0, arm64                                           |
| Database          | PostgreSQL 18.6 Homebrew native binary                     |
| Host              | Apple M1 Max, 10 logical CPUs, macOS/Darwin 25.6.0         |
| Durability        | `fsync=on`, `synchronous_commit=on`, `full_page_writes=on` |
| PostgreSQL memory | `shared_buffers=128MB`; `work_mem` stated per campaign     |
| Virtualization    | None for database, brokers, clients, or timing             |
| Evidence class    | Local engineering; the host was not quiesced               |

The 1-minute host load ranged from 3.45 to 7.02 during the tuned and batch
campaigns. The clean control ran under still higher load, 8.59 to 11.72. This
load difference is why small before/after throughput deltas are treated as
inconclusive even when medians differ.

Every measured sample used a fresh `initdb` cluster, database, broker process
set, ports, namespace, queue, producer clients, and consumer clients. It
reconciled accepted IDs, invoked IDs, duplicate delivery, terminal database
rows, deadlocks, WAL, blocks, transactions, and temporary bytes. Integrity
failure aborts a sample instead of becoming a timing result.

## Bottleneck investigation

### 1. Shared completion-metric rows

`pg_stat_activity` sampling showed concurrent `ACKB` transactions waiting on
the same `(namespace, queue, metric_type, minute)` metric bucket. The winning
transaction previously performed the bucket write, total write, and pruning as
separate statements while the other broker transactions waited.

The candidate uses a focused metric writer that:

- normalizes and sorts affected queues to preserve a canonical lock order;
- prunes old buckets before acquiring the current hot bucket;
- updates the bucket and lifetime total in one CTE statement; and
- preserves exact totals when metric buckets are disabled; and
- keeps `prevTS` and its matching minute count monotonic across reversed
  transaction arrival.

Four-store concurrency tests complete 400 jobs against the same metric key and
assert exact bucket, total, and lifetime values. A deterministic two-transaction
regression reverses timestamp arrival and proves the newer metric timestamp is
not overwritten. The batch-100 A/B result is
consistent with shorter contention: `ACKB` p95 improved by 34.3%. Exact causal
attribution is not claimed because the candidate also changes event catch-up
and removes a redundant queue-state transaction.

### 2. Event catch-up feedback loop

At 100,000 jobs, a broker falling behind the bounded event window observed a
prune watermark and refreshed the complete queue. Four brokers could then run
wide `SELECT * ... ORDER BY created_at, id` refreshes concurrently. Sampling
observed `BuffileWrite`, full-queue refreshes, and multi-gigabyte broker physical
footprints in the first diagnostic.

The retained candidate reads up to 4,096 journal events per catch-up query and
limits each authoritative projection query to 1,000 IDs. The event batch is
large enough to reduce catch-up round trips but remains bounded; the smaller
projection batch avoids replacing one large journal read with `ANY(10000+)`
projection queries.

One-shot 100,000-job diagnostics are not publication distributions, but they
identified the tuning knee:

| Journal batch |    Lifecycle | `PULLB` p95 | `ACKB` p95 | PostgreSQL temp bytes |
| ------------: | -----------: | ----------: | ---------: | --------------------: |
|         1,000 | 5,068 jobs/s |      193 ms |     222 ms |               1.71 GB |
|         4,096 | 5,537 jobs/s |      132 ms |     161 ms |                448 MB |
|        10,000 | 5,166 jobs/s |      156 ms |     179 ms |                792 MB |

The 4,096 candidate improved the single diagnostic lifecycle by 9.3% and cut
temporary bytes by 73.8% relative to 1,000. A 10,000-event read was slower, so
the retained cap is 4,096 rather than the complete default retention window.
Full refresh was reduced but not eliminated under this deliberately extreme
burst; production monitoring should still include queue-refresh health and
PostgreSQL temporary bytes.

### 3. Transaction overhead

The claim path previously inserted the queue-state sentinel in an autonomous
transaction even though the following locked transaction already creates the
same row safely when absent. Removing that redundant transaction reduces commit
count without changing TTL semantics or lock order. TTL expiry remains in its
own transaction: folding expiry into claim would acquire destruction/job locks
before queue state and was rejected as an unsafe lock-order optimization.

The final batch-100 candidate used a median 1,538 commits per sample versus
1,613 for the clean control (-4.7%).

### 4. Rejected optimizations

Performance changes were retained only when the complete lifecycle improved or
the operational trade-off was useful:

- a combined FIFO/LIFO feature probe increased `PULLB` tail latency and was
  reverted;
- a queue snapshot ordering index eliminated some sort spill but made wide
  index scans slower than sequential scan plus sort and was reverted;
- non-blocking automatic retention sweeps removed advisory-lock waits but opened
  roughly 21,000 transactions per 100,000-job sample versus about 12,800, so the
  change was reverted; and
- `work_mem=32MB` eliminated spill but reduced the four-broker lifecycle median
  by 8.2%, so it is not recommended from this evidence.

## Clean-control versus code candidate

Both rows use PostgreSQL 18.6, four brokers, 10,000 jobs, batch 100, four
producers, 16 consumers, pool size 12, polling interval 25 ms, one discarded
warm-up, and seven measured fresh samples. These aggressive test-helper settings
are intentionally not the production recommendation.

| Metric            | Clean control |     Candidate |     Delta |
| ----------------- | ------------: | ------------: | --------: |
| Lifecycle median  |  6,740 jobs/s |  6,776 jobs/s |     +0.5% |
| Lifecycle CV      |         5.27% |         3.26% |    -38.1% |
| Mean CI95         |   6,359–7,011 |   6,515–6,920 |  overlaps |
| Admission median  | 17,754 jobs/s | 18,294 jobs/s |     +3.0% |
| Processing median | 10,864 jobs/s | 10,881 jobs/s |     +0.2% |
| `PUSHB` p95       |       37.5 ms |       34.3 ms |     -8.5% |
| `PULLB` p95       |       83.8 ms |      100.7 ms |    +20.2% |
| `ACKB` p95        |      165.8 ms |      108.9 ms |    -34.3% |
| Commits/sample    |         1,613 |         1,538 |     -4.7% |
| WAL bytes/job     |         7,054 |         7,279 |     +3.2% |
| Median temp bytes |             0 |             0 | unchanged |

The throughput confidence intervals overlap and the host loads differed, so
the +0.5% lifecycle value is not proof of a throughput win. The lower ACK tail,
commit count, and CV are the defensible benefits; the higher PULL tail and WAL
are explicit regressions to keep visible.

## Batch-size and pool sweeps

The preliminary four-broker batch sweep used three fresh measured samples per
cell at pool 12 and polling interval 25 ms:

| Batch | Lifecycle median |    CV |
| ----: | ---------------: | ----: |
|    50 |     5,826 jobs/s | 1.20% |
|   100 |     6,323 jobs/s | 3.33% |
|   250 |     7,712 jobs/s | 2.33% |
|   500 |     7,660 jobs/s | 4.39% |

Batch 250 was 22.0% faster than 100 in this sweep; 500 was 0.7% slower than 250. This establishes 250 as the observed knee, not a universal maximum.

With batch 250 and polling interval 250 ms, three-sample pool medians were:

| Pool connections per broker | Lifecycle median |
| --------------------------: | ---------------: |
|                           4 |     8,060 jobs/s |
|                           6 |     7,922 jobs/s |
|                          10 |     7,965 jobs/s |
|                          12 |     7,894 jobs/s |

The distributions were noisy and close. No evidence supports pools above 4 for
this fixed total of 16 consumers. More connections increase PostgreSQL memory
and scheduling pressure without creating more application work.

## Seven-sample batch comparison

The final batch comparison fixes PostgreSQL 18.6, four brokers, pool size 4,
polling interval 250 ms, four producers, 16 consumers, and default 4 MB
`work_mem`.

| Metric               |     Batch 100 |     Batch 250 |           Delta |
| -------------------- | ------------: | ------------: | --------------: |
| Lifecycle median     |  7,478 jobs/s |  8,362 jobs/s |          +11.8% |
| Lifecycle CV         |         1.40% |         4.93% | higher variance |
| Mean CI95            |   7,383–7,577 |   7,923–8,679 |      no overlap |
| Admission median     | 19,995 jobs/s | 24,150 jobs/s |          +20.8% |
| Processing median    | 11,948 jobs/s | 12,822 jobs/s |           +7.3% |
| `PUSHB` p95          |       34.8 ms |       64.1 ms |  larger command |
| `PULLB` p95          |       94.3 ms |      155.8 ms |  larger command |
| `ACKB` p95           |      100.3 ms |      183.5 ms |  larger command |
| Commits/sample       |         1,076 |           627 |          -41.7% |
| WAL bytes/job        |         8,075 |        10,649 |          +31.9% |
| Temp spill incidence |           1/7 |           7/7 |       increased |
| Median temp bytes    |             0 |       24.1 MB |       increased |

Batch 250 improves jobs per second by amortizing round trips and transactions,
but each command takes longer and WAL/temp usage increases. Applications that
optimize individual command latency rather than aggregate throughput may prefer
batch 100.

## Tuned topology curve

With batch 250, pool size 4, polling interval 250 ms, and default 4 MB
`work_mem`, all 210,000 measured IDs across 21 samples were accepted, invoked,
and completed exactly once. Duplicate invocations and PostgreSQL deadlocks were
zero.

| Brokers | Admission median | Processing median | Lifecycle median |    CV |    Mean CI95 |
| ------: | ---------------: | ----------------: | ---------------: | ----: | -----------: |
|       1 |           19,244 |            13,914 |            8,112 | 7.12% |  7,400–8,442 |
|       2 |           24,590 |            17,831 |           10,075 | 5.83% | 9,561–10,651 |
|       4 |           24,150 |            12,822 |            8,362 | 4.93% |  7,923–8,679 |

Two brokers were 24.2% faster than one. Four were 17.0% slower than two and
3.1% faster than one. Four-broker claim shares remained between 20.0% and 27.5%
across samples, so reduced efficiency was database contention rather than one
idle broker.

## `work_mem` sweep

The four-broker batch-250 cell was repeated with seven samples at each setting:

| `work_mem` | Lifecycle median |    CV |   Mean CI95 | Spill samples | Median temp bytes |
| ---------: | ---------------: | ----: | ----------: | ------------: | ----------------: |
|       4 MB |     8,362 jobs/s | 4.93% | 7,923–8,679 |           7/7 |           24.1 MB |
|       8 MB |     8,510 jobs/s | 6.93% | 8,006–9,103 |           6/7 |           15.0 MB |
|      32 MB |     7,678 jobs/s | 5.30% | 7,160–7,898 |           0/7 |                 0 |

The 8 MB and 4 MB confidence intervals overlap; the apparent +1.8% median is
inconclusive. At 32 MB the intervals nearly separate in the wrong direction.
Eliminating every spill is therefore not a throughput objective by itself.
`work_mem` is allocated per sort/hash operation and can multiply across pools,
so production changes require connection-aware memory budgeting.

## Operational recommendations

1. Start four-broker throughput testing with batch 250, pool size 4 per broker,
   polling interval 250 ms, and default `work_mem`.
2. Prefer two brokers when the objective is maximum throughput on a similar
   ten-core single-database host; use four for broker availability or front-end
   capacity, not an expectation of linear database scaling.
3. Use batch 100 when lower per-command tail latency matters more than aggregate
   throughput or when larger batch WAL/temp growth is unacceptable.
4. Monitor `pg_stat_database.temp_bytes`, WAL bytes/job, transaction rate,
   queue-refresh health, and ACK/PULL tails together. A faster median that moves
   excessive work to WAL or temp storage is not a free win.
5. Re-run the matrix on production-class storage and payload sizes. These local
   figures are not capacity guarantees and must not be compared directly with
   container, VM, or another host's results.

## Correctness validation

Performance evidence was accepted only after a separate functional campaign.
Each Homebrew major used a fresh durable cluster and ran all 58 PostgreSQL test
files, including four independent broker processes, generated `fast-check`
histories, lifecycle races, event convergence, and the new shared-metric
contention regressions:

| PostgreSQL | Passed | Failed | Assertions |
| ---------- | -----: | -----: | ---------: |
| 15.19      |    224 |      0 |      4,904 |
| 16.15      |    224 |      0 |      4,944 |
| 17.11      |    224 |      0 |      5,013 |
| 18.6       |    224 |      0 |      5,009 |

The repository lifecycle model separately passed 11 tests and 84,814
assertions. The disposable Docker sandbox then passed 8,245 unit tests, 489 TCP
integration assertions, and 332 embedded integration assertions with no
failure, OOM, or reported resource anomaly. PostgreSQL tests skipped by that
SQLite-oriented sandbox are the tests run explicitly in the four-version table
above. This validation is correctness evidence, not benchmark evidence.

## Raw evidence

Raw JSON is intentionally ignored by Git and retained locally under
`artifacts/benchmarks/`:

| Campaign                               | Artifact                                                     | SHA-256                                                            |
| -------------------------------------- | ------------------------------------------------------------ | ------------------------------------------------------------------ |
| Clean control, four brokers, batch 100 | `postgres-performance-control-2026-08-26T18-55-08.011Z.json` | `223bf8b9339385a11f3fa48d66c7ce5d0f909f31735936519f3b71221eab0f67` |
| Tuned 1/2/4 brokers, batch 250, 4 MB   | `postgres-versions-2026-08-26T19-30-11.032Z.json`            | `7a054d8d4547751736619f6776addc8010ddf5c55d69ddc0278967c000ec5208` |
| Four brokers, batch 100, 4 MB          | `postgres-versions-2026-08-26T19-33-37.614Z.json`            | `0d35a1059e84734e0085200fa745ea1f93c5bf3b76b5c5896ab41288547e5c54` |
| Four brokers, batch 250, 8 MB          | `postgres-versions-2026-08-26T19-38-55.634Z.json`            | `9ef5a812ad6ea81ff7ba0bb802d603df1fa5d269ad5211f54701fce2687e12ed` |
| Four brokers, batch 250, 32 MB         | `postgres-versions-2026-08-26T19-37-36.903Z.json`            | `bb84f1074826fb1a418f502855c2611ff608b502be2f5692337393a2668722de` |
| Final batch-100 code candidate         | `postgres-versions-2026-08-26T19-41-48.946Z.json`            | `35cf6b415d78391c61db7b9f406ebe15d663fc3b5d20e22f6a4a29112cf756cb` |

The runner now records `runtimeWorktreeStatus` and configured `workMem` so a
dirty performance candidate cannot be mistaken for its committed `HEAD:src`
tree. Older artifacts in this report predate those two identity fields; their
exact command configuration and modified runtime files are stated above.
