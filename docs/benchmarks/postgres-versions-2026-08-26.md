# Native PostgreSQL 15–18 Engineering Benchmark — 2026-08-26

## Executive summary

This report compares the same bunqueue PostgreSQL workload on native PostgreSQL
15.19, 16.15, 17.11, and 18.6. It measures one, two, and four independent broker
processes against one database while keeping the total producer and consumer
connection counts fixed. Docker and virtual machines were not used for timing.

Correctness is the primary acceptance gate. Across 84 measured samples,
840,000 accepted job IDs were invoked and completed exactly once. There were no
duplicate invocations, failed samples, PostgreSQL deadlocks, or temporary-file
spills. Twelve discarded warm-ups added another 120,000 valid jobs, for 960,000
jobs exercised by the complete campaign.

The versions were close on this workload. PostgreSQL 18.6 had the highest
one- and four-broker lifecycle medians; PostgreSQL 15.19 led the two-broker row
by 0.7%, inside overlapping 95% confidence intervals. Two brokers outperformed
one for every version. Four brokers remained faster than one but slower than
two on this ten-core host, showing the cost of added database contention at a
fixed total of 16 consumers.

## Identity and environment

| Field             | Value                                                   |
| ----------------- | ------------------------------------------------------- |
| Date              | 2026-08-26, Europe/Rome                                 |
| bunqueue commit   | `fd29bbcf6b780cdcc49a8dfc84f4e52c1db20f42`              |
| Runtime tree      | `dcd0d727cd2413f38d26a5517ed8cd990c8f2906` (`HEAD:src`) |
| Bun               | 1.4.0, arm64                                            |
| OS                | macOS 26.6.2 / Darwin 25.6.0                            |
| Hardware          | Apple M1 Max, 10 logical CPUs                           |
| PostgreSQL        | 15.19, 16.15, 17.11, and 18.6 Homebrew native binaries  |
| Broker pool       | 12 PostgreSQL connections per broker                    |
| Event polling     | 25 ms                                                   |
| `work_mem`        | Fresh-cluster default, 4 MB                             |
| Virtualization    | None for PostgreSQL, brokers, clients, or timing        |
| One-minute load   | 2.99 at start, 4.79 at completion                       |
| Campaign duration | 718.6 seconds                                           |

The runtime source matched the committed tree. The uncommitted worktree changes
during measurement were the benchmark harness and documentation only; there was
no `src/` diff. Homebrew services remained stopped. Every PostgreSQL process
used a temporary data directory and was removed after its sample.

## Measurement protocol

- Each sample initialized a new PostgreSQL cluster and database, then launched
  one, two, or four independent bunqueue broker OS processes with unique
  TCP/HTTP ports, namespace, broker IDs, and queue.
- Durability was explicitly kept on: `fsync=on`, `synchronous_commit=on`, and
  `full_page_writes=on`. Every server used `shared_buffers=128MB` and
  `max_connections=100`; the sample queried these settings before timing.
- A sample admitted 10,000 jobs in `PUSHB` batches of 100. Four producer
  connections were fixed across every topology. Each payload contained its
  integer sequence plus a 100-byte string.
- After admission, 16 consumer connections were fixed across every topology.
  They used `PULLB` batches of 100 and acknowledged through the next broker,
  forcing cross-broker database authority whenever more than one broker ran.
- Each broker used a 12-connection PostgreSQL pool and a 25 ms event polling
  interval. The fresh clusters retained their 4 MB default `work_mem`.
- Broker/schema startup, PostgreSQL startup, integrity queries, and cleanup
  were outside the timed interval. Lifecycle time contains admission followed
  by claim and acknowledgement of all 10,000 jobs.
- One fresh warm-up per version/topology was discarded. Seven fresh measured
  samples remained for each of the 12 cells. Version and topology order rotated
  between rounds to reduce monotonic thermal and background-load bias.
- The representative rate is the sample median. CV is sample standard
  deviation divided by mean. The 95% interval is a two-sided Student's t
  confidence interval for the mean with six degrees of freedom. It is not a
  capacity guarantee.
- Every sample rejected missing, unknown, or duplicate IDs, token-count
  mismatch, failed commands, nonterminal database rows, a nonzero deadlock
  delta, or a broker/process failure.

Reproduce the dated protocol with:

```bash
BUNQUEUE_PG_BENCH_POOL_SIZE=12 \
BUNQUEUE_PG_BENCH_POLL_INTERVAL_MS=25 \
BUNQUEUE_PG_BENCH_WORK_MEM=4MB \
bun run bench:postgres:versions
```

The runner resolves `postgresql@15` through `postgresql@18` from Homebrew by
default. `BUNQUEUE_PG_BIN_<major>` may point at another native installation.
Current runner defaults use pool size 10 and 250 ms polling; the explicit
overrides above preserve the historical campaign instead of silently measuring
a different topology.

## Throughput results

All rates are jobs/s. Admission is durable `PUSHB`; processing is durable
`PULLB` plus `ACKB`; lifecycle is their sequential end-to-end interval.

| PostgreSQL | Brokers | Admission median | Processing median | Lifecycle median | Lifecycle CV |   Mean CI95 |
| ---------- | ------: | ---------------: | ----------------: | ---------------: | -----------: | ----------: |
| 15.19      |       1 |           16,658 |            11,404 |            6,854 |        3.45% | 6,682–7,122 |
| 15.19      |       2 |           19,732 |            15,296 |            8,494 |        2.40% | 8,307–8,684 |
| 15.19      |       4 |           19,012 |            12,199 |            7,406 |        1.52% | 7,298–7,506 |
| 16.15      |       1 |           16,667 |            11,505 |            6,870 |        2.91% | 6,678–7,047 |
| 16.15      |       2 |           19,798 |            13,952 |            8,124 |        3.72% | 7,937–8,503 |
| 16.15      |       4 |           18,881 |            11,613 |            7,168 |        2.52% | 7,056–7,393 |
| 17.11      |       1 |           16,189 |            11,157 |            6,550 |        2.01% | 6,470–6,715 |
| 17.11      |       2 |           19,452 |            13,600 |            8,004 |        2.99% | 7,761–8,202 |
| 17.11      |       4 |           18,473 |            11,921 |            7,288 |        3.70% | 7,021–7,519 |
| 18.6       |       1 |           17,020 |            11,557 |            6,945 |        2.86% | 6,694–7,058 |
| 18.6       |       2 |           20,295 |            14,716 |            8,432 |        3.22% | 8,218–8,723 |
| 18.6       |       4 |           20,182 |            12,742 |            7,788 |        1.57% | 7,640–7,865 |

Relative to one broker, the two-broker lifecycle median increased by 23.9% on
15.19, 18.2% on 16.15, 22.2% on 17.11, and 21.4% on 18.6. Moving from two to
four brokers reduced the median by 12.8%, 11.8%, 8.9%, and 7.6% respectively.
This is a saturation curve for this host and workload, not a universal broker
count recommendation.

Across versions, the fastest-to-slowest lifecycle spread was 6.0% with one
broker, 6.1% with two, and 8.6% with four. PostgreSQL 18.6 admission medians
were the highest in all three topologies, but admission alone did not determine
the complete lifecycle result.

## Tail latency, WAL, and fairness

Each latency cell is the median of the seven per-sample p95 command latencies,
not a percentile pooled across samples. WAL is the median workload delta divided
by 10,000 jobs. Claim share is the minimum–maximum per-broker percentage seen
across the seven samples.

| PostgreSQL | Brokers | `PUSHB` p95 | `PULLB` p95 | `ACKB` p95 | WAL bytes/job | Claim share |
| ---------- | ------: | ----------: | ----------: | ---------: | ------------: | ----------: |
| 15.19      |       1 |     31.7 ms |    135.1 ms |   111.6 ms |         7,545 |        100% |
| 15.19      |       2 |     27.0 ms |     67.9 ms |    90.9 ms |         8,858 |      47–53% |
| 15.19      |       4 |     31.2 ms |     79.4 ms |   150.6 ms |         7,878 |      22–27% |
| 16.15      |       1 |     32.4 ms |    130.9 ms |   115.8 ms |         7,473 |        100% |
| 16.15      |       2 |     27.0 ms |     68.1 ms |   111.7 ms |         8,913 |      48–52% |
| 16.15      |       4 |     31.0 ms |     81.3 ms |   142.2 ms |         7,757 |      22–29% |
| 17.11      |       1 |     32.2 ms |    128.8 ms |   126.3 ms |         7,261 |        100% |
| 17.11      |       2 |     27.8 ms |     68.7 ms |   114.2 ms |         8,601 |      47–53% |
| 17.11      |       4 |     32.0 ms |     82.5 ms |   135.1 ms |         7,825 |      23–27% |
| 18.6       |       1 |     32.0 ms |    129.2 ms |   119.3 ms |         7,449 |        100% |
| 18.6       |       2 |     27.8 ms |     70.4 ms |   105.3 ms |         8,894 |      49–51% |
| 18.6       |       4 |     31.1 ms |     73.7 ms |   160.9 ms |         8,274 |      22–28% |

Two-broker work remained close to 50/50. Four-broker work remained close to
25% each. The higher four-broker ACK tail is consistent with its lower
lifecycle rate: more independent broker pools add transaction and event-stream
contention even though consumer count is unchanged.

## Integrity evidence

| Invariant                    | Measured result |
| ---------------------------- | --------------: |
| Measured samples             |  84/84 accepted |
| Accepted IDs                 |         840,000 |
| Unique invoked IDs           |         840,000 |
| Authoritative completed rows |         840,000 |
| Duplicate invocations        |               0 |
| PostgreSQL deadlocks         |               0 |
| PostgreSQL temporary bytes   |               0 |

The benchmark reads `pg_stat_database`, `pg_stat_wal`, and the authoritative
`bunqueue_jobs` rows after every sample. A fast sample with incomplete state is
therefore an error, not a result.

## Interpretation and limits

1. All four PostgreSQL majors executed the same multi-broker lifecycle
   correctly. The performance differences are smaller than the topology
   effect and several confidence intervals overlap.
2. PostgreSQL 18.6 is not declared universally faster. It led admission in all
   topologies and lifecycle with one and four brokers; PostgreSQL 15.19 led the
   two-broker median by less than one percent.
3. Four brokers do not provide four times the throughput of one database on a
   ten-core laptop. They provide availability and horizontal broker capacity,
   while shared lock/event/WAL work still has one PostgreSQL bottleneck.
4. The host was native but not dedicated. Load rose during the campaign, so
   these figures are local engineering evidence and cannot be compared as
   absolute capacity against another machine or virtualized result.
5. The workload uses small successful jobs, batch size 100, no handler work,
   and one database. Payload size, transaction latency, storage device, pool
   sizing, retention, and production query mix can change the ordering.

## Raw evidence

The complete machine-readable report is retained locally at
`artifacts/benchmarks/postgres-versions-2026-08-26T18-13-50.637Z.json` with
SHA-256
`ff462590ca80856a94418bd57220a6df97d6a91c7a8f1ca060668898197eaeaa`.
Generated artifacts are intentionally excluded from repository commits. The
tracked harness emits the same raw schema and all derived rate summaries.
