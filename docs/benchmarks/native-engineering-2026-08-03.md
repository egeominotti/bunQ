# Native Engineering Benchmark — 2026-08-03

## Executive summary

This report records the final native macOS benchmark campaign for bunqueue
v2.8.56. It covers every maintained queue, transport, Workflow, Flow,
dependency, event, stress, and million-job runner. Performance was measured on
the host, never in Docker or an OrbStack Machine. Functional isolation was
validated separately in containers and isolated Ubuntu and Debian Machines.

Correctness is the primary result: every accepted processing sample reconciled
its exact accepted and invoked IDs, duplicate count, terminal broker state, and
failure count. Repeated fresh-process samples are reported separately from
single-run engineering diagnostics.

| Representative workload | Native result |
| --- | ---: |
| Single Workflow Engine, linear Embedded / TCP | 247 / 313 workflows/s median |
| 12 Workflow Engine instances, Embedded / TCP | 758 / 618 workflows/s median |
| Comprehensive Embedded processing, 50,000 jobs | 52,361 jobs/s |
| Comprehensive TCP + SQLite processing, 50,000 jobs | 145 jobs/s |
| TCP batch-notify, 100,000 jobs / 10 workers | 1,900 jobs/s end-to-end |
| Internal in-memory lifecycle, 1,000,000 jobs | 20,858 jobs/s processing |
| Internal Worker lifecycle, 100,000 jobs | 116,994 jobs/s |

The Embedded comprehensive path is in-memory. The TCP path includes a durable
SQLite broker and per-job network lifecycle, so those rows are not a
serialization-only comparison.

## Identity and environment

| Field | Value |
| --- | --- |
| Date | 2026-08-03, Europe/Rome |
| Candidate snapshot SHA-256 | `7024311a34261b5579c10a5d866f3814e682907c6f24e752b022bc0ff483aa4b` |
| Package version | `2.8.56` |
| Bun | 1.3.14+0d9b296af, arm64 |
| OS | macOS 26.6, build 25G72 |
| Hardware | Mac13,1; Apple M1 Max; 10 logical CPUs; 32 GiB |
| Virtualization | None; all performance runs were native |

The host was not globally quiescent: unrelated user Docker services remained
active and were neither stopped nor modified. The initial load average was
4.76 / 3.87 / 3.72. Every bunqueue benchmark used its own process, queue names,
ports, and temporary SQLite state where the runner supported them. These are
release engineering measurements, not dedicated-host capacity claims.

## Measurement and acceptance rules

- Workflow tables use one discarded warm-up and three fresh measured
  processes per row. Median is representative and CV is population
  coefficient of variation.
- Fix-impact records a distribution and a correctness object with each timing.
- Legacy comprehensive, batch-notify, push/bulk, job-list, and internal runners
  are single-campaign diagnostics and are not promoted to repeated results.
- A processing row is accepted only after exact ID and authoritative terminal
  reconciliation with zero duplicates and failures.
- Workflow samples must also reconcile persisted state and lifecycle events.
- Tuned protocol runs declare `RATE_LIMIT_MAX_REQUESTS=1000000`; the production
  default remains 10,000 requests per client per 60 seconds.
- No Docker or VM timing is used in this report.

## Repeated Workflow Engine results

### Single engine

Configuration: one warm-up, three measured fresh processes, concurrency 32,
start batch 100, and the default protocol limit.

| Mode / scenario | Executions/run | Median workflows/s | p05 / p95 | CV | Integrity |
| --- | ---: | ---: | ---: | ---: | --- |
| Embedded linear | 1,000 | 247 | 244 / 247 | 0.57% | pass |
| Embedded parallel | 500 | 251 | 248 / 251 | 0.57% | pass |
| Embedded compensation | 200 | 238 | 238 / 239 | 0.20% | pass |
| Embedded signal | 200 | 211 | 211 / 212 | 0.22% | pass |
| TCP linear | 1,000 | 313 | 308 / 318 | 1.30% | pass |
| TCP parallel | 500 | 314 | 305 / 324 | 2.47% | pass |
| TCP compensation | 200 | 319 | 313 / 325 | 1.54% | pass |
| TCP signal | 200 | 273 | 272 / 274 | 0.30% | pass |

Compensation intentionally fails the forward path and passes only after both
reverse operations complete exactly once. Signal samples park and resume every
execution exactly once.

### Horizontal scale

Configuration: 5,000 workflows per instance, one warm-up, three measured
barriers, Embedded concurrency 128, TCP concurrency 64, and
`RATE_LIMIT_MAX_REQUESTS=1000000`.

| Mode / instances | Median workflows/s | p05 / p95 | CV | Speedup | Efficiency | Integrity |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| Embedded ×1 | 198 | 196 / 199 | 0.63% | 1.00× | 100.00% | pass |
| Embedded ×4 | 586 | 571 / 591 | 1.46% | 2.96× | 73.99% | pass |
| Embedded ×8 | 770 | 745 / 785 | 2.15% | 3.89× | 48.61% | pass |
| Embedded ×12 | 758 | 750 / 795 | 2.55% | 3.83× | 31.90% | pass |
| TCP ×1 | 227 | 225 / 232 | 1.29% | 1.00× | 100.00% | pass |
| TCP ×4 | 666 | 665 / 668 | 0.19% | 2.93× | 73.35% | pass |
| TCP ×8 | 651 | 650 / 665 | 1.04% | 2.87× | 35.85% | pass |
| TCP ×12 | 618 | 617 / 626 | 0.65% | 2.72× | 22.69% | pass |

The ten-core host saturates before 12 instances. This is a host capacity curve,
not evidence that additional instances have no cost.

## Core fix-impact distributions

Every correctness object passed.

| Operation and workload | Median | p95 | Correctness boundary |
| --- | ---: | ---: | --- |
| Recover 10,001 active SQLite jobs | 1,710.147 ms | 1,725.588 ms | 0 active; 10,001 pending, ready, indexed, counted |
| In-memory descending page, 50,000 jobs | 3.608 ms | 4.044 ms | exact indexes 49,999…49,900 |
| SQLite prioritized page | 2.604 ms | 2.929 ms | 100/100 prioritized |
| SQLite deep page | 0.653 ms | 0.797 ms | exact ID; indexed ordering |
| Pull past 5,000 blocked group jobs | 1.774 ms | 3.080 ms | eligible job returned in every sample |
| Global stats, 50,000 jobs | 1.870 ms | 2.883 ms | exact ready count |
| 200-queue summary, 50,000 jobs | 5.030 ms | 6.773 ms | exact queues and ready count |
| Sparse temporal lookup behind 500,000 jobs | 0.001 ms | 0.008 ms | exact first ID |
| Temporal removal behind 500,000 jobs | 0.002 ms | 0.137 ms | exact final size |
| Notify 10,000 waiters | 0.966 ms | 1.731 ms | all drained; surplus coalesced |
| 100,000 delayed add/remove pairs | 20.879 ms | 22.819 ms | no retained heap entries |

## Queue and transport diagnostics

### Comprehensive public API curve

The accepted TCP run used an isolated SQLite broker with 100,000 retained
completed jobs and a 1,000,000-request limit. Embedded reset its in-memory
manager after every scale.

| Mode | Scale | `add()` ops/s | `addBulk()` jobs/s | Process jobs/s |
| --- | ---: | ---: | ---: | ---: |
| Embedded, in-memory | 1,000 | 62,311 | 179,076 | 32,297 |
| Embedded, in-memory | 5,000 | 144,576 | 367,938 | 52,180 |
| Embedded, in-memory | 10,000 | 193,586 | 422,835 | 53,329 |
| Embedded, in-memory | 50,000 | 156,294 | 299,251 | 52,361 |
| TCP, SQLite | 1,000 | 7,256 | 9,409 | 166 |
| TCP, SQLite | 5,000 | 5,181 | 5,202 | 160 |
| TCP, SQLite | 10,000 | 4,148 | 4,271 | 158 |
| TCP, SQLite | 50,000 | 2,435 | 2,476 | 145 |

An earlier TCP attempt correctly failed its terminal check after all work was
processed because the shared broker's default 50,000-completion retention had
already retained 4,000 rows from earlier cells; only 46,000 of the final
50,000 remained queryable. This was benchmark orchestration, not job loss. Its
log is retained, and the full campaign was rerun with a declared 100,000-row
retention window. No invalid row was used as a performance result.

### TCP batch notification and fairness

| Scenario | Push jobs/s | End-to-end jobs/s | Wake p99 | Fairness | Integrity |
| --- | ---: | ---: | ---: | ---: | --- |
| 10k, 10 workers ×5 | 16,700 | 921 | 57 ms | 1.00 | 10,000 exact |
| 10k, 50 workers ×1 | 4,200 | 736 | 188 ms | 0.99 | 10,000 exact |
| 50k, 20 workers ×10 | 8,500 | 1,800 | 132 ms | 0.97 | 50,000 exact |
| 10k, batch 10, 30 workers ×3 | 2,500 | 873 | 319 ms | 0.97 | 10,000 exact |
| 100k, 10 workers ×20 | 11,000 | 1,900 | 103 ms | 0.99 | 100,000 exact |

### Push/bulk and TCP diagnostics

The three-run push/bulk medians were 162,512 / 326,377 jobs/s for Embedded
`add` / `addBulk` at 50,000 jobs, and 2,548 / 2,497 jobs/s for TCP+SQLite.
The fresh TCP serialization runner measured 3,538 pipelined pushes/s, 3,999
`PUSHB` jobs/s, and sequential RTT p50 194 µs / p99 1,050 µs.

The exact 5,000-job TCP processing sweep measured 162, 854, 839, and 818
jobs/s at concurrency/batch 10/20, 50/50, 100/100, and 200/200 respectively,
with zero duplicates or missing IDs in every row.

### SQLite versus Redis comparison

Both native services and their state were disposable. Each product processed
exactly 10,000 accepted IDs.

| Product and topology | Push | Bulk | Push p99 | Process |
| --- | ---: | ---: | ---: | ---: |
| bunqueue TCP + durable SQLite | 4,262/s | 2,300/s | 55.55 ms | 653/s |
| BullMQ + in-memory Redis 8.10.0 | 59,095/s | 46,200/s | 2.64 ms | 19,176/s |

This is intentionally not presented as a storage-equivalent comparison.

## Additional diagnostics

- Local auto-batching: sequential direct/batched 3,276/4,130 ops/s;
  concurrent 1k 7,687/7,561; concurrent 5k 6,149/6,517; explicit bulk 1k
  8,266 and 5k 7,755 jobs/s.
- Job listing at 20,000 rows: waiting 61.5 µs, delayed 109.4 µs, all states
  327.0 µs, no filter 503.5 µs, page size 10 at 45.9 µs.
- Flow over TCP at 20 children: sequential 6.50 ms, parallel 1.63 ms,
  `FlowProducer.add` 3.37 ms, and bulk-then-link 3.35 ms.
- Parent-completion latency averaged 30.6 µs for a single dependency, 41.8 µs
  for a chain, and 42.7 µs for fan-out.
- Internal lifecycle loops averaged 242,556 push, 271,171 pull, and 190,844
  ACK operations/s.
- Internal Worker processing completed 100,000 jobs at 116,994 jobs/s with
  exactly 100,000 completion events.
- The million-job run pushed at 359,712 jobs/s and processed at 20,858 jobs/s,
  with exact one-million pull and completion ID sets.
- Stress diagnostics passed throughput, multi-queue concurrency, 340 MB/s
  payload, strict priority, retry, memory, and 100,000-job batch assertions.
  The shared-process RSS signal was +188 MiB; the separate post-GC ultimate
  check fell from 55 MiB to 14 MiB after 150,000 jobs, so the former is not
  treated as proof of a retained engine leak.
- Functional benchmark scripts passed 67/67 client checks, 74/74 feature
  checks, and 52/52 production-readiness checks.

## Raw evidence

Local raw JSON, logs, service metadata, and the rejected comprehensive sample
are retained under `artifacts/benchmarks/2026-08-03-v2.8.56/`. Generated
artifacts are intentionally excluded from the package and repository commit.

## Interpretation

1. Correctness gates are part of every result. Fast output with missing ACKs,
   truncated retained state, duplicates, or unfinished jobs is rejected.
2. The host was not dedicated, so comparison with prior dates is descriptive,
   not a statistically controlled regression claim.
3. Persistence dominates the legacy Embedded/TCP curve; in-memory Embedded and
   durable TCP+SQLite numbers are different topologies.
4. The default protocol safety limit is operationally real. Capacity tuning is
   always explicit.
5. The 2026-07-30 dedicated Ryzen report remains the public capacity reference;
   this campaign is final-candidate engineering evidence.

## Related documents

- [Benchmarking and Performance Evidence](../features/benchmarks.md)
- [Native Engineering Benchmark — 2026-08-02](./native-engineering-2026-08-02.md)
- [Native Engineering Benchmark — 2026-07-30](./native-engineering-2026-07-30.md)
- [Test Isolation and Reproducibility](../testing.md)
