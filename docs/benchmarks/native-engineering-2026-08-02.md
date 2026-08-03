# Native Engineering Benchmark — 2026-08-02

## Executive summary

This report records the complete native v2.8.56 release-candidate benchmark
campaign on an Apple M1 Max. It is an engineering audit, not a replacement for
the publication-grade Ryzen 9 capacity report from 2026-07-30. Repeated
fresh-process Workflow samples are reported separately from single-run
diagnostics, and every table names its topology and integrity boundary.

The most important outcome is correctness: every accepted processing sample
reconciled terminal broker state and exact job or workflow identities. The
campaign also exposed four benchmark-runner defects. Each was reproduced with
a failing regression before the runner was corrected and rerun.

Representative final measurements are:

| Workload | Native result |
| --- | ---: |
| Single Workflow Engine, linear Embedded / TCP | 268 / 344 workflows/s median |
| 12 Workflow Engine instances, Embedded / TCP | 751 / 691 workflows/s median |
| Comprehensive Embedded processing, 50,000 jobs | 58,661 jobs/s |
| Comprehensive TCP + SQLite processing, 50,000 jobs | 148 jobs/s |
| TCP batch-notify, 100,000 jobs / 10 workers | 1,900 jobs/s end-to-end |
| Internal in-memory lifecycle, 1,000,000 jobs | 21,541 jobs/s |
| Internal `PUSH` / `PULL` / `ACK` loops | 264,763 / 281,339 / 200,982 ops/s |

The large difference between the comprehensive Embedded and TCP processing
rows is expected for that legacy runner: Embedded uses the shared in-memory
manager, while TCP uses a durable SQLite broker and per-job network lifecycle.
It is not a protocol-only comparison.

## Identity and environment

| Field | Value |
| --- | --- |
| Date | 2026-08-02, Europe/Rome |
| Candidate base revision | `adf5ff8e60622d192af9cb11e27b9641758dd004` plus the v2.8.56 worktree |
| Package version | `2.8.56` |
| Bun | 1.3.14, arm64 |
| OS | macOS 26.6, build 25G72 |
| CPU | Apple M1 Max, 10 logical CPUs |
| Memory | 32 GiB |
| Virtualization | None; all performance runs were native |

The host was not claimed to be globally quiescent: unrelated user processes
remained present, including an existing broker on port 6789 that was neither
used nor stopped. Every campaign owned separate queues and, where applicable,
fresh temporary SQLite files and non-conflicting ports. This is why the report
is engineering evidence rather than a new public headline benchmark.

## Measurement and acceptance rules

- Workflow Engine tables use one discarded warm-up and three measured fresh
  processes per row. Median is representative; CV is population coefficient
  of variation.
- The fix-impact harness records repeated distributions and a correctness
  object with every timing.
- Legacy comprehensive, batch-notify, push/bulk, job-list, and internal runners
  are diagnostic single campaigns. Their values are not mixed into the
  repeated Workflow distributions.
- Processing throughput is accepted only after accepted IDs, unique invoked
  IDs, invocation count, completed count, failed count, and every nonterminal
  state agree.
- Workflow throughput is accepted only after persisted terminal state and
  lifecycle-event conservation pass.
- The operational protocol limit remains 10,000 requests per client per
  60-second window by default. Tuned runs use
  `RATE_LIMIT_MAX_REQUESTS=1000000` and are labelled accordingly.
- No Docker or VM benchmark result appears in this report. Docker was reserved
  for the functional release gates.

## Repeated Workflow Engine results

### Single engine

Configuration: one warm-up, three measured processes, concurrency 32, start
batch 100, default protocol limit, and exact persisted integrity checks.

| Mode / scenario | Executions per run | Median workflows/s | p05 / p95 | CV | Integrity |
| --- | ---: | ---: | ---: | ---: | --- |
| Embedded linear | 1,000 | 268 | 268 / 271 | 0.53% | pass |
| Embedded parallel | 500 | 272 | 264 / 276 | 1.84% | pass |
| Embedded compensation | 200 | 261 | 259 / 264 | 0.79% | pass |
| Embedded signal | 200 | 235 | 234 / 237 | 0.53% | pass |
| TCP linear | 1,000 | 344 | 343 / 346 | 0.36% | pass |
| TCP parallel | 500 | 359 | 358 / 361 | 0.35% | pass |
| TCP compensation | 200 | 346 | 346 / 351 | 0.68% | pass |
| TCP signal | 200 | 302 | 301 / 305 | 0.56% | pass |

Parallel is inline Workflow Engine parallel bookkeeping, not distributed child
workers. Compensation samples intentionally fail their forward path and are
accepted only after both reverse operations complete exactly once. Signal
samples must park and resume exactly once.

### Horizontal scale

Configuration: 5,000 executions per instance, one warm-up, three measured
barriers, Embedded concurrency 128, TCP concurrency 64, and tuned protocol
limit `RATE_LIMIT_MAX_REQUESTS=1000000`.

| Mode / instances | Median workflows/s | p05 / p95 | CV | Speedup | Efficiency | Integrity |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| Embedded ×1 | 213 | 212 / 214 | 0.38% | 1.00× | 100.00% | pass |
| Embedded ×4 | 570 | 569 / 572 | 0.22% | 2.68× | 66.90% | pass |
| Embedded ×8 | 784 | 771 / 829 | 3.13% | 3.68× | 46.01% | pass |
| Embedded ×12 | 751 | 722 / 757 | 2.06% | 3.53× | 29.38% | pass |
| TCP ×1 | 240 | 239 / 242 | 0.52% | 1.00× | 100.00% | pass |
| TCP ×4 | 678 | 678 / 678 | 0.00% | 2.83× | 70.63% | pass |
| TCP ×8 | 704 | 699 / 705 | 0.37% | 2.93× | 36.67% | pass |
| TCP ×12 | 691 | 652 / 693 | 2.78% | 2.88× | 23.99% | pass |

The Apple host is effectively saturated before 12 instances. The result is a
host-capacity curve, not a claim that adding instances has zero cost.

## Core fix-impact distributions

The full profile ran after the TCP backpressure fix. Every correctness object
passed.

| Operation and workload | Median | p95 | Correctness boundary |
| --- | ---: | ---: | --- |
| Recover 10,001 active SQLite jobs | 1,626.411 ms | 1,631.958 ms | 0 active; 10,001 pending, ready, indexed, and counted |
| In-memory descending page, 50,000 jobs | 3.450 ms | 4.791 ms | exact indexes 49,999…49,900 |
| SQLite prioritized page | 2.383 ms | 2.458 ms | 100/100 prioritized |
| SQLite deep page | 0.645 ms | 0.664 ms | exact ID; covering index; no temporary sort |
| Pull past 5,000 blocked group jobs | 1.700 ms | 2.834 ms | eligible `B1` returned in every sample |
| Global stats, 50,000 jobs | 1.393 ms | 1.699 ms | exact 50,000 ready |
| 200-queue summary, 50,000 jobs | 4.537 ms | 6.426 ms | 200 queues; exact ready count |
| Sparse temporal queue lookup behind 500,000 jobs | 0.001 ms | 0.007 ms | exact first ID |
| Temporal removal behind 500,000 jobs | 0.002 ms | 0.132 ms | exact final size |
| Notify 10,000 waiters | 0.944 ms | 1.544 ms | all drained; surplus coalesced |
| 100,000 delayed add/remove pairs | 25.298 ms | 40.769 ms | zero live and retained heap entries |

## Queue and transport diagnostics

### Comprehensive public API curve

This final run used a fresh native process. Embedded reset its shared manager
after each scale. TCP used an isolated broker and SQLite file with a declared
1,000,000-request limit and 100,000 completed-job retention. The TCP database
grows across its scale cells, so the result is a sustained diagnostic.

| Mode | Scale | `add()` ops/s | `addBulk()` jobs/s | Process jobs/s |
| --- | ---: | ---: | ---: | ---: |
| Embedded, in-memory | 1,000 | 64,074 | 214,259 | 32,251 |
| Embedded, in-memory | 5,000 | 112,500 | 399,012 | 52,913 |
| Embedded, in-memory | 10,000 | 165,928 | 470,233 | 56,291 |
| Embedded, in-memory | 50,000 | 164,141 | 345,781 | 58,661 |
| TCP, SQLite | 1,000 | 6,349 | 9,952 | 152 |
| TCP, SQLite | 5,000 | 6,338 | 6,686 | 153 |
| TCP, SQLite | 10,000 | 4,511 | 4,651 | 152 |
| TCP, SQLite | 50,000 | 2,626 | 2,640 | 148 |

The 50,000-job TCP drain took longer than the former 180-second diagnostic
deadline. At that old deadline, conservation was still exact: 26,100 completed
plus 23,900 waiting, with no other states. The runner now validates and prints
`BENCH_TIMEOUT_MS` and defaults to 600 seconds; the final sample then completed
all 50,000 identities with zero failures or duplicates.

### TCP batch notification and Worker fairness

Each row reconciled every accepted and invoked identity against authoritative
terminal state. The owned broker bound an operating-system-assigned port and
retained all terminal rows needed by the largest scenario.

| Scenario | Push jobs/s | End-to-end jobs/s | Wake-up p99 | Worker fairness | Integrity |
| --- | ---: | ---: | ---: | ---: | --- |
| 10k, 10 workers ×5 | 18,300 | 913 | 58 ms | 1.00 | 10,000 / 10,000 |
| 10k, 50 workers ×1 | 5,000 | 762 | 160 ms | 1.00 | 10,000 / 10,000 |
| 50k, 20 workers ×10 | 10,000 | 2,000 | 105 ms | 0.98 | 50,000 / 50,000 |
| 10k, batch 10, 30 workers ×3 | 4,000 | 1,300 | 90 ms | 0.99 | 10,000 / 10,000 |
| 100k, 10 workers ×20 | 17,100 | 1,900 | 117 ms | 0.99 | 100,000 / 100,000 |

The same 100,000-job sample was first run under the production protocol limit.
It failed closed at 58,799 completed, 41,001 waiting, and 200 active jobs after
the request budget rejected ACK batches. Only the explicitly tuned
`RATE_LIMIT_MAX_REQUESTS=1000000` rerun above is reported as capacity.

### Push/bulk median diagnostic

Each cell is the median of three repetitions. Embedded is in-memory; TCP uses
the isolated SQLite endpoint. The campaign intentionally grows state across
scales and is therefore not publication-grade fresh-state evidence.

| Mode | Scale | `add()` ops/s | `addBulk()` jobs/s |
| --- | ---: | ---: | ---: |
| Embedded, in-memory | 1,000 | 213,220 | 388,928 |
| Embedded, in-memory | 5,000 | 191,042 | 494,905 |
| Embedded, in-memory | 10,000 | 264,623 | 550,925 |
| Embedded, in-memory | 50,000 | 174,481 | 362,128 |
| TCP, SQLite | 1,000 | 7,924 | 7,120 |
| TCP, SQLite | 5,000 | 6,241 | 6,080 |
| TCP, SQLite | 10,000 | 4,392 | 4,347 |
| TCP, SQLite | 50,000 | 2,649 | 2,580 |

The final runner process exited naturally after the last median. Before the
release fix it printed the table but remained alive because the Embedded
singleton was not shut down.

### TCP serialization and concurrency sweep

The fresh TCP serde diagnostic reported 3,767 pipelined individual pushes/s,
4,250 `PUSHB` jobs/s, and sequential RTT p50 168 µs / p99 850 µs.

The fresh 5,000-job worker sweep produced:

| Concurrency / batch | End-to-end jobs/s | Integrity |
| --- | ---: | --- |
| 10 / 20 | 135 | 5,000 exact; zero duplicates |
| 50 / 50 | 830 | 5,000 exact; zero duplicates |
| 100 / 100 | 805 | 5,000 exact; zero duplicates |
| 200 / 200 | 822 | 5,000 exact; zero duplicates |

A deliberate default-limit run rejected four ACKs and observed four
redeliveries, then exited nonzero. That is retained as operational evidence for
the 10,000-request safety boundary, not reported as engine throughput.

### SQLite versus Redis comparison

The isolated native comparison processed 10,000 jobs with exact identity and
terminal reconciliation:

| Product and topology | Push | Bulk | Push p99 | Process |
| --- | ---: | ---: | ---: | ---: |
| bunqueue TCP + durable SQLite | 4,467/s | 2,300/s | 54.27 ms | 735/s |
| BullMQ + in-memory Redis | 61,383/s | 50,500/s | 2.44 ms | 19,981/s |

This is not a storage-equivalent contest. It answers what the named default
topologies did on the host: durable local SQLite versus an already-running
in-memory Redis service. No speedup claim is inferred across those durability
boundaries.

## Additional engineering diagnostics

- Local auto-batching: sequential 2,911/3,620 ops/s; concurrent 1k
  7,007/7,042, concurrent 5k 6,360/6,847, explicit bulk 1k 8,877 and bulk 5k
  7,655 jobs/s.
- Job listing at 20,000 rows: waiting 53.6 µs, delayed 107.2 µs, all states
  294.6 µs, no filter 470.2 µs, and page size 10 at 43.2 µs.
- Flow creation over fresh TCP: at 20 children, sequential adds 4.98 ms,
  parallel adds 1.40 ms, `FlowProducer.add` 3.06 ms, and bulk-then-link 3.88 ms.
- Internal algorithm diagnostics, dependency latency, full-client, full-feature,
  event-payload, completed-event, stress, and ultimate suites all completed
  their functional assertions.
- Internal protocol loops averaged 264,763 `PUSH`, 281,339 `PULL`, and 200,982
  `ACK` operations/s.
- Internal Worker processing completed 100,000 jobs at 125,125 jobs/s with
  exactly 100,000 completion events.
- The internal million-job run pushed at 379,651 jobs/s and completed the full
  lifecycle at 21,541 jobs/s with exact one-million pulled and completed ID
  sets.
- A shared-process stress run showed +157.3 MiB RSS growth, while the separate
  ultimate post-GC check dropped from 55 MiB to 14 MiB. The former is an
  investigation signal, not evidence of a retained engine leak.
- The functional inventory completed 67/67 Client SDK checks, 74/74 full-feature
  checks, and 52/52 ultimate production-readiness checks.

## Benchmark-runner defects found by the campaign

Four final-run blockers were converted to ordinary regressions:

1. Batch-notify retained only the production default 50,000 completed jobs but
   required an authoritative 100,000-job terminal count. Its owned manager now
   retains the scenario maximum.
2. Comprehensive reused one Embedded singleton across scales, allowing prior
   completions to consume the bounded terminal window. It now shuts the manager
   down after every scale, including failure paths.
3. Comprehensive used a fixed 180-second processing deadline that was shorter
   than a correct durable 50,000-job run. It now validates, prints, and uses a
   600-second `BENCH_TIMEOUT_MS` default without weakening integrity checks.
4. Push/bulk printed its final table but did not terminate because its Embedded
   singleton remained active. The entrypoint now shuts it down in `finally`.

The broader release campaign also hardened TCP short-write handling, the TCP
process sweep, comparison-runner identity checks, dynamic endpoint selection,
protocol-limit reporting, import safety, deterministic cleanup, and the
300-line source ceiling. Invalid or truncated benchmark samples now fail
closed instead of being published.

## Raw evidence retained locally

The repeated JSON reports are retained in the release workspace under:

- `artifacts/benchmarks/2026-08-02-v2.8.56/workflow-post-gate.json`
- `artifacts/benchmarks/2026-08-02-v2.8.56/workflow-scale-post-gate.json`
- `artifacts/benchmarks/2026-08-02-v2.8.56/fix-impact-post-gate.json`

The terminal output for the remaining diagnostic runners was reviewed during
the campaign. Generated benchmark artifacts are intentionally not package
contents and are not included in the npm tarball.

## Interpretation

1. The July Ryzen 9 report remains the public capacity reference because it
   uses deeper repeated fresh-process sampling on a dedicated host.
2. The M1 campaign is release-candidate engineering evidence: it proves that
   the corrected runners fail closed, complete exact workloads, and remain
   usable across Embedded and TCP topologies.
3. Persistence and topology dominate several legacy curves. An in-memory
   Embedded row must not be compared to a durable TCP+SQLite row as if only
   serialization differed.
4. The default protocol safety cap is a real operational constraint. Tuning it
   for a capacity study must always be explicit.
5. Correctness gates are part of the benchmark. A higher rate from lost ACKs,
   truncated completion retention, duplicate delivery, or unfinished jobs is
   rejected rather than celebrated.

## Related documents

- [Benchmarking and Performance Evidence](../features/benchmarks.md)
- [Native Engineering Benchmark — 2026-07-30](./native-engineering-2026-07-30.md)
- [Core Fix Impact Benchmark — 2026-07-16](./fix-impact-2026-07-16.md)
- [Test Isolation and Reproducibility](../testing.md)
