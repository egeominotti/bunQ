# Native Engineering Benchmark — 2026-07-30

## Executive summary

This report measures bunqueue queue and Workflow Engine performance on one
native high-end Linux host. It is an engineering capacity study, not a single
headline contest: every row names the operation, topology, persistence mode,
sample count, aggregation, and integrity boundary.

The representative results are:

| Workload | Representative result |
| --- | ---: |
| Internal in-memory batched push, 1M jobs | **729,395 jobs/s** median |
| Internal in-memory full lifecycle, 1M jobs | **311,915 jobs/s** median |
| Public Embedded on-disk sustained `addBulk`, 50K cell | **186,384 jobs/s** median |
| TCP `PUSHB`, fresh broker/database, 50K jobs | **158,779 jobs/s** median |
| TCP no-work worker drain, concurrency 50 | **17,256 jobs/s** median |
| Sequential durable Embedded/TCP add | **60,835 / 27,191 ops/s** median |
| Single Workflow Engine, linear Embedded/TCP | **2,700 / 3,187 workflows/s** median |
| 12 tuned Workflow Engines, Embedded/TCP | **25,873 / 17,496 workflows/s** median |

The Workflow Engine campaign reconciled 63,000 measured single-engine
executions and 750,000 measured horizontal executions. Including discarded
warm-ups, the horizontal runner checked 1,000,000 workflow executions. Every
sample passed terminal-state and event conservation.

## Identity and environment

| Field | Value |
| --- | --- |
| Date | 2026-07-30, Europe/Rome |
| Engine revision | `af027f04d2064b701ee2243eac737d74b8d87706` |
| Runtime source changes during measurement | None |
| Bun | 1.3.14, x64 |
| OS | Linux 7.0.0-14 |
| CPU | AMD Ryzen 9 9950X3D, 16 physical cores / 32 threads |
| Memory | 59 GiB |
| CPU governor | `performance` |
| Virtualization | None detected |

The Workflow benchmark runner was added to the candidate worktree after the
queue campaign; it does not modify runtime source. All performance
measurements ran natively. Docker was used only for the functional sandbox,
which completed before native performance runs.

An initial exploratory run was excluded before aggregation because it overlapped
an existing Docker sandbox and did not isolate the Embedded data path. No number
from that run appears below.

## Method

### Sampling and statistics

- Each primary single-instance row uses one discarded warm-up process followed
  by repeated measured fresh processes.
- The 1M-job queue campaign uses 21 measured runs.
- TCP serde, TCP lifecycle, and durable campaigns use 21 measured runs.
- Workflow linear uses 21 measured runs per mode; parallel, compensation, and
  signal use 7 per mode.
- Horizontal campaigns use 3 measured barriers after one discarded barrier.
- Median is the representative value. Tables also show p05/p95 or min/max and
  population coefficient of variation (CV).
- Latency percentiles are calculated inside each run. Workflow tables report
  the median of run-level percentiles.

### Isolation

Every measured Workflow sample uses:

- a new Bun process;
- a unique queue and workflow name;
- a fresh temporary directory;
- a fresh SQLite workflow database;
- in TCP mode, a separate fresh broker process, broker database, and dynamic
  TCP/HTTP ports;
- explicit close and removal of database, WAL, and SHM files.

Queue campaigns used the same fresh-process/state discipline in their native
orchestration. The public API sustained curve intentionally grows one database
through its 1K, 5K, 10K, and 50K cells; that exception is part of the workload,
not hidden state.

### Integrity

Timing is accepted only with the matching invariants:

- queue campaigns reconcile pushed, pulled, completed, duplicate, missing, and
  invalid job IDs plus final counts;
- workflow campaigns inspect every persisted execution;
- linear, parallel, and signal runs must be `completed`;
- compensation runs must be `failed` with `rollbackStatus='completed'` and both
  reversals recorded exactly once;
- lifecycle, step, waiting, signal, failure, and compensation events must
  conserve exactly.

## Queue engine results

### Internal batched 1M-job lifecycle

This is `src/benchmark/million-jobs.bench.ts`: 16 queues and 16 workers over the
internal batched QueueManager API, `removeOnComplete:true`, no `dataPath`.
It measures the in-memory engine, not public `Queue.addBulk` and not SQLite
write throughput.

| Phase | Samples | Median | Min–max | CV |
| --- | ---: | ---: | ---: | ---: |
| Push | 21 | 729,395 jobs/s | 693,001–747,943 | 1.94% |
| Process | 21 | 541,712 jobs/s | 523,286–558,036 | 1.40% |
| Complete lifecycle | 21 | 311,915 jobs/s | 303,674–316,556 | 1.21% |

All 21M jobs passed pull-time and completed-event integrity: no missing index,
duplicate ID, or invalid payload.

### Public API, on-disk sustained curve

`bench:pushbulk` was run in eight fresh campaigns; the first was discarded.
Each cell is already the median of three repetitions. The table is the median
of the seven campaign medians for the final 50K cell.

| Mode | `add()` | `addBulk()` |
| --- | ---: | ---: |
| Embedded, on-disk SQLite | 147,818 jobs/s | 186,384 jobs/s |
| TCP, on-disk SQLite | 127,476 jobs/s | 87,319 jobs/s |

Each campaign executes the lower scales first against the same database.
Therefore this is a sustained/grown-database result. It must not be compared
directly with a fresh-database `PUSHB` microbenchmark.

### TCP producer and round-trip

`BENCH_N=50000 BENCH_RUNS=21 bun run bench:tcp` used a fresh server and database
for each operation.

| Operation | Median | Min–max | CV |
| --- | ---: | ---: | ---: |
| Pipelined individual push, auto-batch disabled | 80,978 ops/s | 78,331–83,806 | 2.06% |
| `PUSHB`, 50K jobs | 158,779 jobs/s | 146,496–163,501 | 3.25% |

The final process then measured 5,000 sequential localhost round trips:

| Operation | p50 | p99 |
| --- | ---: | ---: |
| Buffered TCP add | 14 µs | 292 µs |

### TCP worker drain and concurrency knee

The primary lifecycle campaign preloaded 20K jobs, then drained them with one
TCP worker at concurrency 50. One warm-up and 21 fresh measured
server/database pairs produced:

| Metric | Result |
| --- | ---: |
| Median | 17,256 jobs/s |
| p05 / p95 | 16,455 / 17,864 jobs/s |
| CV | 2.86% |
| Median / p95 duration | 1,159 / 1,215 ms |

All 420K jobs completed exactly once and final queue counts were clean.

The separate 10K-job sweep used one warm-up plus seven measured fresh samples
per point:

| Worker concurrency | Median drain |
| ---: | ---: |
| 32 | 17,607 jobs/s |
| 48 | 17,541 jobs/s |
| 64 | 17,191 jobs/s |
| 96 | 15,830 jobs/s |
| 128 | 15,094 jobs/s |
| 192 | 14,081 jobs/s |

The useful knee for this localhost no-work processor is 32–48. More
concurrency reduces throughput.

### Synchronous durability

Each of 21 fresh processes measured 2,000 `durable:true` sequential adds after
50 warm-up operations.

| Mode | Throughput median | p05 / p95 | CV | Run-median latency p50 / p95 / p99 |
| --- | ---: | ---: | ---: | ---: |
| Embedded | 60,835 ops/s | 53,698 / 63,002 | 7.83% | 15 / 25 / 59 µs |
| TCP | 27,191 ops/s | 21,590 / 29,089 | 8.90% | 30 / 57 / 120 µs |

All 43,050 operations per mode, including warm-ups, were present in queue
counts.

## Queue horizontal scaling

### Embedded

Twelve independent processes each executed the 1M-job lifecycle against their
own state. Three barriers produced 2,022,926, 2,008,704, and 1,996,340 jobs/s.

Median aggregate throughput was **2,008,704 jobs/s**. The campaign reconciled
36M/36M jobs. Peak combined RSS was about 37.4 GiB, dominated by the twelve
1M-ID integrity sets; sampled CPU peaked near 2,513% and swap stayed unused.

### TCP

Twelve broker/client pairs each preloaded 50K jobs and started workers from one
barrier.

| Campaign | Conservative barrier | Exact drain | Integrity |
| ---: | ---: | ---: | ---: |
| 1 | 46,529 jobs/s | 46,937 jobs/s | 600K / 600K |
| 2 | 46,324 jobs/s | 46,729 jobs/s | 600K / 600K |
| 3 | 46,645 jobs/s | 47,054 jobs/s | 600K / 600K |

Median exact drain was **46,937 jobs/s**. Peak combined RSS was about 5.2 GiB
and sampled CPU about 2,249%. The 2.7× gain over one pair is the observed host
capacity; multiplying a single-pair peak by 12 would be wrong.

## Workflow Engine results

### Workloads

| Scenario | Execution graph | Correctness boundary |
| --- | --- | --- |
| Linear | validate → transform → persist | 3 steps, terminal `completed` |
| Parallel | prepare → 3 inline `Promise.all` steps → join | 5 step completions, terminal `completed` |
| Compensation | reserve → charge → intentional failure | terminal `failed`, two reverse compensations |
| Signal | request → wait for `approved` → finish | waiting and signal exactly once, then `completed` |

Parallel steps execute inside one workflow node job; this scenario measures
Workflow Engine parallel bookkeeping and join behavior, not distributed child
workers.

The concurrency sweep at the same 1,000-execution linear scale found an
Embedded plateau at 64–128 and a TCP plateau at 32–64. Final configurations
were Embedded 128 and TCP 64. TCP 64 was confirmed over 21 samples at
3,187 workflows/s (CV 1.77%), versus TCP 32 at 3,020 (CV 1.72%).

### Single-engine saturation

| Mode/scenario | Runs × executions | Throughput median | p05 / p95 | CV | Run-median latency p50 / p95 / p99 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Embedded linear | 21 × 1,000 | 2,700 wf/s | 2,570 / 2,792 | 2.86% | 305,564 / 342,783 / 346,139 µs |
| Embedded parallel | 7 × 500 | 2,118 wf/s | 1,997 / 2,173 | 2.70% | 202,750 / 219,812 / 221,523 µs |
| Embedded compensation | 7 × 500 | 2,055 wf/s | 2,005 / 2,118 | 2.05% | 199,776 / 225,617 / 228,057 µs |
| Embedded signal | 7 × 500 | 1,928 wf/s | 1,883 / 1,961 | 1.39% | 71,455 / 84,673 / 86,378 µs resume |
| TCP linear | 21 × 1,000 | 3,187 wf/s | 3,075 / 3,261 | 1.77% | 260,412 / 284,289 / 288,198 µs |
| TCP parallel | 7 × 500 | 2,456 wf/s | 2,368 / 2,528 | 2.48% | 172,855 / 186,354 / 186,917 µs |
| TCP compensation | 7 × 500 | 2,239 wf/s | 2,218 / 2,384 | 2.87% | 177,362 / 200,878 / 204,270 µs |
| TCP signal | 7 × 500 | 2,234 wf/s | 2,204 / 2,306 | 1.72% | 56,576 / 64,922 / 65,729 µs resume |

These are saturated-batch `workflow:started`-event-to-terminal latencies: an
execution includes time waiting behind other executions after its lifecycle
event begins. The throughput interval is broader and starts before the first
`Engine.start()` call. Neither metric is idle single-workflow service latency.

TCP being faster in this workload does not mean the network is free. The TCP
topology gives the broker and Workflow Store separate processes and separate
SQLite files; Embedded shares one file across the store and queue connections.
That extra parallelism outweighs localhost protocol cost for these no-op steps.

For the signal phase:

| Mode | Park throughput median | Resume throughput median | Park latency p50 / p95 run medians | Resume latency p50 / p95 |
| --- | ---: | ---: | ---: | ---: |
| Embedded | 3,205 wf/s | 4,837 wf/s | 127,060 / 139,070 µs | 71,455 / 84,673 µs |
| TCP | 3,518 wf/s | 6,149 wf/s | 113,533 / 122,988 µs | 56,576 / 64,922 µs |

Across both modes, the measured single-engine campaign contains:

- 63,000 workflow executions and `workflow:started` events;
- 189,000 completed step events;
- 56,000 successful workflow completions;
- 7,000 intentional workflow failures;
- 14,000 successful compensation outcomes.

### Default protocol safety cap

The TCP Workflow Engine has an important operational boundary. The broker
limits each protocol client to 10,000 requests per 60-second window by default.
One workflow expands into multiple queue commands.

| Linear executions | Default-cap result |
| ---: | ---: |
| 1,000 | 3,214 wf/s; 311 ms |
| 2,000 | 3,769 wf/s; 531 ms |
| 3,000 | 4,005 wf/s; 749 ms |
| 3,500 | 58 wf/s; 60,159 ms; p95 60,088 ms |

The 3,500-execution result reproduced. During the plateau, read-only SQLite
inspection showed:

- workflow store: 2,604 completed, 896 running at node index 2;
- broker: 9,570 completed jobs, 896 waiting, 34 active.

At window turnover the process emitted `Rate limit exceeded` for ACK batches,
then completed all executions. Lowering TCP partial-frame timeout, client
command timeout, or worker cleanup interval to 5 seconds did not move the
60-second plateau. Raising only
`RATE_LIMIT_MAX_REQUESTS=1000000` restored three fresh 3,500-execution runs to
3,814–4,106 workflows/s, median **3,855**, with 852–918 ms durations.

Interpretation: the 60-second result is the configured protocol safety cap, not
SQLite checkpoint degradation. It is still the correct default-topology
behavior and must not be hidden. Capacity campaigns that tune the limit are
labelled tuned.

### Horizontal tuned capacity

The scale campaign used 5,000 linear executions per instance, one discarded
warm-up, three measured barriers, Embedded concurrency 128, TCP concurrency 64,
and `RATE_LIMIT_MAX_REQUESTS=1000000`.

| Mode / instances | Median wf/s | p05 / p95 | CV | Speedup | Efficiency |
| --- | ---: | ---: | ---: | ---: | ---: |
| Embedded ×1 | 3,194 | 3,117 / 3,234 | 1.53% | 1.00× | 100% |
| Embedded ×4 | 10,579 | 10,555 / 10,940 | 1.65% | 3.31× | 82.80% |
| Embedded ×8 | 19,379 | 19,234 / 19,785 | 1.20% | 6.07× | 75.84% |
| Embedded ×12 | **25,873** | 25,293 / 26,114 | 1.34% | 8.10× | 67.50% |
| TCP ×1 | 4,207 | 4,090 / 4,251 | 1.62% | 1.00× | 100% |
| TCP ×4 | 11,738 | 11,627 / 12,324 | 2.57% | 2.79× | 69.75% |
| TCP ×8 | 17,407 | 17,338 / 17,422 | 0.21% | 4.14× | 51.72% |
| TCP ×12 | **17,496** | 17,006 / 17,655 | 1.59% | 4.16× | 34.66% |

At ×12, barrier skew stayed below 1 ms. Embedded peaked near 1,028% sampled CPU
and 2,549 MiB RSS. TCP peaked near 2,415% CPU and 3,861 MiB RSS. TCP is
effectively saturated by ×8 on this shared host; ×12 consumes more CPU without
material throughput gain.

The 24 measured horizontal barriers contain 750,000 exact workflow executions.
The eight discarded warm-up barriers add 250,000 more, and each warm-up also
passes the same integrity scan before being discarded statistically.

## Functional and endurance evidence

The isolated sandbox on the same runtime revision passed:

| Suite | Pass | Fail | Skip | Anomalies |
| --- | ---: | ---: | ---: | --- |
| Unit/model | 6,250 | 0 | 3 | Mixed-suite memory growth signal |
| TCP integration | 430 | 0 | 0 | None |
| Embedded integration | 273 | 0 | 0 | None |

The unit suite's end-to-start RSS increase was treated as an investigation
signal, not a leak conclusion. Three focused fresh-process TCP chaos soaks then
completed:

- 30K jobs and 27 worker kill/reconnect cycles per run;
- 90K total jobs and 81 kill cycles;
- server-authoritative completion reconciliation;
- flat p99 after the initial cold probe;
- WAL around 4.1–4.3 MB;
- post-compaction/post-GC collection bounds passed.

An exploratory `SOAK_KILL_MS=300` override failed only its kill-count
expectation: 30 ms reconnect overhead permits 36 rather than the expected 37
kills in 12 seconds. Job conservation and latency passed up to that assertion;
the supported 400 ms profile passed 3/3.

## Capacity interpretation

1. The highest single-process queue rate is the internal in-memory batched path.
   It is not an on-disk public API claim.
2. For on-disk public producers, the sustained Embedded curve and fresh TCP
   `PUSHB` result are the relevant figures.
3. Producer ingestion substantially exceeds TCP worker drain. Capacity planning
   must size consumers from the ~17K jobs/s single-worker knee, not 159K
   producer ingestion.
4. `durable:true` is a separate service level and should never be averaged with
   buffered mode.
5. A single Workflow Engine sustains roughly 2–3K multi-node no-op workflows/s
   at the 500–1,000 execution scales measured here.
6. Workflow scale-out is useful through 12 Embedded instances. TCP reaches a
   host-level plateau around 8 pairs and should not be extrapolated linearly.
7. The default 10K-request protocol cap is reachable by bursty workflows.
   Operators should tune it from expected command expansion and retain
   rate-limit telemetry; benchmarks must state the override.

## Reproduction

Queue runners:

```bash
bun run src/benchmark/million-jobs.bench.ts
BENCH_N=50000 BENCH_RUNS=21 bun run bench:tcp
bun run bench:pushbulk
```

Workflow single-engine campaign:

```bash
BENCH_MODE=embedded \
BENCH_RUNS=7 BENCH_RUNS_LINEAR=21 BENCH_WARMUPS=1 \
BENCH_N_LINEAR=1000 BENCH_N_PARALLEL=500 \
BENCH_N_COMPENSATION=500 BENCH_N_SIGNAL=500 \
BENCH_CONCURRENCY=128 BENCH_OUTPUT=/tmp/workflow-embedded.json \
bun run bench:workflow

BENCH_MODE=tcp \
BENCH_RUNS=7 BENCH_RUNS_LINEAR=21 BENCH_WARMUPS=1 \
BENCH_N_LINEAR=1000 BENCH_N_PARALLEL=500 \
BENCH_N_COMPENSATION=500 BENCH_N_SIGNAL=500 \
BENCH_CONCURRENCY=64 BENCH_OUTPUT=/tmp/workflow-tcp.json \
bun run bench:workflow
```

Tuned horizontal campaign:

```bash
RATE_LIMIT_MAX_REQUESTS=1000000 \
BENCH_INSTANCES=1,4,8,12 BENCH_RUNS=3 BENCH_WARMUPS=1 BENCH_N=5000 \
BENCH_CONCURRENCY_EMBEDDED=128 BENCH_CONCURRENCY_TCP=64 \
BENCH_OUTPUT=/tmp/workflow-scale.json \
bun run bench:workflow:scale
```

Run without `BUNQUEUE_EMBEDDED=1`; that test-preload variable would force
supposed TCP clients into Embedded mode.

## Limits of inference

- Results are one native host and one revision, not a hardware-independent SLA.
- Localhost TCP avoids real network delay.
- No-op workflow handlers isolate orchestration overhead; real application
  handlers can dominate both throughput and latency.
- Horizontal instances share one kernel, scheduler, memory subsystem, loopback
  stack, and storage device. The campaign identifies aggregate host capacity,
  not a single contention source.
- Process CPU is sampled and RSS includes runtime/allocator behavior; neither is
  an exact component allocation.
- Memory slope in a shared test process is not proof of a leak.

Raw per-sample JSON, logs, time output, and resource observations were retained
locally under the sandbox artifact tree. All temporary SQLite, WAL, and SHM
files and benchmark processes were removed after measurement.

## Related documents

- [Benchmarking and Performance Evidence](../features/benchmarks.md)
- [Workflow Engine](../features/workflow-engine.md)
- [Core Fix Impact Benchmark — 2026-07-16](./fix-impact-2026-07-16.md)
- [Test Isolation and Reproducibility](../testing.md)
