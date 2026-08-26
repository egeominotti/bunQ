# Benchmarking and Performance Evidence

> **Category:** Engineering tooling · **Primary runners:**
> `bench/workflow-engine.ts`, `bench/workflow-engine/scale.ts`,
> `bench/postgres-versions.ts`, `bench/tcp-bench.ts`, `bench/fix-impact.ts`,
> `src/benchmark/million-jobs.bench.ts`

## Purpose

bunqueue keeps performance measurements separate from functional validation.
Tests answer whether a behavior is correct; benchmarks quantify one named
operation under a named topology. A benchmark result is publishable only when
the workload, persistence mode, process boundary, sample count, aggregation,
hardware, runtime revision, correctness checks, and known limits are stated.

The final v2.8.56 native host campaign is documented in
[Native Engineering Benchmark — 2026-08-03](../benchmarks/native-engineering-2026-08-03.md).
It covers every maintained queue, transport, Workflow, Flow, dependency,
event, stress, and million-job runner in Embedded and TCP modes.

The native PostgreSQL compatibility campaign is documented in
[Native PostgreSQL 15–18 Engineering Benchmark — 2026-08-26](../benchmarks/postgres-versions-2026-08-26.md).
It compares one, two, and four independent brokers on fresh PostgreSQL 15.19,
16.15, 17.11, and 18.6 clusters with seven measured samples per cell.

The follow-up
[PostgreSQL 18 Multi-Broker Performance Analysis — 2026-08-26](../benchmarks/postgres-performance-analysis-2026-08-26.md)
profiles ACK metric contention, event catch-up, queue refreshes, transaction
count, batch size, pool size, and `work_mem`. It records both retained and
rejected optimizations and separates throughput from latency, WAL, and spill
trade-offs.

## Evidence levels

| Level       | Required properties                                                                                                                                                   | Intended use                                    |
| ----------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------- |
| Publication | Fresh process and state per sample; warm-up discarded; repeated samples; median plus tails/variance; native host; exact integrity checks; raw machine-readable output | Documentation, release notes, capacity planning |
| Engineering | Controlled fresh state and correctness checks, but fewer samples or a narrowly targeted harness                                                                       | Regression analysis and tuning                  |
| Diagnostic  | One process, accumulated state, fixed ports, timing-only output, or no distribution                                                                                   | Locating a bottleneck; never a headline number  |
| Functional  | A feature exercise that prints timings incidentally                                                                                                                   | Correctness only                                |

Docker results are never published as performance figures. Containers remain
the authoritative functional isolation boundary, but share the host kernel,
CPU scheduler, memory, and storage device.

## Measurement contract

Every publishable native campaign follows these rules:

1. Record the Git revision, worktree status, Bun version, OS, CPU, logical CPU
   count, memory, governor/power mode, and whether virtualization is present.
2. Wait for functional containers and other benchmark processes to stop.
3. Use a fresh Bun process, unique queue names, dynamic ports, and fresh SQLite
   paths for every sample. TCP Workflow Engine samples use one client-local
   workflow database and a different broker database.
4. Discard at least one warm-up process. Do not mix a warm run into the measured
   distribution.
5. Report the median, p05/p95 or min/max, coefficient of variation, and latency
   percentiles where meaningful. A maximum is not a representative result.
6. Reconcile accepted, delivered, completed, failed, missing, duplicate, and
   invalid IDs. Workflow samples additionally reconcile lifecycle events,
   terminal state, completed steps, signals, and compensation outcomes.
7. Close every `Engine`, `Queue`, `Worker`, manager, server, and database, then
   remove the database, `-wal`, and `-shm` files.
8. Retain raw JSON/logs alongside the derived report. Never reconstruct raw
   samples from a rounded table.

Throughput values from different operations are not ratios. Internal
`QueueManager.pushBatch`, public `Queue.addBulk`, TCP `PUSHB`, durable sequential
`add`, worker drain, and complete workflow execution have different work and
different durability boundaries.

## Persistence and topology labels

The label must say what was actually measured:

- **Embedded in-memory:** no `dataPath`; the QueueManager has no SQLite storage.
- **Embedded on-disk:** `dataPath` points to a fresh SQLite file. The workflow
  store and embedded queue use separate connections to that same file.
- **TCP on-disk:** the broker owns its SQLite file. A TCP Workflow Engine also
  owns a separate client-local workflow SQLite file.
- **Buffered:** the normal write-behind queue path.
- **Durable:** `durable: true`, where the timed operation waits for synchronous
  persistence.

`bench/comprehensive.ts` does not pass a `dataPath` to its Embedded queues.
Its historical Embedded charts are therefore in-memory, despite older public
copy describing them as direct SQLite writes. Use `bench:pushbulk` with an
explicit data path or the July 30 campaign for on-disk public-API figures.

## Workflow Engine runners

### Single-engine campaign

`bun run bench:workflow` drives four scenarios:

| Scenario       | Graph                                         | Terminal invariant                                      | Primary metric                             |
| -------------- | --------------------------------------------- | ------------------------------------------------------- | ------------------------------------------ |
| `linear`       | Three top-level no-op steps                   | `completed`, 3 step completions                         | End-to-end workflow throughput/latency     |
| `parallel`     | Prepare → 3 inline parallel steps → join      | `completed`, 5 step completions                         | Parallel bookkeeping and join              |
| `compensation` | Two compensatable steps → intentional failure | `failed`, rollback `completed`, 2 compensation outcomes | Forward failure plus reverse unwind        |
| `signal`       | Request → `waitFor('approved')` → finish      | waiting and signal counts exact, then `completed`       | Total, park, and resume throughput/latency |

The coordinator launches each sample in a new process and temporary directory.
TCP samples also launch a new broker on dynamic TCP/HTTP ports. The measured
interval begins immediately before `Engine.start()` calls and ends at the last
terminal event. Setup, server startup, integrity scans, and cleanup are outside
that interval. Signal resume latency begins immediately before `Engine.signal`.

Key controls:

| Environment variable    |      Default | Meaning                                                     |
| ----------------------- | -----------: | ----------------------------------------------------------- |
| `BENCH_MODE`            |        `all` | `embedded`, `tcp`, or both                                  |
| `BENCH_SCENARIOS`       |     all four | Comma-separated scenario list                               |
| `BENCH_RUNS`            |          `7` | Measured fresh processes per scenario                       |
| `BENCH_RUNS_LINEAR`     | `BENCH_RUNS` | Scenario-specific run override                              |
| `BENCH_WARMUPS`         |          `1` | Discarded fresh processes                                   |
| `BENCH_N_LINEAR`        |       `1000` | Executions per linear sample                                |
| `BENCH_N_PARALLEL`      |        `500` | Executions per parallel sample                              |
| `BENCH_N_COMPENSATION`  |        `200` | Executions per rollback sample                              |
| `BENCH_N_SIGNAL`        |        `200` | Executions per wait/signal sample                           |
| `BENCH_CONCURRENCY`     |         `32` | Internal Workflow worker concurrency                        |
| `BENCH_START_BATCH`     |        `100` | Concurrent `Engine.start`/`signal` calls per producer batch |
| `BENCH_OUTPUT`          |        unset | JSON output path                                            |
| `BENCH_COMMAND_TIMEOUT` |      `30000` | TCP client command timeout, milliseconds                    |

Any sample that exits non-zero, fails conservation, ends in the wrong state, or
writes to stderr—including its TCP broker—aborts the campaign.
`BENCH_ALLOW_STDERR=1` exists only for deliberate fault diagnostics.

### Horizontal campaign

`bun run bench:workflow:scale` launches independent engines behind a common
wall-clock barrier. Each child has its own process, queue, workflow store, and
broker/ports in TCP mode. Aggregate throughput uses the earliest child start
and latest terminal event, not the sum of per-process rates. The report includes
start skew, combined RSS, sampled process CPU, speedup, efficiency, and event
conservation.

Defaults are `1,4,8,12` instances, 3 measured campaigns after one warm-up,
5,000 executions per instance, Embedded concurrency 128 and TCP concurrency 64.
`BENCH_INSTANCES` must start with `1` so speedup has a real same-workload
baseline.

The TCP server's protocol safety limit defaults to 10,000 requests per client
per 60-second window. A high-volume workflow expands into several queue
commands, so a scale test can measure the safety cap rather than the engine.
When intentionally measuring tuned capacity, set and report, for example:

```bash
RATE_LIMIT_MAX_REQUESTS=1000000 bun run bench:workflow:scale
```

The default-cap result remains operationally important and must be reported
separately; raising the limit is not a correction to a default-topology result.

## Runner catalogue

### Maintained performance runners

| Runner                                                   | Scope                                                                            | Status and caveat                                                                                                                                                                                                                      |
| -------------------------------------------------------- | -------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `bench/postgres-versions.ts` + `postgres-versions/*`     | Native PostgreSQL 15–18, one/two/four process brokers, durable batched lifecycle | Publication-grade local engineering harness; fresh cluster and broker processes per sample, rotated order, exact ID/database/deadlock/spill evidence, explicit pool/poll/`work_mem`, and dirty-runtime identity in JSON                |
| `bench/workflow-engine.ts` + `workflow-engine/sample.ts` | Embedded/TCP workflow throughput, latency, signals, rollback                     | Publication-grade; fresh process/state and JSON                                                                                                                                                                                        |
| `bench/workflow-engine/scale.ts`                         | Horizontal Workflow Engine scale curve                                           | Publication-grade when the protocol cap and resource sampling are reported                                                                                                                                                             |
| `bench/fix-impact.ts` + `fix-impact/*`                   | Before/after recovery, queries, scheduling, indexes, waiters, heap retention     | Publication-grade cross-revision harness; correctness travels with timing                                                                                                                                                              |
| `bench/tcp-bench.ts`                                     | Pipelined push, `PUSHB`, sequential RTT                                          | Engineering runner; fresh broker/database per operation, derived and reported non-bottlenecking protocol cap, explicit process shutdown, and database/WAL/SHM cleanup                                                                  |
| `src/benchmark/million-jobs.bench.ts`                    | Internal batched push/process lifecycle with two integrity sets                  | Engineering runner; in-memory internal API, not public Queue or SQLite throughput                                                                                                                                                      |
| `bench/comparison/run.ts` + `comparison/*`               | bunqueue TCP/SQLite versus BullMQ/Redis                                          | Comparative campaign; isolated endpoints and run ID; both processing samples require exact accepted/invoked ID equality, zero duplicate delivery, zero failed/nonterminal jobs, and authoritative broker completion before timing ends |

### Targeted diagnostics

| Runner                                           | Measures                                         | Important limitation                                                                                                                                                                                                                                                                                                                                                                        |
| ------------------------------------------------ | ------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `bench/comprehensive.ts`                         | Legacy Embedded/TCP curves                       | Embedded is in-memory and resets its shared manager per scale; TCP uses the `BENCH_HOST`/`BENCH_PORT` endpoint and grows broker/database state across scales; processing reconciles IDs and authoritative terminal counts before its deadline                                                                                                                                               |
| `bench/pushbulk-delta.ts`                        | Public `add`/`addBulk` before/after deltas       | Embedded is in-memory; TCP uses the configured external endpoint; three repetitions per cell and campaign state grows across scales; always shuts down the shared manager after reporting or errors                                                                                                                                                                                         |
| `bench/tcp-process-sweep.ts`                     | Worker concurrency/batch-size knee               | External fresh broker; explicitly forces TCP even under the test preload; configurable host/port/scale/cases; rejects missing or invalid direct-call ports instead of falling back to `6789`; its self-hosted integrity gate resolves the actual dynamic listener port; active lease renewal, accepted/invoked ID equality, authoritative terminal counts and Worker errors fail the sample |
| `bench/local-autobatch.ts`                       | Sequential/concurrent add with batching toggle   | Fixed port and local server lifecycle                                                                                                                                                                                                                                                                                                                                                       |
| `bench/job-list-perf.ts`                         | `getJobs` pages across queue sizes               | In-process accumulated state; timing diagnostic                                                                                                                                                                                                                                                                                                                                             |
| `scripts/bench-tcp-batch-notify.ts`              | Wakeup latency, drain, worker fairness           | Self-hosted dynamic port; retains all 100k terminal rows needed by its largest sample; accepted/invoked ID and authoritative terminal reconciliation; deterministic cleanup                                                                                                                                                                                                                 |
| `scripts/tcp/bench-flow-parallel.ts`             | Flow sibling creation sequential versus parallel | Existing TCP server; small sample                                                                                                                                                                                                                                                                                                                                                           |
| `src/benchmark/dependency-latency.bench.ts`      | Parent ACK → child eligible latency              | Internal QueueManager microbenchmark                                                                                                                                                                                                                                                                                                                                                        |
| `src/benchmark/algorithm-optimizations.bench.ts` | Algorithmic hot paths                            | Microbenchmark; mixed operations and legacy claims                                                                                                                                                                                                                                                                                                                                          |
| `src/benchmark/throughput.bench.ts`              | Sequential internal push/pull/ack                | Warm-up and timing utility, not isolated publication evidence                                                                                                                                                                                                                                                                                                                               |
| `src/benchmark/worker.bench.ts`                  | Internal push → pull → ack → events              | Functional performance smoke                                                                                                                                                                                                                                                                                                                                                                |
| `src/benchmark/stress.bench.ts`                  | Volume, latency, memory observations             | Stress diagnostic; not repeated fresh-process statistics                                                                                                                                                                                                                                                                                                                                    |

### Functional scripts with incidental timing

`src/benchmark/completed-events.bench.ts`,
`src/benchmark/event-payload-test.ts`,
`src/benchmark/full-client-test.ts`,
`src/benchmark/full-features-test.ts`, and
`src/benchmark/ultimate-test.ts` primarily verify behavior. Their printed
durations are not benchmark claims. `test/fix-impact-benchmark.test.ts` verifies
the before/after harness contract; it does not establish host performance.

The comparative runner defaults to bunqueue `127.0.0.1:6789` and Redis
`127.0.0.1:6379`. Reproducible campaigns should instead launch disposable
native services and set `BENCH_BUNQUEUE_HOST`, `BENCH_BUNQUEUE_PORT`,
`BENCH_REDIS_HOST`, `BENCH_REDIS_PORT`, and `BENCH_RUN_ID`. The optional
`BENCH_ITERATIONS` override changes the default 10,000-operation workload and
must be reported with the result.

The legacy comprehensive and push/bulk-delta TCP runners default to
`localhost:6789`. Set `BENCH_HOST` and `BENCH_PORT` to target a disposable,
isolated native broker. Both runners explicitly select TCP mode, including when
`BUNQUEUE_EMBEDDED` is present in the environment. Their Embedded samples do
not configure `dataPath` and therefore measure the shared in-memory manager,
not SQLite persistence. The comprehensive and batch-notify processing samples
fail instead of reporting throughput unless accepted and invoked IDs match
exactly and `getJobCounts()` reports the requested completed count with zero
failed or nonterminal jobs before the deadline. Batch-notify binds its owned
broker to an operating-system-assigned port so concurrent native tools cannot
collide with it. Its self-hosted `QueueManager` sets `maxCompletedJobs` to the
largest 100,000-job scenario, preventing the default 50,000-row retention bound
from invalidating the authoritative completion check. Comprehensive shuts down
the shared Embedded manager after every scale, so retained completions from an
earlier sample cannot consume the 50,000-job window of a later sample.
Comprehensive uses a 600,000ms processing deadline by default for its durable
50,000-job TCP sample and prints the effective value. `BENCH_TIMEOUT_MS` may
override it with a positive safe integer; invalid, zero, negative, fractional,
or nonnumeric values fail before the campaign starts.
Push/bulk-delta closes every per-sample Queue and shuts down the shared Embedded
manager in its entrypoint `finally`. It therefore exits naturally after the
last median and performs the same teardown when a sample throws.

The TCP serde runner sizes its disposable broker's protocol limit from
`BENCH_N` and the fixed round-trip sample count so the benchmark's own default
workload cannot trip the core's operational 10,000-request safety cap. It prints
the effective limit and whether it was derived. An explicitly supplied
`RATE_LIMIT_MAX_REQUESTS` is never raised silently, which keeps deliberate cap
experiments valid. `BENCH_TMP_DIR` selects the disposable database directory;
each broker is awaited during shutdown and its database, WAL, and SHM files are
removed before the next sample.

## Publication checklist

Before updating a number in README or the public site:

- identify the exact runner, revision, operation, mode, persistence, and scale;
- retain raw per-sample output and show the aggregation;
- state tuning that changes defaults, especially `RATE_LIMIT_MAX_REQUESTS`;
- include correctness totals and any stderr/anomaly review;
- distinguish a protective limit from an engine bottleneck;
- keep historical results dated and labelled instead of silently mixing hosts;
- run the functional sandbox independently of the native campaign;
- link the engineering report, not only a marketing chart.

## Related documents

- [Native PostgreSQL 15–18 Engineering Benchmark — 2026-08-26](../benchmarks/postgres-versions-2026-08-26.md)
- [PostgreSQL 18 Multi-Broker Performance Analysis — 2026-08-26](../benchmarks/postgres-performance-analysis-2026-08-26.md)
- [Native Engineering Benchmark — 2026-08-03](../benchmarks/native-engineering-2026-08-03.md)
- [Native Engineering Benchmark — 2026-08-02](../benchmarks/native-engineering-2026-08-02.md)
- [Native Engineering Benchmark — 2026-07-30](../benchmarks/native-engineering-2026-07-30.md)
- [Core Fix Impact Benchmark — 2026-07-16](../benchmarks/fix-impact-2026-07-16.md)
- [Test Isolation and Reproducibility](../testing.md)
- [Workflow Engine](./workflow-engine.md)
- [Client Transport](./client-transport.md)
- [Persistence](./persistence.md)
