# Benchmarking and Performance Evidence

> **Category:** Engineering tooling · **Primary runners:**
> `bench/workflow-engine.ts`, `bench/workflow-engine/scale.ts`,
> `bench/tcp-bench.ts`, `bench/fix-impact.ts`,
> `src/benchmark/million-jobs.bench.ts`

## Purpose

bunqueue keeps performance measurements separate from functional validation.
Tests answer whether a behavior is correct; benchmarks quantify one named
operation under a named topology. A benchmark result is publishable only when
the workload, persistence mode, process boundary, sample count, aggregation,
hardware, runtime revision, correctness checks, and known limits are stated.

The current native host campaign is documented in
[Native Engineering Benchmark — 2026-07-30](../benchmarks/native-engineering-2026-07-30.md).
It covers queue ingestion and draining, durable writes, TCP latency, horizontal
scaling, and the Workflow Engine in Embedded and TCP modes.

## Evidence levels

| Level | Required properties | Intended use |
| --- | --- | --- |
| Publication | Fresh process and state per sample; warm-up discarded; repeated samples; median plus tails/variance; native host; exact integrity checks; raw machine-readable output | Documentation, release notes, capacity planning |
| Engineering | Controlled fresh state and correctness checks, but fewer samples or a narrowly targeted harness | Regression analysis and tuning |
| Diagnostic | One process, accumulated state, fixed ports, timing-only output, or no distribution | Locating a bottleneck; never a headline number |
| Functional | A feature exercise that prints timings incidentally | Correctness only |

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

| Scenario | Graph | Terminal invariant | Primary metric |
| --- | --- | --- | --- |
| `linear` | Three top-level no-op steps | `completed`, 3 step completions | End-to-end workflow throughput/latency |
| `parallel` | Prepare → 3 inline parallel steps → join | `completed`, 5 step completions | Parallel bookkeeping and join |
| `compensation` | Two compensatable steps → intentional failure | `failed`, rollback `completed`, 2 compensation outcomes | Forward failure plus reverse unwind |
| `signal` | Request → `waitFor('approved')` → finish | waiting and signal counts exact, then `completed` | Total, park, and resume throughput/latency |

The coordinator launches each sample in a new process and temporary directory.
TCP samples also launch a new broker on dynamic TCP/HTTP ports. The measured
interval begins immediately before `Engine.start()` calls and ends at the last
terminal event. Setup, server startup, integrity scans, and cleanup are outside
that interval. Signal resume latency begins immediately before `Engine.signal`.

Key controls:

| Environment variable | Default | Meaning |
| --- | ---: | --- |
| `BENCH_MODE` | `all` | `embedded`, `tcp`, or both |
| `BENCH_SCENARIOS` | all four | Comma-separated scenario list |
| `BENCH_RUNS` | `7` | Measured fresh processes per scenario |
| `BENCH_RUNS_LINEAR` | `BENCH_RUNS` | Scenario-specific run override |
| `BENCH_WARMUPS` | `1` | Discarded fresh processes |
| `BENCH_N_LINEAR` | `1000` | Executions per linear sample |
| `BENCH_N_PARALLEL` | `500` | Executions per parallel sample |
| `BENCH_N_COMPENSATION` | `200` | Executions per rollback sample |
| `BENCH_N_SIGNAL` | `200` | Executions per wait/signal sample |
| `BENCH_CONCURRENCY` | `32` | Internal Workflow worker concurrency |
| `BENCH_START_BATCH` | `100` | Concurrent `Engine.start`/`signal` calls per producer batch |
| `BENCH_OUTPUT` | unset | JSON output path |
| `BENCH_COMMAND_TIMEOUT` | `30000` | TCP client command timeout, milliseconds |

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

| Runner | Scope | Status and caveat |
| --- | --- | --- |
| `bench/workflow-engine.ts` + `workflow-engine/sample.ts` | Embedded/TCP workflow throughput, latency, signals, rollback | Publication-grade; fresh process/state and JSON |
| `bench/workflow-engine/scale.ts` | Horizontal Workflow Engine scale curve | Publication-grade when the protocol cap and resource sampling are reported |
| `bench/fix-impact.ts` + `fix-impact/*` | Before/after recovery, queries, scheduling, indexes, waiters, heap retention | Publication-grade cross-revision harness; correctness travels with timing |
| `bench/tcp-bench.ts` | Pipelined push, `PUSHB`, sequential RTT | Engineering runner; fresh broker/database per operation, but callers must also verify WAL/SHM cleanup |
| `src/benchmark/million-jobs.bench.ts` | Internal batched push/process lifecycle with two integrity sets | Engineering runner; in-memory internal API, not public Queue or SQLite throughput |
| `bench/comparison/run.ts` | bunqueue TCP versus BullMQ/Redis | Comparative campaign; requires fresh Redis/broker state and version capture outside the script |

### Targeted diagnostics

| Runner | Measures | Important limitation |
| --- | --- | --- |
| `bench/comprehensive.ts` | Legacy Embedded/TCP curves | Embedded is in-memory; TCP needs an existing server; state is accumulated by scale |
| `bench/pushbulk-delta.ts` | Public `add`/`addBulk` before/after deltas | Existing server; three repetitions per cell; campaign state grows across scales |
| `bench/tcp-process-sweep.ts` | Worker concurrency/batch-size knee | Fixed server/port and one sample per point |
| `bench/local-autobatch.ts` | Sequential/concurrent add with batching toggle | Fixed port and local server lifecycle |
| `bench/job-list-perf.ts` | `getJobs` pages across queue sizes | In-process accumulated state; timing diagnostic |
| `scripts/bench-tcp-batch-notify.ts` | Wakeup latency, drain, worker fairness | Self-hosted fixed port; targeted transport experiment |
| `scripts/tcp/bench-flow-parallel.ts` | Flow sibling creation sequential versus parallel | Existing TCP server; small sample |
| `src/benchmark/dependency-latency.bench.ts` | Parent ACK → child eligible latency | Internal QueueManager microbenchmark |
| `src/benchmark/algorithm-optimizations.bench.ts` | Algorithmic hot paths | Microbenchmark; mixed operations and legacy claims |
| `src/benchmark/throughput.bench.ts` | Sequential internal push/pull/ack | Warm-up and timing utility, not isolated publication evidence |
| `src/benchmark/worker.bench.ts` | Internal push → pull → ack → events | Functional performance smoke |
| `src/benchmark/stress.bench.ts` | Volume, latency, memory observations | Stress diagnostic; not repeated fresh-process statistics |

### Functional scripts with incidental timing

`src/benchmark/completed-events.bench.ts`,
`src/benchmark/event-payload-test.ts`,
`src/benchmark/full-client-test.ts`,
`src/benchmark/full-features-test.ts`, and
`src/benchmark/ultimate-test.ts` primarily verify behavior. Their printed
durations are not benchmark claims. `test/fix-impact-benchmark.test.ts` verifies
the before/after harness contract; it does not establish host performance.

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

- [Native Engineering Benchmark — 2026-07-30](../benchmarks/native-engineering-2026-07-30.md)
- [Core Fix Impact Benchmark — 2026-07-16](../benchmarks/fix-impact-2026-07-16.md)
- [Test Isolation and Reproducibility](../testing.md)
- [Workflow Engine](./workflow-engine.md)
- [Client Transport](./client-transport.md)
- [Persistence](./persistence.md)
