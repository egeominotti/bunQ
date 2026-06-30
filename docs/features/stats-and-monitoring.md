# Stats, Metrics & Monitoring

> **Category:** Observability · **Source:** `src/application/statsManager.ts`, `src/application/metricsExporter.ts`, `src/application/throughputTracker.ts`, `src/application/latencyTracker.ts`

## Purpose

This module computes and exposes the server's operational telemetry: queue depth counts (waiting/prioritized/delayed/active/dlq/completed), cumulative job counters, per-queue breakdowns, internal collection sizes for memory debugging, throughput rates, and operation latency percentiles. It is the single source of truth behind the `Stats`, `Metrics`, and `Prometheus` TCP commands, the `/stats`, `/metrics`, `/prometheus`, `/health`, `/heapstats`, and `/dashboard` HTTP endpoints, the periodic stats log line, and the WebSocket/SSE dashboard pushes. It deliberately performs read-only aggregation over live data structures — it never mutates queue state (except `compactMemory`, which reclaims memory).

## Responsibilities & Scope

Owns:

- **Snapshot aggregation** (`statsManager.ts`): `getStats`, `getMemoryStats`, `getPerQueueStats`, `getQueueJobCounts`, `compactMemory`.
- **Throughput rates** (`throughputTracker.ts`): EMA-smoothed push/pull/complete/fail rates per second via a global singleton `throughputTracker`.
- **Latency histograms** (`latencyTracker.ts`): push/pull/ack duration histograms exposing averages and p50/p95/p99 via a global singleton `latencyTracker`.
- **Prometheus exposition format** (`metricsExporter.ts`): `generatePrometheusMetrics` builds the full text/plain Prometheus payload (gauges, counters, per-queue labelled series, and latency histograms).

Does NOT own:

- Counter *increments*. The cumulative counters (`totalPushed`, `totalPulled`, `totalCompleted`, `totalFailed`, and the per-queue equivalents) are mutated by the push/pull/ack operations, not here — see [Job Lifecycle](./job-lifecycle.md). This module only reads `ctx.metrics.*.value` and `ctx.perQueueMetrics`.
- The actual collections it measures (jobIndex, completedJobs, shards, locks). They live in [Core Queue Engine](./core-queue-engine.md) and [Data Structures](./data-structures.md).
- HTTP routing / TCP command dispatch and auth (delegated to [HTTP / REST / SSE / WebSocket API](./http-api.md), [TCP Server Command Handlers](./tcp-server-handlers.md), [Security: TLS, Auth, CORS](./security-tls-auth.md)).
- Worker/webhook stats merged into Prometheus output — those come from [Worker Registry & Management](./workers-management.md) and [Webhooks, Events & Job Logs](./webhooks-and-events.md).

## Dependencies

Internal:

- `../shared/hash` — `SHARD_COUNT` and `shardIndex(name)` to iterate shards and locate a queue's owning shard (`statsManager.ts:6`).
- `./types` — `StatsContext`, the read-only view of QueueManager internals (`statsManager.ts:7`, defined at `src/application/types.ts:129`).
- `../shared/histogram` — the `Histogram` class backing each latency series (`latencyTracker.ts:6`, `src/shared/histogram.ts`).
- `./latencyTracker` — imported by `metricsExporter.ts:9` to append histogram lines.
- `./workerManager`, `./webhookManager`, `./statsManager` (`PerQueueStats`) — type/data inputs to `generatePrometheusMetrics` (`metricsExporter.ts:6-8`).

External / runtime:

- Bun globals: `Date.now()` (rate/uptime math), `Bun.nanoseconds()` at the call sites that feed `latencyTracker`, `process.memoryUsage()` (in the HTTP/handler callers), `Bun.gc` and `bun:jsc` `heapStats()` (in `httpEndpoints.ts`). No SQLite access — all aggregation is over in-memory structures.

## Public Interface

### Exported functions — `statsManager.ts`

```typescript
function getStats(
  ctx: StatsContext,
  cronScheduler: { getStats(): { total: number; pending: number } }
): QueueStats                                                    // statsManager.ts:64

function getMemoryStats(ctx: StatsContext): MemoryStats          // statsManager.ts:124

function getPerQueueStats(
  ctx: StatsContext, queueNames: Set<string>
): Map<string, PerQueueStats>                                    // statsManager.ts:171

function getQueueJobCounts(queueName: string, ctx: StatsContext): {
  waiting; prioritized; delayed; active; completed; failed;
  'waiting-children'; totalCompleted; totalFailed;             // all number
}                                                                // statsManager.ts:219

function compactMemory(ctx: StatsContext): void                  // statsManager.ts:305
```

### Exported from `metricsExporter.ts`

```typescript
function generatePrometheusMetrics(
  stats: QueueStats,
  workerManager: WorkerManager,
  webhookManager: WebhookManager,
  perQueueStats?: Map<string, PerQueueStats>
): string                                                        // metricsExporter.ts:29
```

### Exported from `throughputTracker.ts`

```typescript
class ThroughputTracker {
  readonly pushRate, pullRate, completeRate, failRate: RateTracker;
  getRates(): { pushPerSec; pullPerSec; completePerSec; failPerSec }  // throughputTracker.ts:51
}
export const throughputTracker = new ThroughputTracker();        // singleton, line 67
// RateTracker.increment(n = 1) / getRate() — line 20 / 25
```

### Exported from `latencyTracker.ts`

```typescript
class LatencyTracker {
  readonly push, pull, ack: Histogram;
  toPrometheus(): string;                                        // latencyTracker.ts:15
  getAverages(): { pushMs; pullMs; ackMs };                      // latencyTracker.ts:26
  getPercentiles(): { push:{p50,p95,p99}; pull:{...}; ack:{...} } // latencyTracker.ts:35
}
export const latencyTracker = new LatencyTracker();              // singleton, line 61
```

### TCP commands handled

| Command | Handler | Output |
| --- | --- | --- |
| `Stats` | `handleStats` (`handlers/management.ts:98`) | `StatsResponse` (`stats` payload: waiting/active/delayed/dlq/completed/failed/uptime/pushPerSec/pullPerSec) |
| `Metrics` | `handleMetrics` (`handlers/management.ts:128`) | `MetricsResponse` — `{ ok, metrics: { totalCompleted, totalFailed, … } }` (totals + avgLatencyMs/avgProcessingMs/memoryUsageMb) |

> The client SDK's `queue.getMetrics('completed'|'failed')` reads this `metrics` payload over TCP — `completed → metrics.totalCompleted`, `failed → metrics.totalFailed` (`client/queue/workers.ts`). It must **not** read `response.stats` (no such key on a `Metrics` reply — that always returned `0`).
| `Prometheus` | `handlePrometheus` (`handlers/monitoring.ts:298`) | `data({ metrics })` — full Prometheus text |
| `Ping` | `handlePing` (`handlers/monitoring.ts:116`) | `data({ pong: true, time: Date.now() })` |

Dispatch table: `handlerRoutes.ts:333-350`.

### HTTP endpoints (all `GET`)

| Path | Function | Body |
| --- | --- | --- |
| `/prometheus` | inline in `http.ts:179` → `queueManager.getPrometheusMetrics()` | `text/plain; version=0.0.4; charset=utf-8` |
| `/metrics` | `metricsEndpoint` (`httpEndpoints.ts:397`) | JSON `{ ok, metrics: { totalPushed, totalPulled, totalCompleted, totalFailed } }` |
| `/stats` | `statsEndpoint` (`httpEndpoints.ts:161`) | JSON stats + rates + memory + collections |
| `/health` | `healthEndpoint` (`httpEndpoints.ts:57`) | health, uptime, version, queue summary, memory; `ok:false`/`degraded` when disk full |
| `/heapstats` | `heapStatsEndpoint` (`httpEndpoints.ts:125`) | `bun:jsc` heap breakdown + `collections` |
| `/gc` (`POST`) | `gcEndpoint` (`httpEndpoints.ts:101`) | forces `Bun.gc(true)` + `compactMemory()`, returns before/after heap |
| `/dashboard`, `/dashboard/queues`, `/dashboard/:queue` | `dashboardOverviewEndpoint` / `dashboardQueuesEndpoint` / `dashboardQueueDetailEndpoint` (`httpEndpoints.ts:196/295/326`) | aggregated dashboard JSON |

Note: `/metrics` returns **JSON**, while `/prometheus` returns the **Prometheus text format**. They are not interchangeable. The TCP `Metrics` command and HTTP `/metrics` produce different shapes; the Prometheus exposition is only via `Prometheus`/`/prometheus`.

### CLI commands

`bunqueue stats` → `Stats`; `bunqueue metrics` → `Prometheus`; `bunqueue health` → `Stats`; `bunqueue ping` → `Ping` (`src/cli/commands/monitor.ts:9-20`).

## Data Models

See [data-model](../data-model.md) for full definitions. Key shapes defined here:

**`QueueStats`** (`statsManager.ts:18`): `waiting`, `prioritized`, `delayed`, `active`, `dlq`, `completed`, `'waiting-children'` (numbers); `totalPushed`/`totalPulled`/`totalCompleted`/`totalFailed` (**`bigint`**); `uptime` (ms), `cronJobs`, `cronPending`.

**`PerQueueStats`** (`statsManager.ts:35`): `waiting`, `prioritized`, `delayed`, `active`, `dlq`.

**`MemoryStats`** (`statsManager.ts:43`): sizes of `jobIndex`, `completedJobs`, `jobResults`, `jobLogs`, `customIdMap`, `jobLocks`, `clientJobs`, `clientJobsTotal`, `pendingDepChecks`, `stalledCandidates`, plus per-shard aggregates `processingTotal`, `queuedTotal`, `waitingDepsTotal`, `temporalIndexTotal`, `delayedHeapTotal`.

**`StatsData`** / **`MetricsData`** (TCP wire) — `src/domain/types/response.ts:127` / `:145`. `bigint` counters are coerced via `Number(...)` before serialization (`handlers/management.ts:108,133-136`).

## Business Logic / Control Flow

### `getStats` (`statsManager.ts:64`)

1. Loop over all `SHARD_COUNT` shards. Per shard accumulate `delayedJobs`, `dlqJobs` (from `shard.getStats()`) and `active` from `processingShards[i].size` (`:78-81`).
2. `waiting-children` = `shard.waitingChildren.size + shard.waitingDeps.size` summed over shards (`:85`). Both sets are counted because `getJobState`/`getJobs` report flow parents *and* dependency-blocked jobs under the single `waiting-children` state — counting only one undercounts versus state/list (the "#95 class" invariant).
3. Split ready jobs into `waiting` vs `prioritized` by scanning every queue's jobs: only jobs with `runAt <= now` count; `priority > 0` → prioritized, else waiting (`:88-98`). This is an O(total queued jobs) scan.
4. Pull cron totals from `cronScheduler.getStats()` and read cumulative counters off `ctx.metrics.*.value` (bigint). `uptime = Date.now() - ctx.startTime`.

### `getQueueJobCounts` (`statsManager.ts:219`)

Locates the queue's owning shard via `shardIndex(queueName)`, classifies its jobs (here `runAt > now` → delayed, else priority split — note this includes delayed in the queue scan, unlike `getStats` which only counts ready jobs), counts active by scanning **all** processing shards for matching `job.queue` (`:256-262`), counts `completed` by scanning the entire `jobIndex` for `type==='completed'` entries that are still in `completedJobs` (`:266-270`), and reads `failed` from `shard.getDlq(queueName).length`. `'waiting-children'` uses the `countByQueue` helper (`:10`) over both waiting sets. Per-queue cumulative totals come from `ctx.perQueueMetrics` (populated by ack — `operations/ack.ts:126`).

### `getPerQueueStats` (`statsManager.ts:171`)

For each name in `queueNames`, look up its shard queue and classify (`runAt > now` → delayed). DLQ via `shard.getDlqCount(name)`. A final pass over all processing shards increments `active` per matching queue (`:204-211`).

### Prometheus exposition (`metricsExporter.ts:29`)

Builds a `string[]` of `# HELP`/`# TYPE`/value triples: global gauges (waiting, prioritized, delayed, active, dlq, completed, uptime_seconds, cron_jobs_total), counters (`*_total`), worker stats (`workerManager.getStats()`), webhook stats. If `perQueueStats` is non-empty, appends labelled series `bunqueue_queue_jobs_*{queue="..."}` (`:113-148`). Finally appends `latencyTracker.toPrometheus()` (`:151-155`), which emits cumulative-bucket histograms for `bunqueue_push_duration_ms`, `bunqueue_pull_duration_ms`, `bunqueue_ack_duration_ms`. `uptime` is converted ms→s via `Math.floor(stats.uptime / 1000)` (`:81`).

### Throughput rates (`throughputTracker.ts`)

`RateTracker.increment(n)` is O(1), called from push/pull/ack hot paths (`operations/push.ts:269,345`, `operations/pull.ts:119,165`, `operations/ack.ts:134,173,255`, `operations/ackHelpers.ts:279`). `getRate()` (`:25`) computes `count / elapsedSeconds` then folds it into an EMA with `alpha = 0.3` (`lastRate = alpha*current + (1-alpha)*lastRate`), resets the counter, and stamps `lastCalcTime`. Rates are rounded to 2 decimals in `getRates()`.

### Latency (`latencyTracker.ts` + `histogram.ts`)

Each operation observes `(Bun.nanoseconds() - startNs) / 1e6` ms into its `Histogram`. `Histogram.observe` (`histogram.ts:22`) binary-searches the bucket and increments cumulative buckets; `percentile(p)` (`:54`) walks buckets until cumulative count ≥ target; `getAverages` returns `sum/count` (0 when count is 0). Default buckets: `[0.1,0.5,1,2.5,5,10,25,50,100,250,500,1000,2500,5000,10000]` ms (`histogram.ts:7`).

### Periodic stats log (`bootstrap.ts:214`)

A `setInterval` every `config.statsIntervalMs` calls `getStats` + `getMemoryStats` + worker stats and logs a single line (waiting/active/delayed/completed/dlq, connection counts, worker `active/total`, heap/rss, and internal sizes `idx`/`locks`/`clients`).

## Concurrency & Locking

These aggregation functions are **lock-free reads** that iterate live structures (shard queues, `processingShards`, `jobIndex`, the bounded maps/sets) without acquiring the shard/jobIndex locks documented in [Concurrency & Locking](./concurrency-and-locking.md). Because Bun is single-threaded per event loop, a snapshot reflects whatever state exists between awaited operations; counts can momentarily skew when read mid-operation (e.g. a job counted as both active and still in a queue during a transition). The trackers are plain in-process singletons with no synchronization; `increment` and `getRate` mutate non-atomically but are safe under the single-threaded model.

`compactMemory` (`statsManager.ts:305`) **does** mutate: it compacts priority queues whose stale ratio exceeds 10% (`q.needsCompaction(0.1)`), deletes empty `clientJobs` entries, and prunes orphaned `jobLocks` whose job is no longer in `processing` state. It takes no explicit lock — callers (`/gc`, MCP `compact_memory`) invoke it synchronously on the event loop.

## Edge Cases & Failure Modes

- **bigint vs number:** `QueueStats` totals are `bigint`; the TCP `Stats`/`Metrics` handlers and HTTP JSON endpoints coerce with `Number(...)`, which loses precision above 2^53. Prometheus interpolates the `bigint` directly (exact).
- **`waiting-children` double-set invariant:** counting only `waitingChildren` or only `waitingDeps` undercounts vs `getJobState`/`getJobs`. Both `getStats:85` and `getQueueJobCounts` include both (the "#95 class" comment).
- **delayed classification differs:** `getStats` counts only `runAt <= now` jobs toward waiting/prioritized and never adds delayed from the queue scan (delayed comes from `shard.getStats().delayedJobs`), while `getQueueJobCounts`/`getPerQueueStats` classify `runAt > now` jobs as `delayed` from the queue scan. Keep this in mind when reconciling global vs per-queue numbers.
- **O(N) / O(Q·N) scans:** `getStats` scans all queued jobs; `getQueueJobCounts` scans the whole `jobIndex` for completed and all processing shards for active. Calling these per dashboard event over many queues can degrade to O(N²) — historically a source of the SSE/per-event regression (`sseHandler.ts:153` comment; the SSE handler early-returns with zero connected clients to avoid it).
- **EMA warm-up & idle:** the first `getRate()` seeds `lastRate` directly (no smoothing); if `elapsed < 0.1s` it returns the stale `lastRate` to avoid divide-by-tiny blowups (`throughputTracker.ts:29`). During idle gaps, rate decays toward 0 only when next sampled. The internal `count` resets on every `getRate`, so calling it from multiple consumers (HTTP poll + WS/SSE interval + `/dashboard`) splits observations across callers and can understate rates.
- **Histogram is unbounded-in-time:** counters/sum never auto-reset (only `reset()` exists, unused in the hot path), so percentiles/averages are lifetime values, not windowed. Percentiles are bucket-granular (return a bucket boundary, not interpolated) and cap at the largest bucket (10000 ms).
- **Empty histogram:** `getAverages` guards `count > 0` returning 0; `percentile` returns 0 when count is 0.
- **`/metrics` ≠ Prometheus:** a common gotcha — `/metrics` is JSON; scrapers must target `/prometheus`.
- **No persistence:** rates, histograms, and `uptime` reset on restart; cumulative counters live only in memory (`ctx.metrics`), not SQLite.

## Configuration

| Env / option | Default | Effect |
| --- | --- | --- |
| `STATS_INTERVAL_MS` (`config.statsIntervalMs`) | `300000` (5 min) | Interval of the periodic stats log line (`config/resolve.ts:57`, `bootstrap.ts:243`). Does not affect on-demand endpoints. |
| `METRICS_AUTH` (`config.requireAuthForMetrics`) | `false` | When `true`, the `/prometheus` endpoint requires auth before serving (`config/resolve.ts:51`, `http.ts:180`). |
| `RateTracker` `alpha` | `0.3` | EMA smoothing (~3-sample memory). Not env-configurable; constructor default (`throughputTracker.ts:15`). |
| `Histogram` buckets | `DEFAULT_BUCKETS` | Fixed ms bucket boundaries; not env-configurable (`histogram.ts:7`). |

CORS for these GET endpoints is governed by the server CORS config; see [Security: TLS, Auth, CORS](./security-tls-auth.md).

## Related Docs

- [Job Lifecycle (push / pull / ack / fail)](./job-lifecycle.md) — where the counters and latency/throughput trackers are incremented.
- [Job Queries & Queue Control](./job-queries-and-control.md) — `GetJobCounts`, state classification semantics.
- [HTTP / REST / SSE / WebSocket API](./http-api.md) — routing for `/stats`, `/metrics`, `/prometheus`, `/health`, `/dashboard`, and the SSE/WS stats pushes.
- [TCP Server Command Handlers](./tcp-server-handlers.md) — dispatch of `Stats`/`Metrics`/`Prometheus`/`Ping`.
- [Worker Registry & Management](./workers-management.md) and [Webhooks, Events & Job Logs](./webhooks-and-events.md) — sources of the worker/webhook series in Prometheus output.
- [Background Tasks](./background-tasks.md) — cleanup/compaction context for `compactMemory` and memory bounds.
- [bunqueue Cloud Dashboard Integration](./cloud-integration.md) and [Native MCP Server](./mcp-server.md) — additional consumers of these stats.
- [Security: TLS, Auth, CORS](./security-tls-auth.md) — `METRICS_AUTH` and CORS handling.
- [architecture](../architecture.md) · [data-model](../data-model.md)
