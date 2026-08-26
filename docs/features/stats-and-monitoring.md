# Stats, Metrics & Monitoring

> **Category:** Observability · **Source:** `src/application/metricsExporter.ts`,
> `src/application/latencyTracker.ts`, `src/application/workerManager.ts`,
> `src/shared/storageHealth.ts`, `src/infrastructure/server/http*.ts`,
> `src/infrastructure/server/ws/snapshots.ts`, `monitoring/`

## Purpose

The monitoring surface reports queue state, lifetime counters, worker capacity,
latency, process/runtime identity, connections, persistence health and scheduled
backup outcomes. It feeds the Prometheus endpoint, HTTP health probes, TCP/CLI
diagnostics, the bundled Grafana dashboard and the bundled
Prometheus/Alertmanager rules.

## Queue metric journal

The Bun `Queue` API exposes durable, queue-scoped terminal metrics independently
of the process-wide HTTP/Prometheus surfaces:

```ts
queue.getMetrics('completed' | 'failed', (start = 0), (end = -1));
queue.trimEvents(maxLength);
```

`QueueTelemetryJournal` records every lifecycle event in a per-queue journal
and updates completed/failed counters only for terminal outcomes; a failed
attempt that will retry is not counted as a terminal failure. Metric data is a
continuous sequence of one-minute buckets in newest-first order, including the
current minute and zero-filled gaps. `start`/`end` are inclusive list indexes;
`end=-1` selects through the oldest retained bucket. `meta.count` is cumulative,
while top-level `count` is the pre-pagination bucket count.

With SQLite, `queue_events`, `queue_metrics_meta`, and
`queue_metric_buckets` survive restart. Defaults retain 10,000 journal entries
per queue and 20,160 minute points per queue/type. `trimEvents` removes old
journal entries without changing metrics; `obliterate` removes both. The TCP
`Metrics` queue form and `TrimEvents` command invoke the same manager methods as
embedded mode.

With PostgreSQL, `bunqueue_events`, `bunqueue_metric_buckets`, and
`bunqueue_metric_totals` provide the corresponding namespace-scoped durable
journal and terminal totals across brokers. Queue metric reads and event trims
use async durable manager methods; SQLite calls keep their existing synchronous
path. Process-local latency and uptime values remain per broker.

## Endpoint Contracts

| Endpoint                     | Authentication      | Healthy response              | Degraded response                                            |
| ---------------------------- | ------------------- | ----------------------------- | ------------------------------------------------------------ |
| `GET /healthz`, `/live`      | never               | `200 OK`, plain `OK`          | remains liveness-only                                        |
| `GET /ready`                 | never               | `200 { ok:true, ready:true }` | `503` for disk-full or any persistent storage error          |
| `GET /health`                | never               | `200`, detailed JSON          | `503`, `ok:false`, `status:"degraded"` for any storage error |
| `GET /prometheus`            | optional            | Prometheus text               | `503` if `METRICS_AUTH=true` but no token exists             |
| `GET /stats`                 | general bearer auth | queue/rate/memory JSON        | normal request error semantics                               |
| `GET /metrics`               | general bearer auth | small lifetime-counter JSON   | normal request error semantics                               |
| `GET /heapstats`, `POST /gc` | general bearer auth | debug JSON                    | normal request error semantics                               |

Health, liveness and readiness bypass rate limiting and authentication so an
orchestrator can always inspect the process. `/health` includes real TCP,
WebSocket and SSE connection counts, uptime, version, state counts and memory in
MiB. When SQLite reports a full disk it includes `storage.diskFull`, the
actionable SQLite error, and `since`. Any PostgreSQL runtime subsystem error also
makes both `/health` and `/ready` return 503 even though `diskFull` remains
false. Non-disk diagnostics are projected to `Internal server error` at every
client boundary; the detailed SQL/network message remains internal. `/healthz`
only establishes that the process/event loop can respond. The WebSocket/SSE
health snapshot uses the same degraded predicate, and Prometheus reports
`bunqueue_storage_degraded 1` independently of
`bunqueue_storage_disk_full`.

When `METRICS_AUTH=true`, `/prometheus` requires a bearer token from
`AUTH_TOKENS`. A true flag with an empty token set fails closed with 503 instead
of silently publishing operational data. With the flag false, `/prometheus`
remains public for scraper compatibility.

## Prometheus Metrics

`generatePrometheusMetrics` emits canonical names and types:

| Family                     | Metrics                                                                                                                                                                                                          |
| -------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| State gauges               | `bunqueue_jobs_waiting`, `bunqueue_jobs_prioritized`, `bunqueue_jobs_delayed`, `bunqueue_jobs_active`, `bunqueue_jobs_completed`, `bunqueue_jobs_dlq`                                                            |
| Lifetime counters          | `bunqueue_jobs_pushed_total`, `bunqueue_jobs_pulled_total`, `bunqueue_jobs_completed_total`, `bunqueue_jobs_failed_total`                                                                                        |
| Server/registration gauges | `bunqueue_uptime_seconds`, `bunqueue_cron_jobs_registered`, `bunqueue_workers_registered`, `bunqueue_workers_active`, `bunqueue_webhooks_registered`, `bunqueue_webhooks_enabled`                                |
| Worker capacity            | `bunqueue_worker_active_jobs`, `bunqueue_worker_concurrency_slots`                                                                                                                                               |
| Worker counters            | `bunqueue_workers_processed_total`, `bunqueue_workers_failed_total`                                                                                                                                              |
| Storage                    | `bunqueue_storage_degraded`, `bunqueue_storage_disk_full`, optional `bunqueue_sqlite_database_size_bytes`                                                                                                        |
| Process memory             | `bunqueue_process_heap_used_bytes`, `bunqueue_process_heap_total_bytes`, `bunqueue_process_resident_memory_bytes`                                                                                                |
| Standard process           | `process_cpu_seconds_total`, `process_start_time_seconds`, `process_resident_memory_bytes`, `process_heap_bytes`                                                                                                 |
| Identity/connections       | `bunqueue_build_info`, `bunqueue_connections{transport="tcp\|websocket\|sse"}`                                                                                                                                   |
| Cardinality                | `bunqueue_queue_metrics_exported`, `bunqueue_queue_metrics_omitted`                                                                                                                                              |
| Backup state               | `bunqueue_backup_enabled`, `bunqueue_backup_scheduler_running`, `bunqueue_backup_in_progress`, `bunqueue_backup_interval_seconds`, `bunqueue_backup_retention`, `bunqueue_backup_consecutive_failures`           |
| Backup outcomes            | `bunqueue_backup_attempts_total`, `bunqueue_backup_successes_total`, `bunqueue_backup_failures_total`, `bunqueue_backup_overlap_rejections_total`, last success/failure timestamps, duration and compressed size |

Registration values are gauges and deliberately do not use the `_total` suffix.
The capacity denominator for overload calculations is configured concurrency
slots across workers whose heartbeat is still active, not the number of worker
registrations.

Five labelled gauges provide per-queue state:

```text
bunqueue_queue_jobs_waiting{queue="emails"} 30
bunqueue_queue_jobs_prioritized{queue="emails"} 4
bunqueue_queue_jobs_delayed{queue="emails"} 2
bunqueue_queue_jobs_active{queue="emails"} 8
bunqueue_queue_jobs_dlq{queue="emails"} 0
```

Queue labels escape backslashes, quotes and newlines before exposition. The
complete text payload ends with a line-feed, as required by the Prometheus text
parser.

The queue label dimension is capped at 100 queue names by default. Selection is
the deterministic insertion-order prefix, and
`exported + omitted == registered queues` on every scrape. Set
`METRICS_MAX_QUEUES=0` to disable all per-queue series or configure
`telemetry.maxPrometheusQueues`; the unlabelled global totals remain exact.
This caps scrape cost and live series cardinality without placing tenant/job
identifiers in labels.

## Runtime and Backup Semantics

`bunqueue_build_info{version,bun_version} 1` gives deployment systems a bounded
identity series. Standard `process_*` collectors use seconds and bytes so
generic Prometheus dashboards work without bunqueue-specific adapters; the
existing namespaced memory gauges remain for compatibility. The connection
family has exactly three fixed label values.

Backup metrics are always present and zero-initialized. When scheduled backup is
enabled, the server injects the manager's current state into QueueManager
without global mutable telemetry. An attempt satisfies:

```text
attempts_total = successes_total + failures_total + (in_progress ? 1 : 0)
```

Overlap rejection is counted separately because it never starts an attempt.
Last-event metrics export Unix timestamps, not an incrementing “age”; PromQL
uses `time() - bunqueue_backup_last_success_timestamp_seconds`. A successful
attempt resets `consecutive_failures`. Disabling/stopping the scheduler does not
erase lifetime outcome counters.

## Latency Semantics

The runtime records observations internally in milliseconds, preserving the
existing TCP averages/percentiles API. Prometheus exposition scales bucket
bounds and sums to SI seconds:

- `bunqueue_push_duration_seconds`
- `bunqueue_pull_duration_seconds`
- `bunqueue_ack_duration_seconds`

Each is a histogram with `_bucket`, `_sum` and `_count`. Default bucket bounds
in seconds are `0.0001`, `0.0005`, `0.001`, `0.0025`, `0.005`, `0.01`,
`0.025`, `0.05`, `0.1`, `0.25`, `0.5`, `1`, `2.5`, `5`, `10`, and `+Inf`.

```promql
histogram_quantile(
  0.99,
  sum by (le) (rate(bunqueue_push_duration_seconds_bucket[5m]))
)
```

Histograms and cumulative counters are process-lifetime values and reset on
restart. Use `rate()` to produce time-windowed throughput or percentiles.

## Worker Capacity Invariants

`WorkerManager` exposes exact registry aggregates and time-filtered capacity:

- number of registered and currently active workers;
- total active jobs reported across workers;
- total configured concurrency slots across currently active workers;
- processed and failed totals.

Active-job and outcome totals use running counters; active-worker count and
capacity require one pass to exclude workers beyond `WORKER_TIMEOUT_MS`.
Register, re-register, heartbeat, increment, completion, failure, timeout
cleanup and unregister update the matching state exactly once. Model-based
tests generate those transitions, backup attempt outcomes and bounded queue
selection. After every command they check worker aggregates, backup counter
conservation, selection bounds and `exported + omitted` equality.
Processed/failed lifetime counters are not decremented when a local worker
unregisters; only its active-job contribution is removed. Monitoring responses
therefore retain completed worker history after disconnect in memory/SQLite.

In PostgreSQL mode, dashboard overviews, per-queue worker HTTP responses, and
periodic WebSocket/SSE stats snapshots obtain worker and cron rows through the
durable manager methods. They therefore represent the namespace-wide fleet, not
only registrations observed by the broker serving the request. Memory/SQLite
continues to use the original synchronous local registries.

## Bundled Stack

`docker compose --profile monitoring up -d` starts:

- `prom/prometheus:v3.13.1`, scraping bunqueue every five seconds;
- `prom/alertmanager:v0.33.1`, wired from Prometheus;
- `grafana/grafana:13.1.0`, with datasource UID `prometheus` and the dashboard
  provisioned from `monitoring/grafana/dashboards/bunqueue.json`.

The default Alertmanager receiver is local-only: alerts are visible in the UI
but no external address is contacted. Operators must add their own email,
PagerDuty, Slack, webhook or other receiver. The default Grafana password is a
local demo value and can be overridden with `GRAFANA_ADMIN_PASSWORD`. Compose
binds the monitoring UIs to loopback; intentional remote access must add
authentication/TLS and a unique Grafana secret. The bundled Grafana process
also disables suggested-plugin preinstallation, update checks and anonymous
usage reporting. This prevents startup-time background downloads and
unsolicited egress; install and pin any additional plugin explicitly in a
derived deployment.

The dashboard includes server/storage state, ready backlog (waiting plus
prioritized), DLQ, throughput, all global states, a multi-select queue variable,
per-queue breakdowns, p50/p95/p99 latency, a seconds-based heatmap, worker
capacity/utilization, memory, SQLite size, registrations, connections, backup
freshness/outcomes, omitted queue count and firing alert count.

## Alert Semantics

`monitoring/alert_rules.yml` contains thirteen rules:

- DLQ high and high failure ratio;
- ready backlog above 10,000, counting waiting **and prioritized** jobs;
- backlog with zero active workers;
- scrape target down;
- persistent storage degraded for one minute;
- enabled backup scheduler stopped, stale backups and recent failed attempts;
- per-queue metrics capped, making queue drill-down intentionally incomplete;
- completion throughput below 1/s only while new work arrives and backlog
  exists, so a legitimately idle broker does not alert;
- active jobs above 95% of configured concurrency slots;
- many active jobs with no completions.

The rules are starting points. Thresholds and evaluation windows must be tuned
to the deployment's traffic, batch size and SLOs. Alertmanager routing is
separate from rule evaluation. `monitoring/alert_rules.test.yml` is a promtool
rule-unit suite covering backup freshness both before and after the first
successful backup:

```bash
promtool test rules monitoring/alert_rules.test.yml
```

## Other Consumers and Caveats

`bunqueue metrics` returns Prometheus text through the TCP `Prometheus` command.
`bunqueue stats` renders the TCP `Stats` response; uptime is stored in
milliseconds internally and displayed as seconds. The HTTP `/metrics` endpoint
is JSON and is not a Prometheus scrape target.

The `/stats` in-process EMA rates are sampled state: multiple consumers can
affect their decay/reset cadence. Prefer Prometheus counter rates for durable
alert logic. Per-queue gauges carry user-controlled queue labels: never encode
job IDs, user IDs or tenant IDs into queue names, and keep the configured cap
low enough for the Prometheus retention and fleet size.

## Configuration

| Setting              | Default  | Effect                                                                             |
| -------------------- | -------- | ---------------------------------------------------------------------------------- |
| `METRICS_AUTH`       | `false`  | Require an `AUTH_TOKENS` bearer token on `/prometheus`; fail closed if none exists |
| `METRICS_MAX_QUEUES` | `100`    | Maximum queue names exported as labelled series; `0` disables per-queue metrics    |
| `STATS_INTERVAL_MS`  | `300000` | Periodic server log interval; does not control scraping                            |
| `LOG_LEVEL`          | `info`   | `debug`, `info`, `warn`, or `error`                                                |
| `LOG_FORMAT`         | `text`   | `json` enables structured log lines                                                |

## Related Documentation

- [HTTP API](./http-api.md)
- [Security](./security-tls-auth.md)
- [Worker management](./workers-management.md)
- [Configuration](./configuration.md)
- [Production readiness testing](./production-readiness-testing.md)
- [PostgreSQL 18.6 Multi-Broker Persistence](./postgres-multibroker.md)
