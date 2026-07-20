---
title: "Monitoring: Prometheus, Grafana & Health Checks"
description: Watch bunqueue in production. Prometheus metrics, a ready-made Grafana dashboard, alert rules, and Kubernetes health probes.
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/guide/monitoring.png
---

<div class="bq-wrap bq-hero">
  <span class="bq-eyebrow">server · monitoring</span>
  <h1 class="bq-hero-h1 bq-bench-h1">Prometheus, probes, <em>live events.</em></h1>
  <p class="bq-hero-sub">The bunqueue server exposes everything a production setup needs to watch it: a Prometheus metrics endpoint, health probes for Kubernetes, ready-made alert rules, and a Grafana dashboard.</p>
</div>

## Quick Start

The fastest way to see it all: bunqueue ships a pre-configured monitoring stack.

```bash
# Start bunqueue + Prometheus + Alertmanager + Grafana
docker compose --profile monitoring up -d
```

- **Grafana**: http://localhost:3000 (admin/bunqueue)
- **Prometheus**: http://localhost:9090
- **Alertmanager**: http://localhost:9093

The bundled versions are pinned for reproducible deployments. `admin/bunqueue`
is a local demo credential. The Compose profile binds all three monitoring UIs
to `127.0.0.1`; set `GRAFANA_ADMIN_PASSWORD` to a unique secret and put any
intentional remote access behind authentication/TLS. The default Alertmanager
receiver is local-only and sends no external notifications until you configure
one. The bundled Grafana service disables suggested-plugin preinstallation,
update checks, and anonymous usage reporting, so startup is deterministic and
does not make background catalog or telemetry requests. Add and pin any plugins
you need explicitly in your own deployment.

Already running Prometheus? Just point it at the metrics endpoint:

```yaml
# prometheus.yml
scrape_configs:
  - job_name: 'bunqueue'
    scrape_interval: 5s
    static_configs:
      - targets: ['localhost:6790']
    metrics_path: /prometheus
```

## Prometheus Endpoint

Metrics live at `/prometheus` on the HTTP port (default 6790):

```bash
curl http://localhost:6790/prometheus
```

The endpoint returns Prometheus text format 0.0.4 with an explicit content type
and trailing newline, which Prometheus 3 requires. It is unauthenticated by
default so scrapers work out of the box. Set `METRICS_AUTH=true` to require a
bearer token from `AUTH_TOKENS`, then add `bearer_token: 'your-auth-token'` to
the scrape config. If auth is required but `AUTH_TOKENS` is empty, the endpoint
fails closed with `503` rather than becoming public.

### Server-wide metrics

| Metric | Type | Description |
|--------|------|-------------|
| `bunqueue_jobs_waiting` | gauge | Jobs waiting in queue |
| `bunqueue_jobs_prioritized` | gauge | Prioritized jobs (priority > 0) |
| `bunqueue_jobs_delayed` | gauge | Delayed jobs |
| `bunqueue_jobs_active` | gauge | Jobs being processed |
| `bunqueue_jobs_completed` | gauge | Completed jobs in memory |
| `bunqueue_jobs_dlq` | gauge | Jobs in the dead letter queue |
| `bunqueue_jobs_pushed_total` | counter | Total jobs pushed |
| `bunqueue_jobs_pulled_total` | counter | Total jobs pulled |
| `bunqueue_jobs_completed_total` | counter | Total jobs completed |
| `bunqueue_jobs_failed_total` | counter | Total jobs failed |
| `bunqueue_uptime_seconds` | gauge | Server uptime |
| `bunqueue_cron_jobs_registered` | gauge | Registered cron jobs |
| `bunqueue_workers_registered` | gauge | Registered workers |
| `bunqueue_workers_active` | gauge | Active workers |
| `bunqueue_worker_active_jobs` | gauge | Jobs currently held by registered workers |
| `bunqueue_worker_concurrency_slots` | gauge | Configured worker concurrency capacity |
| `bunqueue_workers_processed_total` | counter | Jobs processed by workers |
| `bunqueue_workers_failed_total` | counter | Jobs failed by workers |
| `bunqueue_webhooks_registered` | gauge | Registered webhooks |
| `bunqueue_webhooks_enabled` | gauge | Enabled webhooks |
| `bunqueue_storage_degraded` | gauge | Persistent storage is degraded (0/1) |
| `bunqueue_storage_disk_full` | gauge | SQLite reported a full disk (0/1) |
| `bunqueue_sqlite_database_size_bytes` | gauge | SQLite main-file size (persistent mode only) |
| `bunqueue_process_heap_used_bytes` | gauge | Process heap currently used |
| `bunqueue_process_heap_total_bytes` | gauge | Process heap allocation |
| `bunqueue_process_resident_memory_bytes` | gauge | Resident set size |
| `bunqueue_build_info{version,bun_version}` | gauge | Server and Bun runtime identity |
| `bunqueue_connections{transport}` | gauge | Current TCP, WebSocket, and SSE connections |
| `process_cpu_seconds_total` | counter | Standard process CPU time collector |
| `process_start_time_seconds` | gauge | Standard process start timestamp |
| `process_resident_memory_bytes` | gauge | Standard process resident memory collector |
| `process_heap_bytes` | gauge | Standard process heap collector |

### Per-queue metrics

Five gauges carry a `queue` label so you can filter and aggregate per queue: `bunqueue_queue_jobs_waiting`, `bunqueue_queue_jobs_prioritized`, `bunqueue_queue_jobs_delayed`, `bunqueue_queue_jobs_active`, and `bunqueue_queue_jobs_dlq`.

```
bunqueue_queue_jobs_waiting{queue="emails"} 30
bunqueue_queue_jobs_waiting{queue="payments"} 12
bunqueue_queue_jobs_active{queue="emails"} 5
```

Per-queue output is capped at 100 queue names by default because every unique
label value creates five time series per server. Configure
`METRICS_MAX_QUEUES`, or `telemetry.maxPrometheusQueues` in the config file;
`0` disables labelled per-queue metrics while global totals remain available.
`bunqueue_queue_metrics_exported` and `bunqueue_queue_metrics_omitted` make a
capped view explicit. Never embed job, user, request, or tenant IDs in queue
names.

### Backup metrics

Scheduled S3 backup exports zero-initialized, label-free metrics:

| Metric | Type | Description |
|--------|------|-------------|
| `bunqueue_backup_enabled` | gauge | Scheduled backup is enabled |
| `bunqueue_backup_scheduler_running` | gauge | The scheduler timer is active |
| `bunqueue_backup_in_progress` | gauge | One backup attempt is active |
| `bunqueue_backup_interval_seconds` | gauge | Configured schedule interval |
| `bunqueue_backup_retention` | gauge | Configured retained backup count |
| `bunqueue_backup_attempts_total` | counter | Attempts actually started |
| `bunqueue_backup_successes_total` | counter | Successful attempts |
| `bunqueue_backup_failures_total` | counter | Failed attempts |
| `bunqueue_backup_overlap_rejections_total` | counter | Requests rejected while an attempt was active |
| `bunqueue_backup_consecutive_failures` | gauge | Failures since the last success |
| `bunqueue_backup_last_success_timestamp_seconds` | gauge | Unix timestamp of the last success, or 0 |
| `bunqueue_backup_last_failure_timestamp_seconds` | gauge | Unix timestamp of the last failure, or 0 |
| `bunqueue_backup_last_duration_seconds` | gauge | Duration of the last attempt |
| `bunqueue_backup_last_size_bytes` | gauge | Compressed size of the last successful backup |

Calculate freshness in PromQL from the timestamp:

```text
time() - bunqueue_backup_last_success_timestamp_seconds
```

### Latency histograms

Push, pull, and ack latency are exposed as Prometheus histograms: `bunqueue_push_duration_seconds`, `bunqueue_pull_duration_seconds`, and `bunqueue_ack_duration_seconds`, each with `_bucket`, `_sum`, and `_count` series. Use them for p99 alerts:

```text
histogram_quantile(0.99, sum by (le) (rate(bunqueue_push_duration_seconds_bucket[5m])))
```

See [Built-in Telemetry](/guide/telemetry/) for bucket boundaries and details.

## Health Endpoints

Kubernetes-compatible probes, no auth, no rate limit:

```bash
curl http://localhost:6790/health    # detailed health with memory stats
curl http://localhost:6790/healthz   # liveness probe (alias: /live), plain "OK"
curl http://localhost:6790/ready     # readiness probe
```

`/health` reports per-state job counts, connections, memory, uptime, and version:

```json
{
  "ok": true,
  "status": "healthy",
  "uptime": 3600,
  "version": "x.y.z",
  "queues": { "waiting": 42, "active": 8, "delayed": 3, "completed": 120, "dlq": 0 },
  "connections": { "tcp": 0, "ws": 1, "sse": 0 },
  "memory": { "heapUsed": 45, "heapTotal": 80, "rss": 210 }
}
```

When the disk fills up, `/health` returns `503`, `ok` flips to `false`, `status` becomes `"degraded"`, and a `storage` block appears with `diskFull: true`, the underlying error, and a `since` timestamp. `/ready` also returns `503`; `/healthz` stays a pure liveness signal. The connection block reports the real TCP, WebSocket, and SSE counts.

## Alert Rules

Pre-configured Prometheus alerts ship in `monitoring/alert_rules.yml`:

| Alert | Condition | Severity |
|-------|-----------|----------|
| `BunqueueDLQHigh` | DLQ > 100 for 5m | critical |
| `BunqueueHighFailureRate` | Failure > 5% for 5m | warning |
| `BunqueueQueueBacklog` | Waiting + prioritized > 10k for 10m | warning |
| `BunqueueNoWorkers` | 0 active workers + ready backlog | critical |
| `BunqueueServerDown` | Server unreachable | critical |
| `BunqueueStorageDegraded` | Persistent storage degraded for 1m | critical |
| `BunqueueBackupSchedulerDown` | Backup enabled but scheduler inactive | critical |
| `BunqueueBackupStale` | No success within two configured intervals | critical |
| `BunqueueBackupFailures` | Failed attempt in the last 15m | warning |
| `BunqueueQueueMetricsOmitted` | Per-queue cardinality cap reached | warning |
| `BunqueueLowThroughput` | < 1 completion/s while work arrives and backlog exists | warning |
| `BunqueueWorkerOverload` | Active jobs / concurrency slots > 95% | warning |
| `BunqueueJobsStuck` | Active jobs, no completions | warning |

Each rule looks like this; copy and tune the thresholds for your workload:

```yaml
- alert: BunqueueDLQHigh
  expr: bunqueue_jobs_dlq > 100
  for: 5m
  labels:
    severity: critical
  annotations:
    summary: "High number of jobs in DLQ"
    description: "{{ $value }} jobs are in the dead letter queue."
```

## Grafana Dashboard

The bundled dashboard (`monitoring/grafana/dashboards/bunqueue.json`) covers
server/storage status, job counts, throughput, a multi-select queue filter,
per-queue breakdowns, p50/p95/p99 latency and a seconds-based heatmap,
active-worker capacity/utilization, process memory, SQLite size, webhooks, cron,
connections, backup freshness/outcomes, omitted queue count, and firing-alert
indicators.

The docker compose stack loads it automatically. To import it into an existing Grafana: Dashboards → Import → upload the JSON → select your Prometheus datasource.

## CLI and Debug Access

```bash
bunqueue metrics        # Prometheus text format from the terminal
bunqueue stats          # human-readable server stats
bunqueue stats --json   # same, as JSON
```

For troubleshooting there are two debug endpoints (both require a bearer token when `AUTH_TOKENS` is set):

```bash
curl http://localhost:6790/heapstats   # heap object breakdown
curl -X POST http://localhost:6790/gc  # force garbage collection
```

## Logging

Configure log level and format at startup via environment variables or the [config file](/guide/configuration/):

```bash
LOG_LEVEL=debug bun run src/main.ts    # debug, info, warn, error (default: info)
LOG_FORMAT=json bun run src/main.ts    # structured JSON output for log shippers
```

## Best Practices

1. **Scrape interval**: 5-15 seconds gives near-real-time visibility
2. **Alerts**: start with the included rules, tune thresholds for your workload
3. **Per-queue dashboards**: filter with the `{queue="..."}` label
4. **Cardinality**: keep `METRICS_MAX_QUEUES` bounded and alert when queues are omitted
5. **Backup freshness**: page on a stopped scheduler or no success within two intervals
6. **Latency SLOs**: alert on histogram quantiles, e.g. `histogram_quantile(0.99, sum by (le) (rate(bunqueue_push_duration_seconds_bucket[5m]))) > 0.05`
7. **Throughput**: the `/stats` endpoint exposes live `pushPerSec` / `pullPerSec` rates, see [Built-in Telemetry](/guide/telemetry/)

:::tip[Related Guides]
- [Built-in Telemetry](/guide/telemetry/) - What is measured and how to read it
- [Troubleshooting](/troubleshooting/) - Diagnose common issues
- [Production Deployment](/guide/deployment/) - Deploy with monitoring
:::
