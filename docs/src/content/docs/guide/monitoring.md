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
# Start bunqueue + Prometheus + Grafana
docker compose --profile monitoring up -d
```

- **Grafana**: http://localhost:3000 (admin/bunqueue)
- **Prometheus**: http://localhost:9090

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

The endpoint is unauthenticated by default so scrapers work out of the box. Set `METRICS_AUTH=true` to require a bearer token from `AUTH_TOKENS`, then add `bearer_token: 'your-auth-token'` to the scrape config.

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
| `bunqueue_cron_jobs_total` | gauge | Registered cron jobs |
| `bunqueue_workers_total` | gauge | Registered workers |
| `bunqueue_workers_active` | gauge | Active workers |
| `bunqueue_workers_processed_total` | counter | Jobs processed by workers |
| `bunqueue_workers_failed_total` | counter | Jobs failed by workers |
| `bunqueue_webhooks_total` | gauge | Total webhooks |
| `bunqueue_webhooks_enabled` | gauge | Enabled webhooks |

### Per-queue metrics

Five gauges carry a `queue` label so you can filter and aggregate per queue: `bunqueue_queue_jobs_waiting`, `bunqueue_queue_jobs_prioritized`, `bunqueue_queue_jobs_delayed`, `bunqueue_queue_jobs_active`, and `bunqueue_queue_jobs_dlq`.

```
bunqueue_queue_jobs_waiting{queue="emails"} 30
bunqueue_queue_jobs_waiting{queue="payments"} 12
bunqueue_queue_jobs_active{queue="emails"} 5
```

### Latency histograms

Push, pull, and ack latency are exposed as Prometheus histograms: `bunqueue_push_duration_ms`, `bunqueue_pull_duration_ms`, and `bunqueue_ack_duration_ms`, each with `_bucket`, `_sum`, and `_count` series. Use them for p99 alerts:

```promql
histogram_quantile(0.99, rate(bunqueue_push_duration_ms_bucket[5m]))
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
  "version": "2.8.30",
  "queues": { "waiting": 42, "active": 8, "delayed": 3, "completed": 120, "dlq": 0 },
  "connections": { "tcp": 0, "ws": 1, "sse": 0 },
  "memory": { "heapUsed": 45, "heapTotal": 80, "rss": 210 }
}
```

When the disk fills up, `ok` flips to `false`, `status` becomes `"degraded"`, and a `storage` block appears with `diskFull: true`, the underlying error, and a `since` timestamp.

## Alert Rules

Pre-configured Prometheus alerts ship in `monitoring/alert_rules.yml`:

| Alert | Condition | Severity |
|-------|-----------|----------|
| `BunqueueDLQHigh` | DLQ > 100 for 5m | critical |
| `BunqueueHighFailureRate` | Failure > 5% for 5m | warning |
| `BunqueueQueueBacklog` | Waiting > 10k for 10m | warning |
| `BunqueueNoWorkers` | 0 workers + waiting jobs | critical |
| `BunqueueServerDown` | Server unreachable | critical |
| `BunqueueLowThroughput` | < 1 job/s for 10m | warning |
| `BunqueueWorkerOverload` | Utilization > 95% | warning |
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

The bundled dashboard (`monitoring/grafana/dashboards/bunqueue.json`) covers job counts, throughput, per-queue breakdowns, success and failure rates, latency percentiles and heatmaps, worker utilization, webhooks, cron, and visual alert indicators.

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
4. **Latency SLOs**: alert on histogram quantiles, e.g. `histogram_quantile(0.99, rate(bunqueue_push_duration_ms_bucket[5m])) > 50`
5. **Throughput**: the `/stats` endpoint exposes live `pushPerSec` / `pullPerSec` rates, see [Built-in Telemetry](/guide/telemetry/)

:::tip[Related Guides]
- [Built-in Telemetry](/guide/telemetry/) - What is measured and how to read it
- [Troubleshooting](/troubleshooting/) - Diagnose common issues
- [Production Deployment](/guide/deployment/) - Deploy with monitoring
:::
