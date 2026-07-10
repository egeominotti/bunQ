---
title: "Bunqueue Monitoring with Prometheus & Grafana"
description: Set up Prometheus metrics, Grafana dashboards, and health checks for bunqueue. Monitor backlogs, failure rates, DLQ alerts, and workers.
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/server-mode.png
---

<div class="bq-wrap bq-hero">
  <span class="bq-eyebrow">server · monitoring</span>
  <h1 class="bq-hero-h1 bq-bench-h1">Prometheus, probes, <em>live events.</em></h1>
  <p class="bq-hero-sub">bunqueue exposes Prometheus-compatible metrics for production monitoring. This guide covers the built-in metrics endpoint, Kubernetes health probes, alert rules, and a ready-to-use Grafana dashboard.</p>

  <div class="bq-proof">
    <span><b>25+</b> metrics on /prometheus</span>
    <span><b>8</b> pre-configured alert rules</span>
    <span><b>3</b> health endpoints for Kubernetes</span>
    <span><b>1</b> docker compose command to start it all</span>
  </div>
</div>

:::tip[Using AI agents?]
AI agents connected via MCP can query stats, job counts, per-queue metrics, and Prometheus data via natural language using `bunqueue_get_stats`, `bunqueue_get_queue_stats`, `bunqueue_get_prometheus_metrics`, and `bunqueue_get_memory_stats` tools. See [MCP Server](/guide/mcp/).
:::

## Quick Start with Docker Compose

bunqueue includes a pre-configured monitoring stack:

```bash
# Start bunqueue + Prometheus + Grafana
docker compose --profile monitoring up -d
```

Access the dashboards:
- **Grafana**: http://localhost:3000 (admin/bunqueue)
- **Prometheus**: http://localhost:9090

## Prometheus Endpoint

bunqueue exposes metrics at `/prometheus` on the HTTP port (default 6790):

```bash
curl http://localhost:6790/prometheus
```

The endpoint is unauthenticated by default so scrapers work out of the box. Set `METRICS_AUTH=true` to require a bearer token from `AUTH_TOKENS` (see the `bearer_token` scrape config below).

### Available Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `bunqueue_jobs_waiting` | gauge | Jobs waiting in queue |
| `bunqueue_jobs_prioritized` | gauge | Prioritized jobs (priority > 0) |
| `bunqueue_jobs_delayed` | gauge | Delayed jobs |
| `bunqueue_jobs_active` | gauge | Jobs being processed |
| `bunqueue_jobs_completed` | gauge | Completed jobs in memory |
| `bunqueue_jobs_dlq` | gauge | Jobs in dead letter queue |
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

#### Per-Queue Metrics

All per-queue metrics include a `queue` label for filtering and aggregation:

| Metric | Type | Description |
|--------|------|-------------|
| `bunqueue_queue_jobs_waiting{queue="..."}` | gauge | Waiting jobs per queue |
| `bunqueue_queue_jobs_prioritized{queue="..."}` | gauge | Prioritized jobs per queue |
| `bunqueue_queue_jobs_delayed{queue="..."}` | gauge | Delayed jobs per queue |
| `bunqueue_queue_jobs_active{queue="..."}` | gauge | Active jobs per queue |
| `bunqueue_queue_jobs_dlq{queue="..."}` | gauge | DLQ jobs per queue |

#### Latency Histograms

Operation latency is tracked with Prometheus histograms (bucket boundaries in milliseconds):

| Metric | Type | Description |
|--------|------|-------------|
| `bunqueue_push_duration_ms` | histogram | Push operation latency |
| `bunqueue_pull_duration_ms` | histogram | Pull operation latency |
| `bunqueue_ack_duration_ms` | histogram | Ack operation latency |

Each histogram exposes `_bucket{le="..."}`, `_sum`, and `_count` series. Default bucket boundaries: `0.1, 0.5, 1, 2.5, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000` ms.

### Example Output

```
# HELP bunqueue_jobs_waiting Number of jobs waiting in queue
# TYPE bunqueue_jobs_waiting gauge
bunqueue_jobs_waiting 42

# HELP bunqueue_jobs_active Number of jobs being processed
# TYPE bunqueue_jobs_active gauge
bunqueue_jobs_active 8

# HELP bunqueue_jobs_pushed_total Total jobs pushed
# TYPE bunqueue_jobs_pushed_total counter
bunqueue_jobs_pushed_total 150432

# Per-queue breakdown
bunqueue_queue_jobs_waiting{queue="emails"} 30
bunqueue_queue_jobs_waiting{queue="payments"} 12
bunqueue_queue_jobs_active{queue="emails"} 5

# Latency histograms
# HELP bunqueue_push_duration_ms Push operation latency in milliseconds
# TYPE bunqueue_push_duration_ms histogram
bunqueue_push_duration_ms_bucket{le="0.1"} 120
bunqueue_push_duration_ms_bucket{le="0.5"} 145000
bunqueue_push_duration_ms_bucket{le="1"} 150000
bunqueue_push_duration_ms_bucket{le="+Inf"} 150432
bunqueue_push_duration_ms_sum 12045.3
bunqueue_push_duration_ms_count 150432
```

## Prometheus Configuration

Add bunqueue to your `prometheus.yml`:

```yaml
scrape_configs:
  - job_name: 'bunqueue'
    scrape_interval: 5s
    static_configs:
      - targets: ['localhost:6790']
    metrics_path: /prometheus
```

With authentication:

```yaml
scrape_configs:
  - job_name: 'bunqueue'
    scrape_interval: 5s
    static_configs:
      - targets: ['localhost:6790']
    metrics_path: /prometheus
    bearer_token: 'your-auth-token'
```

## Grafana Dashboard

The included dashboard provides:

### Overview Row
- Jobs Waiting, Delayed, Active, Completed, DLQ
- Active Workers, Cron Jobs, Uptime

### Throughput & Performance
- Job throughput (pushed/pulled/completed/failed per second)
- Queue depth over time (stacked area chart)
- Per-queue breakdown with label filtering

### Success & Failure Analysis
- Success rate gauge (with thresholds)
- Failure rate gauge (5-minute window)
- Completed vs Failed bar chart

### Latency & SLOs
- Push/Pull/Ack latency histograms (p50, p95, p99)
- Operation latency heatmap
- SLO compliance tracking via histogram quantiles

### Workers & Processing
- Worker count over time
- Worker throughput (processed/failed per second)
- Worker utilization gauge

### Webhooks & Cron
- Webhook status
- Cron job count
- Lifetime totals

### Alerts & Health
- Visual alert indicators for:
  - DLQ > 100 jobs
  - Failure rate > 5%
  - Queue backlog > 10,000
  - No active workers
  - Server health

## Alert Rules

Pre-configured Prometheus alerts in `monitoring/alert_rules.yml`:

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

### Example Alert Rule

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

## CLI Metrics

View metrics from the command line:

```bash
# Prometheus text format
bunqueue metrics

# Server stats (human-readable)
bunqueue stats

# Server stats as JSON
bunqueue stats --json
```

## Health Endpoints

bunqueue provides Kubernetes-compatible health endpoints (no auth, no rate limit):

```bash
# Detailed health (includes memory stats)
curl http://localhost:6790/health

# Kubernetes liveness probe (alias: /live), returns plain "OK"
curl http://localhost:6790/healthz

# Kubernetes readiness probe
curl http://localhost:6790/ready
```

`/health` reports `status: "healthy"` with per-state job counts, connection counts, memory usage, uptime, and version. When the disk fills up, `ok` flips to `false`, `status` becomes `"degraded"`, and a `storage` block appears with `diskFull: true`, the underlying error, and a `since` timestamp:

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

## Debug Endpoints

For troubleshooting:

```bash
# Heap statistics
curl http://localhost:6790/heapstats

# Force garbage collection
curl -X POST http://localhost:6790/gc
```

Both require a bearer token when `AUTH_TOKENS` is set:

```bash
curl -H "Authorization: Bearer $TOKEN" http://localhost:6790/heapstats
```

## File Structure

```
monitoring/
├── prometheus.yml              # Prometheus config
├── alert_rules.yml             # Alert definitions
└── grafana/
    ├── provisioning/
    │   ├── datasources/        # Auto-configure Prometheus
    │   └── dashboards/         # Auto-load dashboards
    └── dashboards/
        └── bunqueue.json       # Complete dashboard
```

## Custom Dashboards

Import the dashboard JSON directly:

1. Open Grafana → Dashboards → Import
2. Upload `monitoring/grafana/dashboards/bunqueue.json`
3. Select Prometheus datasource
4. Click Import

## Logging

Configure log level at startup via environment variable or [config file](/guide/configuration/):

```bash
LOG_LEVEL=debug bun run src/main.ts    # debug, info, warn, error
LOG_FORMAT=json bun run src/main.ts    # structured JSON output
```

Or in `bunqueue.config.ts`:

```typescript
defineConfig({
  logging: { level: 'debug', format: 'json' },
});
```

Log levels are filtered at runtime. Only messages at or above the configured level are emitted (debug < info < warn < error). Default is `info`.

## Best Practices

1. **Scrape interval**: Use 5-15 seconds for real-time visibility
2. **Retention**: Keep 15+ days for trend analysis
3. **Alerts**: Start with the included rules, tune thresholds for your workload
4. **Per-queue monitoring**: Use `{queue="..."}` labels to filter dashboards per queue
5. **Latency SLOs**: Set alerts on histogram quantiles (e.g., `histogram_quantile(0.99, bunqueue_push_duration_ms_bucket) > 50`)
6. **Throughput rates**: Monitor `pushPerSec` and `pullPerSec` from `/stats` for real-time throughput

:::tip[Related Guides]
- [Built-in Telemetry](/guide/telemetry/) - Latency histograms, throughput rates, and log configuration
- [Troubleshooting](/troubleshooting/) - Diagnose common issues
- [Production Deployment](/guide/deployment/) - Deploy with monitoring
:::
