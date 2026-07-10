---
title: "Built-in Telemetry: Latency Histograms & Throughput"
description: Built-in telemetry for bunqueue. Prometheus latency histograms, EMA throughput rates, per-queue metrics, and structured JSON logs.
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/server-mode.png
---

<div class="bq-wrap bq-hero">
  <span class="bq-eyebrow">server · telemetry</span>
  <h1 class="bq-hero-h1 bq-bench-h1">See inside the <em>process.</em></h1>
  <p class="bq-hero-sub">bunqueue includes built-in telemetry for production observability: latency histograms, EMA throughput rates, per-queue metrics, and structured logs. All instrumentation is zero-allocation and adds less than 0.003% overhead, about 25ns per operation.</p>

  <div class="bq-proof">
    <span><b>~25ns</b> overhead per operation</span>
    <span><b>0</b> allocations on the hot path</span>
    <span><b>15</b> histogram buckets, 0.1ms to 10s</span>
    <span><b>8+</b> observability platforms supported</span>
  </div>
</div>

## Latency Histograms

Every push, pull, and ack operation is timed using `Bun.nanoseconds()` and recorded in Prometheus-compatible histograms.

### Metrics

| Metric | Description |
|--------|-------------|
| `bunqueue_push_duration_ms` | Time to push a job (includes lock acquisition and shard insertion) |
| `bunqueue_pull_duration_ms` | Time to pull a job from queue |
| `bunqueue_ack_duration_ms` | Time to acknowledge a completed job |

### Bucket Boundaries

Default boundaries (in milliseconds):

```
0.1, 0.5, 1, 2.5, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000
```

### Prometheus Output

Each histogram exposes three series:

```
# HELP bunqueue_push_duration_ms Push operation latency in milliseconds
# TYPE bunqueue_push_duration_ms histogram
bunqueue_push_duration_ms_bucket{le="0.1"} 120
bunqueue_push_duration_ms_bucket{le="0.5"} 89500
bunqueue_push_duration_ms_bucket{le="1"} 145000
bunqueue_push_duration_ms_bucket{le="2.5"} 149800
bunqueue_push_duration_ms_bucket{le="+Inf"} 150000
bunqueue_push_duration_ms_sum 12045.3
bunqueue_push_duration_ms_count 150000
```

### Percentiles

Use Prometheus `histogram_quantile()` for SLO tracking:

```promql
# p99 push latency
histogram_quantile(0.99, rate(bunqueue_push_duration_ms_bucket[5m]))

# p50 pull latency
histogram_quantile(0.50, rate(bunqueue_pull_duration_ms_bucket[5m]))
```

### Accessing latency

Percentiles come from the `/prometheus` histograms with `histogram_quantile()`, and averages can be derived from the `_sum` and `_count` series. Latency averages are also returned by the `Metrics` TCP command as `avgLatencyMs` and `avgProcessingMs`, accessible over the TCP protocol (the SDK). Note that the `bunqueue metrics` CLI prints Prometheus text, not these JSON fields, and the HTTP `/metrics` endpoint only exposes the `total*` counters. The internal `latencyTracker` is not a public package export, so consume it over these interfaces:

```bash
curl http://localhost:6790/prometheus    # bunqueue_push_duration_ms_bucket, _sum, _count
# avgLatencyMs / avgProcessingMs: Metrics TCP command via the SDK, not the CLI or an HTTP endpoint
```

## Throughput Tracking

Real-time throughput is tracked using Exponential Moving Average (EMA) with `alpha=0.3`. This provides smooth rate estimates with O(1) cost and zero allocations.

### Available Rates

| Rate | Description |
|------|-------------|
| `pushPerSec` | Jobs pushed per second |
| `pullPerSec` | Jobs pulled per second |
| `completePerSec` | Jobs completed (acked) per second |
| `failPerSec` | Jobs failed per second |

### Accessing Rates

All four rates are exposed via the `/stats` HTTP endpoint. They are not part of the `/prometheus` output; in Prometheus, derive rates from the `bunqueue_jobs_*_total` counters with `rate()`. The internal `throughputTracker` is not a public package export, so read the rates over HTTP:

```bash
curl http://localhost:6790/stats
```

```json
{
  "ok": true,
  "stats": {
    "waiting": 120,
    "active": 8,
    "pushPerSec": 12500,
    "pullPerSec": 12480,
    "completePerSec": 12460,
    "failPerSec": 2
  }
}
```

The real response also includes `delayed`, `dlq`, `completed`, `failed`, `uptime`, the `total*` counters, and a `memory` block; the example above is trimmed to the rate fields.

## Per-Queue Metrics

All queue metrics include a `queue` label for drill-down by queue name. This enables per-queue dashboards and alerts in Grafana.

### Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `bunqueue_queue_jobs_waiting{queue="..."}` | gauge | Waiting jobs in specific queue |
| `bunqueue_queue_jobs_prioritized{queue="..."}` | gauge | Prioritized jobs (priority > 0) in specific queue |
| `bunqueue_queue_jobs_delayed{queue="..."}` | gauge | Delayed jobs in specific queue |
| `bunqueue_queue_jobs_active{queue="..."}` | gauge | Active jobs in specific queue |
| `bunqueue_queue_jobs_dlq{queue="..."}` | gauge | DLQ entries for specific queue |

### Prometheus Output

```
bunqueue_queue_jobs_waiting{queue="emails"} 30
bunqueue_queue_jobs_waiting{queue="payments"} 12
bunqueue_queue_jobs_active{queue="emails"} 5
bunqueue_queue_jobs_delayed{queue="payments"} 100
bunqueue_queue_jobs_dlq{queue="emails"} 2
```

### Grafana Queries

```promql
# Waiting jobs for a specific queue
bunqueue_queue_jobs_waiting{queue="emails"}

# Total active jobs across all queues
sum(bunqueue_queue_jobs_active)

# Top 5 queues by backlog
topk(5, bunqueue_queue_jobs_waiting)
```

### Programmatic Access

```typescript
const perQueue = queueManager.getPerQueueStats();
// Map<string, { waiting, prioritized, delayed, active, dlq }>

const emailStats = perQueue.get('emails');
// { waiting: 30, prioritized: 0, delayed: 0, active: 5, dlq: 2 }
```

## Log Level Configuration

Configure log verbosity at startup with the `LOG_LEVEL` environment variable.

### Levels

| Level | Priority | Description |
|-------|----------|-------------|
| `debug` | 0 | Verbose debugging output |
| `info` | 1 | General operational messages (default) |
| `warn` | 2 | Warning conditions |
| `error` | 3 | Error conditions only |

Messages below the configured level are filtered with an early return (zero cost).

### Configuration

```bash
# Environment variable
LOG_LEVEL=debug bun run src/main.ts

# Combined with JSON output
LOG_LEVEL=warn LOG_FORMAT=json bun run src/main.ts
```

### Changing the level

Configure the log level with the `LOG_LEVEL` environment variable (and `LOG_FORMAT`) shown above. The internal `Logger` is not a public package export, so set logging via env vars rather than importing it at runtime.

## Integrations

bunqueue exposes a standard Prometheus `/prometheus` endpoint and structured JSON logs. This makes it compatible out-of-the-box with all major observability platforms.

:::note[OpenTelemetry]
bunqueue does not ship a native OpenTelemetry SDK, OTLP exporter, or distributed tracing. To feed an OpenTelemetry pipeline, scrape `/prometheus` with an OpenTelemetry Collector using the Prometheus receiver.
:::

### Metrics (Prometheus Scraping)

| Platform | How to Connect |
|----------|----------------|
| **Prometheus** | Native scrape on `/prometheus` |
| **Grafana Cloud** | Grafana Agent or Alloy scraping `/prometheus` |
| **Axiom** | Prometheus remote write or scrape from `/prometheus` |
| **Datadog** | Agent with `openmetrics` check on `/prometheus` |
| **New Relic** | Prometheus remote write or `nri-prometheus` |
| **Victoria Metrics** | Direct scrape, drop-in Prometheus replacement |
| **Chronosphere** | Prometheus remote write |
| **Splunk Observability** | OpenTelemetry Collector with Prometheus receiver |

### Log Aggregation (`LOG_FORMAT=json`)

| Platform | How to Connect |
|----------|----------------|
| **Axiom** | Axiom agent or Fluent Bit shipping stdout JSON |
| **Grafana Loki** | Promtail or Alloy collecting stdout JSON |
| **Elasticsearch (ELK)** | Filebeat shipping JSON logs |
| **Datadog Logs** | Datadog Agent collecting stdout |
| **Splunk** | Universal Forwarder or HTTP Event Collector |
| **CloudWatch** | CloudWatch Agent on stdout |

### Example: Axiom

```yaml
# prometheus.yml - Axiom as remote write target
remote_write:
  - url: https://api.axiom.co/v1/integrations/prometheus
    bearer_token: "xaat-your-axiom-token"
    remote_timeout: 30s
```

### Example: Datadog

```yaml
# datadog agent conf.d/openmetrics.d/conf.yaml
instances:
  - prometheus_url: http://localhost:6790/prometheus
    namespace: bunqueue
    metrics:
      - bunqueue_*
```

### Example: Grafana Cloud

```yaml
# alloy config
prometheus.scrape "bunqueue" {
  targets    = [{"__address__" = "localhost:6790"}]
  metrics_path = "/prometheus"
  forward_to = [prometheus.remote_write.grafana_cloud.receiver]
}
```

## Architecture

### Performance Characteristics

| Component | Overhead | Allocations |
|-----------|----------|-------------|
| Latency histogram | ~20ns per `observe()` | Zero (Float64Array) |
| Throughput counter | ~5ns per `increment()` | Zero (primitive counter) |
| Per-queue stats | O(queues) per scrape | Map allocation on scrape only |
| Log filtering | ~2ns per filtered message | Zero (early return) |

### How It Works

1. **Latency**: `Bun.nanoseconds()` captures start time before the operation. After completion, the delta is converted to milliseconds and recorded in a `Histogram` using binary search over fixed bucket boundaries.

2. **Throughput**: Each operation increments a counter. On rate calculation (triggered by `/stats` requests), an EMA smooths the raw count into a per-second rate.

3. **Per-queue**: On `/prometheus` scrape, the system iterates known queue names and aggregates stats from the designated shard (O(1) lookup via `shardIndex(queueName)`).

4. **Log filtering**: A priority lookup table maps level strings to integers. The `log()` method compares the message level against the configured minimum and returns immediately if below threshold.

## Alerting Examples

```yaml
# Alert on high p99 latency
- alert: BunqueueHighPushLatency
  expr: histogram_quantile(0.99, rate(bunqueue_push_duration_ms_bucket[5m])) > 50
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "Push p99 latency above 50ms"

# Alert on per-queue backlog
- alert: BunqueueQueueBacklog
  expr: bunqueue_queue_jobs_waiting{queue="payments"} > 1000
  for: 10m
  labels:
    severity: critical
  annotations:
    summary: "Payments queue backlog exceeds 1000 jobs"

# Alert on low throughput
- alert: BunqueueLowThroughput
  expr: rate(bunqueue_jobs_pushed_total[5m]) < 1
  for: 10m
  labels:
    severity: warning
  annotations:
    summary: "Push throughput dropped below 1 job/sec"
```
