---
title: "Built-in Telemetry: Latency & Throughput Out of the Box"
description: bunqueue measures its own latency and throughput with no setup. Prometheus histograms, live per-second rates, and structured logs.
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/guide/telemetry.png
---

<div class="bq-wrap bq-hero">
  <span class="bq-eyebrow">server · telemetry</span>
  <h1 class="bq-hero-h1 bq-bench-h1">Telemetry that sees inside the <em>process.</em></h1>
  <p class="bq-hero-sub">Every push, pull, and ack is timed and counted automatically. You get latency histograms, live throughput rates, and structured logs without writing any instrumentation code.</p>
</div>

This page explains what bunqueue measures and where to read each number. For the full metric list, scrape config, dashboards, and alert rules, see [Monitoring](/guide/monitoring/).

## Latency Histograms

Every push, pull, and ack operation is timed and recorded in a Prometheus histogram (a set of counters that tracks how many operations fell under each duration threshold, which lets you compute percentiles later):

| Metric | Description |
|--------|-------------|
| `bunqueue_push_duration_ms` | Time to push a job |
| `bunqueue_pull_duration_ms` | Time to pull a job from a queue |
| `bunqueue_ack_duration_ms` | Time to acknowledge a completed job |

Each exposes `_bucket`, `_sum`, and `_count` series on `/prometheus`, with bucket boundaries at `0.1, 0.5, 1, 2.5, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000` ms.

```bash
curl http://localhost:6790/prometheus
```

Compute percentiles in Prometheus with `histogram_quantile()`:

```promql
# p99 push latency
histogram_quantile(0.99, rate(bunqueue_push_duration_ms_bucket[5m]))

# p50 pull latency
histogram_quantile(0.50, rate(bunqueue_pull_duration_ms_bucket[5m]))
```

Averages can be derived from `_sum / _count`. Latency averages are also returned by the `Metrics` TCP command as `avgLatencyMs` and `avgProcessingMs` (over the TCP protocol / SDK; the `bunqueue metrics` CLI prints Prometheus text instead, and the HTTP `/metrics` endpoint only exposes the `total*` counters).

## Throughput Rates

The server tracks live per-second rates using an exponential moving average (a smoothing technique that favors recent activity, so the number reacts quickly without jitter):

| Rate | Description |
|------|-------------|
| `pushPerSec` | Jobs pushed per second |
| `pullPerSec` | Jobs pulled per second |
| `completePerSec` | Jobs completed (acked) per second |
| `failPerSec` | Jobs failed per second |

Read them from the `/stats` HTTP endpoint:

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

The real response also includes `delayed`, `dlq`, `completed`, `failed`, `uptime`, the `total*` counters, and a `memory` block; the example is trimmed to the rate fields.

These rates are not part of the `/prometheus` output. In Prometheus, derive rates from the counters instead:

```promql
rate(bunqueue_jobs_pushed_total[5m])
```

## Per-Queue Drill-Down

The per-queue gauges (`bunqueue_queue_jobs_waiting{queue="..."}` and friends, listed in [Monitoring](/guide/monitoring/#per-queue-metrics)) let you build per-queue dashboards and alerts:

```promql
bunqueue_queue_jobs_waiting{queue="emails"}   # backlog of one queue
sum(bunqueue_queue_jobs_active)                # active jobs across all queues
topk(5, bunqueue_queue_jobs_waiting)           # top 5 queues by backlog
```

Programmatically, in embedded mode:

```typescript
const perQueue = queueManager.getPerQueueStats();
// Map<string, { waiting, prioritized, delayed, active, dlq }>
```

## Log Levels

Set verbosity with `LOG_LEVEL` (`debug`, `info`, `warn`, `error`; default `info`) and switch to structured JSON with `LOG_FORMAT=json`:

```bash
LOG_LEVEL=warn LOG_FORMAT=json bun run src/main.ts
```

Messages below the configured level are dropped. The internal `Logger` is not a public package export, so configure logging via env vars or the [config file](/guide/configuration/).

## Feeding Other Platforms

bunqueue speaks two universal formats: Prometheus metrics on `/prometheus` and JSON logs on stdout. Anything that can scrape Prometheus or ship stdout can consume them.

- **Metrics**: Prometheus and Victoria Metrics scrape directly; Grafana Cloud via Alloy/Agent; Datadog via the `openmetrics` check; New Relic, Axiom, and Chronosphere via Prometheus remote write; Splunk Observability via an OpenTelemetry Collector with the Prometheus receiver.
- **Logs** (`LOG_FORMAT=json`): Loki via Promtail/Alloy, ELK via Filebeat, Datadog Agent, Splunk forwarder, CloudWatch Agent, or any shipper that reads stdout.

Example, Datadog agent config:

```yaml
# conf.d/openmetrics.d/conf.yaml
instances:
  - prometheus_url: http://localhost:6790/prometheus
    namespace: bunqueue
    metrics:
      - bunqueue_*
```

:::note[OpenTelemetry]
bunqueue does not ship a native OpenTelemetry SDK, OTLP exporter, or distributed tracing. To feed an OpenTelemetry pipeline, scrape `/prometheus` with an OpenTelemetry Collector using the Prometheus receiver.
:::

:::tip[Related Guides]
- [Monitoring](/guide/monitoring/) - Full metric reference, Grafana dashboard, alert rules
- [Environment Variables](/guide/env-vars/) - LOG_LEVEL, LOG_FORMAT, METRICS_AUTH
:::
