---
title: "IoT & Edge: MQTT to Job Queue on a Gateway"
description: Run bunqueue on edge gateways (Raspberry Pi, ARM64). Bridge MQTT sensors to a persisted job queue with retries, DLQ and offline buffering, no Redis.
---

<div class="bq-wrap bq-hero">
  <span class="bq-eyebrow">guide · iot &amp; edge</span>
  <h1 class="bq-hero-h1 bq-bench-h1">Queue at the edge, drain to the <em>center.</em></h1>
  <p class="bq-hero-sub">bunqueue fits where a Redis + BullMQ stack does not: a single Bun process with one SQLite file, running on an edge gateway next to your sensors. This guide covers the recommended IoT architecture, the MQTT bridge pattern, offline buffering, and secure forwarding to a central server with native TLS.</p>

  <div class="bq-proof">
    <span><b>~30</b> lines for the MQTT bridge</span>
    <span><b>SQLite WAL</b> offline buffering</span>
    <span><b>TLS</b> uplink to the central server</span>
    <span><b>0</b> Redis containers</span>
  </div>
</div>

## Where bunqueue fits (and where it doesn't)

| Scenario | Fit |
| --- | --- |
| Edge gateway (Raspberry Pi 4/5, Jetson, ARM64/x64 mini-PC) | ✅ embedded queue, store-and-forward |
| Backend telemetry ingestion (absorb bursts, retry, DLQ) | ✅ |
| Offline-first buffering (flaky uplink) | ✅ SQLite WAL persistence |
| Replacement for an MQTT broker | ❌ use Mosquitto/EMQX, bridge into bunqueue |
| Directly on microcontrollers (ESP32, ARMv7 32-bit) | ❌ Bun requires ARM64/x64, devices talk to the gateway |

The pattern that works:

```
sensors ──MQTT──► broker (Mosquitto/EMQX) ──► bridge ──► bunqueue ──► Worker
                                                          (SQLite)      │
                                                                        ▼
                                                        backend / TSDB / alerts
```

Devices keep speaking MQTT (their native protocol). The bridge, a ~30 line
Bun script, subscribes to topics and turns each message into a persisted job.
From there you get everything a queue gives you that a broker does not:
retries with backoff, dead letter queue, priorities, delayed jobs, cron
aggregations, and a durable buffer when the uplink is down.

## The MQTT bridge

Full runnable version in
[`examples/mqtt-bridge/`](https://github.com/egeominotti/bunqueue/tree/main/examples/mqtt-bridge).
The core is this:

```typescript
import mqtt from 'mqtt';
import { Queue, Worker } from 'bunqueue/client';

// Embedded queue: no server process, persisted to SQLite on the gateway
const queue = new Queue('telemetry', {
  embedded: true,
  dataPath: './edge-queue.db',
});

const client = mqtt.connect('mqtt://localhost:1883');
client.on('connect', () => client.subscribe('sensors/#'));

client.on('message', (topic, payload) => {
  void queue.add(
    'reading',
    { topic, payload: JSON.parse(payload.toString()), receivedAt: Date.now() },
    { attempts: 5 }
  );
});

// Process locally, or forward to your backend
const worker = new Worker(
  'telemetry',
  async (job) => {
    // POST to backend, write to TSDB, trigger alerts...
    return { processed: true };
  },
  { embedded: true, dataPath: './edge-queue.db', concurrency: 10 }
);
```

Run it:

```bash
bun add mqtt
MQTT_URL=mqtt://localhost:1883 bun examples/mqtt-bridge/index.ts

# publish a test reading
mosquitto_pub -t sensors/temp/room1 -m '{"temp":21.5}'
```

## Offline buffering

The embedded queue writes to SQLite (WAL mode) on the gateway. If the worker's
forwarding target is unreachable, jobs fail and are retried with exponential
backoff; after `attempts` exhausted they land in the DLQ instead of being
lost. When connectivity returns, `queue.retryDlq()` re-enqueues them.

For readings you cannot afford to lose even across a power cut in the 10ms
write-buffer window, use durable mode per job:

```typescript
await queue.add('critical-alarm', data, { durable: true }); // immediate fsync
```

Throughput trade-off: buffered ~100k jobs/sec, durable ~10k jobs/sec, both
far beyond typical sensor rates.

## Forwarding to a central server (TLS)

To process centrally instead of on the gateway, point the queue at a remote
bunqueue over TCP with [native TLS](/guide/tls/):

```typescript
const queue = new Queue('telemetry', {
  connection: {
    host: 'queue.example.com',
    port: 6789,
    tls: true, // or { caFile: './ca.pem' } for a private CA
    token: Bun.env.BQ_TOKEN,
  },
});
```

Central server:

```bash
bunqueue start \
  --tls-cert /etc/bunqueue/cert.pem \
  --tls-key /etc/bunqueue/key.pem \
  --auth-tokens "$TOKEN" \
  --data-path /var/lib/bunqueue/queue.db
```

### Built-in store-and-forward: `queue.forward()`

The recommended hybrid, embedded queue on the gateway as the offline buffer,
drained to the central server when the uplink is healthy, is a one-liner:

```typescript
const local = new Queue('telemetry', { embedded: true, dataPath: './edge.db' });

const forwarder = local.forward({
  to: { host: 'queue.example.com', port: 6789, tls: true, token: Bun.env.BQ_TOKEN },
  queue: 'telemetry-ingest', // optional remote name (default: same)
  concurrency: 4,
});

forwarder.on('forwarded', ({ id, remoteId }) => console.log(`→ ${id} as ${remoteId}`));
forwarder.on('error', (err) => console.error('uplink:', err.message));

// later: await forwarder.close();
```

Semantics:

- **Nothing lost offline**: a remote failure fails the job locally → local
  retry with backoff → local DLQ after `attempts`. When the uplink returns,
  `local.retryDlq()` re-enqueues buffered readings.
- **Dedup on re-forward**: each forwarded job carries the deterministic
  remote jobId `fwd:<queue>:<localId>`; the server dedupes custom jobIds, so
  a re-forward after a crash or retry is idempotent **within the server's
  retention window** (custom-id map is a bounded LRU, and `removeOnComplete`
  on the remote evicts the entry). For strict exactly-once across long
  outages, keep `removeOnComplete: false` remotely or dedupe downstream.
- Priority is preserved; pass `durable: true` to fsync each job server-side.

## Cron aggregations on the gateway

Downsample locally before forwarding, cheaper uplink, less central load:

```typescript
await queue.upsertJobScheduler('aggregate-5m', { every: 5 * 60 * 1000 }, {
  name: 'aggregate',
  data: { window: '5m' },
});
```

## Hardware notes

- **Runtime**: Bun runs on Linux/macOS ARM64 and x64. Raspberry Pi 4/5 with a
  64-bit OS works; 32-bit ARMv7 boards (Pi Zero/2, most ESP-class hardware) do
  not, those devices publish MQTT to the gateway instead.
- **Footprint**: single process, no Redis container. SQLite file size is the
  main disk consideration, bound it with `removeOnComplete`, DLQ `maxAge`/
  `maxEntries`, and periodic `queue.clean(graceMs, limit)`.
- **Reliability**: enable [S3 backup](/guide/backup/) on gateways with object
  storage access, or ship the SQLite file with your own sync.

## See also

- [Native TLS](/guide/tls/), cert setup, client options, self-signed certs
- [`examples/mqtt-bridge/`](https://github.com/egeominotti/bunqueue/tree/main/examples/mqtt-bridge), runnable bridge
- [Stall Detection](/guide/stall-detection/) and [DLQ](/guide/dlq/), what happens to stuck/poison readings
