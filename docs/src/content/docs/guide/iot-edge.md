---
title: "IoT & Edge: Buffer Locally, Forward to the Center"
description: Run bunqueue on edge gateways (Raspberry Pi, ARM64). Bridge MQTT sensors to a persisted job queue, buffer offline, and forward to a central server over TLS.
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/guide/iot-edge.png
---

<div class="bq-wrap bq-hero">
  <span class="bq-eyebrow">guide · iot &amp; edge</span>
  <h1 class="bq-hero-h1 bq-bench-h1">Queue at the edge, drain to the <em>center.</em></h1>
  <p class="bq-hero-sub">This page shows how to run bunqueue on an edge gateway: turn MQTT sensor messages into persisted jobs, keep them safe while the uplink is down, and forward them to a central server when it comes back.</p>
</div>

bunqueue fits where a Redis + BullMQ stack does not: a single Bun process with one SQLite file, running on the gateway next to your sensors. No containers, no broker for the queue itself.

:::note[Runtime]
Everything on this page uses the Bun `bunqueue` package: embedded mode and `queue.forward()` are Bun-only. The polyglot [SDKs](/guide/sdks/) are not affected, they can still produce and consume on the central server the gateway forwards to.
:::

## Is it the right fit?

| Scenario | Fit |
| --- | --- |
| Edge gateway (Raspberry Pi 4/5, Jetson, ARM64/x64 mini-PC) | Yes, embedded queue plus store-and-forward |
| Backend telemetry ingestion (absorb bursts, retry, DLQ) | Yes |
| Offline-first buffering over a flaky uplink | Yes, jobs persist to SQLite on the gateway |
| Replacing an MQTT broker | No, keep Mosquitto/EMQX and bridge into bunqueue |
| Running directly on microcontrollers (ESP32, 32-bit ARM) | No, Bun needs ARM64/x64; those devices publish MQTT to the gateway |

The pattern that works:

```
sensors ──MQTT──► broker (Mosquitto/EMQX) ──► bridge ──► bunqueue ──► Worker
                                                          (SQLite)      │
                                                                        ▼
                                                        backend / TSDB / alerts
```

Devices keep speaking MQTT, their native protocol. A small bridge script subscribes to topics and turns each message into a job. From there you get what a broker alone does not give you: retries with backoff, a dead letter queue (DLQ, a parking lot for messages that failed all retries), priorities, delayed jobs, and a durable buffer when the uplink is down.

## The MQTT bridge

A full runnable version lives in [`examples/mqtt-bridge/`](https://github.com/egeominotti/bunqueue/tree/main/examples/mqtt-bridge). The core is this:

```typescript
import mqtt from 'mqtt';
import { Queue, Worker } from 'bunqueue/client';

// Embedded mode: the queue runs inside this process, no server needed,
// persisted to a SQLite file on the gateway.
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

// Process locally, or POST to your backend
const worker = new Worker(
  'telemetry',
  async (job) => {
    // write to TSDB, trigger alerts, forward...
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

## Forwarding to a central server

The recommended hybrid: the embedded queue on the gateway is the offline buffer, and `queue.forward()` drains it to a central bunqueue server whenever the uplink is healthy.

```typescript
const local = new Queue('telemetry', { embedded: true, dataPath: './edge.db' });

const forwarder = local.forward({
  to: { host: 'queue.example.com', port: 6789, tls: true, token: Bun.env.BQ_TOKEN },
  queue: 'telemetry-ingest', // optional remote queue name (default: same as local)
  concurrency: 4,            // parallel forwards (default: 4)
});

forwarder.on('forwarded', ({ id, remoteId }) => console.log(`${id} -> ${remoteId}`));
forwarder.on('error', (err) => console.error('uplink:', err.message));

// later: await forwarder.close();
```

Central server, with native TLS (encrypted connections without a reverse proxy):

```bash
bunqueue start \
  --tls-cert /etc/bunqueue/cert.pem \
  --tls-key /etc/bunqueue/key.pem \
  --auth-tokens "$TOKEN" \
  --data-path /var/lib/bunqueue/queue.db
```

What `forward()` guarantees:

- **Nothing is lost while offline.** If the remote push fails, the job fails locally, retries with backoff, and after its attempts land in the local DLQ. When the uplink returns, `local.retryDlq()` re-enqueues everything buffered.
- **Re-forwards do not duplicate.** Each forwarded job carries a deterministic remote job id, `fwd:<local queue>:<local job id>`, and the server treats a repeated custom id as a no-op. So a retry or a crash mid-forward never creates the job twice on the server.
- **Priority is preserved.** Pass `durable: true` in the forward options to have the server write each forwarded job to disk immediately.

TLS certificate setup and client options (custom CA, self-signed) are in the [Native TLS guide](/guide/tls/).

## Offline buffering and durability

The embedded queue persists to SQLite in WAL mode (writes go to a fast append-only sidecar file, so a power cut cannot corrupt the database). By default writes are batched for up to 10 ms; for readings you cannot afford to lose even inside that window, mark them durable, meaning written to disk before `add()` returns:

```typescript
await queue.add('critical-alarm', data, { durable: true });
```

Throughput trade-off: buffered ~100k jobs/sec, durable ~10k jobs/sec. Both are far beyond typical sensor rates.

## Downsampling on the gateway

Aggregate locally before forwarding, for a cheaper uplink and less central load. This schedules a recurring job every 5 minutes:

```typescript
await queue.upsertJobScheduler('aggregate-5m', { every: 5 * 60 * 1000 }, {
  name: 'aggregate',
  data: { window: '5m' },
});
```

See [Cron & Scheduled Jobs](/guide/cron/) for cron expressions and timezones.

## Hardware notes

- **Runtime**: Bun runs on Linux/macOS ARM64 and x64. Raspberry Pi 4/5 with a 64-bit OS works. 32-bit boards (Pi Zero/2, ESP-class hardware) do not run Bun; those devices publish MQTT to the gateway instead.
- **Disk**: the SQLite file is the main consideration. Bound it with `removeOnComplete` (drop completed jobs), DLQ `maxAge`/`maxEntries`, and periodic `queue.clean(graceMs, limit)`.
- **Backups**: on gateways with object storage access, enable [S3 backup](/guide/backup/), or ship the SQLite file with your own sync.

## Gotchas

- **Forward dedup has a window.** The server remembers custom job ids in a bounded cache, and `removeOnComplete` on the remote evicts entries. A re-forward long after the original completed and was evicted can be accepted again. For strict exactly-once across long outages, keep `removeOnComplete: false` on the remote queue or dedupe downstream.
- **`forwarder.on('error')` is observability, not control flow.** Failed forwards are already handled by the local retry and DLQ path; the event just tells you the uplink is unhappy.
- **One process per SQLite file.** Run the bridge, worker, and forwarder in the same process (as above), or switch to a local bunqueue server if you need several processes on the gateway.

## See also

- [Native TLS](/guide/tls/), certificate setup and client options
- [`examples/mqtt-bridge/`](https://github.com/egeominotti/bunqueue/tree/main/examples/mqtt-bridge), the runnable bridge
- [Stall Detection](/guide/stall-detection/) and [DLQ](/guide/dlq/), what happens to stuck or poison readings
