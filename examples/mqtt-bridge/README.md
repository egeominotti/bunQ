# MQTT → bunqueue bridge (IoT / Edge)

Turn MQTT sensor messages into persisted bunqueue jobs with retries, DLQ and
offline buffering. Designed for edge gateways (Raspberry Pi 4/5, any
ARM64/x64 Linux box running Bun).

```
device → MQTT broker (Mosquitto/EMQX) → bridge → bunqueue (SQLite) → Worker
```

## Why

- **Offline-first**: network down? Readings persist in SQLite on the gateway
  and are processed when connectivity returns.
- **Backpressure & retries**: a flaky backend gets retried with backoff;
  poison messages land in the DLQ instead of being lost.
- **No Redis**: a single Bun process, one SQLite file. Fits hardware where a
  Redis + BullMQ stack does not.

## Run

```bash
bun add mqtt
MQTT_URL=mqtt://localhost:1883 MQTT_TOPIC='sensors/#' bun examples/mqtt-bridge/index.ts
```

Test it by publishing a reading:

```bash
mosquitto_pub -t sensors/temp/room1 -m '{"temp":21.5}'
```

## Configuration

| Env var | Default | Purpose |
| --- | --- | --- |
| `MQTT_URL` | `mqtt://localhost:1883` | Broker URL |
| `MQTT_TOPIC` | `sensors/#` | Topic filter to subscribe |
| `BUNQUEUE_DATA_PATH` | `./edge-queue.db` | SQLite path for the local queue |

## Forwarding to a central bunqueue (with TLS)

To process on a central server instead of the gateway, replace the embedded
queue with a TCP connection — bunqueue supports native TLS:

```typescript
const queue = new Queue<SensorReading>('telemetry', {
  connection: {
    host: 'queue.example.com',
    port: 6789,
    tls: true, // or { caFile: './ca.pem' } for a private CA
    token: Bun.env.BQ_TOKEN,
  },
});
```

Server side:

```bash
bunqueue start --tls-cert ./cert.pem --tls-key ./key.pem --auth-tokens $TOKEN
```
