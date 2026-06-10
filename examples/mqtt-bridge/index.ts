/**
 * MQTT → bunqueue bridge
 *
 * IoT devices speak MQTT; bunqueue speaks its own TCP protocol. This bridge
 * sits in the middle: it subscribes to MQTT topics and turns every message
 * into a persisted bunqueue job — retries, DLQ, priorities and offline
 * buffering included.
 *
 *   device → MQTT broker (Mosquitto/EMQX/...) → this bridge → bunqueue → Worker
 *
 * Run on an edge gateway (Raspberry Pi 4/5, any ARM64/x64 Linux box):
 *
 *   bun add mqtt
 *   MQTT_URL=mqtt://localhost:1883 bun examples/mqtt-bridge/index.ts
 *
 * The queue runs embedded (SQLite on disk): if the network to your backend is
 * down, readings accumulate locally and are processed when it returns.
 */

import mqtt from 'mqtt';
import { Queue, Worker } from 'bunqueue/client';

const MQTT_URL = Bun.env.MQTT_URL ?? 'mqtt://localhost:1883';
const TOPIC = Bun.env.MQTT_TOPIC ?? 'sensors/#';
const DATA_PATH = Bun.env.BUNQUEUE_DATA_PATH ?? './edge-queue.db';

interface SensorReading {
  topic: string;
  payload: unknown;
  receivedAt: number;
}

// 1. Embedded queue — no server process, persisted to SQLite on the gateway
const queue = new Queue<SensorReading>('telemetry', {
  embedded: true,
  dataPath: DATA_PATH,
});

// 2. Bridge: every MQTT message becomes a job
const client = mqtt.connect(MQTT_URL);

client.on('connect', () => {
  console.log(`[bridge] connected to ${MQTT_URL}, subscribing to ${TOPIC}`);
  client.subscribe(TOPIC, (err) => {
    if (err) console.error('[bridge] subscribe failed:', err.message);
  });
});

client.on('message', (topic, payload) => {
  let parsed: unknown;
  try {
    parsed = JSON.parse(payload.toString());
  } catch {
    parsed = payload.toString(); // non-JSON payloads pass through as strings
  }
  queue
    .add('reading', { topic, payload: parsed, receivedAt: Date.now() }, { attempts: 5 })
    .catch((err: unknown) => {
      console.error('[bridge] enqueue failed:', err instanceof Error ? err.message : err);
    });
});

client.on('error', (err) => {
  console.error('[bridge] mqtt error:', err.message);
});

// 3. Worker: process readings (forward to your backend, aggregate, alert...)
const worker = new Worker<SensorReading>(
  'telemetry',
  async (job) => {
    // Replace with your logic: HTTP POST to backend, write to TSDB, etc.
    console.log(`[worker] ${job.data.topic}:`, job.data.payload);
    return { processed: true };
  },
  { embedded: true, dataPath: DATA_PATH, concurrency: 10 }
);

worker.on('failed', (job, err) => {
  console.error(`[worker] job ${job?.id} failed:`, err.message);
});

// Graceful shutdown
const shutdown = async () => {
  console.log('\n[bridge] shutting down...');
  await new Promise<void>((resolve) => client.end(false, {}, () => resolve()));
  await worker.close();
  await queue.close();
  process.exit(0);
};
process.on('SIGINT', () => void shutdown());
process.on('SIGTERM', () => void shutdown());

console.log(`[bridge] queue persisted at ${DATA_PATH}`);
