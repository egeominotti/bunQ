#!/usr/bin/env bun
/**
 * Cloud Dashboard Simulation — HEAVY
 * 16 queues, 80+ workers, high throughput, delayed jobs, varied errors, flows
 * Runs for 24h generating heavy traffic for the dashboard.
 */

import { Queue, Worker, FlowProducer } from '../src/client';

const TCP_PORT = parseInt(process.env.TCP_PORT ?? '6789');
const DURATION_MS = parseInt(process.env.DURATION_MS ?? String(2 * 60 * 60 * 1000));
const PUSH_INTERVAL_MS = 300; // push every 300ms

const TCP_OPTS = { port: TCP_PORT, commandTimeout: 60000, pingInterval: 0 };

const QUEUES = [
  // High throughput queues
  { name: 'emails',           pushRate: 20, errorRate: 0.03, minMs: 30,   maxMs: 300,  concurrency: 8,  workers: 5 },
  { name: 'payments',         pushRate: 15, errorRate: 0.07, minMs: 80,   maxMs: 500,  concurrency: 6,  workers: 4 },
  { name: 'notifications',    pushRate: 30, errorRate: 0.02, minMs: 10,   maxMs: 100,  concurrency: 10, workers: 5 },
  { name: 'webhooks',         pushRate: 25, errorRate: 0.12, minMs: 20,   maxMs: 250,  concurrency: 8,  workers: 5 },
  { name: 'analytics',        pushRate: 20, errorRate: 0.01, minMs: 30,   maxMs: 300,  concurrency: 6,  workers: 4 },
  // Medium throughput queues
  { name: 'reports',          pushRate: 8,  errorRate: 0.05, minMs: 200,  maxMs: 1500, concurrency: 4,  workers: 3 },
  { name: 'image-resize',     pushRate: 12, errorRate: 0.06, minMs: 150,  maxMs: 800,  concurrency: 6,  workers: 4 },
  { name: 'exports',          pushRate: 6,  errorRate: 0.08, minMs: 400,  maxMs: 2000, concurrency: 4,  workers: 3 },
  { name: 'video-transcode',  pushRate: 4,  errorRate: 0.10, minMs: 500,  maxMs: 3000, concurrency: 3,  workers: 2 },
  { name: 'pdf-generation',   pushRate: 8,  errorRate: 0.04, minMs: 200,  maxMs: 1200, concurrency: 4,  workers: 3 },
  // Low throughput / heavy processing
  { name: 'ml-inference',     pushRate: 3,  errorRate: 0.15, minMs: 800,  maxMs: 5000, concurrency: 2,  workers: 2 },
  { name: 'data-pipeline',    pushRate: 5,  errorRate: 0.06, minMs: 300,  maxMs: 2000, concurrency: 3,  workers: 2 },
  { name: 'search-index',     pushRate: 10, errorRate: 0.03, minMs: 50,   maxMs: 400,  concurrency: 5,  workers: 3 },
  { name: 'audit-log',        pushRate: 15, errorRate: 0.01, minMs: 10,   maxMs: 80,   concurrency: 8,  workers: 4 },
  { name: 'cache-invalidate', pushRate: 18, errorRate: 0.02, minMs: 5,    maxMs: 50,   concurrency: 10, workers: 4 },
  { name: 'billing-sync',     pushRate: 6,  errorRate: 0.09, minMs: 100,  maxMs: 600,  concurrency: 4,  workers: 3 },
];

const ERRORS = [
  'Connection refused',
  'Timeout after 30000ms',
  'Rate limit exceeded (429)',
  'Internal server error (500)',
  'Invalid payload: missing required field "email"',
  'S3 upload failed: bucket not found',
  'SMTP relay error: authentication failed',
  'Database deadlock detected',
  'Out of memory',
  'DNS resolution failed',
  'TLS handshake timeout',
  'Circuit breaker open: payments-service',
  'Kafka partition leader unavailable',
  'Redis CLUSTERDOWN: cluster is down',
  'gRPC UNAVAILABLE: upstream connect error',
  'JWT token expired',
  'Stripe API: card_declined (insufficient_funds)',
  'Elasticsearch: index_not_found_exception',
  'Lambda invocation: Task timed out after 15s',
  'PostgreSQL: too many connections for role "app"',
];

process.on('uncaughtException', () => {});
process.on('unhandledRejection', () => {});

const queues: Queue[] = [];
const workers: Worker[] = [];
let totalPushed = 0;
let totalCompleted = 0;
let totalFailed = 0;
const startTime = Date.now();

async function main() {
  const totalRate = QUEUES.reduce((s, q) => s + q.pushRate, 0);
  const totalWorkers = QUEUES.reduce((s, q) => s + q.workers, 0);
  console.log(`=== Cloud Dashboard Simulation — HEAVY ===`);
  console.log(`${QUEUES.length} queues | ${totalWorkers} workers | ~${Math.round(totalRate * (1000 / PUSH_INTERVAL_MS))} jobs/s\n`);

  for (const q of QUEUES) {
    queues.push(new Queue(q.name, { connection: TCP_OPTS }));
  }

  // --- Cron jobs ---
  const CRONS = [
    { id: 'daily-cleanup',       queue: 'reports',       pattern: '0 3 * * *',    data: { task: 'cleanup', target: 'old-reports' } },
    { id: 'health-check',        queue: 'notifications', pattern: '*/2 * * * *',  data: { task: 'health-check', services: ['db', 'redis', 'api'] } },
    { id: 'hourly-digest',       queue: 'emails',        pattern: '0 * * * *',    data: { task: 'digest', type: 'hourly-summary' } },
    { id: 'invoice-generation',  queue: 'payments',      pattern: '0 9 * * 1-5',  data: { task: 'generate-invoices', currency: 'EUR' } },
    { id: 'image-cache-purge',   queue: 'image-resize',  pattern: '30 4 * * *',   data: { task: 'purge-cache', olderThanDays: 7 } },
    { id: 'webhook-retry-sweep', queue: 'webhooks',      pattern: '*/15 * * * *', data: { task: 'retry-failed-webhooks', maxRetries: 5 } },
    { id: 'analytics-rollup',    queue: 'analytics',     pattern: '0 */6 * * *',  data: { task: 'rollup', granularity: '1h' } },
    { id: 'export-stale-check',  queue: 'exports',       pattern: '*/30 * * * *', data: { task: 'check-stale-exports', maxAgeMins: 60 } },
    { id: 'nightly-backup',      queue: 'reports',          pattern: '0 2 * * *',    data: { task: 'backup', destination: 's3://bunqueue-backups' } },
    { id: 'payment-reconcile',   queue: 'payments',         pattern: '0 0 * * *',    data: { task: 'reconcile', provider: 'stripe' } },
    { id: 'ml-model-refresh',    queue: 'ml-inference',     pattern: '0 4 * * *',    data: { task: 'refresh-model', version: 'v3.2' } },
    { id: 'pipeline-compaction', queue: 'data-pipeline',    pattern: '0 3 * * 0',    data: { task: 'compact', retention: '90d' } },
    { id: 'search-reindex',      queue: 'search-index',     pattern: '0 5 * * 1',    data: { task: 'full-reindex', index: 'products' } },
    { id: 'audit-archive',       queue: 'audit-log',        pattern: '0 1 * * *',    data: { task: 'archive', olderThanDays: 30 } },
    { id: 'cache-warmup',        queue: 'cache-invalidate', pattern: '0 6 * * *',    data: { task: 'warmup', regions: ['us', 'eu', 'ap'] } },
    { id: 'billing-month-close', queue: 'billing-sync',     pattern: '0 0 1 * *',    data: { task: 'month-close', generateInvoices: true } },
    { id: 'video-cleanup',       queue: 'video-transcode',  pattern: '0 4 * * *',    data: { task: 'cleanup-temp', maxAgeMins: 120 } },
    { id: 'pdf-template-sync',   queue: 'pdf-generation',   pattern: '*/30 * * * *', data: { task: 'sync-templates', source: 'git' } },
  ];

  const cronQueue = queues[0]; // use first queue's connection for cron setup
  for (const cron of CRONS) {
    const targetQueue = queues.find((_, i) => QUEUES[i].name === cron.queue) ?? cronQueue;
    try {
      await targetQueue.upsertJobScheduler(cron.id, { pattern: cron.pattern }, { name: cron.id, data: cron.data });
      console.log(`  cron: ${cron.id} → ${cron.queue} (${cron.pattern})`);
    } catch (e) {
      console.error(`  cron FAIL: ${cron.id}`, (e as Error).message);
    }
  }
  console.log(`${CRONS.length} cron jobs registered\n`);

  for (const q of QUEUES) {
    for (let w = 0; w < q.workers; w++) {
      const worker = new Worker(
        q.name,
        async (job) => {
          const processingTime = q.minMs + Math.random() * (q.maxMs - q.minMs);
          await Bun.sleep(processingTime);

          if (Math.random() < q.errorRate) {
            throw new Error(ERRORS[Math.floor(Math.random() * ERRORS.length)]);
          }

          // Progress updates for longer jobs
          if (processingTime > 500) {
            try {
              await job.updateProgress(25);
              await Bun.sleep(processingTime * 0.05);
              await job.updateProgress(50);
              await Bun.sleep(processingTime * 0.05);
              await job.updateProgress(75);
              await Bun.sleep(processingTime * 0.05);
              await job.updateProgress(100);
            } catch {}
          }

          // Job logs for some jobs
          if (Math.random() < 0.15) {
            try {
              await job.log(`Processing ${job.name} — step 1/3 complete`);
              await job.log(`Processing ${job.name} — step 2/3 complete`);
              await job.log(`Processing ${job.name} — finished in ${Math.round(processingTime)}ms`);
            } catch {}
          }

          return { ok: true, ms: Math.round(processingTime), processed: Date.now() };
        },
        {
          concurrency: q.concurrency,
          connection: TCP_OPTS,
          heartbeatInterval: 10000,
          useLocks: true,
        },
      );

      worker.on('completed', () => { totalCompleted++; });
      worker.on('failed', () => { totalFailed++; });
      worker.on('error', () => {});
      workers.push(worker);
    }
  }

  // --- Flow producer ---
  const flowProducer = new FlowProducer({ connection: TCP_OPTS });
  let totalFlows = 0;

  // Flow templates — realistic multi-step pipelines
  const FLOW_TEMPLATES = [
    // E-commerce order processing (delayed children so flow stays visible across snapshots)
    (uid: string) => ({
      name: `process-order-${uid}`,
      queueName: 'payments',
      data: { type: 'order', orderId: uid, step: 'finalize' },
      opts: { delay: 30000 },
      children: [
        {
          name: `validate-payment-${uid}`,
          queueName: 'payments',
          data: { type: 'order', orderId: uid, step: 'validate-payment' },
          opts: { delay: 20000 },
          children: [
            {
              name: `charge-stripe-${uid}`,
              queueName: 'billing-sync',
              data: { type: 'order', orderId: uid, step: 'charge', provider: 'stripe' },
              opts: { delay: 10000 },
            },
          ],
        },
        {
          name: `check-inventory-${uid}`,
          queueName: 'data-pipeline',
          data: { type: 'order', orderId: uid, step: 'inventory-check', warehouse: 'us-east' },
          opts: { delay: 15000 },
        },
        {
          name: `send-confirmation-${uid}`,
          queueName: 'emails',
          data: { type: 'order', orderId: uid, step: 'confirmation-email' },
          opts: { delay: 12000 },
        },
      ],
    }),
    // User onboarding pipeline
    (uid: string) => ({
      name: `onboard-complete-${uid}`,
      queueName: 'notifications',
      data: { type: 'onboarding', userId: uid, step: 'complete' },
      opts: { delay: 25000 },
      children: [
        {
          name: `create-profile-${uid}`,
          queueName: 'data-pipeline',
          data: { type: 'onboarding', userId: uid, step: 'profile' },
          opts: { delay: 10000 },
        },
        {
          name: `send-welcome-${uid}`,
          queueName: 'emails',
          data: { type: 'onboarding', userId: uid, step: 'welcome-email' },
          opts: { delay: 8000 },
        },
        {
          name: `setup-defaults-${uid}`,
          queueName: 'analytics',
          data: { type: 'onboarding', userId: uid, step: 'defaults' },
          opts: { delay: 12000 },
        },
      ],
    }),
    // Report generation pipeline (deep tree, long delays)
    (uid: string) => ({
      name: `publish-report-${uid}`,
      queueName: 'exports',
      data: { type: 'report', reportId: uid, step: 'publish' },
      opts: { delay: 45000 },
      children: [
        {
          name: `generate-pdf-${uid}`,
          queueName: 'pdf-generation',
          data: { type: 'report', reportId: uid, step: 'pdf' },
          opts: { delay: 25000 },
          children: [
            {
              name: `fetch-data-${uid}`,
              queueName: 'data-pipeline',
              data: { type: 'report', reportId: uid, step: 'fetch-data' },
              opts: { delay: 10000 },
            },
            {
              name: `render-charts-${uid}`,
              queueName: 'image-resize',
              data: { type: 'report', reportId: uid, step: 'charts' },
              opts: { delay: 15000 },
            },
          ],
        },
        {
          name: `notify-stakeholders-${uid}`,
          queueName: 'notifications',
          data: { type: 'report', reportId: uid, step: 'notify' },
          opts: { delay: 8000 },
        },
      ],
    }),
    // ML inference pipeline
    (uid: string) => ({
      name: `ml-result-${uid}`,
      queueName: 'ml-inference',
      data: { type: 'ml', batchId: uid, step: 'aggregate' },
      opts: { delay: 35000 },
      children: [
        {
          name: `ml-preprocess-${uid}`,
          queueName: 'data-pipeline',
          data: { type: 'ml', batchId: uid, step: 'preprocess' },
          opts: { delay: 10000 },
        },
        {
          name: `ml-predict-${uid}`,
          queueName: 'ml-inference',
          data: { type: 'ml', batchId: uid, step: 'predict', model: 'v3.2' },
          opts: { delay: 20000 },
        },
        {
          name: `ml-cache-${uid}`,
          queueName: 'cache-invalidate',
          data: { type: 'ml', batchId: uid, step: 'cache-result' },
          opts: { delay: 5000 },
        },
      ],
    }),
    // Video processing pipeline
    (uid: string) => ({
      name: `video-deliver-${uid}`,
      queueName: 'webhooks',
      data: { type: 'video', videoId: uid, step: 'deliver-webhook' },
      opts: { delay: 40000 },
      children: [
        {
          name: `video-transcode-${uid}`,
          queueName: 'video-transcode',
          data: { type: 'video', videoId: uid, step: 'transcode', format: '1080p' },
          opts: { delay: 20000 },
          children: [
            {
              name: `video-thumbnail-${uid}`,
              queueName: 'image-resize',
              data: { type: 'video', videoId: uid, step: 'thumbnail' },
              opts: { delay: 8000 },
            },
          ],
        },
        {
          name: `video-index-${uid}`,
          queueName: 'search-index',
          data: { type: 'video', videoId: uid, step: 'index-metadata' },
          opts: { delay: 10000 },
        },
        {
          name: `video-audit-${uid}`,
          queueName: 'audit-log',
          data: { type: 'video', videoId: uid, step: 'audit-upload' },
          opts: { delay: 5000 },
        },
      ],
    }),
  ];

  // Push flows every 500ms — high rate so flows are always visible in snapshots
  const flowTimer = setInterval(async () => {
    const uid = Math.random().toString(36).slice(2, 10);
    const template = FLOW_TEMPLATES[totalFlows % FLOW_TEMPLATES.length];
    try {
      await flowProducer.add(template(uid));
      totalFlows++;
    } catch (e) {
      if (totalFlows < 5) console.error(`  flow FAIL:`, (e as Error).message);
    }
  }, 500);

  const pushTimer = setInterval(async () => {
    for (let i = 0; i < QUEUES.length; i++) {
      const q = QUEUES[i];
      const queue = queues[i];
      const batch: { name: string; data: Record<string, unknown>; opts?: Record<string, unknown> }[] = [];

      for (let j = 0; j < q.pushRate; j++) {
        const isDelayed = Math.random() < 0.12;
        const uid = Math.random().toString(36).slice(2, 10);
        const GROUPS = ['billing', 'onboarding', 'support', 'enterprise', 'free-tier', 'internal', 'partner', 'vip'];
        const TAG_POOL = ['urgent', 'batch', 'retry', 'scheduled', 'manual', 'api', 'webhook', 'cron', 'high-value', 'internal', 'bulk', 'realtime', 'low-priority', 'idempotent'];
        const REGIONS = ['us-east-1', 'us-west-2', 'eu-west-1', 'eu-central-1', 'ap-south-1', 'ap-northeast-1'];
        const tags = TAG_POOL.filter(() => Math.random() < 0.25);
        batch.push({
          name: `${q.name}-${Date.now()}-${j}`,
          data: {
            type: q.name,
            payload: {
              id: uid,
              email: `user-${uid}@example.com`,
              amount: Math.round(Math.random() * 10000) / 100,
              currency: ['USD', 'EUR', 'GBP', 'JPY'][Math.floor(Math.random() * 4)],
              items: Math.floor(Math.random() * 20) + 1,
            },
            ts: Date.now(),
            metadata: {
              source: ['sim', 'api', 'webhook', 'cron', 'manual'][Math.floor(Math.random() * 5)],
              region: REGIONS[Math.floor(Math.random() * REGIONS.length)],
              traceId: `trace-${uid}-${Date.now().toString(36)}`,
              userAgent: Math.random() < 0.3 ? 'BunQueue-SDK/2.6' : undefined,
            },
          },
          opts: {
            priority: Math.floor(Math.random() * 10),
            ...(isDelayed ? { delay: 1000 + Math.floor(Math.random() * 10000) } : {}),
            ...(Math.random() < 0.12 ? { jobId: `custom-${q.name}-${uid}` } : {}),
            ...(tags.length > 0 ? { tags } : {}),
            ...(Math.random() < 0.2 ? { groupId: GROUPS[Math.floor(Math.random() * GROUPS.length)] } : {}),
            ...(Math.random() < 0.1 ? { timeout: 15000 + Math.floor(Math.random() * 45000) } : {}),
            ...(Math.random() < 0.05 ? { ttl: 60000 + Math.floor(Math.random() * 300000) } : {}),
            ...(Math.random() < 0.03 ? { lifo: true } : {}),
            attempts: 1 + Math.floor(Math.random() * 5),
            backoff: 1000 + Math.floor(Math.random() * 4000),
          },
        });
      }

      try {
        await queue.addBulk(batch);
        totalPushed += batch.length;
      } catch {}
    }
  }, PUSH_INTERVAL_MS);

  const statusTimer = setInterval(() => {
    const elapsed = Math.round((Date.now() - startTime) / 1000);
    const rate = Math.round(totalPushed / elapsed);
    console.log(`[${elapsed}s] pushed=${totalPushed} (${rate}/s) completed=${totalCompleted} failed=${totalFailed} flows=${totalFlows}`);
  }, 60_000);

  await Bun.sleep(DURATION_MS);

  console.log('Draining...');
  clearInterval(pushTimer);
  clearInterval(flowTimer);
  await Bun.sleep(15_000);

  clearInterval(statusTimer);
  const elapsed = Math.round((Date.now() - startTime) / 1000);
  console.log(`[${elapsed}s] pushed=${totalPushed} (${Math.round(totalPushed / elapsed)}/s) completed=${totalCompleted} failed=${totalFailed} flows=${totalFlows}`);
  console.log(`DONE — pushed=${totalPushed} completed=${totalCompleted} failed=${totalFailed} flows=${totalFlows}`);

  try { flowProducer.close(); } catch {}
  for (const w of workers) try { await w.close(); } catch {}
  for (const q of queues) try { q.obliterate(); } catch {}
  await Bun.sleep(500);
  process.exit(0);
}

main().catch((err) => {
  console.error('Fatal:', err);
  process.exit(1);
});
