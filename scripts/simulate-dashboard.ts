#!/usr/bin/env bun
/**
 * Dashboard Simulation — REALISTIC
 * Simulates a real SaaS product: variable rates, business-hour patterns,
 * realistic failures, delays, priorities, flows and crons.
 */

import { Queue, Worker, FlowProducer } from '../src/client';

const PORT = parseInt(process.env.TCP_PORT ?? '6789');
const conn  = { connection: { port: PORT } };

// ── Queues ────────────────────────────────────────────────────────────────────

const emailQ  = new Queue<any>('emails',       conn);
const payQ    = new Queue<any>('payments',      conn);
const notifQ  = new Queue<any>('notifications', conn);
const reportQ = new Queue<any>('reports',       conn);
const importQ = new Queue<any>('data-import',   conn);
const mediaQ  = new Queue<any>('media-process', conn);
const auditQ  = new Queue<any>('audit-log',     conn);

// ── Helpers ───────────────────────────────────────────────────────────────────

const rand  = (min: number, max: number) => Math.floor(Math.random() * (max - min + 1)) + min;
const sleep = (ms: number) => Bun.sleep(ms);
const pick  = <T>(arr: T[]): T => arr[Math.floor(Math.random() * arr.length)];
const maybe = (prob: number) => Math.random() < prob;

// Simulated "business hour" multiplier — peak 9-18, slow nights
function activityMultiplier(): number {
  const h = new Date().getHours();
  if (h >= 9 && h < 18) return 1.0;
  if (h >= 7 && h < 9)  return 0.5;
  if (h >= 18 && h < 22) return 0.4;
  return 0.15; // night
}

const users = [
  'alice@acme.com','bob@acme.com','carol@widget.io','dave@widget.io',
  'eve@startupxyz.com','frank@startupxyz.com','grace@bigcorp.com',
  'henry@bigcorp.com','iris@freelance.dev','jack@freelance.dev',
];
const userIds = users.map((_, i) => `usr_${String(i+1).padStart(4,'0')}`);
const amounts = [990, 1990, 2990, 4990, 9990, 19990, 49990, 99990];
const currencies = ['USD', 'EUR', 'GBP', 'CAD'];

let stats = { added: 0, completed: 0, failed: 0 };

// ── Workers ───────────────────────────────────────────────────────────────────

new Worker('emails', async (job) => {
  await job.updateProgress(25);
  await sleep(rand(80, 350));
  await job.updateProgress(75);
  await sleep(rand(40, 150));
  if (maybe(0.03)) throw new Error('SMTP 550: Mailbox unavailable');
  if (maybe(0.01)) throw new Error('Connection timeout to smtp.provider.io');
  stats.completed++;
  return { messageId: `<${Date.now()}@mail.acme.com>`, accepted: true };
}, { concurrency: 10, batchSize: 10, ...conn });

new Worker('payments', async (job) => {
  await job.updateProgress(10);
  await sleep(rand(200, 800));
  await job.updateProgress(50);
  await sleep(rand(100, 400));
  await job.updateProgress(85);
  await sleep(rand(50, 200));
  if (maybe(0.06)) throw new Error('Card declined: insufficient funds');
  if (maybe(0.02)) throw new Error('Gateway timeout after 30s');
  if (maybe(0.01)) throw new Error('3DS authentication failed');
  stats.completed++;
  const amount = job.data.amount as number;
  return { transactionId: `ch_${Date.now()}`, amount, status: 'succeeded' };
}, { concurrency: 8, batchSize: 8, ...conn });

new Worker('notifications', async (job) => {
  await sleep(rand(10, 80));
  if (maybe(0.02)) throw new Error('Device token no longer valid');
  if (maybe(0.005)) throw new Error('FCM rate limit exceeded');
  stats.completed++;
  return { delivered: true, platform: job.data.platform };
}, { concurrency: 20, batchSize: 20, ...conn });

new Worker('reports', async (job) => {
  await job.updateProgress(5);
  await sleep(rand(500, 2000));
  await job.updateProgress(35);
  await sleep(rand(400, 1500));
  await job.updateProgress(70);
  await sleep(rand(200, 800));
  await job.updateProgress(95);
  await sleep(rand(100, 300));
  if (maybe(0.02)) throw new Error('Query execution timeout (30s)');
  stats.completed++;
  return { rows: rand(50, 500000), sizeKB: rand(10, 5000) };
}, { concurrency: 3, batchSize: 3, ...conn });

new Worker('data-import', async (job) => {
  const rows = job.data.rows as number ?? 1000;
  await job.updateProgress(10);
  await sleep(rand(200, 600));
  await job.updateProgress(40);
  await sleep(rand(200, 600));
  await job.updateProgress(75);
  await sleep(rand(100, 400));
  await job.updateProgress(95);
  if (maybe(0.04)) throw new Error(`CSV parse error at row ${rand(1, rows)}: unexpected token`);
  if (maybe(0.01)) throw new Error('S3 read timeout');
  stats.completed++;
  return { imported: rows, skipped: rand(0, Math.floor(rows * 0.02)) };
}, { concurrency: 5, batchSize: 5, ...conn });

new Worker('media-process', async (job) => {
  await job.updateProgress(10);
  await sleep(rand(300, 1200));
  await job.updateProgress(45);
  await sleep(rand(300, 1000));
  await job.updateProgress(80);
  await sleep(rand(200, 600));
  if (maybe(0.05)) throw new Error('FFmpeg: unsupported codec');
  if (maybe(0.02)) throw new Error('Disk quota exceeded');
  stats.completed++;
  return { outputUrl: `https://cdn.acme.com/media/${Date.now()}.webp`, durationMs: rand(500, 8000) };
}, { concurrency: 4, batchSize: 4, ...conn });

new Worker('audit-log', async () => {
  await sleep(rand(5, 30));
  stats.completed++;
  return { logged: true };
}, { concurrency: 20, batchSize: 30, ...conn });

// ── Event listeners ───────────────────────────────────────────────────────────

for (const q of ['emails','payments','notifications','reports','data-import','media-process','audit-log']) {
  new Worker(q, async () => {}, { concurrency: 0, ...conn }).on('failed', () => { stats.failed++; });
}

// ── Producers ─────────────────────────────────────────────────────────────────

// ~50 audit job/sec — user activity è il volume dominante
async function produceUserActivity() {
  while (true) {
    const batch = Array.from({ length: rand(8, 15) }, () => ({
      name: 'event',
      data: {
        userId: pick(userIds),
        action: pick(['page_view','button_click','form_submit','file_upload','search','logout','api_call']),
        page: pick(['/dashboard','/settings','/billing','/reports','/upload','/profile','/api/v1/jobs']),
        ip: `${rand(1,254)}.${rand(0,255)}.${rand(0,255)}.${rand(1,254)}`,
        userAgent: pick(['Chrome/120','Firefox/121','Safari/17','Edge/120']),
      },
    }));
    await auditQ.addBulk(batch);
    stats.added += batch.length;
    await sleep(rand(200, 400));
  }
}

// ~20-25 notification/sec
async function produceNotifications() {
  while (true) {
    const batch = Array.from({ length: rand(4, 8) }, () => ({
      name: pick(['push','in-app','sms']),
      data: {
        userId: pick(userIds),
        title: pick(['New message','Order update','Payment received','Alert','Reminder']),
        body: `Notification for ${pick(userIds)}`,
        platform: pick(['ios','android','web']),
      },
      priority: rand(5, 15),
    }));
    await notifQ.addBulk(batch);
    stats.added += batch.length;
    await sleep(rand(200, 500));
  }
}

// ~8-10 email/sec
async function produceEmails() {
  while (true) {
    const batch = Array.from({ length: rand(3, 6) }, () => ({
      name: pick(['send','welcome','invoice','reset','digest','promo']),
      data: { to: pick(users), userId: pick(userIds), template: pick(['welcome','invoice','reset','promo','alert']) },
      priority: rand(5, 20),
    }));
    await emailQ.addBulk(batch);
    stats.added += batch.length;
    await sleep(rand(400, 800));
  }
}

// ~5 payment/sec
async function producePayments() {
  while (true) {
    const userId = pick(userIds);
    const amount = pick(amounts);
    const type   = maybe(0.82) ? 'charge' : maybe(0.7) ? 'refund' : 'payout';
    await payQ.add(type, {
      userId, amount,
      currency: pick(currencies),
      orderId: `ord_${Date.now()}`,
      method: pick(['card','sepa','paypal']),
    }, { priority: type === 'refund' ? 20 : 10 });
    stats.added++;
    await sleep(rand(150, 300));
  }
}

// ~1 signup/sec (new user onboarding)
async function produceSignups() {
  while (true) {
    const userId = pick(userIds);
    const email  = pick(users);
    await Promise.all([
      emailQ.add('welcome', { to: email, userId, template: 'welcome' }, { priority: 15 }),
      notifQ.add('push',    { userId, title: 'Welcome!', body: 'Your account is ready', platform: pick(['ios','android','web']) }, { priority: 10 }),
      auditQ.add('event',   { userId, action: 'signup', ip: `34.${rand(0,255)}.${rand(0,255)}.${rand(1,254)}` }),
    ]);
    stats.added += 3;
    await sleep(rand(800, 1500));
  }
}

// Media uploads — sporadici
async function produceMediaUploads() {
  while (true) {
    await mediaQ.add(pick(['transcode','resize','thumbnail']), {
      userId: pick(userIds),
      fileId: `file_${Date.now()}`,
      originalName: `upload_${rand(1000,9999)}.${pick(['mp4','mov','jpg','png','webp'])}`,
      sizeBytes: rand(50000, 50000000),
    }, { priority: rand(3, 12) });
    stats.added++;
    await sleep(rand(2000, 6000));
  }
}

// Report — pochi, su richiesta
async function produceReports() {
  while (true) {
    await reportQ.add('generate', {
      type: pick(['revenue','signups','churn','usage','billing','funnel']),
      range: pick(['today','yesterday','last_7d','last_30d','mtd']),
      requestedBy: pick(userIds),
      format: pick(['pdf','csv','json']),
    }, { priority: rand(2, 8) });
    stats.added++;
    await sleep(rand(5000, 15000));
  }
}

// Import — batch sporadici
async function produceImports() {
  while (true) {
    await importQ.add('csv', {
      source: pick(['crm','erp','shopify','stripe','hubspot']),
      filename: `sync_${new Date().toISOString().slice(0,10)}_${rand(1,9)}.csv`,
      rows: rand(500, 50000),
      triggeredBy: maybe(0.3) ? 'manual' : 'scheduler',
    }, { priority: rand(1, 6), delay: maybe(0.2) ? rand(5000, 30000) : 0 });
    stats.added++;
    await sleep(rand(8000, 25000));
  }
}

// ── Flows ─────────────────────────────────────────────────────────────────────

const flow = new FlowProducer(conn);

// Order fulfillment flow
async function produceOrderFlows() {
  while (true) {
    if (maybe(activityMultiplier() * 0.5)) {
      try {
        await flow.addBulkThen(
          [
            { name: 'charge',     queueName: 'payments',    data: { userId: pick(userIds), amount: pick(amounts), currency: pick(currencies) } },
            { name: 'inventory',  queueName: 'data-import', data: { sku: `SKU_${rand(1000,9999)}`, warehouse: pick(['EU','US','APAC']) } },
          ],
          { name: 'push', queueName: 'notifications', data: { title: 'Order confirmed', body: `Your order #${rand(10000,99999)} is confirmed` } }
        );
        stats.added += 3;
      } catch {}
    }
    await sleep(rand(4000, 12000));
  }
}

// Onboarding flow: verify email → setup profile → send guide
async function produceOnboardingFlows() {
  while (true) {
    if (maybe(activityMultiplier() * 0.2)) {
      try {
        await flow.addChain([
          { name: 'verify',  queueName: 'emails',       data: { to: pick(users), template: 'verify-email' } },
          { name: 'setup',   queueName: 'data-import',  data: { userId: pick(userIds), step: 'profile-init' } },
          { name: 'guide',   queueName: 'emails',       data: { to: pick(users), template: 'getting-started' } },
        ]);
        stats.added += 3;
      } catch {}
    }
    await sleep(rand(6000, 20000));
  }
}

// Payout batch: payments parent waits for multiple fraud-check children
async function producePayoutFlows() {
  while (true) {
    if (maybe(activityMultiplier() * 0.3)) {
      try {
        const recipients = Array.from({ length: rand(2, 4) }, () => ({
          name: 'fraud-check',
          queueName: 'audit-log',
          data: { userId: pick(userIds), amount: pick(amounts), type: 'payout-verify' },
        }));
        await flow.addBulkThen(
          recipients,
          {
            name: 'payout',
            queueName: 'payments',
            data: { batch: true, recipients: recipients.length, currency: pick(currencies) },
          }
        );
        stats.added += recipients.length + 1;
      } catch {}
    }
    await sleep(rand(5000, 15000));
  }
}

// ── Crons ─────────────────────────────────────────────────────────────────────

async function registerCrons() {
  try { await reportQ.upsertJobScheduler('hourly-stats',      { pattern: '0 * * * *'    }, { name: 'generate', data: { type: 'hourly-stats', auto: true } }); } catch {}
  try { await reportQ.upsertJobScheduler('daily-report',      { pattern: '0 7 * * *'    }, { name: 'generate', data: { type: 'daily-report', auto: true } }); } catch {}
  try { await reportQ.upsertJobScheduler('weekly-digest',     { pattern: '0 9 * * 1'    }, { name: 'generate', data: { type: 'weekly-digest', auto: true } }); } catch {}
  try { await emailQ.upsertJobScheduler('promo-newsletter',   { pattern: '0 10 * * 2,4' }, { name: 'send',     data: { template: 'newsletter', segment: 'active' } }); } catch {}
  try { await importQ.upsertJobScheduler('nightly-sync',      { pattern: '0 2 * * *'    }, { name: 'csv',      data: { source: 'crm', type: 'full-sync' } }); } catch {}
  try { await auditQ.upsertJobScheduler('audit-archive',      { pattern: '0 1 * * *'    }, { name: 'archive',  data: { olderThanDays: 90 } }); } catch {}
  try { await mediaQ.upsertJobScheduler('media-cleanup',      { pattern: '0 3 * * *'    }, { name: 'cleanup',  data: { olderThanDays: 30 } }); } catch {}
  try { await payQ.upsertJobScheduler('payout-batch',         { pattern: '0 16 * * 1-5' }, { name: 'payout',   data: { type: 'batch', currency: 'USD' } }); } catch {}
  console.log('Crons registered (8)');
}

// ── Stats ─────────────────────────────────────────────────────────────────────

console.log(`Connecting to bunqueue at port ${PORT}...`);
console.log('REALISTIC mode — 7 queues, business-hour patterns\n');

await registerCrons();

setInterval(() => {
  console.log(
    `[${new Date().toISOString().slice(11,19)}]` +
    `  added=${stats.added}` +
    `  completed=${stats.completed}` +
    `  failed=${stats.failed}` +
    `  pending=${stats.added - stats.completed - stats.failed}`
  );
}, 5000);

// Launch all producers
produceUserActivity();
produceNotifications();
produceEmails();
producePayments();
produceSignups();
produceMediaUploads();
produceReports();
produceImports();
produceOrderFlows();
produceOnboardingFlows();
producePayoutFlows();
