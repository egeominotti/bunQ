import type { SimulatorEngine } from './engine';

export interface Scenario {
  id: string;
  label: string;
  hint: string;
  apply: (engine: SimulatorEngine) => void;
}

const EMAIL_JOBS = ['send-welcome', 'send-receipt', 'send-digest', 'send-reset'];
const rand = (n: number) => Math.floor(Math.random() * n);

// One-click demos. Each narrates a single queue mechanic; none resets
// the world, so effects compose like they would on a real server.
export const SCENARIOS: Scenario[] = [
  {
    id: 'burst',
    label: 'Burst of 80',
    hint: 'Bulk-push 80 jobs and watch workers chew through the backlog.',
    apply: (engine) => {
      engine.emit('scenario', 'scenario: burst of 80 jobs');
      engine.pushBulk('emails', 80, (i) => ({
        name: EMAIL_JOBS[i % EMAIL_JOBS.length],
        priority: rand(10),
      }));
      ensureWorker(engine, 'emails', 4);
    },
  },
  {
    id: 'priority',
    label: 'Priorities win',
    hint: 'P9 jobs pushed last still jump the whole P1 backlog.',
    apply: (engine) => {
      engine.emit('scenario', 'scenario: priorities win');
      const q = engine.queueFor('renders');
      q.durationRange = [1100, 2000]; // slow worker makes the ordering visible
      engine.pushBulk('renders', 16, () => ({ name: 'draft-render', priority: 1 }));
      engine.pushBulk('renders', 5, () => ({ name: 'URGENT-render', priority: 9 }));
      ensureWorker(engine, 'renders', 1);
    },
  },
  {
    id: 'delayed',
    label: 'Scheduled jobs',
    hint: 'Delayed jobs sit in the delayed lane until their clock expires.',
    apply: (engine) => {
      engine.emit('scenario', 'scenario: scheduled jobs');
      engine.pushBulk('reminders', 10, (i) => ({
        name: 'send-reminder',
        delay: 2000 + i * 900,
      }));
      ensureWorker(engine, 'reminders', 2);
    },
  },
  {
    id: 'failures',
    label: 'Failure storm',
    hint: '55% failure rate: retries back off exponentially, then hit the DLQ.',
    apply: (engine) => {
      engine.emit('scenario', 'scenario: failure storm');
      engine.setFailureRate(0.55);
      engine.pushBulk('payments', 24, () => ({ name: 'charge-card', priority: rand(5) }));
      ensureWorker(engine, 'payments', 3);
    },
  },
  {
    id: 'ratelimit',
    label: 'Rate limited',
    hint: '8 worker slots, but the queue only releases 4 jobs per second.',
    apply: (engine) => {
      engine.emit('scenario', 'scenario: rate limited');
      engine.setRateLimit('api-calls', 4);
      engine.pushBulk('api-calls', 50, () => ({ name: 'sync-crm' }));
      ensureWorker(engine, 'api-calls', 8);
    },
  },
];

// Small ambient workload so the board is alive on page load.
export function warmup(engine: SimulatorEngine): void {
  if (engine.seeded) return;
  engine.seeded = true;
  engine.pushBulk('emails', 10, (i) => ({
    name: EMAIL_JOBS[i % EMAIL_JOBS.length],
    priority: rand(6),
    delay: i > 6 ? 3000 + i * 700 : 0,
  }));
  ensureWorker(engine, 'emails', 2);
}

function ensureWorker(engine: SimulatorEngine, queue: string, concurrency: number): void {
  const existing = engine.workers.find((w) => w.queue === queue && w.status === 'running');
  if (existing) {
    existing.concurrency = Math.max(existing.concurrency, concurrency);
    return;
  }
  engine.createWorker(queue, concurrency);
}
