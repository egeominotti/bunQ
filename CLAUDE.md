# bunqueue

**MANDATORY — SKEPTIC AGENT**: before every `git commit` and every `git push`,
invoke the `skeptic` agent and wait for its verdict. It must read the complete
staged diff, identify at least three potential failure points per changed file,
and verify race conditions, error handling, idempotency, type safety and test
coverage. `FAIL` blocks commit and push; `CONDITIONAL` requires the user's
explicit confirmation. The project profile is `.claude/agents/skeptic.md`.

**ENGLISH ONLY**: keep all repository content in English, including source
comments, tests, technical documentation, user documentation and agent
configuration.

"When I report a bug, don't start by trying to fix it. Instead, start by writing a test that reproduces the bug. Then, have subagents try to fix the bug and prove it with a passing test."

**MANDATORY: After ANY code modification, run the isolated validation before committing:**

```bash
bun run test:sandbox
```

It builds the current worktree once and runs ALL THREE required suites concurrently in separate disposable containers:

```bash
bun test                                # Unit tests (~5000 tests)
bun scripts/tcp/run-all-tests.ts        # TCP integration tests (~50 suites)
bun scripts/embedded/run-all-tests.ts   # Embedded integration tests (~35 suites)
```

Never commit without all three passing. No exceptions.

**MANDATORY for core queue semantics: run the real-broker model before the
sandbox:**

```bash
bun run test:model
```

Run it after every change to lifecycle transitions, SQLite/write buffering,
startup or stall recovery, priority/FIFO/LIFO/delay scheduling, FIFO groups,
dependencies, dedup/custom IDs, leases, limits, TTL, counters, temporal indexes,
or `jobIndex`. The `fast-check` model executes generated command histories
against a fresh TCP broker and SQLite database, including actual `SIGKILL`
restarts. It checks conservation, no loss/resurrection/duplicate active lease,
retry and stall bounds, legal transitions, ordering, resource limits,
counter/index coherence, idempotent recovery, and DLQ exactly-once.

On failure, retain the seed, minimized command history, and replay path. If the
engine is wrong, add a deterministic `test/repro-model-*.test.ts` that fails
before changing runtime code; if the specification is wrong, correct the model
with evidence. Never weaken an invariant to make a campaign green. Deeper replay:

```bash
BUNQUEUE_MODEL_RUNS=500 BUNQUEUE_MODEL_COMMANDS=150 \
BUNQUEUE_MODEL_SEED=-1959189325 bun run test:model
```

This targeted command does not replace `bun run test:sandbox`; plain `bun test`
inside the sandbox runs the model again.

**MANDATORY: After ANY modification under `sdk/`, run before handoff (and run
again before committing if the SDK changed afterward):**

```bash
bun run test:sandbox:sdk
```

It runs every official external SDK's native tests and shared protocol
conformance checks in isolated disposable containers. SDK changes require both
full gates (`test:sandbox` and `test:sandbox:sdk`); targeted native tests do not
replace either one. Review `artifacts/test-sandbox-sdk/<timestamp>/summary.md`
and its JSON/NDJSON artifacts before handoff.

### Test isolation rules

- Run repository tests in isolated OrbStack Machines by default. A macOS-host test is diagnostic only and is not final validation evidence.
- Use a fresh disposable Ubuntu 24.04 Machine on the Mac's native architecture (4 CPUs, request 16 GiB memory, at least 32 GiB disk) as the canonical local Linux gate. On Apple Silicon this is `arm64`; native `amd64` is covered independently by GitHub Actions `ubuntu-latest`. For every release, repeat the native product suites in a fresh Debian 13 Machine on the same host-native architecture. Record the effective cgroup limits because OrbStack may clamp requested resources.
- A translated `amd64` Machine on Apple Silicon is diagnostic only. Repeat any result on a native-architecture Machine before using it as release evidence; process-heavy Bun tests may behave differently under Rosetta.
- Create both Machines with `--isolated --isolate-network`. Never use `--mount` or `--forward-ssh-agent`, and never expose the repository, home directory, Docker socket, SSH agent, credentials, tokens, real `.env` files, or ignored files. A tracked placeholder-only template such as `.env.example` is allowed only after review. Copy an explicit sanitized snapshot over OrbStack's built-in SSH transport, including all intended worktree changes but excluding `.git`, dependencies, artifacts, SQLite files, generated output, and secrets.
- Pin the same Bun version as CI and use the frozen lockfile. Directly inside each Machine run `bun test`, the TCP runner, the embedded runner, typecheck, Oxlint, and Oxfmt. Run `git diff --check` on the host before snapshot transfer because `.git` is deliberately excluded. Record the OS, architecture, kernel, Bun version/revision, commands, exit codes, durations, and exact totals before deleting the Machine.
- OrbStack Machines share OrbStack's underlying Linux kernel. Treat them as strong filesystem/process/network isolation, not as a separate-kernel or physical security boundary.
- The Machine gates supplement `bun run test:sandbox` and `bun run test:sandbox:sdk`; they do not replace them. Benchmarks remain native macOS-only and must never be published from a Machine or container.
- The three top-level suite containers run in parallel by default; use `BUNQUEUE_TEST_SEQUENTIAL=1` only to diagnose resource contention.
- Isolation covers processes, writable filesystems, `/tmp`, network namespaces, ports, databases, and environment. It is not physical isolation: containers share the OrbStack VM kernel plus host CPU, memory, and disk scheduling. Reproduce parallel-only failures sequentially before classifying them as application bugs.
- Use `artifacts/test-sandbox/<timestamp>/` for complete, untruncated suite logs; failed containers remain available for inspection.
- Use `artifacts/test-sandbox-sdk/<timestamp>/` for the equivalent SDK logs,
  telemetry and retained failed containers.
- Every sandbox run must also preserve per-suite resource samples (`*.metrics.ndjson`), per-suite JSON, and aggregate `summary.json`/`summary.md`. Review CPU, memory, PID, I/O, duration, slow-test, OOM, and anomaly KPIs before handoff.
- Container memory growth is an investigation signal, not automatic proof of a leak; individual tests share the same process and heap. Confirm leaks with focused repeated-process and post-GC evidence.
- The sandbox must not mount the repository, Docker socket, credentials, or the user's home, and test containers must have no external network.
- Every TCP functional test file gets a fresh server, dynamic TCP/HTTP ports, and a unique temporary SQLite directory. Never reuse a developer server or database.
- CI pins the same Bun version and frozen lockfile as the sandbox. Do not replace either with `latest` or an unlocked install.
- Docker Compose is only for functional tests that require external services, with a unique project name and disposable volumes.
- Benchmarks are always native, never Docker/VM. Every sample needs a fresh server, database, ports, queues, and competitor state.

Full rationale and commands: `docs/testing.md`.

**MANDATORY: Keep the technical documentation in `/docs` in sync with the code.**

Any change to the code MUST update the corresponding technical documentation under `docs/` in the **same** change-set — never as a follow-up. This keeps the docs continuously aligned with the current state of the codebase.

- The internal technical reference lives at `docs/README.md` (index), `docs/architecture.md`, `docs/data-model.md`, and `docs/features/<slug>.md` (one file per module).
- When you touch a module, update its `docs/features/<slug>.md` (purpose, public interface, control flow, edge cases, config). New module → add a feature doc and link it from `docs/README.md` and `docs/architecture.md`.
- Changing a type, SQLite table/index, TCP command, HTTP endpoint, env var, or default → update `docs/data-model.md` and/or the relevant feature doc.
- Adding/removing a component or changing a data flow → update `docs/architecture.md` (component diagram, flows, module map).
- The user-facing site (`docs/src/content/docs/**`, Astro) and `docs/src/content/docs/changelog.md` are separate; updating the internal `/docs` reference does not replace the changelog rule below.

A code change is not complete until `/docs` reflects it. No exceptions.

**MANDATORY: After every commit, ALWAYS:**

1. Bump version in `package.json`
2. Update changelog in `docs/src/content/docs/changelog.md`
3. `git push origin main`
4. `bun publish` to publish new version to npm

No exceptions. Every commit = new version + changelog + npm publish.

High-performance job queue server for Bun. SQLite persistence, cron jobs, priorities, DLQ, S3 backups.

## Architecture Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              CLIENT                                          │
│  Queue.add() ─────┐                              ┌───── Worker.process()    │
│  Queue.addBulk() ─┤                              │                          │
│                   ▼                              ▼                          │
│            ┌──────────┐                   ┌──────────┐                      │
│            │ TcpPool  │◄─── msgpack ────► │ TcpPool  │                      │
│            └────┬─────┘                   └────┬─────┘                      │
└─────────────────┼──────────────────────────────┼────────────────────────────┘
                  │ TCP :6789                    │
┌─────────────────┼──────────────────────────────┼────────────────────────────┐
│                 ▼           SERVER             ▼                            │
│          ┌───────────┐                  ┌───────────┐                       │
│          │ TcpServer │                  │ TcpServer │                       │
│          └─────┬─────┘                  └─────┬─────┘                       │
│                │                              │                             │
│                ▼                              ▼                             │
│   ┌────────────────────────────────────────────────────────────┐           │
│   │                    QueueManager                             │           │
│   │  ┌─────────────────────────────────────────────────────┐   │           │
│   │  │              N Shards (auto-detected)                │   │           │
│   │  │  ┌─────────┬─────────┬─────────┬─────────┐          │   │           │
│   │  │  │ Shard 0 │ Shard 1 │   ...   │ Shard N │          │   │           │
│   │  │  │┌───────┐│┌───────┐│         │┌───────┐│          │   │           │
│   │  │  ││PQueue ││PQueue ││         ││PQueue ││          │   │           │
│   │  │  │└───────┘│└───────┘│         │└───────┘│          │   │           │
│   │  │  └─────────┴─────────┴─────────┴─────────┘          │   │           │
│   │  └─────────────────────────────────────────────────────┘   │           │
│   │                           │                                 │           │
│   │  ┌────────────────────────┼────────────────────────────┐   │           │
│   │  │  jobIndex (Map)        │   completedJobs (Set)      │   │           │
│   │  │  customIdMap (LRU)     │   jobResults (LRU)         │   │           │
│   │  └────────────────────────┼────────────────────────────┘   │           │
│   └───────────────────────────┼─────────────────────────────────┘           │
│                               │                                             │
│   ┌───────────────────────────┼─────────────────────────────────┐           │
│   │                           ▼                                 │           │
│   │  ┌─────────────┐    ┌──────────┐    ┌─────────────┐        │           │
│   │  │ WriteBuffer │───►│ SQLite   │◄───│ Recovery /  │        │           │
│   │  │ (10ms batch)│    │ WAL Mode │    │ SQL queries │        │           │
│   │  └─────────────┘    └──────────┘    └─────────────┘        │           │
│   │                      Persistence                            │           │
│   └─────────────────────────────────────────────────────────────┘           │
│                                                                             │
│   ┌───────────────────────────────────────────────────────────┐             │
│   │  Background Tasks                                          │             │
│   │  • Scheduler (cron, delayed jobs)    • Stall detector     │             │
│   │  • DLQ maintenance (retry, expire)   • Lock expiration    │             │
│   │  • Cleanup (memory bounds)           • S3 backup          │             │
│   └───────────────────────────────────────────────────────────┘             │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Request Flow:**

1. **PUSH**: Client → TcpPool → TcpServer → QueueManager → Shard[hash(queue)] → PriorityQueue → WriteBuffer → SQLite
2. **PULL**: Client → TcpServer → QueueManager → Shard → PriorityQueue.pop() → Job (state: active)
3. **ACK**: Client → TcpServer → AckBatcher → Shard.complete() → jobResults (LRU) + completedJobs (Set)
4. **FAIL**: Client → TcpServer → Shard.fail() → retry (backoff) OR → DLQ (max attempts)

## Directory Structure

```
src/
├── cli/              # CLI interface (commands/, client.ts, output.ts)
├── client/           # Embedded SDK (Queue, Worker, FlowProducer, QueueGroup)
│   ├── queue/        # Queue with DLQ, stall detection
│   ├── worker/       # Worker with heartbeat, ack batching
│   └── tcp/          # Connection pool, reconnection
├── domain/           # Pure business logic (types/, queue/)
│   └── queue/        # Shard, PriorityQueue, DlqShard, UniqueKeyManager
├── application/      # Use cases (operations/, managers)
│   ├── operations/   # push, pull, ack, query, queueControl
│   └── *Manager.ts   # DLQ, Events, Workers, JobLogs, Stats
├── infrastructure/   # External (persistence/, server/, scheduler/, backup/)
└── shared/           # Utilities (hash, lock, lru, skipList, minHeap)
```

## Code Guidelines

- **MAX 300 lines per file** - split if larger
- One concern per file (Single Responsibility)
- Export only what's needed

## Sharding (auto-detected from CPU cores)

Shard count is automatically calculated based on CPU cores (power of 2, max 64):

```typescript
// Calculated at startup based on navigator.hardwareConcurrency
// Examples: 4 cores → 4 shards, 10 cores → 16 shards, 20 cores → 32 shards
const SHARD_COUNT = calculateShardCount(); // Power of 2, capped at 64
const SHARD_MASK = SHARD_COUNT - 1;
const shardIndex = (key: string) => fnv1aHash(key) & SHARD_MASK;
```

## Lock Hierarchy (acquire in order)

1. `jobIndex` → 2. `completedJobs` → 3. `shards[N]` → 4. `processingShards[N]`

```typescript
// CORRECT: read first, then acquire lock
const completed = completedJobs.has(id);
const shard = await shards[idx].acquire();
try {
  /* work */
} finally {
  shard.release();
}
```

## Memory Bounds

| Collection    | Max Size | Eviction   |
| ------------- | -------- | ---------- |
| completedJobs | 50,000   | FIFO batch |
| jobResults    | 10,000   | LRU        |
| jobLogs       | 10,000   | LRU        |
| customIdMap   | 50,000   | LRU        |

Cleanup runs every 10s. Evicts 10% when full.

## Environment Variables

```bash
# Server
TCP_PORT=6789              HTTP_PORT=6790
HOST=0.0.0.0               BUNQUEUE_DATA_PATH=./data/bunq.db
AUTH_TOKENS=token1,token2  CORS_ALLOW_ORIGIN=*
METRICS_AUTH=false          # Require auth for /metrics endpoint
TCP_SOCKET_PATH=           # RESERVED, not wired: TCP always binds HOST:TCP_PORT
HTTP_SOCKET_PATH=          # Unix socket for HTTP (works)
TLS_CERT_FILE=             # PEM cert — native TLS on TCP+HTTP (with TLS_KEY_FILE)
TLS_KEY_FILE=              # PEM private key (both or neither; partial = startup error)

# Data path (priority: BUNQUEUE_DATA_PATH > BQ_DATA_PATH > DATA_PATH > SQLITE_PATH)
# Or set programmatically: new Queue('q', { embedded: true, dataPath: './data/q.db' })

# S3 Backup
S3_BACKUP_ENABLED=0        S3_BUCKET=my-bucket
S3_ACCESS_KEY_ID=          S3_SECRET_ACCESS_KEY=
S3_REGION=us-east-1        S3_ENDPOINT=
S3_BACKUP_INTERVAL=21600000  S3_BACKUP_RETENTION=7

# Timeouts
SHUTDOWN_TIMEOUT_MS=30000  STATS_INTERVAL_MS=300000
WORKER_TIMEOUT_MS=30000    LOCK_TIMEOUT_MS=5000
WEBHOOK_MAX_RETRIES=3      WEBHOOK_RETRY_DELAY_MS=1000

# Cloud (bunqueue.io dashboard)
BUNQUEUE_CLOUD_URL=        BUNQUEUE_CLOUD_API_KEY=
BUNQUEUE_CLOUD_INSTANCE_ID=    # REQUIRED for cloud mode
BUNQUEUE_CLOUD_INSTANCE_NAME=  # defaults to hostname
BUNQUEUE_CLOUD_INTERVAL_MS=15000
BUNQUEUE_CLOUD_USE_WEBSOCKET=true
BUNQUEUE_CLOUD_USE_HTTP=true
BUNQUEUE_CLOUD_REMOTE_COMMANDS=true   # opt out with 'false'
BUNQUEUE_CLOUD_INCLUDE_JOB_DATA=true  # opt out with 'false'
BUNQUEUE_CLOUD_REDACT_FIELDS=  # comma-separated
BUNQUEUE_CLOUD_EVENTS=         # event filter, comma-separated
BUNQUEUE_CLOUD_BUFFER_SIZE=720
BUNQUEUE_CLOUD_CIRCUIT_BREAKER_THRESHOLD=5
BUNQUEUE_CLOUD_CIRCUIT_BREAKER_RESET_MS=60000
BUNQUEUE_CLOUD_SIGNING_SECRET=  # HMAC signing
```

## TCP Protocol Commands

**Core:** `PUSH`, `PUSHB`, `PULL`, `PULLB`, `ACK`, `ACKB`, `FAIL`

**Query:** `GetJob`, `GetState`, `GetResult`, `GetJobs`, `GetJobCounts`, `GetProgress`, `Count`

**Control:** `Pause`, `Resume`, `Drain`, `Obliterate`, `Clean`, `Cancel`, `Promote`, `Update`, `ChangePriority`

**DLQ:** `Dlq`, `RetryDlq`, `PurgeDlq`

**Cron:** `Cron`, `CronDelete`, `CronList`

**Monitor:** `Stats`, `Metrics`, `Prometheus`, `Ping`, `Heartbeat`, `JobHeartbeat`

**Workers:** `RegisterWorker`, `UnregisterWorker`, `ListWorkers`

**Webhooks:** `AddWebhook`, `RemoveWebhook`, `ListWebhooks`

**Rate:** `RateLimit`, `RateLimitClear`, `SetConcurrency`, `ClearConcurrency`

## CLI Usage

```bash
# Server
bunqueue start --tcp-port 6789 --data-path ./data/queue.db

# Client
bunqueue push <queue> <json> [--priority N] [--delay ms]
bunqueue pull <queue> [--timeout ms]
bunqueue ack <id> [--result json]
bunqueue fail <id> [--error msg]
bunqueue job get|state|cancel|promote|discard <id>
bunqueue queue pause|resume|drain|obliterate <queue>
bunqueue dlq list|retry|purge <queue>
bunqueue cron list|add|delete
bunqueue stats|metrics|health
```

## Simple Mode (Bunqueue)

All-in-one Queue + Worker with routes, middleware, and cron:

```typescript
import { Bunqueue } from 'bunqueue/client';

const app = new Bunqueue('tasks', {
  embedded: true,
  // Single processor OR named routes
  routes: {
    'send-email': async (job) => { return { sent: true }; },
    'send-sms': async (job) => { return { sent: true }; },
  },
  concurrency: 10,
});

// Middleware (onion model: mw1 → mw2 → processor → mw2 → mw1)
app.use(async (job, next) => {
  const start = Date.now();
  const result = await next();
  console.log(`${job.name} took ${Date.now() - start}ms`);
  return result;
});

// Cron
await app.cron('daily', '0 9 * * *', { type: 'report' });
await app.every('ping', 30000, { type: 'health' });

// Events, add, control
app.on('completed', (job, result) => {});
await app.add('send-email', { to: 'user@test.com' });
app.pause(); app.resume();
await app.close();
```

Key: `Bunqueue` = wrapper around `Queue` + `Worker`. Use `processor` OR `routes`, not both. Middleware wraps the processor. Cron delegates to `queue.upsertJobScheduler()`. For distributed (separate producer/consumer), use `Queue` + `Worker` directly.

Source: `src/client/bunqueue.ts`

## Workflow Engine

Multi-step orchestration with saga compensation, branching, parallel steps, retry, nested workflows, loops (doUntil/doWhile), forEach, map, schema validation, subscribe, observability, and human-in-the-loop signals:

```typescript
import { Workflow, Engine } from 'bunqueue/workflow';

const flow = new Workflow('order')
  .step('validate', async (ctx) => {
    const { orderId } = ctx.input as { orderId: string };
    return { orderId };
  })
  .step('charge', async (ctx) => {
    return { txId: 'tx_123' };
  }, { compensate: async () => { /* auto-rollback on failure */ }, retry: 3 })
  .parallel((w) => w
    .step('notify-warehouse', async () => ({ notified: true }))
    .step('notify-email', async () => ({ sent: true }))
  )
  .branch((ctx) => (ctx.steps['classify'] as { tier: string }).tier)
  .path('vip', (w) => w.step('vip-handler', async () => ({ discount: 20 })))
  .path('basic', (w) => w.step('basic-handler', async () => ({ discount: 0 })))
  .forEach(                                        // iterate over items
    (ctx) => (ctx.input as { items: unknown[] }).items,
    'process-item', async (ctx) => { return { item: ctx.steps.__item }; },
  )
  .map('summary', (ctx) => ({ total: 42 }))       // synchronous transform
  .doUntil(                                         // loop until condition
    (ctx) => (ctx.steps['poll'] as any)?.ready,
    (w) => w.step('poll', async () => ({ ready: true })),
    { maxIterations: 10 },
  )
  .waitFor('approval', { timeout: 86400000 })      // pauses until engine.signal(), 24h timeout
  .subWorkflow('payment', (ctx) => ({ amount: 99 }))  // nested workflow
  .step('finalize', async (ctx) => {
    const decision = ctx.signals['approval'];
    const payment = ctx.steps['sub:payment'];
    return { done: true };
  });

const engine = new Engine({ embedded: true, onEvent: (e) => console.log(e.type) });
engine.register(flow);
engine.on('step:retry', (e) => logger.warn(e));
const run = await engine.start('order', { orderId: 'ORD-1' });
await engine.signal(run.id, 'approval', { approved: true });
engine.archive(7 * 24 * 60 * 60 * 1000); // archive old executions
const unsub = engine.subscribe(run.id, (e) => console.log(e.type)); // per-execution subscribe
```

Key: `Workflow` = pure DSL builder (step/branch/path/parallel/waitFor/subWorkflow/doUntil/doWhile/forEach/map). `Engine` = facade over Queue + Worker + SQLite Store + Executor + Emitter. Each step runs as a bunqueue job on `__wf:steps` queue. Features: retry with exponential backoff, parallel via Promise.allSettled, nested workflows via polling, signal timeout, loops (doUntil/doWhile/forEach), map transforms, schema validation AND coercion (duck-typed `.parse()`, its return value is used), per-execution subscribe, typed events (15 types), cleanup/archival. Compensation runs in reverse start order on failure, and an unresolved failed reversal keeps the chain parked instead of reporting a clean rollback.

Source: `src/client/workflow/` (workflow.ts, engine.ts, executor.ts, store.ts, runner.ts, emitter.ts, loops.ts, types.ts, compensator.ts, recovery.ts, waitFor.ts, rollbackControl.ts, storeSignals.ts, storeCodec.ts, identity.ts, clock.ts, unwindPlan.ts, admission.ts)

The decision logic is pure and separately testable: `unwindPlan.ts` decides what an
unwind does with each record, `admission.ts` decides whether a node job may run, and
`clock.ts` is the single source of time and randomness, so a seed can drive the engine's
own non-determinism.

## Client SDK

```typescript
import { Queue, Worker } from 'bunqueue/client';

// Queue (TCP mode)
const queue = new Queue<T>('emails', { connection: { port: 6789 } });

// Queue (TCP + TLS): tls: true (system CAs) | { caFile } | { rejectUnauthorized: false }
const tlsQueue = new Queue<T>('emails', {
  connection: { host: 'queue.example.com', port: 6789, tls: { caFile: './ca.pem' } },
});

// Store-and-forward (edge → central): drains local jobs to a remote server.
// Remote failure → local retry/DLQ (nothing lost); deterministic remote jobId
// fwd:<queue>:<localId> dedupes re-forwards within the server's custom-id
// retention window (bounded LRU; removeOnComplete evicts). Source: src/client/forwarder.ts
const fwd = embeddedQueue.forward({
  to: { host: 'central', port: 6789, tls: true },
  queue: 'ingest', // optional remote name
});
fwd.on('forwarded', (info) => {});
await fwd.close();

// Queue (embedded mode — programmatic dataPath, no env var needed)
const embeddedQueue = new Queue<T>('emails', {
  embedded: true,
  dataPath: './data/myapp.db',
});
await queue.add('send', { email: 'user@test.com' });
await queue.add('payment', data, { durable: true }); // Immediate disk write
queue.pause();
queue.resume();
queue.drain();
queue.obliterate();

// Worker
const worker = new Worker(
  'emails',
  async (job) => {
    await job.updateProgress(50);
    return { sent: true };
  },
  { concurrency: 5, heartbeatInterval: 10000 }
);

worker.on('completed', (job, result) => {});
worker.on('failed', (job, err) => {});

// Stall Detection (embedded only)
queue.setStallConfig({ stallInterval: 30000, maxStalls: 3, gracePeriod: 5000 });

// DLQ (embedded only)
queue.setDlqConfig({ autoRetry: true, maxAge: 604800000, maxEntries: 10000 });
const entries = queue.getDlq({ reason: 'timeout' });
queue.retryDlq();
queue.purgeDlq();

// Auto-batching (TCP mode, enabled by default)
// Transparently batches concurrent add() calls into PUSHB commands.
// Strategy: flush immediately if idle, buffer during in-flight flush.
// Sequential await has zero overhead; concurrent adds get ~3x speedup.
const queue2 = new Queue('jobs', {
  autoBatch: { maxSize: 50, maxDelayMs: 5 },  // defaults
});
// Sequential: no penalty, each add() sends immediately
for (const item of items) {
  await queue2.add('task', item); // same speed as without batching
}
// Concurrent: adds batch into a single PUSHB round-trip (~3x faster)
await Promise.all([
  queue2.add('a', { x: 1 }),
  queue2.add('b', { x: 2 }),
  queue2.add('c', { x: 3 }),
]);
// Durable jobs bypass the batcher (sent as individual PUSH):
await queue2.add('critical', data, { durable: true });
// Disable auto-batching:
const queue3 = new Queue('jobs', { autoBatch: { enabled: false } });
```

## Job Options

```typescript
interface JobOptions {
  priority?: number; // Higher = sooner
  delay?: number; // ms before processing
  attempts?: number; // Max retries (default: 3)
  backoff?: number; // Retry backoff (default: 1000ms)
  timeout?: number; // Processing timeout
  jobId?: string; // Custom ID (idempotent)
  removeOnComplete?: boolean;
  removeOnFail?: boolean;
  durable?: boolean; // Bypass write buffer
}
```

## Worker Options

```typescript
interface WorkerOptions {
  concurrency?: number; // Parallel jobs (default: 1)
  heartbeatInterval?: number; // Stall detection (default: 10000, 0=disabled)
  batchSize?: number; // Pull batch (default: 10, max: 1000)
  pollTimeout?: number; // Long poll (default: 0, max: 30000)
  useLocks?: boolean; // Lock-based ownership (default: true)
}
```

## SQLite Schema (Key Tables)

```sql
-- Jobs: id, queue, data, priority, state, run_at, attempts, ...
CREATE INDEX idx_jobs_queue_state ON jobs(queue, state);
CREATE INDEX idx_jobs_run_at ON jobs(run_at) WHERE state IN ('waiting','delayed');

-- DLQ: id, job_id, queue, entry (msgpack blob), entered_at
-- Cron: name, queue, data, schedule, repeat_every, next_run, timezone
-- Results: job_id, result, completed_at
```

## Testing

```bash
bun test                                # Run in each required OrbStack Machine
bun scripts/tcp/run-all-tests.ts        # Run in each required OrbStack Machine
bun scripts/embedded/run-all-tests.ts   # Run in each required OrbStack Machine
bun run test:sandbox                    # Additional container-isolated full gate
bun run test:sandbox:sdk                # Additional gate after SDK changes
bun run bench                           # Native macOS benchmarks only
```

## Publishing

Always use `bun publish` (not `npm publish`) to publish to npm.

```bash
bun publish
```

## Performance

| Mode               | Throughput      | Data Loss Risk |
| ------------------ | --------------- | -------------- |
| Buffered (default) | ~100k jobs/sec  | Up to 10ms     |
| Durable            | ~10k jobs/sec   | None           |
| Auto-batch (TCP)   | ~145k ops/s (concurrent), ~10k ops/s (sequential) | None (same as PUSH/PUSHB) |

## Debug Endpoints

```bash
curl http://localhost:6790/health     # Health + memory
curl http://localhost:6790/heapstats  # Object breakdown
curl -X POST http://localhost:6790/gc # Force GC
```

## Memory Debugging

```typescript
import { heapStats } from 'bun:jsc';
Bun.gc(true);
const stats = heapStats();
console.log(stats.objectCount, stats.objectTypeCounts);

// Check internal collections
const mem = queueManager.getMemoryStats();
// jobIndex, completedJobs, processingTotal, queuedTotal, temporalIndexTotal
```

## Background Tasks

| Task            | Interval | Purpose                        |
| --------------- | -------- | ------------------------------ |
| Cleanup         | 10s      | Memory cleanup, orphan removal |
| Stall check     | 5s       | Detect unresponsive jobs       |
| Dependency      | event-driven (30s safety fallback) | Process job dependencies |
| DLQ maintenance | 60s      | Auto-retry, expiration         |
| Lock expiration | 5s       | Remove expired locks           |
