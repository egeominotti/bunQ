# bunqueue-client

The cross-runtime distribution of the canonical `bunqueue/client` API.
Queue, Worker, QueueEvents, QueueGroup, FlowProducer, Simple Mode, groups,
processor batches, options, job objects, and errors come from the same source.
The portable build replaces runtime I/O primitives; it does not maintain a
second implementation of queue or worker behavior.

## Install and run

```sh
npm install bunqueue-client
```

Start a bunqueue broker, then use the same code in Node.js, Deno, or Bun:

```typescript
import { Queue, Worker, QueueEvents } from 'bunqueue-client';

const options = {
  embedded: false,
  connection: { host: '127.0.0.1', port: 6789 },
};
const queue = new Queue<{ to: string }>('emails', options);
const events = new QueueEvents('emails', options);
await events.waitUntilReady();
const worker = new Worker('emails', async (job) => {
  await job.updateProgress(50);
  await job.log('Sending email');
  return { sent: true };
}, { ...options, concurrency: 5 });
worker.on('error', console.error);

const job = await queue.add('welcome', { to: 'user@example.com' });
const result = await queue.waitJobUntilFinished(job.id, events, 30_000);
console.log(result);

await worker.close();
events.close();
await queue.close();
```

Node.js 20+, Bun, Deno 2+, and Cloudflare Workers with `nodejs_compat` can
connect over TCP. Install with npm-compatible tooling in the chosen runtime.
The embedded SQLite engine continues to require Bun; under Bun the package
loads the actual shared engine when `embedded: true` is used. Node and Deno do
not load `bun:sqlite`. Database configuration for TCP belongs on the broker.

## One public contract

Use the [Queue guide](https://bunqueue.dev/guide/queue/),
[Worker guide](https://bunqueue.dev/guide/worker/), and
[Flow guide](https://bunqueue.dev/guide/flow/), replacing the import path
`bunqueue/client` with `bunqueue-client`.

- Connection settings use `connection: { host, port, token, ... }`.
- Constructor defaults, per-job defaults, return shapes, errors, and events
  follow the canonical Bun client.
- Use the `Async` administrative/query variants for authoritative TCP reads
  and ordered operations. The synchronous embedded-only methods retain the
  canonical contract; they do not become asynchronous by changing packages.
- `QueuePro`, `WorkerPro`, and `QueueEventsPro` are the same canonical aliases.
- Native processor batches and broker-authoritative job groups share the
  same implementation and lease/counter transitions as the Bun client.
- `SandboxedWorker` shares its pool logic, with a portable worker-thread
  adapter. It remains experimental execution isolation, not a security
  boundary. Processor modules must be executable by the host runtime.

Low-level `Connection`, `ConnectionPool`, and telemetry helpers remain
additive exports. They do not redefine canonical Queue or Worker types.

## Migrating the historical SDK API

Earlier SDK examples used flat connection options, different method defaults,
and different Job fields. The default entry now follows `bunqueue/client`.
An explicit compatibility entry retains that historical API:

```typescript
import { Queue, Worker, Bunqueue } from 'bunqueue-client/legacy';
```

See [LEGACY.md](./LEGACY.md) for the historical contract. Compatibility tests
continue to run separately; they do not stand in for canonical parity tests.

## Parity gates

```sh
bun run build
bun run test:parity
bun run test:shared-contract
bun run test:canonical
```

The build consumes the repository's canonical sources and emits a manifest
covering source hashes and every generated artifact. The parity checker
rejects missing/stale artifacts, missing exports, changed signatures,
constructors, overloads, options, nested type references, and event contracts.
The unchanged native documentation suites also run against the built package
with real embedded engines and TCP brokers. Differential and generated-history
tests check results, states, counters, and lifecycle events.

Bun, Node, and Deno run the canonical public scenarios in CI. Protocol
conformance and Cloudflare Workers exercise the built package too. A failed
parity gate blocks the SDK build/publish preparation; it cannot be bypassed by
updating an API count or accepting a new snapshot.

Only `msgpackr` is needed at runtime for protocol serialization. Build and test
tooling stays in development dependencies. The Bun-only backend is loaded
conditionally and retains the same storage/runtime requirements as bunqueue.
