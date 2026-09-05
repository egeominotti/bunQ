# Canonical client parity

## Source of truth

`src/client/index.ts` defines the public client contract. The default
`sdk/typescript/src/index.ts` re-exports that contract; it does not implement
another Queue, Worker, FlowProducer, QueueEvents, QueueGroup, or Bunqueue.
`scripts/build-portable-client.ts` bundles those same implementations for
Node.js, Bun, Deno, and Workers. The former network SDK remains available at
the explicit `bunqueue-client/legacy` compatibility entry.

Canonical constructor options, defaults, overloads, return values, job
objects, processor errors, events, native batches, and group rules therefore
change together. For TCP operations, the canonical synchronous/Async
distinction is preserved. Replacing the package name does not turn a
synchronous embedded-only method into a network query.

## Runtime boundaries

The portable build substitutes only the following boundaries:

- `client/tcp/transport.ts`: Node-compatible sockets, TLS, byte framing,
  backpressure, and socket lifecycle. Reconnection, command ownership,
  responses, subscriptions, and public health state remain canonical.
- `client/manager.ts` and `application/dlqManager.ts`: delegates to the actual
  embedded backend when running on Bun. A separate conditional chunk keeps
  `bun:sqlite` out of Node/Deno startup. Embedded mode retains its Bun
  requirement; the network client does not pretend to emulate SQLite.
- A closed AST transform maps environment, timer, file, hashing, UUIDv7,
  hardware-concurrency, and worker-thread primitives. Every remaining `Bun`
  identifier fails the build, including computed access, destructuring and
  runtime aliases. Static imports, re-exports, dynamic literal imports and
  CommonJS references to `bun` or `bun:*` also fail, including type-only
  references. Literal module aliases, concatenation and `bun:` template prefixes
  are checked as well. Worker-constructor arguments undergo the same recursive check.
  Comments and ordinary strings are preserved; application processor paths
  remain dynamically importable.
- SandboxedWorker keeps its canonical pool and lifecycle; a worker-thread
  bridge executes the unchanged generated wrapper. This remains execution
  isolation rather than a security boundary.

Portable runtime files live under
`sdk/typescript/src/canonical-transport/`. They must not acquire queue-policy,
job-lifecycle, or scheduling responsibilities. New runtime boundaries require
explicit implementation and regression coverage.

Hardware concurrency preserves the runtime's `navigator.hardwareConcurrency`
value, with `availableParallelism()` as the fallback when no navigator exists.
The portable client and actual embedded engine must derive identical shard
counts under CPU quotas; host CPU inventory cannot select embedded shards.

## Fail-closed build gate

The generated `dist/canonical-manifest.json` covers source SHA-256 hashes,
runtime boundary inputs, the build pipeline, and every generated artifact.
`scripts/check-client-parity.ts` rejects missing files, stale inputs, modified
bundles, path escapes, incomplete inventories, and declaration drift.

The declaration checker resolves the complete exported type graph: overloads,
constructors, public/static members, generic defaults, options, event callback
signatures, return values, and referenced local types. It compares canonical
declarations exactly and requires the public package to preserve every
canonical export. Additive transport helpers are permitted without replacing
canonical names. Workflow declarations are checked separately; that check
does not claim a new portable workflow storage engine.

No accepted-snapshot file or missing-feature exclusion list can turn a
failure into success. `sdk/typescript` build and publish preparation execute
the checker. Root Docker validation generates the package from its sanitized
worktree before any parity tests run.

## Behavioral evidence

- `test/typescript-client-parity.test.ts`: public operation and export coverage.
- `test/client-parity-declarations.test.ts` and
  `test/client-parity-manifest.test.ts`: deliberately changed contracts and
  artifacts must be rejected by the gates.
- `test/client-parity-behavior.test.ts` and `test/tcp-parity-queue-*.test.ts`:
  fresh SQLite brokers, real public operations, results, counters, groups,
  dependency metadata, lifecycle events, and no accidental local database.
- `test/client-parity-model.test.ts`: fast-check histories compare native and
  compiled clients against independent state/conservation expectations.
  Preserve the printed seed, path, and minimized history on failure.
- `scripts/client-parity/preload.ts`: runs unchanged native documentation
  contracts against the compiled package. It substitutes module entrypoints,
  not methods or broker outcomes; both SQLite engines and TCP brokers remain
  real. The preload asserts that the redirection actually took effect.
- `sdk/typescript/tests/canonical-{queue,worker}.mjs`: identical public
  scenarios run in Bun, Node, and Deno with disposable brokers and databases.
- Protocol conformance uses the default package entry. Workers exercises
  canonical Queue/QueueGroup/Flow routes. Historical SDK tests explicitly
  import `/legacy` and are additional compatibility evidence.

## Required commands

Published declarations resolve relative ESM imports explicitly, including directory
indexes. Build freshness also records compiler configuration and dependency locks.
A strict NodeNext consumer regression runs without `skipLibCheck` and
checks payload errors. The package includes authentic Bun and Node type definitions
because the shared public graph exposes embedded manager and SQLite types; these
are type dependencies and do not load the Bun backend in Node or Deno.

```sh
bun run build:client
bun run check:client-parity
bun run --cwd sdk/typescript test:parity
bun run --cwd sdk/typescript test:shared-contract
bun run --cwd sdk/typescript test:canonical
bun run test:model
bun run test:sandbox
bun run test:sandbox:sdk
```

Native CI installs root dependencies from the frozen lockfile and builds the
ignored portable artifacts before running unit tests. SDK release preparation
also installs root build dependencies before compiling and packing the SDK,
because its builder consumes the canonical source tree and root TypeScript
toolchain.

CI requires canonical scenarios in Bun, Node 20/22, and Deno. The SDK sandbox
also runs those runtimes, shared contracts, conformance, and Workers against
the packaged outputs. These checks establish API coverage, shared behavior
implementation, and runtime evidence; they are not performance benchmarks.
