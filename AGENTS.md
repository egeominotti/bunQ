# AGENTS.md

This file applies to the entire repository unless a more specific `AGENTS.md`
exists in a subdirectory.

## Project overview

bunqueue is a high-performance job queue written in TypeScript for Bun. The
server and embedded runtime are Bun-only. Persistence uses `bun:sqlite`; public
clients may run in other runtimes through the network protocol.

## Repository map

- `src/application/`: queue orchestration and use-case logic.
- `src/domain/`: jobs, shards, priority queues, counters, and limiters.
- `src/infrastructure/`: SQLite persistence and transport adapters.
- `src/client/`: public Queue/Worker APIs.
- `test/`: Bun unit, integration, regression, and reproduction tests.
- `docs/`: documentation site.
- `sdk/`: language SDKs and conformance fixtures.

## Working rules

- When a bug is reported, first add a regression test that reproduces it. Only
  after the test fails for the expected reason, use subagents to investigate
  fixes and prove the chosen fix with the passing regression test.
- Preserve user changes. Never run `git reset`, `git checkout`, `git clean`, or
  discard unrelated edits unless the user explicitly requests it.
- Keep changes narrowly scoped to the requested behavior. Do not refactor
  unrelated code while fixing a bug.
- Use `rg`/`rg --files` for repository searches and `apply_patch` for manual
  source edits.
- Runtime source is TypeScript ESM. Follow the existing 2-space, single-quote,
  semicolon style; Biome is authoritative.
- Keep hot queue paths synchronous while a shard lock is held. Do not introduce
  an `await` inside a synchronous critical section.
- Maintain queue invariants together: heap membership, `jobIndex`, processing
  maps, counters, temporal indexes, group ownership, and concurrency slots must
  transition exactly once.
- SQLite filtering, ordering, and pagination must happen in a consistent order.
  Use deterministic tie-breakers for paginated queries and add migrations for
  schema or index changes.
- Do not silently change externally visible scheduling semantics. Add explicit
  regression coverage when priority, delay, FIFO-group, retry, or recovery
  behavior changes.
- Keep source files at or below 300 lines. Give each file one responsibility and
  export only what is needed; split a file instead of growing it beyond that
  boundary.
- Respect the lock hierarchy: `jobIndex` -> `completedJobs` -> `shards[N]` ->
  `processingShards[N]`. Read prerequisite state before acquiring the next lock
  and always release locks in `finally`.

## Documentation

- Every code change must update the corresponding internal technical
  documentation under `/docs` in the same change-set. A code change is not
  complete while its documentation is stale.
- Update `docs/features/<slug>.md` when changing a module. New modules also need
  links from `docs/README.md` and `docs/architecture.md`.
- Update `docs/data-model.md` and/or the relevant feature document when changing
  a public type, SQLite table or index, TCP command, HTTP endpoint, environment
  variable, or default value.
- Update `docs/architecture.md` when components, data flows, or the module map
  change. The Astro user documentation under `docs/src/content/docs/` and its
  changelog are separate from the internal technical reference.

## Validation

Run the narrowest relevant tests while iterating:

```bash
BUNQUEUE_EMBEDDED=1 bun test test/path-to-test.test.ts
bun run test:model
bun run typecheck
biome check src test/path-to-test.test.ts
git diff --check
```

**MANDATORY: Run `bun run test:model` after every change that can affect job
lifecycle, persistence/recovery, scheduling/order, dependencies, deduplication,
leases, rate/concurrency limits, counters, temporal indexes, or `jobIndex`.**
This command runs the `fast-check` asynchronous command model against a real
TCP broker and SQLite. It is targeted iteration evidence, not a replacement for
the final sandbox (the sandbox's unit suite runs it again).

The model must continue to enforce conservation, no loss/resurrection,
exclusive delivery, custom-ID idempotency, retry/stall bounds, legal
transitions, priority/FIFO/LIFO/delay/group/dependency ordering, unique keys,
rate/concurrency/TTL resource rules, counter/index coherence, idempotent
recovery, and DLQ exactly-once. When it fails, record the printed seed,
counterexample, and replay path. Distinguish a model error from an engine
divergence; for a confirmed engine bug, preserve the minimized sequence as a
deterministic `test/repro-model-*.test.ts` regression before fixing it.

**MANDATORY: After ANY code modification, run the isolated validation before
committing:**

```bash
bun run test:sandbox
```

The sandbox builds the current worktree and runs ALL THREE required suites in
parallel, in separate disposable containers:

```bash
bun test                                # Unit tests (~5000 tests)
bun scripts/tcp/run-all-tests.ts        # TCP integration tests (~50 suites)
bun scripts/embedded/run-all-tests.ts   # Embedded integration tests (~35 suites)
```

Do not commit if any suite fails or could not be run. No exceptions.

**MANDATORY: After ANY modification under `sdk/`, run the isolated SDK
validation before handoff (and again before committing if more SDK edits were
made):**

```bash
bun run test:sandbox:sdk
```

This second gate runs the native tests and shared protocol conformance checks
for every official external SDK in separate disposable containers. Both
`test:sandbox` and `test:sandbox:sdk` are required after SDK changes; a targeted
native SDK test is diagnostic feedback, not a replacement for the sandbox.

The exact native commands are a diagnostic fallback when Docker is unavailable,
not proof that sandbox isolation passed. Report the unavailable sandbox as a
blocker before commit.

Add regression tests for every confirmed bug. A useful regression test should
fail on the buggy implementation, exercise the public or real persistence path,
and assert both the returned result and any affected counters/state.

## Test hygiene

- Prefer targeted native tests during iteration; use the sandbox for the final
  full gate. Never run one container per individual unit test.
- Model campaigns can be deepened without editing source:
  `BUNQUEUE_MODEL_RUNS=500 BUNQUEUE_MODEL_COMMANDS=150
  BUNQUEUE_MODEL_SEED=<signed-seed> bun run test:model`. Preserve the sign
  printed by `fast-check`. Never remove or weaken an invariant merely to make a
  generated history pass.
- The three top-level suites run concurrently by default. Use
  `BUNQUEUE_TEST_SEQUENTIAL=1` only to diagnose possible resource contention;
  it runs the same commands with the same isolation.
- Treat the sandbox as process, filesystem, network, database, port, and
  environment isolation, not as separate physical machines. Suite containers
  still share the OrbStack VM kernel, Docker daemon scheduling, CPU, memory,
  and disk I/O. A failure seen only under parallel load must be reproduced with
  `BUNQUEUE_TEST_SEQUENTIAL=1` before attributing it to application behavior.
- Read complete suite output from `artifacts/test-sandbox/<timestamp>/`. On
  failure, preserve the runner's retained container until the cause is known.
- Read SDK-gate output from `artifacts/test-sandbox-sdk/<timestamp>/` with the
  same anomaly and retained-container discipline.
- Every sandbox run must emit complete logs, per-suite NDJSON resource samples,
  per-suite JSON, and the aggregate `summary.json`/`summary.md`. Review reported
  anomalies and slow-test rankings before handoff; do not report only pass/fail.
- Treat memory slope or end-to-start growth as an investigation signal, not
  proof of a leak. Individual tests share a process and heap; confirm suspected
  leaks with a focused repeated-process, post-GC reproduction.
- Never mount the repository, Docker socket, credentials, or the user's home
  directory into a test container. Tests need no external network access.
- TCP functional files must use a fresh server, dynamic ports, and a unique
  temporary SQLite directory. Do not connect tests to an existing server.
- Use unique queue names and unique temporary SQLite paths.
- Close `QueueManager`, Queue, Worker, server, and database instances in cleanup.
- Remove SQLite `.db`, `-wal`, and `-shm` files created by the test.
- Avoid timing-only assertions when state can be observed directly. When testing
  delays or long-polling, use generous bounds and deterministic timestamps.
- Do not weaken an existing test merely to make a behavior change pass; update
  it only when the intended contract has explicitly changed.
- Never publish benchmark results from Docker or a VM. Run benchmarks natively,
  with a fresh process, database, ports, queues, and competitor state per sample.
  Docker Compose is only for functional tests requiring external services and
  must use a unique project name and disposable volumes.

## Commits and handoff

- Do not create commits, push, or publish unless the user explicitly authorizes
  that workflow. Repository policy treats every commit as a release: bump the
  version in `package.json`, update
  `docs/src/content/docs/changelog.md`, push `origin main`, and publish with
  `bun publish` (never `npm publish`). Confirm authorization for the complete
  release workflow before creating the commit.
- Report the files changed, behavior fixed, and exact validation commands/results.
- Call out any test or check that could not be run and why.
