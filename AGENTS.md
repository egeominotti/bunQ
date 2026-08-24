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

- Before every `git commit` and every `git push`, invoke the `skeptic` custom
  subagent and wait for its verdict. It must inspect the complete staged diff,
  identify at least three potential failure points per changed file, verify
  error handling, races, idempotency, type safety and test evidence, and run
  applicable checks. `FAIL` blocks the operation; `CONDITIONAL` requires the
  user's explicit confirmation. The project profiles live at
  `.codex/agents/skeptic.toml` and `.claude/agents/skeptic.md`.
- When a bug is reported, first add a regression test that reproduces it. Only
  after the test fails for the expected reason, use subagents to investigate
  fixes and prove the chosen fix with the passing regression test.
- Preserve user changes. Never run `git reset`, `git checkout`, `git clean`, or
  discard unrelated edits unless the user explicitly requests it.
- Keep all repository content in English, including source comments, tests,
  internal documentation, user-facing documentation and agent configuration.
- Keep changes narrowly scoped to the requested behavior. Do not refactor
  unrelated code while fixing a bug.
- Use `rg`/`rg --files` for repository searches and `apply_patch` for manual
  source edits.
- Runtime source is TypeScript ESM. Follow the existing 2-space, single-quote,
  semicolon style; Oxlint and Oxfmt are authoritative.
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

Run repository tests in an isolated OrbStack Machine by default. A macOS host
run is diagnostic only and does not count as final validation evidence. Run the
narrowest relevant tests while iterating:

```bash
BUNQUEUE_EMBEDDED=1 bun test test/path-to-test.test.ts
bun run test:model
bun run typecheck
bunx oxlint src test/path-to-test.test.ts
bunx oxfmt --check src test/path-to-test.test.ts
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

### OrbStack Machine gate

Final Linux validation must run in a fresh, disposable OrbStack Machine. The
canonical local environment is Ubuntu 24.04 on the Mac's native architecture;
on Apple Silicon this means `arm64`. Allocate 4 CPUs, request 16 GiB of memory,
and at least 32 GiB of disk, then record the effective cgroup limits because
OrbStack may clamp them to its global limits. GitHub Actions `ubuntu-latest`
provides the independent native `amd64` gate. Every release must also run the
same native product suites in a fresh Debian 13 Machine using the Mac's native
architecture.

Do not use a translated `amd64` Machine on Apple Silicon as final evidence.
Rosetta is useful for diagnostics, but process-heavy Bun tests can behave
differently under translation. Record such a run as diagnostic and repeat it
on a native-architecture Machine.

Create Machines with both `--isolated` and `--isolate-network`. Do not use
`--mount` or `--forward-ssh-agent`. Never expose the macOS repository, home
directory, Docker socket, credentials, SSH agent, package-manager tokens, or
real environment files to a Machine. A tracked placeholder-only template such
as `.env.example` is allowed only after its contents have been reviewed.
Transfer an explicit sanitized worktree snapshot over OrbStack's built-in SSH
transport; it must contain all tracked and intended untracked
changes while excluding `.git`, ignored files, `node_modules`, `artifacts`,
SQLite databases, generated test output, secrets, and credentials.

Provision the exact Bun version pinned by CI and install from the frozen
lockfile. Then run, directly in each Machine:

```bash
bun test --parallel=4
bun scripts/tcp/run-all-tests.ts
bun scripts/embedded/run-all-tests.ts
bun run typecheck
bun run check:oxc
```

Use a unique Machine name for every final candidate and delete it after its
logs and environment manifest have been copied to `artifacts/`. Record the
distro, version, architecture, kernel, Bun version/revision, command, exit code,
duration, and exact test totals. A reused Machine or a host-only run is not
release evidence. OrbStack Machines and containers share OrbStack's underlying
Linux kernel, so this is strong filesystem/process/network separation but not a
separate-kernel or physical-machine security boundary.

The Machine gate supplements rather than replaces the container gates below.
Run `git diff --check` on the host before creating the sanitized snapshot,
because the Machine deliberately receives no `.git` directory.
Run benchmarks only on native macOS, never in an OrbStack Machine or container.

**MANDATORY: After ANY code modification, run the isolated validation before
committing:**

```bash
bun run test:sandbox
```

The sandbox builds the current worktree and runs ALL THREE required suites in
parallel, in separate disposable containers:

```bash
bun test --parallel=4                   # Unit tests (four isolated file workers)
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

- Prefer targeted tests in the current isolated development Machine during
  iteration; use fresh disposable Machines and the sandbox for the final full
  gates. A host-native run is diagnostic only. Never run one container per
  individual unit test.
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

- Before every authorized `git commit`, update
  `docs/src/content/docs/changelog.md` for the staged changes and include that
  update in the same commit. The commit message is mandatory and must be a
  non-empty, concise, specific English description of the actual changes;
  generic placeholder messages are forbidden.
- Before every authorized `git push`, verify that each outgoing commit already
  contains its corresponding changelog update and has a compliant commit
  message. Do not edit the changelog solely because a push is being performed.
- Do not create commits, push, bump the package version, or publish unless the
  user explicitly authorizes the relevant action. Version bumps and
  `bun publish` are separate release actions and are not implied by permission
  to commit or push. Never publish with `npm publish`.
- Report the files changed, behavior fixed, and exact validation commands/results.
- Call out any test or check that could not be run and why.
