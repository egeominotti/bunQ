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
bun run typecheck
biome check src test/path-to-test.test.ts
git diff --check
```

**MANDATORY: After ANY code modification, ALWAYS run ALL THREE test suites
before committing:**

```bash
bun test                                # Unit tests (~5000 tests)
bun scripts/tcp/run-all-tests.ts        # TCP integration tests (~50 suites)
bun scripts/embedded/run-all-tests.ts   # Embedded integration tests (~35 suites)
```

Do not commit if any suite fails or could not be run. No exceptions.

Add regression tests for every confirmed bug. A useful regression test should
fail on the buggy implementation, exercise the public or real persistence path,
and assert both the returned result and any affected counters/state.

## Test hygiene

- Use unique queue names and unique temporary SQLite paths.
- Close `QueueManager`, Queue, Worker, server, and database instances in cleanup.
- Remove SQLite `.db`, `-wal`, and `-shm` files created by the test.
- Avoid timing-only assertions when state can be observed directly. When testing
  delays or long-polling, use generous bounds and deterministic timestamps.
- Do not weaken an existing test merely to make a behavior change pass; update
  it only when the intended contract has explicitly changed.

## Commits and handoff

- Do not create commits, push, or publish unless the user explicitly authorizes
  that workflow. Repository policy treats every commit as a release: bump the
  version in `package.json`, update
  `docs/src/content/docs/changelog.md`, push `origin main`, and publish with
  `bun publish` (never `npm publish`). Confirm authorization for the complete
  release workflow before creating the commit.
- Report the files changed, behavior fixed, and exact validation commands/results.
- Call out any test or check that could not be run and why.
