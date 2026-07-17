# Test Isolation and Reproducibility

This document is the authoritative engineering reference for running bunqueue's
test suites without reusing host processes, ports, databases, or environment
state. Performance benchmarks use a different isolation model; see
[Benchmarks are native](#benchmarks-are-native).

## Validation layers

Development uses two layers so feedback remains fast without weakening the
release gate:

1. Run the narrowest relevant test natively while iterating.
2. Before a commit, run `bun run test:sandbox`. It builds the current worktree
   into one test image and runs the required unit, TCP, and embedded suites in
   three independent disposable containers in parallel.

The sandbox executes these exact commands:

```bash
bun test
bun scripts/tcp/run-all-tests.ts
bun scripts/embedded/run-all-tests.ts
```

The Docker build includes uncommitted files from the current worktree, so the
validated source is the source being reviewed. It never bind-mounts the host
repository into a test container.

## Test image

`Dockerfile.test` is deliberately separate from the production `Dockerfile`.
The production image contains only the compiled server; the test image contains
`src/`, `test/`, `scripts/`, `bench/`, configuration, and development dependencies.
`Dockerfile.test.dockerignore` limits the build context to those inputs.

The environment is reproducible:

- Bun is pinned to 1.3.14, matching CI and the published benchmark environment.
- Dependencies use `bun install --frozen-lockfile --ignore-scripts`.
- Timezone is UTC and the process runs as the image's non-root `bun` user.
- OpenSSL is installed because the TLS regression suites generate certificates.

## Runtime containment

`scripts/test-sandbox.ts` builds the image once, then starts one container per
suite concurrently. Parallelism reduces wall-clock time to approximately the
slowest suite instead of the sum of all three durations. Each container has:

- no host bind mounts, Docker socket, home directory, or credentials;
- no external network (`--network none`); TCP/HTTP integration uses loopback;
- all Linux capabilities dropped and `no-new-privileges` enabled;
- PID and memory limits, plus the Docker daemon's CPU ceiling;
- an isolated executable `/tmp` tmpfs for SQLite files and sandboxed workers;
- `--init`, a stop timeout, signal cleanup, and automatic removal after success.

Every run writes the complete, untruncated output of each suite below
`artifacts/test-sandbox/<timestamp>/`. The directory is ignored by Git. On
failure the runner prints the exact log path and retains the failed container,
so the result remains inspectable with `docker logs <container>`; remove it with
`docker rm <container>` after diagnosis.

## Telemetry and engineering KPIs

Every sandbox run emits machine-readable telemetry alongside the complete logs:

```text
artifacts/test-sandbox/<timestamp>/
├── unit|tcp|embedded.log
├── unit|tcp|embedded.metrics.ndjson
├── unit|tcp|embedded.summary.json
├── summary.json
└── summary.md
```

`docker stats` is sampled throughout each suite. Suite summaries contain wall
time, exit code, OOM status, CPU average/p95/peak, memory start/end/delta/p95/
peak/limit/slope, peak PID count, block I/O, network I/O, and pass/fail/skip
counts. TCP and embedded runners also emit one structured record per test file
with its duration and assertion totals. Unit output is parsed into individual
test names and durations, which powers the slow-test ranking in `summary.md`.
That human-readable report also surfaces sample counts, observed tests per
second, the complete memory profile and slope, block I/O, and network I/O; the
JSON and NDJSON artifacts remain the authoritative machine-readable values.

The report flags non-zero exits, OOM kills, memory or PID pressure above 80%, a
strong end-to-start memory-growth signal, and material duration or peak-memory
regressions against the most recent prior telemetry report. Raw samples remain
in NDJSON so an anomaly can be correlated with timestamps in the suite log.

Memory is measured at container level. Individual unit tests share a process,
heap, caches, and garbage collector, so assigning heap ownership to one test
would be misleading. A positive memory slope is a signal to investigate, not
proof of a leak; confirm it with a focused repeated-process reproduction and
post-GC measurements before changing runtime code.

The disposable container layer may be writable because a few tests generate
temporary scripts below the repository path. It cannot write those changes back
to the host because no repository path is mounted.

Resource defaults can be adjusted for constrained machines without changing the
image:

```bash
BUNQUEUE_TEST_MEMORY=4g BUNQUEUE_TEST_CPUS=1 bun run test:sandbox
```

`BUNQUEUE_TEST_CPUS` adds a stricter per-container CPU cap when desired;
otherwise the runner inherits the daemon's configured CPU ceiling.
`BUNQUEUE_TEST_IMAGE` changes the local image tag. The default memory ceiling is
4 GB per container. Use `BUNQUEUE_TEST_SEQUENTIAL=1` to run the same isolated
suites one after another when diagnosing a possible resource-contention failure.

## TCP suite isolation

`scripts/tcp/run-all-tests.ts` is safe to run either natively or inside the
sandbox. For every `test-*.ts` file it:

1. allocates fresh TCP and HTTP ports;
2. creates a unique temporary directory and SQLite database;
3. starts a new server with a small allowlist of inherited environment values;
4. runs one test file with an explicit timeout;
5. emits a structured duration/result marker for telemetry;
6. terminates the test and server, then removes the whole temporary directory.

This prevents queue state, completed-job caches, timers, environment variables,
or a server shutdown in one file from affecting another file. Authentication is
enabled only for the authentication suite. `BUNQUEUE_TEST_FILE_TIMEOUT_MS`
overrides the default three-minute per-file bound.

Every TCP fixture runs against SQLite. `GetJobs` is intentionally eventually
consistent with non-durable `PUSH` while the roughly 10 ms write buffer drains,
so tests that assert an immediate list/filter/pagination result must seed those
jobs with `durable: true`. Do not replace that guarantee with a fixed sleep;
tests specifically covering buffered visibility should poll with a deadline.

## CI

GitHub Actions already gives each job a fresh virtual machine, so the unit, TCP,
and embedded jobs remain parallel rather than starting nested Docker. They use
the same pinned Bun version and frozen lockfile as the local sandbox. A CI job is
the isolation equivalent of one local suite container.

## Benchmarks are native

Do not publish performance figures produced in Docker Desktop or another VM.
Virtualized CPU scheduling, networking, and filesystems distort both latency
and throughput. Benchmark runners must instead use native processes with a new
server, database, queue names, ports, and competitor state for every sample.
Never reuse a developer Redis database or a long-lived bunqueue server.

Docker Compose is reserved for functional tests that genuinely need external
services. Use a unique Compose project name and disposable volumes; never run a
repository-wide `down -v` against an unrelated project.
