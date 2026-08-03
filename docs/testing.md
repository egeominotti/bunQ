# Test Isolation and Reproducibility

This document is the authoritative engineering reference for running bunqueue's
test suites without reusing host processes, ports, databases, or environment
state. Performance benchmarks use a different isolation model; see
[Benchmarks are native](#benchmarks-are-native).

## Validation layers

Development uses complementary layers so feedback remains fast without
weakening the release gate:

1. Run the narrowest relevant test in an isolated OrbStack development Machine
   on the Mac's native architecture while iterating. A macOS-host run is
   diagnostic only.
2. For the final candidate, run the product suites directly in a fresh Ubuntu
   24.04 Machine and, for a release, repeat them in a fresh Debian 13 Machine.
   Both use the Mac's native architecture; GitHub Actions supplies native
   `amd64` coverage when development occurs on Apple Silicon.
3. Before a commit, run `bun run test:sandbox`. It builds the current worktree
   into one test image and runs the required unit, TCP, and embedded suites in
   three independent disposable containers in parallel.
4. When an external SDK changes, also run `bun run test:sandbox:sdk`. It builds
   dedicated TypeScript, Python, PHP, Go, Rust, and Elixir images, then runs
   each SDK's native tests and shared protocol conformance checks in parallel.

The sandbox executes these exact commands:

```bash
bun test
bun scripts/tcp/run-all-tests.ts
bun scripts/embedded/run-all-tests.ts
```

### OrbStack Machine isolation

The canonical local Machine is Ubuntu 24.04 on the Mac's native architecture,
with 4 CPUs, a requested 16 GiB of memory, and at least 32 GiB of disk. The
release compatibility Machine uses Debian 13 on the same architecture. On
Apple Silicon both are `arm64`; GitHub Actions `ubuntu-latest` supplies the
independent native `amd64` gate. Both Machines are disposable and must be
created for one final candidate only:

```bash
machine_run_id="$(date -u +%Y%m%dT%H%M%SZ)"
case "$(uname -m)" in
  arm64) machine_arch=arm64 ;;
  x86_64) machine_arch=amd64 ;;
  *) echo "Unsupported host architecture" >&2; exit 1 ;;
esac
orb create --isolated --isolate-network --arch "${machine_arch}" \
  --cpus 4 --memory 16G --disk 32G ubuntu:24.04 \
  "bunqueue-ci-${machine_run_id}"
orb create --isolated --isolate-network --arch "${machine_arch}" \
  --cpus 4 --memory 16G --disk 32G debian:13 \
  "bunqueue-debian-${machine_run_id}"
```

Do not count a translated `amd64` Machine on Apple Silicon as release evidence.
It is a useful diagnostic target, but Rosetta can change process-heavy Bun
behavior; repeat the complete result on `arm64`. Record both requested resources
and effective cgroup CPU/memory limits because OrbStack can clamp a Machine to
its global resource ceiling.

Do not add `--mount` or `--forward-ssh-agent`. A Machine must never receive the
macOS home directory, repository mount, Docker socket, `.git`, SSH agent,
credentials, registry tokens, `.env` files, ignored files, `node_modules`,
artifacts, SQLite databases, or generated test output. A tracked
placeholder-only template such as `.env.example` is allowed only after its
contents have been reviewed. Build an explicit
sanitized snapshot containing every tracked and intended untracked change,
then transfer that snapshot through OrbStack's built-in SSH endpoint. The
`orb push` command cannot write an isolated Machine's host-exposed read-only
filesystem;
install `rsync` in the Machine and use `rsync -e ssh ... MACHINE@orb:...`
instead. The SSH private key remains on macOS and is not forwarded into the
Machine. This avoids both testing stale committed code and leaking unrelated
host state.

Install the exact Bun version pinned in `.github/workflows/ci.yml` and use
`bun install --frozen-lockfile --ignore-scripts`. Directly inside each Machine,
run the unit, TCP, and embedded commands above plus:

```bash
bun run typecheck
bun run check:biome
```

The Machine gate also supplies a real bounded filesystem for SQLite admission
tests. Create it inside the disposable Machine, never on a host mount:

```bash
sudo mkdir -p /mnt/bunqueue-tinyfs
sudo mount -t tmpfs -o size=16m tmpfs /mnt/bunqueue-tinyfs
sudo chown "$(id -u):$(id -g)" /mnt/bunqueue-tinyfs
BUNQUEUE_TINYFS=/mnt/bunqueue-tinyfs bun test test/repro-disk-full.test.ts
sudo umount /mnt/bunqueue-tinyfs
```

This is a required Machine check, not published benchmark evidence. The normal
unit suite always runs the deterministic storage-rejection injections in
`test/repro-durable-persistence-rejection.test.ts`; the tmpfs run adds real
`SQLITE_FULL` coverage for single and batch admission in Embedded and TCP mode,
terminal custom-ID reuse across restart, preserved completed results/DLQ rows,
and buffered storage-health recovery.

Run `git diff --check` on the macOS host before building the snapshot. The
Machine cannot run Git worktree checks because `.git` is deliberately excluded.

Each run records its distro/version, architecture, kernel, Bun version and
revision, frozen-lockfile installation result, commands, exit codes, durations,
and exact test totals. Copy the manifest and logs back under `artifacts/`, then
delete the Machine. A reused Machine, host-only run, or run against a mounted
worktree is diagnostic evidence only.

`--isolated` removes macOS filesystem and integration access;
`--isolate-network` also blocks direct access to the host and other Machines.
It does not create an independent kernel: OrbStack Machines and containers
share OrbStack's underlying Linux kernel. The Machine layer therefore catches
clean-host, architecture, distribution, and hidden-state problems, while the
container sandbox remains the authoritative offline per-suite containment
layer. Neither is a physical-machine security boundary.

Queue, Worker, Cron, and DLQ guide coverage is tracked explicitly in
[Documented Feature Verification](./features/documented-feature-verification.md).
`test/documented-feature-coverage.test.ts` requires every requested section to
retain both TCP and embedded evidence and checks that every authoritative
shared contract has symmetric wrappers. The functional runners discover those
wrappers by their `test-*.ts` names, so the matrix is exercised by the same
sandbox gate rather than by a separate optional suite.

`test/source-architecture.test.ts` additionally enforces the 300-line source
ceiling, thin QueueManager/Queue/Worker/SandboxedWorker façades, dedicated type
modules, and documentation references that point at focused implementation
modules instead of obsolete line numbers in the façades.

The Docker build includes uncommitted files from the current worktree, so the
validated source is the source being reviewed. It never bind-mounts the host
repository into a test container.

One class of failure escapes every container: the docs site can build from files
that exist on the author's disk but were never committed, so it goes red in CI
only. `bun run check:docs-data` (part of `bun run check`, and a step in the CI docs
job) asserts that everything `docs/src/content/docs/**` references is git-tracked
and that the committed `apiVersions.json` still matches its generator. See
[the generated API reference](./generated-api-reference.md#the-guard-bun-run-checkdocs-data).

The SDK gate writes complete `*.build.log` and suite logs plus the same NDJSON,
per-suite JSON, and aggregate report schema under
`artifacts/test-sandbox-sdk/<timestamp>/`. A failed image build also produces a
failure summary instead of exiting without artifacts. `Dockerfile.sdk-test`
uses one target per toolchain so SDK dependencies and language versions remain
isolated; runtime containers have no external network, credentials, home
mount, repository mount, or Docker socket. The minimal build context retains
the root `.gitignore` because Biome's VCS-aware SDK checks require the same
ignore boundary used in the worktree. Cloudflare's local runtime receives
`CLOUDFLARE_CF_FETCH_ENABLED=false`, preventing its optional `Request.cf`
metadata fetch from weakening the no-network guarantee or delaying the gate.
The TypeScript target also includes Node 22, matching CI, because Wrangler's
`unstable_dev` runner is a Node process even though the SDK and broker use Bun.

The six SDK suites execute:

```bash
# TypeScript
bun run build && bun run check && bun pm pack
bun tests/integration.ts && bun tests/e2e.ts
bun run test:workers
# Python
python -m compileall -q bunqueue tests
python -m build --no-isolation --outdir /tmp/python-package
python tests/test_integration.py && python tests/run_e2e.py
# PHP / Go
composer validate --strict --no-check-publish
find src tests -name '*.php' -print0 | xargs -0 -n1 php -l
php tests/run-e2e.php
test -z "$(gofmt -l .)" && go vet ./... && go list ./...
go test -v ./...
go test -race -run 'Hardening|Regression|Worker' ./...
# Rust / Elixir
cargo fmt --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
cargo package --locked --offline --allow-dirty --no-verify
mix format --check-formatted && mix compile --warnings-as-errors
mix test --slowest 20 && mix hex.build
```

Every suite then runs its language driver through all 17 shared conformance
checks against a fresh real broker.

The conformance runner starts every driver command with `sdk/conformance` as
its working directory. Commands for nested language modules must select their
module explicitly: Go CI uses `go -C drivers/go run .`, while the isolated SDK
sandbox may prebuild the driver and execute `./drivers/go-driver`. Do not use
`go run ./drivers/go`; Go resolves `go.mod` from the command's working directory,
not from the package path, so that form fails before the driver can connect.
The core test image copies this workflow file so its command contract is also
covered by the unit-suite regression test.

TCP test harnesses pass `port: 0` and read the listener's assigned port so the
kernel chooses and binds it atomically. Random high-port probing is not safe
under the parallel unit gate because another listener can claim the port before
the server bind. `test/repro-ci-dynamic-port.test.ts` enforces this contract for
the TCP protocol audit that exercises slowloris protection, write backpressure,
and large-frame reassembly.

## Model-based broker verification

`bun run test:model` runs the `fast-check` asynchronous command model described
in [Model-Based Queue Verification](./features/model-based-testing.md). Each
property run owns a fresh broker subprocess, dynamic ports, queue, TCP client,
and temporary SQLite database. Commands execute only when their model
precondition is valid; after each command the test compares API state, aggregate
counts, lock ownership, SQLite rows, DLQ rows, payloads, priorities, and
persisted queue controls.

The queue model owns the same lease tokens as a real worker. `PULL` and `PULLB`
store the token returned for every active job; active acknowledgements,
heartbeats, failures, `MoveToWait`, `MoveToDelayed`, and `ChangeDelay` must send
that exact token. The same transition commands omit `token` when they act on a
delayed, failed, waiting, or prioritized job with no live lease. Deterministic
command-serialization tests protect this distinction so a model expectation
cannot accidentally bypass the engine's lease enforcement.

The default campaign uses 150 runs and at most 80 generated commands. It is part
of plain `bun test`, so the mandatory `test:sandbox` unit container executes it
without a separate container. For deeper native investigation:

```bash
BUNQUEUE_MODEL_RUNS=500 \
BUNQUEUE_MODEL_COMMANDS=150 \
BUNQUEUE_MODEL_SEED=424242 \
bun run test:model
```

The workflow campaign closes every `Engine`, shuts down the process-wide
embedded `QueueManager`, and only then removes its shared temporary SQLite
directory. This ordering prevents background lock/DLQ timers from touching a
database after teardown and keeps model output free of false disk-I/O alarms.

Failures report a seed, a minimized command history, and a replay path. Convert
every confirmed engine divergence into a deterministic
`test/repro-model-*.test.ts` regression before applying the fix. Model
expectation errors remain model fixes; for example `JobHeartbeatB` deliberately
returns its count in `data.count`, while `ACKB` deliberately returns only the
top-level success envelope and no count.

Each generated run uses a separate broker and an adjacent TCP/HTTP port pair.
Startup is complete only after both HTTP `/ready` and TCP `Hello` succeed. The
harness captures subprocess stderr and retries only a confirmed bind collision;
timeouts and all other startup failures retain their diagnostics and fail the
test instead of being hidden by a generic port-wait timeout.

## SDK hardening matrix

The native suites use each public SDK against real disposable brokers. Every
language covers the same failure classes, with idiomatic mechanics:

| Layer | Required SDK evidence |
| --- | --- |
| Unit / integration / E2E | Pure option and wire logic, real TCP framing, Queue/Worker/Flow business paths, and permanent regressions for fixed bugs |
| Contract | All 17 independent producer/consumer conformance checks |
| Race / idempotency | Many independent connections retry one custom id; many live connections contend for one lease; worker concurrency stays bounded |
| Property / fuzz | Native generators shrink flow plans while checking graph conservation, reciprocal edges, ordering, reserved metadata, and zero I/O on invalid input; malformed/deep/cyclic/extension corpora fail typed and leave the connection usable |
| Mutation | A pinned native mutation engine challenges each SDK's pure flow planner on scheduled/manual CI; surviving non-equivalent mutants require a stronger invariant |
| Chaos / recovery | Hard process termination, half-open timeout, reconnect and durable-job visibility after restart |
| Load / spike | Bounded bulk and worker bursts run in the normal gate, including 512-1500 job spikes |
| Soak / stress | One long-lived SDK connection repeatedly adds, queries, and resets batches for a configurable duration and batch size |
| Security / compatibility | Auth-first and CA-verification regressions, weekly dependency advisories, and the runtime version matrix in `.github/workflows/sdk.yml` |

The bounded cases belong in `test:sandbox:sdk`. Sustained profiles are opt-in
because hours-long tests are not a useful per-edit gate. They run weekly in CI
for 900 seconds per SDK and can be reproduced natively:

```bash
BUNQUEUE_SDK_SOAK_SECONDS=3600 BUNQUEUE_SDK_SOAK_BATCH=100 bun sdk/typescript/tests/soak.ts
BUNQUEUE_SDK_SOAK_SECONDS=3600 BUNQUEUE_SDK_SOAK_BATCH=100 python sdk/python/tests/soak.py
BUNQUEUE_SDK_SOAK_SECONDS=3600 BUNQUEUE_SDK_SOAK_BATCH=100 php sdk/php/tests/soak.php
BUNQUEUE_SDK_SOAK_SECONDS=3600 BUNQUEUE_SDK_SOAK_BATCH=100 \
  go test ./sdk/go -run '^TestSDKSoak$' -timeout 3900s -v
BUNQUEUE_SDK_SOAK_SECONDS=3600 BUNQUEUE_SDK_SOAK_BATCH=100 \
  cargo test --manifest-path sdk/rust/Cargo.toml --test soak -- --ignored
BUNQUEUE_SDK_SOAK_SECONDS=3600 BUNQUEUE_SDK_SOAK_BATCH=100 \
  sh -c 'cd sdk/elixir && mix test --include soak --timeout 3900000 test/soak_test.exs'
```

Go and Elixir are the two runners with a framework timeout shorter than a soak:
`go test` panics at 10 minutes and ExUnit kills a test at 60 seconds, both
regardless of how long the profile is asked to run. Always pass a bound larger
than `BUNQUEUE_SDK_SOAK_SECONDS` (the examples add 300 seconds of slack for
broker startup and teardown); the weekly CI steps derive theirs from the same
variable. The other runners have no implicit test timeout.

Raise `BUNQUEUE_SDK_SOAK_BATCH` to explore client backpressure and the practical
breaking point. That is a diagnostic stress profile, not a stable performance
threshold. Go also exposes `FuzzHardeningPortableWirePayload`; CI fuzzes it for
60 seconds weekly. All SDKs keep deterministic malformed-input corpora in the
normal gate; these are separate from mutation testing, which rewrites planner
code and therefore runs in its own scheduled/manual campaign after the normal
suite is green.

`.github/workflows/sdk-mutation.yml` is that campaign. It pins every mutation
engine, retains the machine-readable/native report, and must never weaken a
language's checked-in ratchet at the command line. The current minimums are
98% for StrykerJS, 97% for mutmut, 100% for Muex, 99% MSI/covered MSI for
Infection, and 99.9% efficacy/coverage for Gremlins. `cargo-mutants` fails on
every viable survivor. A score above a ratchet is not sufficient by itself:
every survivor is classified as a missing assertion or a behaviorally
equivalent mutation, and non-equivalent survivors require a stronger property
before handoff.

Database power-loss, disk-full, WAL integrity, and schema upgrade/downgrade
tests belong to the broker's persistence suite, not to network clients that
cannot control SQLite. SDK crash tests assert the client-visible contract:
durable work survives broker SIGKILL, reconnect succeeds, and the job remains
queryable exactly once. Delivery remains at-least-once; processors must be
idempotent because a crash after side effects but before ACK can re-run a job.

Disk-full admission assertions are fail closed: when an immediate durable
write is rejected, the candidate must be absent from SQLite, queue heaps,
dependency wait sets, `jobIndex`, counters, custom-ID/dedup ownership, queries,
and Worker delivery. A rejected terminal-ID reuse must preserve the old
completed/DLQ generation and result before and after restart. Buffered writes
have a different documented contract: they may be acknowledged before their
10 ms flush and must surface later failure through storage health, retry, and
critical-loss reporting.

Weekly dependency audits require live advisory databases and therefore run in
CI, not inside the deliberately offline sandbox. Performance-regression
thresholds likewise use fresh native processes; Docker/VM measurements are
diagnostic only and must never be published as benchmark results.

## Test image

`Dockerfile.test` is deliberately separate from the production `Dockerfile`.
The production image contains only the compiled server; the test image contains
`src/`, `test/`, `scripts/`, `bench/`, configuration, internal technical
Markdown, and the Astro documentation content exercised by documentation
contracts. Copying the documentation is intentional: section coverage,
language-tab parity, and stale architecture-reference checks must execute in
the sandbox instead of passing vacuously against an empty tree. The allow-listed
configuration also includes `docs/vercel.json`, the CI workflow files, and the
small per-SDK mutation configuration files consumed by structural regressions.
It includes `Dockerfile.test` itself so tests can verify that this contract stays
synchronized with the workflow files. It does not install SDK toolchains or
copy credentials into the core test image. `Dockerfile.test.dockerignore`
limits the build context to those inputs.

Regression helpers that poll state must fail closed when their deadline expires.
Tests concerned with failure classification rather than scheduling explicitly
set `backoff: 0`, so randomized production retry jitter cannot turn an incomplete
observation into a misleading downstream assertion. Migration regressions always
verify the current schema version; schema 30 also has a direct 29-to-30 upgrade
test for the durable `jobs.dlq_retry_state` column.

Wall-clock backoff tests assert the lower bound of each retry's own jitter
window. They never require adjacent exponential delays to be monotonic: the
documented ±50% windows overlap, so a valid second delay can be shorter than a
valid first delay. Unit tests cover the exact random-factor bounds and cap,
while TCP and embedded runners prove that real retry admission respects the
per-attempt floors.

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
SDK summaries combine each language's native test result with shared
conformance checks. For Elixir, the current `Result: N passed` summary is
authoritative (including failed/skipped fields when present); the legacy
`N tests, M failures` format is parsed only when no current summary exists, so
mixed logs are not double-counted.

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
Functional tests that depend on background timers must poll the observable state
with a generous deadline. A fixed sleep proves only that a minimum duration
elapsed; it does not bound when a timer callback runs under parallel CPU or I/O
contention.

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

`test-core-e2e` runs `bun run test:core-e2e` as a distinct required job. Its
TypeScript-compiler inventory currently covers 308 public instance methods
across Queue, Worker, Job, schedulers, DLQ, FlowProducer, QueueGroup, Bunqueue,
SandboxedWorker, Forwarder, Workflow, Engine, event facades, and
TcpConnectionPool. Exported runtime classes are discovered automatically. The
272 dual-mode methods use a fresh embedded SQLite manager and a real
dynamic-port TCP broker with a separate SQLite database; the 13 transport-only
pool methods carry embedded `N/A`, and 23 synchronous snapshots with async
counterparts carry TCP `N/A`.
The suite scans its contracts for test doubles and compares successful coverage
to the applicable discovered surface exactly. It exposes 580 applicable
method-mode checks and uploads the full 308-row Markdown/JSON evidence matrix as
a CI artifact. See
[Core Public API End-to-End Matrix](./features/core-public-api-e2e.md).

`.github/workflows/sdk.yml` is both reusable and directly schedulable. CI calls
it once and waits for an explicit `sdk-gate` that checks the result of all six
language jobs even when an earlier one failed or was cancelled. A root
`quality-gate` similarly checks the public-API E2E job plus every other core,
docs, and SDK result. The version
gate, binary matrix, container publication, and GitHub release are all
transitively downstream; Docker publication also waits for the complete binary
matrix. The TypeScript package publisher is manual, runs the same reusable
six-SDK gate, uses frozen installs and pinned Bun, publishes with `bun publish`,
and creates its tag only after the registry accepts the package.

The finite release-DAG relationships are parsed and mutation-checked by
`test/repro-release-sdk-gate.test.ts`; actionlint validates the full GitHub
Actions syntax and reusable-workflow contracts in the lint job. Actionlint
delegates embedded shell snippets to `shellcheck` only when that executable is
available, so the regression suite also rejects unquoted redirections to
GitHub command files independently of the developer machine. For direct
ShellCheck parity without a host installation, validate an individual workflow
through the pinned image without mounting the repository:

```bash
docker run --rm -i rhysd/actionlint:1.7.12 - < .github/workflows/ci.yml
```

Property-based testing is deliberately not used for this small finite edge set.

## Benchmarks are native

Do not publish performance figures produced in Docker Desktop or another VM.
Virtualized CPU scheduling, networking, and filesystems distort both latency
and throughput. Benchmark runners must instead use native processes with a new
server, database, queue names, ports, and competitor state for every sample.
Never reuse a developer Redis database or a long-lived bunqueue server.

Docker Compose is reserved for functional tests that genuinely need external
services. Use a unique Compose project name and disposable volumes; never run a
repository-wide `down -v` against an unrelated project.
