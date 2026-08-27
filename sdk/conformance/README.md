# bunqueue conformance suite

The machine-checkable version of the [wire protocol spec](../../docs/protocol.md).

A bunqueue client, in any language, is **protocol-conformant** when its
driver passes every check in this suite against a real bunqueue server. This
is the bar for calling a client "official" or "compatible": it either passes
or it does not.

## How it works

```
┌────────────┐   JSON lines    ┌────────────────┐    wire     ┌──────────┐
│ runner.ts  │ ◄─────────────► │ your driver    │ ◄─────────► │ bunqueue │
│ (this kit) │  stdin/stdout   │ (your client)  │   msgpack   │  server  │
└─────┬──────┘                 └────────────────┘             └────▲─────┘
      └────────────── independent verification connection ────────┘
```

The runner spawns a fresh server and your driver, sends operations over
stdin/stdout as JSON lines, and verifies the results **twice**: once through
your driver's answers and once through its own independent wire connection.
A client cannot pass by misreading the protocol consistently.

The implementation keeps those trust boundaries separate: `runner.ts` owns
only CLI orchestration and reporting, `harness.ts` owns server and driver
processes, `wire.ts` is the independent protocol client, the two `checks-*.ts`
modules contain C01-C16, and `check-support.ts` contains shared check utilities.
No module is an SDK implementation, and the runner's public command and output
format remain unchanged.

## Running it

```bash
# from this directory (needs bun; the server is spawned from the repo root)
bun runner.ts --driver "bun drivers/typescript.ts"          # TypeScript SDK
bun runner.ts --driver "../python/.venv/bin/python drivers/python.py"  # Python SDK
bun runner.ts --driver "php drivers/php.php"                # PHP SDK
bun runner.ts --driver "go -C drivers/go run ."             # Go SDK
bun runner.ts --driver \
  "cargo run --quiet --manifest-path ../rust/Cargo.toml --example conformance-driver" # Rust
bun runner.ts --driver \
  "cd ../elixir && mix run ../conformance/drivers/elixir.exs" # Elixir
```

Driver commands always start with `sdk/conformance` as their working directory.
Drivers backed by a nested language module must therefore select that module
explicitly. For Go, `go -C drivers/go run .` enters the driver's module before
resolving `go.mod`; `go run ./drivers/go` is invalid because Go would search for
a module from `sdk/conformance`. The isolated SDK sandbox may instead prebuild
the same driver and pass `./drivers/go-driver`.

Output: one `PASS`/`FAIL` line per check and a final verdict. Exit code 0 =
conformant.

By default the spawned broker uses a unique temporary SQLite database. Set
`BUNQUEUE_CONFORMANCE_POSTGRES_URL` to exercise the identical client driver
against PostgreSQL instead:

```bash
BUNQUEUE_CONFORMANCE_POSTGRES_URL='postgres://bunqueue:secret@localhost:5432/bunqueue' \
  bun runner.ts --driver "bun drivers/typescript.ts"
```

The harness passes the URL only to the broker child. Driver processes inherit
the normal toolchain environment, but a case-insensitive policy removes `BQ_`,
`BUNQUEUE_`, `AWS_`, `S3_`, `POSTGRES_`, libpq `PG*`, known storage/TLS handles,
and delimiter-separated credential names such as `TOKEN`, `PASSWORD`,
`API_KEY`, and `PRIVATE_KEY`. Collision regressions preserve non-secret names
such as `TOKENIZERS_PARALLELISM` and `AUTHORITY_URL`. The TCP endpoint and
optional authentication token are delivered through the driver protocol.
This reduces accidental environment disclosure; it is not a security sandbox
for an untrusted driver, which still runs as repository code inside the SDK
container. The harness creates a unique PostgreSQL namespace for each spawned
broker, confirms process exit with `SIGKILL` escalation when necessary, and
only then deletes every row in the namespace. Startup failures use the same
ordered cleanup and also remove the temporary SQLite directory. The outer SDK
sandbox waits for every started suite, checks every Docker teardown result,
retains failed ownership for a retry, never force-removes an unconfirmed
container name, and reports startup plus cleanup failures together.
Official SDK CI and `bun run test:sandbox:sdk` execute all 18 checks on both
SQLite and PostgreSQL 18.6 for TypeScript, Python, PHP, Go, Rust, and Elixir.

## Driver contract

A driver is any executable that reads one JSON object per line on stdin and
writes one JSON object per line on stdout. Requests carry `id`; answers echo
it: `{"id": 1, "ok": true, ...result}` or `{"id": 1, "ok": false, "error": "..."}`.

| op | request fields | expected answer fields |
|---|---|---|
| `connect` | `host`, `port`, `token?` | — |
| `add` | `queue`, `name`, `data?`, `opts?` | `jobId` |
| `addBulk` | `queue`, `entries: [{name, data?, opts?}]` | `ids` |
| `addFlow` | `queue`, `parentId`, `childId` | `parentId`, `childId` |
| `getJob` | `jobId` | `job` (`{id, name, data, stacktrace?}`) or `null` for missing — **not an error** |
| `getJobByCustomId` | `queue`, `customId` | `job` or `null` |
| `getState` | `jobId` | `state` |
| `getResult` | `jobId` | `result` |
| `count` | `queue` | `count` |
| `isPaused` | `queue` | `paused` |
| `pause` / `resume` | `queue` | — |
| `drain` | `queue` | `count` |
| `promote` | `jobId` | — |
| `upsertScheduler` | `queue`, `schedulerId`, `repeat` (`{pattern?/every?, limit?, tz?}`), `template?` (`{name?, data?, opts?}`) | — |
| `getScheduler` | `schedulerId` | `scheduler` or `null` |
| `removeScheduler` | `schedulerId` | — |
| `waitForJob` | `jobId`, `timeoutMs` | `result` |
| `getDlqCount` | `queue` | `count` |
| `retryDlq` | `queue` | `count` |
| `hello` | — | `protocolVersion`, `capabilities` |
| `process` | `queue`, `behavior` (`ok`\|`failOnce`\|`unrecoverable`\|`deepThrow`), `result?`, `batchSize?`, `until` (`{completed?, failed?, dlq?}`), `timeoutMs` | — when the `until` condition was reached |
| `close` | — | — (then exit 0) |

`process` semantics: run your client's worker on `queue` until the `until`
condition holds (as observed through your own client), then stop it.
Behaviors: `ok` completes jobs with `result`; `failOnce` throws on each
job's FIRST attempt only (track per job id) then completes; `unrecoverable`
throws your SDK's UnrecoverableError equivalent; `deepThrow` throws from
~25 frames deep with the message `BOOM-CONFORMANCE`. `batchSize`, when
present, must be passed to your worker configuration verbatim (check C14
sends 5000: a conformant client clamps to the server max instead of
erroring forever).

## Checks

| # | What it proves | Spec section |
|---|---|---|
| C01 | `Hello` protocol v3 and `separate-job-name` match the server | §2 |
| C02 | PUSH/PUSHB keep top-level job names separate from untouched user data | §5 |
| C03 | int64 guard: big ints become float64, server stays healthy | §4 |
| C04 | `jobId` idempotency on PUSH | §6.1 |
| C05 | PUSHB preserves custom ids (`jobId → customId`) | §6.1 |
| C06 | Not-found lookups map to null, not errors | §6.2 |
| C07 | Delayed state + promote | §6.4 |
| C08 | Process + ACK: result persisted, exactly-once completion | §6.3 |
| C09 | Retry semantics: transient failure then success | §6.3 |
| C10 | Unrecoverable failure skips retries into the DLQ | §6.3 |
| C11 | FAIL stack keeps the raise site within the persisted lines | §6.3 |
| C12 | Scheduler `jobName`, `data`, and `limit` reach distinct wire fields | §6.6 |
| C13 | `WaitJob` timeout clamped to the server bound | §6.2/§9 |
| C14 | Worker batch size clamped to the server max (1000) | §6.3 |
| C15 | Pause / isPaused / resume / drain | §6.4 |
| C16 | Unicode payload integrity through msgpack | §4 |
| C17 | Atomic FlowProducer tree, dependency order, and child-result visibility | §6.5 |
| C18 | Auth handshake on a token-protected server | §2 |

## Adding a new language

Write a driver (usually 100-200 lines), run the suite, iterate until green.
The reference drivers in `drivers/` show the pattern. If a check feels
impossible to pass, read the matching spec section first — every check
encodes a real bug found in a real client.
