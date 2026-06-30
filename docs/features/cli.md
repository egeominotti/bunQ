# CLI

> **Category:** Interface · **Source:** `src/cli/index.ts`, `src/cli/client.ts`, `src/cli/output.ts`, `src/cli/help.ts`, `src/cli/commands/*.ts`

## Purpose
The CLI is the `bunqueue` executable: a single binary that boots the server **or** acts as a thin TCP client against a running server. It parses global options and a command line, builds a msgpack command object, sends it over the TCP wire protocol, and renders the response for the terminal (human-readable or `--json`). It exists so operators can push/pull jobs, inspect/control queues, manage DLQ/cron/webhooks/workers/rate-limits, run diagnostics, and trigger S3 backups without writing code.

## Responsibilities & Scope
Owns:
- **argv parsing** — global flags (`--host`/`--port`/`--token`/`--tls*`/`--json`/`--help`/`--version`) plus per-subcommand flags, and dispatch between server mode and client mode (`src/cli/index.ts:205`, `:325`).
- **Command building** — translating CLI verbs into protocol command objects (`{ cmd, ... }`) under `src/cli/commands/`.
- **TCP client transport for the one-shot CLI** — connect, optional `Auth`, send one command, await one framed msgpack response, close (`src/cli/client.ts:180`).
- **Output formatting** — colorized tables/objects and error rendering, plus a JSON pass-through (`src/cli/output.ts`).
- **Local (non-TCP) commands** — `version`, `doctor`, and `backup` run client-side against HTTP/health or S3 directly.
- **Help text** (`src/cli/help.ts`).

Does NOT own:
- Actual queue logic — delegated to the server. The CLI never touches a Shard or SQLite directly (except `backup`, which uses `S3BackupManager`).
- Server bootstrap internals — `runServer` delegates to `bootServer` / `resolveServerConfig` (see [Configuration & Entrypoint](./configuration.md)).
- The wire framing/encoding algorithm — it reuses `FrameParser` and msgpackr from the [TCP Wire Protocol & Framing](./tcp-protocol.md).
- The persistent connection pool / reconnection / auto-batching used by the SDK — the CLI opens its own single short-lived socket (see [Client Transport](./client-transport.md)).

## Dependencies
Internal:
- [TCP Wire Protocol & Framing](./tcp-protocol.md) — `FrameParser`, `FrameParser.frame()`, `FrameSizeError` from `src/infrastructure/server/protocol`.
- [Client Transport](./client-transport.md) — `buildClientTls` from `src/client/tcp/connection` and `ClientTlsOptions` type.
- [Configuration & Entrypoint](./configuration.md) — `loadConfigFile`, `resolveServerConfig`, `bootServer`, and `resolveToken` (`src/client/resolveToken.ts`).
- [S3 Backup](./backup-s3.md) — `S3BackupManager` for the local `backup` command.
- [Webhooks, Events & Job Logs](./webhooks-and-events.md) — `WEBHOOK_EVENTS` from `src/domain/types/webhook` for `webhook add` event validation.
- [TCP Server Command Handlers](./tcp-server-handlers.md) — the actual handlers for every `cmd` the CLI emits.

External / runtime:
- Bun APIs: `Bun.connect` (TCP+optional TLS), `Bun.env`, `Bun.stringWidth` (ANSI-aware column padding), `fetch` + `AbortSignal.timeout` (doctor/version health probe), `process.argv`/`process.exit`/`process.stdout.isTTY`.
- `msgpackr` (`pack`/`unpack`) for the binary protocol.
- `node:util` `parseArgs` for per-subcommand flag parsing.

## Public Interface

### Exported functions
- `main(): Promise<void>` — entry point; runs only under `import.meta.main` (`src/cli/index.ts:325`, `:414`).
- `parseGlobalOptions(): { options: GlobalOptions; commandArgs: string[] }` (`src/cli/index.ts:205`).
- `executeCommand(command: string, args: string[], options: ClientOptions): Promise<void>` (`src/cli/client.ts:180`).
- `clientTimeoutFor(cmd: Record<string, unknown>): number` (`src/cli/client.ts:43`).
- `formatOutput(response, command, asJson, subcommand?): string` and `formatError(message, asJson): string` (`src/cli/output.ts:422`, `:440`).
- `printHelp()`, `printServerHelp()`, `printPushHelp()`, `printCronAddHelp()`, `printVersion(version)` (`src/cli/help.ts`).
- Command builders: `buildCoreCommand`, `buildJobCommand`, `buildQueueCommand`, `buildDlqCommand`, `buildCronCommand`, `buildWorkerCommand`, `buildWebhookCommand`, `buildRateLimitCommand`, `buildMonitorCommand`, and the backup runners `isBackupCommand` / `executeBackupCommand` / `runDoctor` / `runServer`.
- Shared helpers + `CommandError` in `src/cli/commands/types.ts`: `requireArg`, `parseJsonArg`, `parseNumberArg`, `parseBigIntArg`.

### CLI commands → TCP `cmd`
The CLI itself emits no HTTP; it maps verbs to TCP commands (`src/cli/client.ts:276`):

| CLI command | TCP `cmd` |
| --- | --- |
| `push` / `pull` / `ack` / `fail` | `PUSH` / `PULL` / `ACK` / `FAIL` |
| `job get`/`state`/`result`/`cancel` | `GetJob` / `GetState` / `GetResult` / `Cancel` |
| `job progress`/`update`/`priority` | `Progress` / `Update` / `ChangePriority` |
| `job promote`/`delay`/`discard` | `Promote` / `MoveToDelayed` / `Discard` |
| `job logs`/`log`/`wait` | `GetLogs` / `AddLog` / `WaitJob` |
| `queue list`/`pause`/`resume`/`drain` | `ListQueues` / `Pause` / `Resume` / `Drain` |
| `queue obliterate`/`clean`/`count` | `Obliterate` / `Clean` / `Count` |
| `queue jobs`/`paused` | `GetJobs` / `IsPaused` |
| `dlq list`/`retry`/`purge` | `Dlq` / `RetryDlq` / `PurgeDlq` |
| `cron list`/`add`/`delete` | `CronList` / `Cron` / `CronDelete` |
| `worker list`/`register`/`unregister` | `ListWorkers` / `RegisterWorker` / `UnregisterWorker` |
| `webhook list`/`add`/`remove` | `ListWebhooks` / `AddWebhook` / `RemoveWebhook` |
| `rate-limit set`/`clear` | `RateLimit` / `RateLimitClear` |
| `concurrency set`/`clear` | `SetConcurrency` / `ClearConcurrency` |
| `stats` / `metrics` / `health` / `ping` | `Stats` / `Prometheus` / `Stats` / `Ping` |

`health` reuses `Stats` and lets the formatter render it (`src/cli/commands/monitor.ts:13`). When a `--token` is set, an `Auth` command is sent first on the same socket (`src/cli/client.ts:199`).

### Commands handled locally (no TCP `cmd`)
- `version` → client version + HTTP `/health` probe at TCP-port + 1 (`src/cli/index.ts:359`, `:306`).
- `doctor` → HTTP `/health` diagnostics (`src/cli/index.ts:365` → `runDoctor`).
- `backup now|create|list|restore|status` → `S3BackupManager` directly (`src/cli/index.ts:371`).
- bare `bunqueue` / `start` / a leading `-flag` → server mode via `runServer` (`src/cli/index.ts:344`).

## Data Models
The CLI is schemaless on the wire: every command is a plain `Record<string, unknown>` carrying a `cmd` discriminator, and every response is a `Record<string, unknown>` with an `ok: boolean`. See [data-model](../data-model.md) for the canonical Job shape.

- `GlobalOptions` (`src/cli/index.ts:16`): `{ host: string; port: number; token?: string; tls?: boolean | { rejectUnauthorized?: boolean; caFile?: string }; json: boolean; help: boolean; version: boolean }`.
- `ClientOptions` (`src/cli/client.ts:15`): `{ host; port; token?; tls?; json }`.
- `SocketData` (`src/cli/client.ts:29`): `{ frameParser: FrameParser; resolve; reject }` — one outstanding request per socket.
- `BackupCommandResult` (`src/cli/commands/backup.ts:11`): `{ success: boolean; message: string; data?: unknown }`.

Job fields the formatter reads (`src/cli/output.ts`): `id`, `queue`, `state`, `priority`, `attempts`/`maxAttempts`, `data`, `progress`, `createdAt`, `startedAt`, `completedAt`, `runAt`, `error`. Because the server-side Job carries no explicit `state`, `deriveJobState` infers it from timestamps and errs to `'unknown'` rather than guessing (`src/cli/output.ts:52`).

## Business Logic / Control Flow

### 1. Global parse (`parseGlobalOptions`, `src/cli/index.ts:205`)
Done by hand (not `parseArgs`) so unknown/subcommand flags survive into `commandArgs`. Notable rules:
- `--` ends global parsing; everything after is opaque (`:227`).
- `-t`/`--token` is global **except** after `pull` and `job wait`, where `-t` is that subcommand's `--timeout` and is passed through (`commandOwnsShortT`, `:120`, `:136`).
- TLS flags: `--tls`, `--tls-no-verify`, `--tls-ca <file>` / `--tls-ca=<file>`; `--tls-cert`/`--tls-key` are server flags and pass through (`applyTlsFlag`, `:65`). `--tls-no-verify` or `--tls-ca` imply TLS (`buildTlsOption`, `:104`).
- Attached short flags that shadow a global letter (e.g. `-p10`, `-Hfoo`) are warned about because `strict:false` downstream would silently mis-parse them (`warnAmbiguousAttachedShort`, `:195`).
- Flag-consumers refuse to swallow a following `-flag` as their value (host/port/token/ca), warning instead.
- Port/host fall back to env when not explicit: port via `TCP_PORT`→`BUNQUEUE_TCP_PORT`→`BQ_TCP_PORT` (`resolveEnvPort`, `:34`), host via `HOST`→`BUNQUEUE_HOST`→`BQ_HOST` (`:49`). Token via `resolveToken` (`--token`→`BQ_TOKEN`→`BUNQUEUE_TOKEN`).
- **Server mode** is detected when the first positional is `start`, absent, or starts with `-` (`:277`). In that case explicit `--host`/`--port` are re-injected into `commandArgs` as `--host`/`--tcp-port` so they reach `parseServerArgs` (`:283`).

### 2. Dispatch (`main`, `src/cli/index.ts:325`)
`--version`/`--help` short-circuit first. Then: server mode → `runServer`; `version`/`doctor`/`backup` → local handlers; otherwise → `executeCommand`. Command-specific help is printed for `push`/`cron` when `--help` follows the verb (`:351`). Top-level `try/catch` prints `Error: <msg>` and exits 1.

### 3. Client execution (`executeCommand`, `src/cli/client.ts:180`)
1. **Build first, connect second** — `buildCommand` runs before any socket is opened, so unknown commands and parse errors are reported without a reachable server (`:190`). Unknown command → exit 1.
2. `connect()` opens `Bun.connect` (TLS wrapped if `buildClientTls` returns a value), with a 5s connection timeout (`:171`).
3. If a token is present, send `{ cmd:'Auth', token }`; non-`ok` → "Authentication failed" exit 1 (`:199`).
4. Send the command with a deadline from `clientTimeoutFor` (base 30s; only `PULL`/`WaitJob` add the server-side `timeout` + 10s network buffer, `:43`). One in-flight request per socket: `socket.data.resolve/reject` is set, the framed msgpack is written, the `data` handler unpacks the first frame and resolves.
5. Response handling: `!ok` → `formatOutput` to stderr, exit 1. Two false-success guards: `WaitJob` + `completed:false` → "Job not completed within timeout" exit 1 (`:228`); `GetState` + `state:'unknown'` → "Job not found" exit 1 (`:234`). Otherwise print formatted output to stdout.
6. `RegisterWorker` prints a stderr warning that the registration is transient and dies with the CLI process (`:245`).
7. `finally` closes the socket.

### 4. Output (`formatOutput` → `formatSuccess`, `src/cli/output.ts:422`, `:352`)
`--json` returns `JSON.stringify(response)` verbatim. Otherwise `unwrap` flattens a `{ data: {...} }` envelope (`:296`), then a cascade picks a renderer: job / jobs-table (DLQ-aware) / workers / webhooks / crons / dlqJobs / logs / stats / counts / queues, then scalar shapes (`id`→"Job created", `ids`→verb chosen by command+subcommand, `workerId`, `webhookId`, `cron`, `state`, `result`, `progress`, `paused`, `count`, `metrics`), falling back to `OK` (`:418`). The subcommand (first positional) is forwarded so batch responses read "Drained"/"Cleaned"/"Retried"/"Purged" correctly (`src/cli/client.ts:218`).

### 5. Server mode (`runServer`, `src/cli/commands/server.ts:118`)
Parse CLI flags (`parseCliFlags`, `:37`) → `loadConfigFile(configPath)` → `applyCliFlags` (CLI wins over file config, `:83`) → `resolveServerConfig` → `bootServer`. Invalid ports warn and fall back to defaults (6789/6790).

### 6. Doctor (`runDoctor`, `src/cli/commands/doctor.ts:46`)
Fetches HTTP `/health` (5s timeout). Checks: reachability, server version + mismatch warning vs client `VERSION`, `status === 'healthy'`, uptime, connection counts, queue counts (DLQ > 0 is a warning), memory (RSS > 512 MB warns). Exits `issues > 0 ? 1 : 0`.

### 7. Backup (`executeBackupCommand`, `src/cli/commands/backup.ts:21`)
Requires a data path from `BUNQUEUE_DATA_PATH`→`BQ_DATA_PATH`→`DATA_PATH`→`SQLITE_PATH`; otherwise fails. Validates S3 config via `S3BackupManager.validate()`. `restore` refuses without `--force` (`-f`) and warns to stop the server first (`:128`).

## Concurrency & Locking
No server-side locks are taken here — the CLI is a client. The only concurrency invariant is **one outstanding request per socket**: `SocketData.resolve/reject` is a single slot, set before `write` and cleared on the first frame, timeout, or close (`src/cli/client.ts:55`, `:114`). The CLI sends at most `Auth` then one command sequentially, so the slot never collides. `pull` sets `detach: true` so the server does not auto-release the leased job when this short-lived connection closes, letting a later `ack`/`fail` from a *separate* CLI invocation succeed (`src/cli/commands/core.ts:121`).

## Edge Cases & Failure Modes
- **Exit codes**: `0` success; `1` generic failure (unknown command, `!ok`, auth fail, timeout, `WaitJob` not completed, `GetState` unknown); `2` when a `CommandError` (bad arguments/JSON) escapes (`src/cli/client.ts:254`). Doctor/backup compute their own codes.
- **TLS mismatch hint**: a plaintext client reading a TLS handshake parses garbage as an absurd frame size; `FrameSizeError` is caught and rephrased to suggest `--tls`/`--tls-ca`/`--tls-no-verify` (`src/cli/client.ts:96`).
- **Connect rejection path**: `Bun.connect` may reject directly (TLS handshake refused) instead of firing `connectError`; that promise rejection is surfaced rather than waiting out the 5s timeout (`:163`).
- **Long-poll deadline**: only `PULL`/`WaitJob` extend the client timeout; for other commands a `timeout` field means something else (e.g. job execution timeout) and must not stretch the client wait (`:43`).
- **Argument validation** (`CommandError`, exit 2): `push` enforces `delay >= 0`, `max-attempts >= 1`, `timeout > 0`; `job progress` requires 0–100; `cron add` requires `queue`+`data`+(`schedule`|`every`), rejects `every <= 0`, and **omits** `maxLimit` when `0` so the server stores "unlimited" (sending 0 would mark the cron already exhausted, `src/cli/commands/cron.ts:88`); `webhook add` validates events against `WEBHOOK_EVENTS` and the URL protocol; rate-limit/concurrency require positive limits.
- **Job IDs are passed as strings** (`parseBigIntArg`) so UUIDs, numeric, and custom IDs all work; the server parses them (`src/cli/commands/types.ts:42`).
- **Transient worker registration**: `worker register` over the CLI is gone the instant the process exits (TCP close auto-unregisters); a stderr warning points users to the SDK `Worker` or `bunqueue start --processor` (`src/cli/client.ts:245`).
- **Invalid server response**: a frame that fails `unpack` rejects with "Invalid response from server" (`:121`).
- **Color suppression**: ANSI is emitted only when `process.stdout.isTTY` and `NO_COLOR !== '1'` (`src/cli/output.ts:19`).
- **No retry/reconnect**: unlike the SDK pool, the one-shot CLI does not retry a failed connection or resend on a dropped socket.

## Configuration
Client-affecting env vars / flags (defaults in parentheses):
- `--host`/`-H` (`localhost`) ← `HOST` / `BUNQUEUE_HOST` / `BQ_HOST`.
- `--port`/`-p` (`6789`, TCP) ← `TCP_PORT` / `BUNQUEUE_TCP_PORT` / `BQ_TCP_PORT`. HTTP probes use port + 1.
- `--token`/`-t` ← `BQ_TOKEN` / `BUNQUEUE_TOKEN` (empty string = unset).
- `--tls` / `--tls-ca <file>` / `--tls-no-verify` (plaintext by default).
- `--json` (off), `--help`/`-h`, `--version`/`-v`.
- `NO_COLOR=1` disables ANSI output.
- Connection timeout 5000 ms; command timeout 30000 ms base (`src/cli/client.ts:53`, `:171`).
- `backup` data path ← `BUNQUEUE_DATA_PATH` / `BQ_DATA_PATH` / `DATA_PATH` / `SQLITE_PATH` plus S3 vars consumed by `S3BackupManager.fromEnv`.

Server-start flags (`bunqueue start`, merged over `bunqueue.config.ts`, CLI wins): `--tcp-port` (6789), `--http-port` (6790), `--host` (0.0.0.0), `--data-path`, `--auth-tokens`, `--tls-cert`, `--tls-key`, `--config`/`-c`. Env fallbacks resolved in `resolveServerConfig`. See [Configuration & Entrypoint](./configuration.md) and [Security: TLS, Auth, CORS](./security-tls-auth.md).

## Related Docs
- [TCP Wire Protocol & Framing](./tcp-protocol.md), [TCP Server Command Handlers](./tcp-server-handlers.md)
- [Client Transport](./client-transport.md), [Client SDK: Queue](./client-queue-sdk.md), [Client SDK: Worker](./client-worker-sdk.md)
- [HTTP / REST / SSE / WebSocket API](./http-api.md), [Stats, Metrics & Monitoring](./stats-and-monitoring.md)
- [Configuration & Entrypoint](./configuration.md), [Security: TLS, Auth, CORS](./security-tls-auth.md)
- [Scheduler & Cron](./scheduler-and-cron.md), [Dead Letter Queue (DLQ)](./dead-letter-queue.md), [Webhooks, Events & Job Logs](./webhooks-and-events.md), [Worker Registry & Management](./workers-management.md), [Rate Limiting & Concurrency Control](./rate-limiting-and-concurrency.md), [S3 Backup](./backup-s3.md)
- [architecture](../architecture.md) · [data-model](../data-model.md)
