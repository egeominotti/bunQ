# Configuration & Entrypoint

> **Category:** Infrastructure · **Source:** `src/config/resolve.ts`, `src/config/types.ts`, `src/config/loader.ts`, `src/config/index.ts`, `src/main.ts`, `src/infrastructure/server/storageAdapter.ts`, `src/infrastructure/server/storageManager.ts`, `src/require-bun.ts`, `src/bun-only.ts`, `src/shared/logger.ts`, `src/shared/version.ts`

## Purpose

This module is the configuration layer and process entrypoint of the server. It
auto-discovers an optional `bunqueue.config.{ts,js,mjs}` file, merges it with
environment variables and built-in defaults (config file wins), and produces
strongly-typed resolved config objects (`ResolvedConfig`, `CloudConfig`,
`S3BackupConfig`, TLS options) consumed by the server bootstrap. Storage
resolution keeps memory/SQLite as the default and selects the optional
PostgreSQL 18.6 multi-broker manager only from an explicit driver or URL. It
also owns the `bunqueue` executable's top-level dispatch (bare invocation →
server, anything else → CLI), the structured `Logger`, the package `VERSION`,
and the Bun-only runtime guards that fail fast under Node.

## Responsibilities & Scope

Owns:

- Config file discovery and dynamic import (`loadConfigFile`, `src/config/loader.ts`).
- The `BunqueueConfig` file schema and the `defineConfig` helper (`src/config/types.ts`).
- Env-var/file/default precedence resolution into typed config: `resolveServerConfig`, `resolveCloudConfig`, `resolveBackupConfig`, `resolveTlsServerOptions` (`src/config/resolve.ts`).
- Storage-driver Strategy registration, validation, manager construction, startup
  display, and retryable/coalesced async shutdown
  (`src/infrastructure/server/storageAdapter.ts` and `storageManager.ts`).
  The lifecycle Facade retains adapter ownership after a rejected shutdown.
  PostgreSQL cleanup tracks lease release, worker removal, broker unregister,
  event subscription, SQL pool, and deferred writes independently, so concurrent
  callers share one attempt while a later call can retry unfinished steps.
- Process entrypoint dispatch and logger env-var bootstrap (`src/main.ts`).
- The `Logger` class + per-component logger singletons (`src/shared/logger.ts`).
- The exported package `VERSION` (`src/shared/version.ts`).
- Bun-runtime guards: the throwing stub for the `"node"` export condition (`src/bun-only.ts`) and the defensive top-level check imported first by client entrypoints (`src/require-bun.ts`).

Does NOT own (delegated):

- Actually booting servers, installing signal/crash handlers, the graceful-shutdown loop, the stats interval, and the startup banner — all live in `bootServer` (`src/infrastructure/server/bootstrap.ts`). `main.ts` only loads config and calls `bootServer`.
- CLI flag parsing and command routing — see [CLI](./cli.md) (`src/cli/`).
- TLS socket creation, auth enforcement, CORS — see [Security: TLS, Auth, CORS](./security-tls-auth.md). This module only resolves the cert/key paths and validates that both (or neither) are set.
- S3 backup execution and Cloud telemetry — this module only resolves their config; see [S3 Backup](./backup-s3.md) and [bunqueue Cloud Dashboard Integration](./cloud-integration.md).

## Dependencies

Internal:

- `src/config/types.ts` → `BunqueueConfig`, `defineConfig`.
- `src/config/resolve.ts` imports `CloudConfig` (`src/infrastructure/cloud/types`), `S3BackupConfig` + `DEFAULTS` (`src/infrastructure/backup/s3BackupConfig`).
- `src/main.ts` imports `loadConfigFile`/`resolveServerConfig` (`src/config`), `bootServer` ([Core Queue Engine](./core-queue-engine.md) bootstrap), and `Logger`/`LogLevel` (`src/shared/logger.ts`).
- `src/shared/version.ts` imports the root `package.json`.

External / runtime:

- Bun runtime — `Bun.env` for env reads, `import.meta.main` for entrypoint detection. `bun-only.ts`/`require-bun.ts` exist precisely because there is no Node fallback.
- Node builtins used by the loader: `node:fs` (`existsSync`), `node:path` (`resolve`, `join`), and `os` (`hostname`) in `resolve.ts`.
- Dynamic `import()` of the user config file.

## Public Interface

Exported from `src/config/index.ts`:

```typescript
export function defineConfig(config: BunqueueConfig): BunqueueConfig
export type BunqueueConfig
export function loadConfigFile(explicitPath?: string): Promise<BunqueueConfig | null>
export function resolveServerConfig(fileConfig: BunqueueConfig | null): ResolvedConfig
export function resolveCloudConfig(fileConfig: BunqueueConfig | null, dataPath?: string): CloudConfig | null
export function resolveBackupConfig(fileConfig: BunqueueConfig | null, databasePath: string): S3BackupConfig
export function resolveTlsServerOptions(config: { tlsCertFile?: string; tlsKeyFile?: string }): { certFile: string; keyFile: string } | null
export type ResolvedConfig
```

Re-exported from the package root (`src/main.ts:29-30`), so user config files can `import { defineConfig } from 'bunqueue'`:

```typescript
export { defineConfig } from './config';
export type { BunqueueConfig } from './config';
```

`Logger` (`src/shared/logger.ts`):

```typescript
type LogLevel = 'debug' | 'info' | 'warn' | 'error'
class Logger {
  static enableJsonMode(): void
  static disableJsonMode(): void
  static setLevel(level: LogLevel): void
  debug/info/warn/error(message: string, data?: Record<string, unknown>): void
}
function createLogger(component: string): Logger
// singletons: serverLog, tcpLog, httpLog, wsLog, cronLog, statsLog, storageLog, queueLog, webhookLog, backupLog
```

`src/shared/version.ts`: `export const VERSION = pkg.version`.

No TCP commands, HTTP endpoints, CLI commands, or emitted events are defined in this module.

## Data Models

`ResolvedConfig` (`src/config/resolve.ts:13-28`) — the flat, fully-resolved server shape consumed by `bootServer`:

```typescript
interface ResolvedConfig {
  tcpPort: number;
  httpPort: number;
  hostname: string;
  tcpSocketPath: string | undefined;
  httpSocketPath: string | undefined;
  tlsCertFile: string | undefined;
  tlsKeyFile: string | undefined;
  authTokens: string[];
  dataPath: string | undefined;
  storageDriver: 'memory' | 'sqlite' | 'postgres';
  postgresUrl: string | undefined;
  postgresNamespace: string;
  postgresBrokerId: string | undefined;
  postgresPoolSize: number;
  postgresLeaseDurationMs: number;
  postgresPollIntervalMs: number;
  corsOrigins: string[];
  requireAuthForMetrics: boolean;
  maxPrometheusQueues: number;
  s3BackupEnabled: boolean;
  shutdownTimeoutMs: number;
  statsIntervalMs: number;
}
```

`BunqueueConfig` (`src/config/types.ts`) — the optional config-file schema, with
all sections optional: `server`, `auth`, `storage`, `telemetry`, `cors`,
`cloud`, `backup`, `timeouts`, `webhooks`, `logging`. Note the file schema is a
superset/subset mix of `ResolvedConfig`: e.g. `timeouts.worker`/`timeouts.lock`,
`webhooks.*`, and `logging.*` exist in the file schema but are not surfaced
through `resolveServerConfig` (they are read elsewhere or only via env). See
[data-model](../data-model.md) for the full job/queue types.

`LogEntry` (`src/shared/logger.ts:8-15`): `{ timestamp, level, component, message, reqId?, data? }` — emitted only in JSON mode.

## Business Logic / Control Flow

### Entrypoint dispatch (`src/main.ts`)

1. `if (import.meta.main)` (`main.ts:11`) — the dispatch only runs when this file is the program entry. This guard exists because the root export re-exports `defineConfig`; user config files importing `bunqueue` would otherwise re-run the CLI/server on every import (Issue #85, comment at `main.ts:8-10`).
2. `firstArg = process.argv[2]` (`main.ts:12`). If absent → `startServer()` (`main.ts:19-20`). Otherwise → dynamic `import('./cli/index').then(({ main }) => main())` (`main.ts:22`). The server boots **only** for a bare `bunqueue`; `start` and flag-led argv go through the CLI, which calls the same `bootServer` (`src/cli/commands/server.ts:132`).
3. `startServer` (`main.ts:33-38`): `loadConfigFile()` → `resolveServerConfig(fileConfig)` → `bootServer(fileConfig, config)`.
4. Logger env bootstrap (`main.ts:43-55`), also gated on `import.meta.main`: `LOG_FORMAT === 'json'` → `Logger.enableJsonMode()`; `LOG_LEVEL` (lowercased, validated against the four levels) → `Logger.setLevel()`. `bootServer` re-applies the same logic from file config first, falling back to env (`bootstrap.ts:74-81`).

### Config file loading (`src/config/loader.ts`)

- With `explicitPath`: `resolve()` to absolute; throw `Config file not found: <abs>` if missing (`loader.ts:14-19`).
- Without: iterate `['bunqueue.config.ts', 'bunqueue.config.js', 'bunqueue.config.mjs']` in `process.cwd()`, return the first that exists, else `null` (`loader.ts:22-30`).
- `importConfig` dynamically imports the file and returns `mod.default ?? mod` (`loader.ts:33-36`).

### Resolution precedence (`src/config/resolve.ts`)

Every field follows **config file > env var > default**. Key cases:

- Ports: `parseInt(Bun.env.TCP_PORT ?? '6789', 10)` / `parseInt(Bun.env.HTTP_PORT ?? '6790', 10)` (`resolve.ts:36-37`).
- `dataPath` precedence chain (`resolve.ts:44-49`): `storage.dataPath` → `BUNQUEUE_DATA_PATH` → `BQ_DATA_PATH` → `DATA_PATH` → `SQLITE_PATH`. If all are unset the server runs in-memory (no SQLite).
- Storage driver resolution is explicit-first. `storage.driver` or
  `BUNQUEUE_STORAGE_DRIVER` accepts only `memory`, `sqlite`, or `postgres`; an
  unsupported value throws. Without an explicit driver, `storage.url` /
  `BUNQUEUE_POSTGRES_URL` selects PostgreSQL, otherwise a data path selects
  SQLite, otherwise memory.
- PostgreSQL settings resolve from `storage.url`, `namespace`, `brokerId`,
  `poolSize`, `leaseDurationMs`, and `pollIntervalMs`, with their
  `BUNQUEUE_POSTGRES_*` environment equivalents. Numeric values are normalized
  to positive integers before the runtime applies its safety minimums.
- `authTokens` / `corsOrigins`: comma-split env, `.filter(Boolean)`, default `[]` (`resolve.ts:43`, `:50`).
- `s3BackupEnabled`: `S3_BACKUP_ENABLED` accepts `'1'` or `'true'` (`resolve.ts:52-54`).
- `requireAuthForMetrics`: `METRICS_AUTH === 'true'` (`resolve.ts:51`).
- `maxPrometheusQueues`: non-negative integer from
  `telemetry.maxPrometheusQueues` or `METRICS_MAX_QUEUES`, default `100`.
  Invalid/negative values fall back to the default and `0` disables labelled
  per-queue series.

### Cloud config (`resolveCloudConfig`, `resolve.ts:86-120`)

Returns `null` (disabled) unless **both** `url` and `apiKey` resolve (`resolve.ts:94`). If `instanceId` is missing it logs `[Cloud] BUNQUEUE_CLOUD_INSTANCE_ID is required` and returns `null` (`resolve.ts:96-100`). Trailing slashes are stripped from `url` (`resolve.ts:103`). Booleans default _on_ via `!== 'false'` (`includeJobData`, `useWebSocket`, `useHttp`, `remoteCommands`); `instanceName` defaults to `hostname()`.

### Backup config (`resolveBackupConfig`, `resolve.ts:123-144`)

Reads S3 credentials with AWS fallbacks (`S3_ACCESS_KEY_ID ??
AWS_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY ?? AWS_SECRET_ACCESS_KEY`,
`S3_SESSION_TOKEN ?? AWS_SESSION_TOKEN`, etc.) and propagates
`virtualHostedStyle` to Bun's S3 client. Numeric env are guarded with
`parseInt(...) || DEFAULT` so an empty/NaN env falls back to `S3_DEFAULTS`.
The resolved `databasePath` is mandatory for the backup module.

### TLS resolution (`resolveTlsServerOptions`, `resolve.ts:66-83`)

- Neither cert nor key set → `null` (TLS off).
- Exactly one set → **throws** (`TLS misconfigured: ...`), so the operator never silently serves plaintext when expecting TLS.
- Both set → `{ certFile, keyFile }`. `bootServer` calls this inside try/catch and `process.exit(1)` on the partial-config error before binding any socket (`bootstrap.ts:87-93`).

### Bun-only guards

- `src/bun-only.ts`: top-level `throw` — wired to the `"node"` export condition in `package.json` (`.`, `./client`, `./queue`, `./mcp`, `./workflow` all map `"node"` → `./dist/bun-only.js`). A `node` import fails fast with an actionable message instead of an `ERR_UNSUPPORTED_DIR_IMPORT` resolver crash. Bun resolves the real entry via the higher-priority `"bun"` condition.
- `src/require-bun.ts`: defense-in-depth for bundlers that inline the real client and run it on Node — `if (typeof globalThis.Bun === 'undefined') throw`. Imported **first** by `src/client/index.ts:23` and `src/client/workflow/index.ts:21` so it runs before any module touching `Bun.*` at top level. No-op under Bun; relative-import-free by design.

## Concurrency & Locking

N/A. This module is synchronous config resolution plus one-shot dispatch; it
holds no locks. `bootServer` delegates graceful teardown to
`shutdownCoordinator.ts`. The first signal memoizes one cleanup promise, active
jobs drain within `shutdownTimeoutMs`, optional cleanup is best-effort, and
storage gets two bounded close attempts before the coordinator exits 0 or 1.

## Edge Cases & Failure Modes

- **Idempotent import (Issue #85):** without the `import.meta.main` guards (`main.ts:11`, `:43`), importing `bunqueue` for `defineConfig` would re-run dispatch and the logger env mutation, causing `Failed to listen at 0.0.0.0`. Both side-effecting blocks are gated.
- **Partial TLS config:** one of cert/key set → hard error (`resolve.ts:72-81`); `bootServer` exits 1 (`bootstrap.ts:88-93`).
- **Missing explicit config path:** `loadConfigFile(path)` throws; auto-discovery instead returns `null` and the server proceeds on env+defaults.
- **`importConfig` fallback:** `mod.default ?? mod` tolerates both `export default` and bare module-shaped config (`loader.ts:35`).
- **Empty/NaN numeric env:** ports use `parseInt(env ?? 'default')` (an invalid `TCP_PORT` yields `NaN`, not the default); backup numerics use `parseInt(...) || DEFAULT` so they recover from NaN — an intentional asymmetry to note.
- **Cloud disabled silently:** missing `url`/`apiKey` returns `null` with no log; missing `instanceId` returns `null` _with_ an error log (`resolve.ts:98`).
- **Logger level filtering:** messages below the configured `Logger.level` are dropped (`logger.ts:64`); level state is static/global — `Logger.setLevel`/`enableJsonMode` mutate process-wide state, which is why `main.ts` gates them behind `import.meta.main`.
- **In-memory mode:** an unset data path makes `dataPath` `undefined`;
  when backup is disabled the banner reports `Storage  in-memory · ephemeral`.
  If S3 backup is enabled, `backupStartupError()` makes this a fatal
  configuration error and `bootServer` exits 1 before either listener binds;
  backup cannot silently remain disabled. The banner uses `●` for configured
  endpoints/enabled features, `○` for disabled options, and `•` for neutral
  runtime information; its product line is `One queue. Any language.` because
  only the server and embedded runtime, not the network clients, require Bun.
- **Ambiguous storage:** PostgreSQL plus any SQLite data path is a startup error;
  explicit SQLite without a data path is also an error. No listener binds before
  storage initializes successfully.
- **Backup boundary:** S3 backup is accepted only for persistent SQLite. Enabling
  it for memory or PostgreSQL fails startup rather than pretending to protect a
  database it cannot snapshot.
- **PostgreSQL lifecycle:** `createServerQueueManager()` awaits schema/event
  initialization. On failure it closes the partial pool; graceful shutdown calls
  `shutdownPostgres()` so leases, workers, broker registration, listeners, and
  the SQL pool are released in order. A transient rejection is retried once;
  another signal cannot start a competing cleanup, and permanent failure cannot
  strand the process behind the re-entrancy guard.

## Configuration

Resolved by `resolveServerConfig` (defaults in parentheses):

| Env var                                                             | Config-file path                | Default                      |
| ------------------------------------------------------------------- | ------------------------------- | ---------------------------- |
| `TCP_PORT`                                                          | `server.tcpPort`                | `6789`                       |
| `HTTP_PORT`                                                         | `server.httpPort`               | `6790`                       |
| `HOST`                                                              | `server.host`                   | `0.0.0.0`                    |
| `TCP_SOCKET_PATH`                                                   | `server.tcpSocketPath`          | `undefined`                  |
| `HTTP_SOCKET_PATH`                                                  | `server.httpSocketPath`         | `undefined`                  |
| `TLS_CERT_FILE`                                                     | `server.tlsCertFile`            | `undefined`                  |
| `TLS_KEY_FILE`                                                      | `server.tlsKeyFile`             | `undefined`                  |
| `AUTH_TOKENS` (comma-split)                                         | `auth.tokens`                   | `[]`                         |
| `BUNQUEUE_STORAGE_DRIVER`                                           | `storage.driver`                | inferred from URL/data path  |
| `BUNQUEUE_DATA_PATH` > `BQ_DATA_PATH` > `DATA_PATH` > `SQLITE_PATH` | `storage.dataPath`              | `undefined` (in-memory)      |
| `BUNQUEUE_POSTGRES_URL`                                             | `storage.url`                   | `undefined`                  |
| `BUNQUEUE_POSTGRES_NAMESPACE`                                       | `storage.namespace`             | `default`                    |
| `BUNQUEUE_BROKER_ID`                                                | `storage.brokerId`              | generated host/PID/random ID |
| `BUNQUEUE_POSTGRES_POOL_SIZE`                                       | `storage.poolSize`              | `10`                         |
| `BUNQUEUE_POSTGRES_LEASE_DURATION_MS`                               | `storage.leaseDurationMs`       | `30000`                      |
| `BUNQUEUE_POSTGRES_POLL_INTERVAL_MS`                                | `storage.pollIntervalMs`        | `250`                        |
| `CORS_ALLOW_ORIGIN` (comma-split)                                   | `cors.origins`                  | `[]`                         |
| `METRICS_AUTH` (`=== 'true'`)                                       | `auth.requireAuthForMetrics`    | `false`                      |
| `METRICS_MAX_QUEUES`                                                | `telemetry.maxPrometheusQueues` | `100`                        |
| `S3_BACKUP_ENABLED` (`1`/`true`)                                    | `backup.enabled`                | `false`                      |
| `SHUTDOWN_TIMEOUT_MS`                                               | `timeouts.shutdown`             | `30000`                      |
| `STATS_INTERVAL_MS`                                                 | `timeouts.stats`                | `300000`                     |

Logging (applied in `main.ts` and re-applied in `bootServer`): `LOG_FORMAT=json` (file: `logging.format`) enables JSON mode; `LOG_LEVEL=debug|info|warn|error` (file: `logging.level`) sets the floor (`info` default). Cloud and S3 env vars resolved by `resolveCloudConfig`/`resolveBackupConfig` — see [bunqueue Cloud Dashboard Integration](./cloud-integration.md) and [S3 Backup](./backup-s3.md).

## Related Docs

- [CLI](./cli.md) — the other entrypoint path; reuses `loadConfigFile`/`resolveServerConfig` and the shared `bootServer`.
- [Security: TLS, Auth, CORS](./security-tls-auth.md) — consumers of the TLS/auth/CORS resolution.
- [S3 Backup](./backup-s3.md), [bunqueue Cloud Dashboard Integration](./cloud-integration.md) — consumers of `resolveBackupConfig`/`resolveCloudConfig`.
- [PostgreSQL 18.6 Multi-Broker Persistence](./postgres-multibroker.md) — consumer
  of the resolved PostgreSQL driver, URL, namespace, broker, pool, lease, and
  polling fields.
- [Core Queue Engine (QueueManager & Shards)](./core-queue-engine.md) — `bootServer` wiring and the `dataPath` consumer.
- [Stats, Metrics & Monitoring](./stats-and-monitoring.md) — the periodic stats interval driven off `statsIntervalMs`.
- [architecture](../architecture.md), [data-model](../data-model.md).
