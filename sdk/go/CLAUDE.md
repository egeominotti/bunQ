# bunqueue Go SDK — development guide

Go client SDK for the bunqueue server. Speaks ONLY the native TCP protocol
(msgpack). Parity references: the official TypeScript client in
`../../src/client/` (TCP mode) and the sibling SDKs in `../python/`, `../php/`
and `../typescript/`.

## Non-negotiable rules

1. **Never touch the bunqueue core** (`../../src/`): this SDK lives here only.
2. **Single runtime dependency: `github.com/vmihailenco/msgpack/v5`.**
3. **jsSafe is load-bearing — NEVER remove it** (`wire.go`): any int outside
   the int32 range must travel as float64. msgpack int64 → the server
   (msgpackr) decodes BigInt → arithmetic/serialization crash.
4. **Compact ints on encode** (`encodeFrame`, connection.go): the encoder MUST
   run with `UseCompactInts(true)`, or even tiny int64 values are written as
   msgpack int64 and hit the same BigInt crash class.
5. **Ext id 0 = msgpackr `undefined`**: the registered ext decoder maps it to
   a placeholder; removing it makes any response with an undefined field fail
   as "unknown ext id".
6. **Protocol source of truth**: `../../src/domain/types/command.ts`
   (commands), `../../src/domain/types/response.ts` (responses),
   `../../src/infrastructure/server/handlers/` (actual shapes). When in
   doubt, read the handler — never guess.
7. ~300 lines max per file; every new method → matching e2e test.
8. All docs, comments, identifiers in English.

## Module map

| File | Role |
|---|---|
| `wire.go` | Protocol consts, `compact`, `jsSafe`, `jobPayload`, decode helpers (`asInt` handles every int width — interface decode returns int8/uint8/...) |
| `errors.go` | `ConnectionError`, `CommandTimeoutError`, `CommandError`, `AuthError`, `UnrecoverableError`, not-found detection |
| `connection.go` | Mutex-serialized sync connection: framing, Auth-first, lazy reconnect + generation, timeout teardown (half-open guard), TLS verify-by-default, `encodeFrame`, ext-0 decoder |
| `options.go` | `JobOptions` → wire fields (attempts→maxAttempts, deduplication→uniqueKey+dedup, debounce→debounceId/Ttl); unknown keys error |
| `job.go` | Job wrapper: accessors + per-id operations (progress, log, extend lock) |
| `queue.go` | Queue core: `Add`, `AddBulk` (jobId→customId rename) |
| `queue_query.go` / `queue_control.go` / `queue_admin.go` | Query / control / admin areas (not-found→nil, unwrap contracts, scheduler limit→maxLimit, webhook `id` vs `webhookId`, RateLimit `limit`) |
| `worker.go` | Concurrent worker: PULLB loop, bounded goroutine pool, heartbeat goroutine (disabled ≤ 0 / `DisableHeartbeat`), panic recovery with real stacks, ACK-gated completion, re-register on generation change |
| `flow.go` | FlowProducer: children-first trees, `UpdateParent`, chains, `GetFlow`, rollback |
| `*_test.go` | e2e suites against a real server (`TestMain` shared instance + dedicated ones for crash/auth) |

## Wire gotchas (mirror of ../python/CLAUDE.md — all apply)

- Frame = u32 BE length + standard msgpack map; Auth is the first command.
- Job name INSIDE `data`; `Data()` strips it.
- Wrapped in `data`: GetLogs→logs, ListWorkers→workers, GetChildrenValues→values,
  AddWebhook→webhookId, ListWebhooks→webhooks, Ping→pong. Top-level: IsPaused→paused,
  CronGet→cron, GetProgress→progress/message, PULLB→jobs+tokens, PUSH→id, Count→count.
- FAIL requires an ACTIVE job (pull first, pass the token); supports
  `unrecoverable` and `stack` (server keeps the FIRST `stackTraceLimit` lines,
  default 10 — lead with the message).
- A scheduler that reached its execution limit is REMOVED server-side
  (CronGet → not found).
- `GetJobs` reads from SQLite (10ms write buffer): tests must `waitUntil`.
- Webhook URLs to localhost are SSRF-rejected: use https://example.com in tests.

## Tests

```bash
go vet ./...
go test ./... -count=1 -timeout 600s
```

Requires `bun` on PATH and the repo checkout (the harness walks up to find
`src/main.ts` and spawns a real server on a random port).
