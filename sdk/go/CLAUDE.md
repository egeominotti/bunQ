# bunqueue Go SDK — development guide

Go client SDK for the bunqueue server. Speaks ONLY the native TCP protocol
(msgpack). Parity references: the official TypeScript client in
`../../src/client/` (TCP mode) and the sibling SDKs in `../python/`, `../php/`
and `../typescript/`.
The cross-module correctness checklist is [`INVARIANTS.md`](INVARIANTS.md).

## Non-negotiable rules

1. **Never touch the bunqueue core** (`../../src/`): this SDK lives here only.
2. **Single production dependency: `github.com/vmihailenco/msgpack/v5`.**
   Rapid 1.3.0 is test-only; Gremlins is installed as a pinned external tool.
3. **jsSafe is load-bearing — NEVER remove it** (`wire.go`): any int outside
   the int32 range must travel as float64. msgpack int64 → the server
   (msgpackr) decodes BigInt → arithmetic/serialization crash.
4. **Compact ints on encode** (`encodeFrame`, connection_wire.go): the encoder MUST
   run with `UseCompactInts(true)`, or even tiny int64 values are written as
   msgpack int64 and hit the same BigInt crash class.
5. **Ext id 0 = msgpackr `undefined`**: the registered ext decoder and recursive
   normalizer map it to `nil`; removing either makes responses with undefined
   fields fail or expose a private placeholder.
6. **Protocol source of truth**: `../../src/domain/types/command.ts`
   (commands), `../../src/domain/types/response.ts` (responses),
   `../../src/infrastructure/server/handlers/` (actual shapes). When in
   doubt, read the handler — never guess.
7. ~300 lines max per file; every new method → matching e2e test.
8. All docs, comments, identifiers in English.

## Module map

| File | Role |
|---|---|
| `wire.go` / `wire_safety.go` | Protocol constants/helpers plus reflection-recursive `jsSafe`: typed integers, cycle/key rejection and `time.Time` normalization |
| `errors.go` | `ConnectionError`, `CommandTimeoutError`, `CommandError`, `AuthError`, `UnrecoverableError`, not-found detection |
| `connection.go` / `connection_wire.go` | Mutex-serialized sync connection, framing, Auth-first, lazy reconnect + generation, absolute timeout teardown, TLS verify-by-default, compact encoding and recursive ext-0 normalization |
| `telemetry.go` | Optional payload-free lifecycle/command telemetry, delivered outside the connection mutex with panic isolation |
| `options.go` | `JobOptions` → wire fields (attempts→maxAttempts, deduplication→uniqueKey+dedup, debounce→debounceId/Ttl); unknown keys error |
| `job.go` | Job wrapper: accessors + per-id operations (progress, log, extend lock) |
| `queue.go` | Queue core: `Add`, `AddBulk` (jobId→customId rename) |
| `queue_query.go` / `queue_control.go` / `queue_admin.go` | Query / control / admin areas (not-found→nil, unwrap contracts, scheduler booleans and unique key, webhook `id` vs `webhookId`, rate duration/TTL) |
| `worker.go` / `worker_process.go` | Concurrent worker with independent pull, command and heartbeat sockets; bounded goroutine pool, optional heartbeats, panic stacks, ACK-gated completion and generation-safe registration |
| `flow_planner.go` / `flow_id.go` | Pure atomic graph planning, validation and secure preallocated IDs |
| `flow_snapshots.go` / `flow_commit.go` | Pure authoritative snapshot validation plus the single `PUSHF` transport boundary |
| `flow.go` | Public `FlowProducer`, result-tree construction, flat chains and `GetFlow` |
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
- Flow validation must finish before `callFlow`. The planner owns reciprocal
  links and rejects user topology fields; snapshot ID and queue must both match.
  Never reintroduce backpatch/rollback commands.
- `ChainStep` intentionally has no `Children`; keep flat chains a compile-time
  guarantee instead of adding a runtime-only nested shape.

## Tests

```bash
go vet ./...
go test -run 'FlowPlanner|FlowProducerRejectsOwnedTopology|FlowCommit|RandomFlowID' -count=1 -v ./...
# Gremlins 0.6.0 installed with the command in README:
gremlins unleash --config .gremlins.yaml
go test -v ./... -count=1 -timeout 600s
go test -race -run 'Hardening|Regression|Worker' ./...
# -timeout must exceed the soak: `go test` panics at its own 10m default.
BUNQUEUE_SDK_SOAK_SECONDS=3600 go test -run '^TestSDKSoak$' -timeout 3900s -v
cd ../.. && bun run test:sandbox:sdk
```

Requires `bun` on PATH and the repo checkout (the harness walks up to find
`src/main.ts` and spawns a real server on a random port). Any SDK change must
finish with the isolated `test:sandbox:sdk` gate.
Preserve Rapid's seed and failfile before fixing a generated failure; the
mutation report is `build/gremlins.json`.
