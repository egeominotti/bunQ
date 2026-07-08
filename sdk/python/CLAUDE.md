# bunqueue Python SDK — development guide

Python client SDK for the bunqueue server. Speaks ONLY the native TCP
protocol (msgpack). Parity reference: the official TypeScript client in
`../../src/client/` (TCP mode — embedded/sandboxed/QueueEvents are excluded
by design: they require the in-process Bun runtime).

## Non-negotiable rules

1. **Never touch the bunqueue core** (`../../src/`): this SDK lives here only.
2. **Max 250 lines per file** — split into modules/mixins, don't compress.
3. **Single runtime dependency: `msgpack`** — nothing else.
4. **Protocol source of truth**: `../../src/domain/types/command.ts`
   (commands), `../../src/domain/types/response.ts` (responses),
   `../../src/infrastructure/server/handlers/` (actual shapes). When in
   doubt, read the handler — never guess.
5. Every new method → matching e2e test in `tests/e2e_*.py`.
6. All docs, comments, and identifiers in English.

## Module map

| File | Role |
|---|---|
| `wire.py` | Wire helpers: `_compact` (drop None), `_js_safe` (int→float64), TLS context |
| `connection.py` | Socket + reader thread, reqId→Future pipelining, auth, lazy reconnect |
| `errors.py` | Hierarchy: `BunqueueError` → Connection/Timeout/Command/Auth + `UnrecoverableError` |
| `events.py` | Thread-safe EventEmitter (on/once/off/emit) |
| `options.py` | Pythonic kwargs → PUSH wire fields (`attempts`→`maxAttempts`, etc.) |
| `job.py` | Job wrapper: properties + per-id operations (progress, log, lock, retry…) |
| `queue.py` | Queue core: produce + control; composes the mixins |
| `queue_query.py` | Query mixin: getJob(s), states, counts, logs, children values |
| `queue_admin.py` | Admin mixin: DLQ, configs, rate limit, schedulers/cron, webhooks, monitoring |
| `worker.py` | Worker lifecycle: start/run/pause/close, events |
| `worker_runtime.py` | Worker runtime mixin: poll loop, job execution, heartbeats, registry |
| `flow.py` | FlowProducer: tree/chain/fan-in, UpdateParent, rollback |

## Wire protocol (VITAL gotchas)

- Frame = 4-byte big-endian u32 length + standard msgpack map. Request
  `{cmd, reqId, ...}`; response `{ok, reqId, ...}`; `ok:false` → `error` field.
- Auth = first command `{cmd:'Auth', token}`. `Hello` for version negotiation.
- **BigInt killer**: Python ints outside the int32 range travel as
  int64/uint64 msgpack → the server (msgpackr) decodes them as `BigInt` →
  arithmetic crash (e.g. `ListWorkers`). `_js_safe()` converts them to
  float64 (exact ≤ 2^53). NEVER remove this conversion.
- The **job name travels inside `data`**: `data = {"name": ..., **user}`.
- Responses **wrapped in `data`**: `GetLogs→data.logs`,
  `ListWorkers→data.workers`, `GetChildrenValues→data.values`,
  `AddWebhook→data.webhookId`, `ListWebhooks→data.webhooks`, `Ping→data.pong`.
- Responses **top-level**: `IsPaused→paused`, `CronGet→cron`,
  `GetProgress→progress/message`, `PULL→job+token`, `PULLB→jobs+tokens`,
  `PUSH→id`, `PUSHB→ids`, `Count/Clean→count`.
- `Progress` only works on **active** jobs (errors on waiting).
- `lifo` orders only among jobs that are **both** lifo; mixed → FIFO by runAt.
- `GetJobs`/`PromoteJobs` read from SQLite: the write buffer flushes every
  ~10ms → in tests wait with `wait_until`, never assert right after a push.
- Not-found (`GetJob`, `GetJobByCustomId`, `CronGet`) → the server answers a
  "not found" error: the SDK maps it to `None`; don't leak the exception.
- Webhooks: localhost/private URLs rejected (SSRF guard); valid events look
  like `job.completed`.
- `retry_job` over TCP = `MoveToWait`; `retry_jobs("failed")` = `RetryDlq`;
  `retry_jobs("completed")` = `RetryCompleted`.
- `FAIL` supports `unrecoverable: true` (skip retries → DLQ) and
  `stack: string[]` (persisted, capped at `stackTraceLimit`).

## Tests

```bash
python3 -m venv .venv && .venv/bin/pip install msgpack   # once
.venv/bin/python tests/test_integration.py   # smoke (8) — also pytest-compatible
.venv/bin/python tests/run_e2e.py            # full e2e (43)
```

Both spawn a real server (`bun src/main.ts` from the repo root, random port,
temp DB). `tests/harness.py` = server fixture + `@test` registry +
`wait_until`. The auth suite uses a dedicated server with `AUTH_TOKENS`.

## New-method checklist

1. Find the command in `command.ts` and the response shape in its handler.
2. Add the method in the right mixin (query/admin/core) — snake_case mirror
   of the TS name (`getJobCounts` → `get_job_counts`).
3. `_compact()` the payload; unwrap the response correctly (see gotchas).
4. Add an e2e test in the matching `tests/e2e_<area>.py`.
5. `run_e2e.py` + `test_integration.py` green. Update the README if the
   public surface changed.
