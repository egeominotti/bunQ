# bunqueue Python SDK — development guide

Python client SDK for the bunqueue server. Speaks ONLY the native TCP
protocol (msgpack). Parity reference: the official TypeScript client in
`../../src/client/` (TCP mode — embedded/sandboxed/QueueEvents are excluded
by design: they require the in-process Bun runtime).

The normative behavioral contract is
[`INVARIANTS.md`](./INVARIANTS.md). Read it before changing transport,
serialization, Queue, Worker, FlowProducer, query, or admin code.

## Non-negotiable rules

1. **Never touch the bunqueue core** (`../../src/`): this SDK lives here only.
2. **Max 250 lines per file** — split into modules/mixins, don't compress.
3. **Single runtime dependency: `msgpack`** — pytest, Hypothesis, and mutmut
   are optional development extras, never runtime dependencies.
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
| `flow.py` | FlowProducer public creation/read API |
| `flow_plan.py` / `flow_plan_legacy.py` | Pure ID allocation and closed tree/chain/fan-in planning |
| `flow_commit.py` | One `PUSHF` call plus exact snapshot ID/queue validation |
| `simple/app.py` + `simple/app_api.py` | `Bunqueue` Simple Mode: constructor + processing pipeline, API mixin |
| `simple/{retry,circuit_breaker,batch,triggers,aging,cancellation,ttl,dedup_debounce}.py` | Simple Mode subsystems, 1:1 with `src/client/bunqueue/` |

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
python3 -m venv .venv && .venv/bin/pip install -e '.[test,mutation]'
.venv/bin/python -m pytest tests/test_flow_plan_property.py \
  tests/test_flow_plan_validation.py tests/test_flow_plan_limits.py \
  tests/test_flow_plan_contract.py tests/test_flow_plan_wire_contract.py \
  tests/test_flow_commit.py \
  --hypothesis-seed=20260730
.venv/bin/mutmut run                       # final gate; Python 3.10+
.venv/bin/python tests/test_integration.py   # smoke (8) — also pytest-compatible
.venv/bin/python tests/run_e2e.py            # full e2e (112)
BUNQUEUE_SDK_SOAK_SECONDS=3600 .venv/bin/python tests/soak.py
```

The integration and E2E runners spawn a real server (`bun src/main.ts` from
the repo root, random port, temp DB). Pure property and mutation tests do not.
Replay failures with the printed Hypothesis seed or `@reproduce_failure` blob.
`tests/harness.py` = server fixture + `@test` registry + `wait_until`. The auth
suite uses a dedicated server with `AUTH_TOKENS`.

## New-method checklist

1. Find the command in `command.ts` and the response shape in its handler.
2. Add the method in the right mixin (query/admin/core) — snake_case mirror
   of the TS name (`getJobCounts` → `get_job_counts`).
3. `_compact()` the payload; unwrap the response correctly (see gotchas).
4. Add an e2e test in the matching `tests/e2e_<area>.py`.
5. `run_e2e.py` + `test_integration.py` green. Update the README if the
   public surface changed.
