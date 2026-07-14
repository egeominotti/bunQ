# bunqueue Wire Protocol Specification

**Protocol version: 2** · Status: stable · Last updated: 2026-07-14

This is the normative specification for writing a bunqueue client in any
language. A client that satisfies every **MUST** in this document and passes
the [conformance suite](../sdk/conformance/README.md) is protocol-conformant.

Normative source of truth for exact field names and validation bounds:
[`src/domain/types/command.ts`](../src/domain/types/command.ts) (requests),
[`src/domain/types/response.ts`](../src/domain/types/response.ts) (responses),
and the handlers in
[`src/infrastructure/server/handlers/`](../src/infrastructure/server/handlers/).
Where this document and the code disagree, the code wins and this document
has a bug: file an issue.

The words MUST / MUST NOT / SHOULD are used as in RFC 2119.

---

## 1. Transport and framing

- Transport is a plain TCP stream (optionally TLS, section 8). Default port
  `6789`.
- Every message, in both directions, is one **frame**:

  ```
  +--------------------+---------------------------+
  | u32 big-endian LEN | LEN bytes of msgpack data |
  +--------------------+---------------------------+
  ```

- The payload is **standard MessagePack** (msgpack spec, no application
  record extensions). The top-level value is always a map with string keys.
- One extension type appears in **server → client** frames: the server's
  encoder (msgpackr) serializes JavaScript `undefined` as **ext type 0**
  (1 zero byte). Clients MUST tolerate ext 0 and decode it as their null
  equivalent; strict decoders that reject unknown extensions will fail on
  otherwise-valid responses (observed on Auth and GetJob replies). Clients
  MUST NOT send ext types themselves.
- Maximum frame size is **64 MiB** (`LEN <= 67108864`). A client MUST NOT
  send a larger frame; the server closes the connection on oversized frames.
- Frames MAY be pipelined: a client may send multiple requests before reading
  responses. Responses carry the request's `reqId` back (section 3), and MAY
  arrive out of order when pipelined. A strictly sequential client can rely
  on ordering but SHOULD still match `reqId`.

## 2. Session establishment

1. Open the TCP (or TLS) connection.
2. If the server was started with `AUTH_TOKENS`, the **first** command MUST
   be `{"cmd": "Auth", "token": "<token>"}`. Every other command on an
   unauthenticated connection fails. A failed `Auth` (`ok: false`) means the
   session is unusable; the client SHOULD close and surface an
   authentication error, distinct from generic command errors.
3. `Hello` is optional protocol negotiation:
   request `{"cmd": "Hello", "protocolVersion": 2, "capabilities": ["pipelining"]}`,
   response `{ok, protocolVersion, capabilities, server, version}`. The
   server currently ignores the client's declared version and always answers
   with its own (`protocolVersion: 2`). Clients SHOULD expose it so version
   drift is observable.

## 3. Message envelope

Request:

```jsonc
{ "cmd": "PUSH", "reqId": "any-string", /* command fields */ }
```

Response:

```jsonc
{ "ok": true,  "reqId": "any-string", /* result fields */ }
{ "ok": false, "reqId": "any-string", "error": "human readable reason" }
```

- `reqId` is an opaque client-chosen string echoed back verbatim. It is
  optional on the wire but REQUIRED for pipelining clients.
- `ok: false` responses MUST be mapped to a typed error by the client, with
  one mandatory special case: **"not found" lookups** (section 6.2).

## 4. Data types — the int64 rule

The server decodes msgpack with `msgpackr` under a JavaScript engine:

- msgpack **int64/uint64 values decode to `BigInt`**, and any arithmetic
  that mixes them with regular numbers **crashes the server handler**
  (e.g. `ListWorkers` computing uptime from a client-sent `startedAt`).
- Therefore a client MUST NOT emit msgpack int64/uint64 anywhere in a frame
  (fields, job data, nested structures, array elements). Any integer outside
  the int32 range `[-2147483648, 2147483647]` MUST be encoded as **float64**
  instead. IEEE 754 float64 is exact up to 2^53, which covers every
  millisecond timestamp until the year 287396.
- Integers within int32 range SHOULD be sent as msgpack ints, in their most
  compact representation. Beware libraries that pick the encoding from the
  static type rather than the value (e.g. Go's `msgpack` encoding an `int64`
  variable holding `3` as a fixed int64): configure compact/smallest-form
  integer encoding, or the int64 rule above is violated for every field.
- Map keys MUST be strings.
- Binary strings SHOULD be avoided in job data (the server round-trips them,
  but cross-language behavior of msgpack `bin` vs `str` differs); prefer
  UTF-8 text or base64.

This is the single most common way to write a broken client. The reference
implementations name the guard `jsSafe` (PHP/TS) / `_js_safe` (Python); it
is applied recursively to every outgoing frame.

## 5. The job payload contract

- The job **name travels inside `data`**: the wire `data` field is
  `{"name": <job name>, ...user data}`. There is no top-level name field on
  PUSH.
- Consequently `data` MUST be a msgpack map. If the user payload is not a
  map (scalar, list), the client MUST wrap it, conventionally as
  `{"name": ..., "payload": <value>}`.
- Collision rule: when the user payload itself contains a `name` key, the
  **user value wins** (the reference client builds `{name, ...data}`, so the
  spread overrides). Clients MUST implement the same precedence — silently
  clobbering a user field is a parity bug.
- Reserved data keys used by flows: `__parentId`, `__parentQueue`,
  `__childrenIds`, `__flowParentId`, `__flowParentIds`. Clients MUST NOT
  strip them and SHOULD NOT let user payloads collide with them.

## 6. Command reference

Exact request field names are normative in `command.ts`. This section defines
the shapes and the rules a client MUST get right. All commands below answer
`{ok: true, ...}` unless stated.

### 6.1 Producing

| Command | Key request fields | Response |
|---|---|---|
| `PUSH` | `queue`, `data`, plus job options (below) | `{id}` |
| `PUSHB` | `queue`, `jobs: [{data, ...JobInput}]` | `{ids: []}` |

Job options on `PUSH` (all optional, exact names): `priority`, `delay`,
`maxAttempts`, `backoff` (ms or `{type, delay, maxDelay}`), `ttl`, `timeout`,
`jobId`, `uniqueKey`, `dedup {ttl, extend, replace}`, `dependsOn: []`,
`parentId`, `childrenIds: []`, `tags: []`, `groupId`, `lifo`,
`removeOnComplete`, `removeOnFail`, `stallTimeout`, `durable`, `repeat`,
`debounceId`, `debounceTtl`, `stackTraceLimit`, `keepLogs`, `sizeLimit`,
`timestamp`, `failParentOnFailure`, `removeDependencyOnFailure`,
`ignoreDependencyOnFailure`, `continueParentOnFailure`.

Client MUSTs:

- SDK-level option names (e.g. `attempts`) MUST be renamed to the wire names
  (`maxAttempts`). A client MUST NOT silently drop an option it advertises —
  reject unknown options loudly (the "client drops a wire-supported field"
  bug class, issue #111).
- **`PUSH` vs `PUSHB` custom id asymmetry**: single `PUSH` takes `jobId`;
  `PUSHB` entries are typed `JobInput`, whose field is **`customId`**. A
  bulk client MUST rename `jobId → customId` per entry or custom ids are
  silently lost.
- `jobId` is idempotent: re-pushing an existing custom id returns the
  existing job's `id` instead of enqueuing a duplicate.

### 6.2 Query

| Command | Request | Response |
|---|---|---|
| `GetJob` | `id` | `{job}` |
| `GetJobByCustomId` | `queue`, `customId` | `{job}` |
| `GetJobs` | `queue`, `state` (string or list), `offset`, `limit` | `{jobs: []}` |
| `GetState` | `id` | `{state}` |
| `GetResult` | `id` | `{result}` |
| `GetProgress` | `id` | `{progress, message}` — top-level |
| `GetJobCounts` | `queue` | `{counts: {waiting, prioritized, delayed, active, completed, failed, ...}}` |
| `Count` | `queue` | `{count}` |
| `WaitJob` | `id`, `timeout` (ms, **0..600000**) | `{completed: bool, result?}` |
| `GetLogs` | `id`, `start?`, `end?` | `{data: {logs: []}}` — **wrapped** |
| `GetChildrenValues` | `id` | `{data: {values: {}}}` — **wrapped** |

Client MUSTs:

- `GetJob`, `GetJobByCustomId`, `CronGet` (and flow reads built on them)
  answer `ok: false` with an error containing "not found" for missing
  entities. The client MUST map this specific case to its language's
  null/None/nil — not throw — and MUST propagate every other error.
- `WaitJob` resolves `{completed: false}` (no result) on timeout: the client
  MUST NOT return that as a legitimate `null` result. The reference behavior
  probes `GetState`: `failed` → failure error, otherwise → timeout error.
  Timeouts beyond the server bound MUST be clamped to `[0, 600000]`, not
  forwarded.
- `GetJobs` reads from SQLite behind a ~10 ms write buffer: results are
  eventually consistent with respect to a just-issued `PUSH`.

### 6.3 Consuming (the worker loop)

| Command | Request | Response |
|---|---|---|
| `PULL` | `queue`, `owner`, `timeout?` (long-poll ms), `lockTtl?` | `{job, token}` — top-level, both `null`-ish when empty |
| `PULLB` | `queue`, `count` (**1..1000**), `timeout?` (0..60000), `owner`, `lockTtl?` | `{jobs: [], tokens: []}` |
| `ACK` | `id`, `token`, `result?` | `{}` |
| `ACKB` | `ids: []`, `tokens: []`, `results?` | `{count}` |
| `FAIL` | `id`, `token`, `error`, `stack?: string[]`, `unrecoverable?: bool` | `{}` |
| `Heartbeat` | `id` (= workerId), `activeJobs`, `processed`, `failed` | `{data: {pong}}` |
| `JobHeartbeatB` | `ids: []`, `tokens: []` | `{count}` — renews the jobs' locks |
| `RegisterWorker` | `workerId`, `name`, `queues: []`, `concurrency`, `hostname`, `pid`, `startedAt` | `{data: {...}}` |
| `UnregisterWorker` | `workerId` | `{}` |
| `ExtendLock` | `id`, `token`, `duration` | `{}` |

Semantics a client MUST implement:

- **Lease model.** A pulled job carries a lock `token`. `ACK`/`FAIL` MUST
  send it back. Locks expire after `lockTtl` (default 30000 ms) unless
  renewed via `JobHeartbeatB`/`ExtendLock`; an expired lock re-queues the
  job (at-least-once delivery).
- **`PULLB.count` MUST be clamped to 1..1000.** The server rejects larger
  counts; an unclamped worker with high concurrency wedges itself in a
  permanent error loop.
- `FAIL` and explicit failure paths require the job to be **ACTIVE** (pulled,
  valid token). Likewise `Progress` only works on active jobs.
- **Stack truncation direction.** The server persists the FIRST
  `stackTraceLimit` lines (default 10) of the `stack` array. A client MUST
  order the array so the exception message and throw site are within those
  first lines (JS/PHP/Go traces lead with the throw site; Python tracebacks
  end with it, so send the LAST lines, at most as many as the server keeps).
- `unrecoverable: true` skips remaining retries and dead-letters the job.
- A heartbeat interval of `0` (or negative/non-finite) at the SDK surface
  MUST disable heartbeats — never turn into a zero-delay loop.
- Worker registration is **per-connection server state**: after any
  reconnect the client MUST re-send `RegisterWorker`, or `skipIfNoWorker`
  schedulers silently stop firing (discussion #103 class).
- Completion callbacks/events MUST be gated on the `ACK`/`FAIL` actually
  reaching the server. If the send fails, the lock expiry retries the job:
  claiming completion would be a lie.

### 6.4 Control

`Pause`/`Resume`/`IsPaused` (`{paused}` top-level), `Drain` (`{count}`),
`Clean` (`grace`, `limit`, `state` → `{ids}`), `Obliterate`, `Cancel` (by
`id`), `Discard`, `Promote`, `PromoteJobs`, `MoveToWait` (= "retry job"),
`MoveToDelayed`, `ChangePriority`, `ChangeDelay`, `Update` (job data),
`UpdateParent` (`childId`, `parentId`).

### 6.5 DLQ

`Dlq` (`queue`, `count?` → `{jobs}` top-level), `RetryDlq` (`queue`,
`jobId?`, `count?` → `{count}`), `PurgeDlq` (→ `{count}`), `RetryCompleted`.
"Retry a failed job" is `MoveToWait`; `RetryDlq` re-queues dead-lettered
entries.

### 6.6 Schedulers (cron)

| Command | Request | Response |
|---|---|---|
| `Cron` | `name`, `queue`, `data`, `schedule?` (cron pattern), `repeatEvery?` (ms), `timezone?`, `immediately?`, **`maxLimit?`**, `uniqueKey?`, `dedup?`, `skipIfNoWorker?`, `skipMissedOnRestart?`, `preventOverlap?`, `priority?`, `jobOptions?` | `{}` |
| `CronGet` | `name` | `{cron}` — top-level; not-found → error |
| `CronList` | — | `{crons: []}` |
| `CronDelete` | `name` | `{}` |

- An SDK "limit" option MUST map to wire **`maxLimit`** (#111). `maxLimit`
  `<= 0` means unlimited.
- `jobOptions` accepts only the `CronJobOptions` subset (`maxAttempts`,
  `backoff`, `timeout`, `delay`, `stallTimeout`, `removeOnComplete`,
  `removeOnFail`) — anything else there is silently ignored by the server,
  so clients MUST map their names into it.
- A scheduler that reaches `maxLimit` is **removed** from the scheduler map:
  a subsequent `CronGet`/`CronDelete` answers "not found". Cleanup code must
  tolerate that.

### 6.7 Webhooks, rate limits, monitoring

- `AddWebhook` (`url`, `events`) → **`{data: {webhookId}}`**. Localhost and
  private-network URLs are rejected (SSRF guard). Event names look like
  `job.completed`.
- `RemoveWebhook` takes **`webhookId`**; `SetWebhookEnabled` takes **`id`**.
  Yes, they differ; both are normative.
- `ListWebhooks` → `{data: {webhooks}}`.
- `RateLimit` takes `queue` and **`limit`** (no duration field exists);
  `RateLimitClear` clears it. `SetConcurrency`/`ClearConcurrency` use `limit`.
- `ListWorkers` → `{data: {workers}}`; `Stats` → `{stats}`; `Metrics` →
  `{metrics}`; `ListQueues` → `{queues}` (names as strings); `Ping` →
  `{data: {pong}}`.

### 6.8 Response wrapping summary

Wrapped in `data`: `GetLogs → data.logs`, `ListWorkers → data.workers`,
`GetChildrenValues → data.values`, `AddWebhook → data.webhookId`,
`ListWebhooks → data.webhooks`, `Ping`/`Heartbeat → data.pong`,
`RegisterWorker → data`.

Top-level: `IsPaused → paused`, `CronGet → cron`, `CronList → crons`,
`GetProgress → progress/message`, `PULL → job+token`, `PULLB → jobs+tokens`,
`PUSH → id`, `PUSHB → ids`, `Count`/`Clean`/`Drain`/`RetryDlq` → `count`,
`Dlq → jobs`, `Stats → stats`, `GetJobCounts → counts`,
`GetFailedChildrenValues`/`GetIgnoredChildrenFailures → values`.

There is no general rule — the wrapping above is historical and normative.

## 7. Flows (parent/child trees)

The flow contract is client-orchestrated over plain `PUSH`:

1. Push **children before their parent**. Children of a not-yet-created
   parent carry data markers `__parentId: "pending"` and `__parentQueue`.
2. Push the parent with `parentId` (if it has one), `childrenIds` and
   `dependsOn` = its children's ids, and `__childrenIds` in data.
3. After the parent is created, send `UpdateParent {childId, parentId}` for
   each child to replace the placeholder.
4. On any failure mid-flow, roll back by `Cancel`-ing every job created so
   far (best-effort).

Chains are the degenerate case: each step `dependsOn` the previous id and
carries `__flowParentId` in data. Fan-in pushes N independent jobs, then a
final job with `childrenIds`/`dependsOn` = those ids.

## 8. TLS

With `TLS_CERT_FILE`/`TLS_KEY_FILE` the server speaks native TLS on the same
port. Clients MUST verify the server certificate by default (issue #109
class) and MAY offer CA-file pinning and an explicit opt-out. A client MUST
NOT silently accept unverified peers.

## 9. Timeouts, reconnection, half-open links

- Clients SHOULD apply a per-command timeout (reference default 30 s;
  long-poll commands get the poll timeout plus headroom).
- A command timeout means the stream state is unknown: the client MUST tear
  the socket down rather than keep reading (half-open guard, issue #94
  class) and reconnect lazily on the next call, re-running `Auth` (and
  `RegisterWorker` for workers).
- The server enforces: `PULLB.timeout <= 60000`, `WaitJob.timeout <=
  600000`, `PULLB.count <= 1000`. Clients MUST clamp rather than surface
  avoidable validation errors for values their API accepts.

## 10. Server limits and defaults (informative)

| Item | Value |
|---|---|
| Default attempts / backoff | 3 / 1000 ms |
| Default lock TTL | 30000 ms |
| `stackTraceLimit` default | 10 (first N lines kept) |
| Write buffer flush | ~10 ms (jobs pushed non-`durable` can be lost in a crash within this window) |
| Frame cap | 64 MiB |
| Max `PULLB` count | 1000 |
| Max long-poll / WaitJob | 60 s / 600 s |

## 11. Conformance

The machine-checkable version of this document is the conformance suite in
[`sdk/conformance/`](../sdk/conformance/). A client is **conformant** when
its driver passes every check against a real server. The suite is the
gatekeeper for calling a client "official": reference SDKs (TypeScript,
Python, PHP, Go) are kept green in CI-style runs before every release.

## 12. Versioning

- The protocol version is a single integer advertised by `Hello`
  (currently **2**). Additive changes (new commands, new optional fields)
  do not bump it; breaking changes to framing, envelope, or existing field
  semantics do.
- This document is versioned with the repository; changes to the wire MUST
  update it in the same change-set (see `CLAUDE.md` docs rule).
