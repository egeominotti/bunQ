<div align="center">

<a href="https://bunqueue.dev">
  <img src="https://raw.githubusercontent.com/egeominotti/bunqueue/main/.github/logo.png" alt="bunqueue logo" width="110" />
</a>

# bunqueue-client (Python)

**The official Python client for [bunqueue](https://bunqueue.dev), the high performance job queue server.**

Native TCP protocol (msgpack, pipelined), one runtime dependency (`msgpack`), sync plus thread based workers.

[![pypi](https://img.shields.io/pypi/v/bunqueue-client?color=d3156d&label=pypi)](https://pypi.org/project/bunqueue-client/)
[![downloads](https://img.shields.io/pypi/dm/bunqueue-client?color=ff4f9f)](https://pypi.org/project/bunqueue-client/)
[![license](https://img.shields.io/badge/license-MIT-1a1a2e)](https://github.com/egeominotti/bunqueue/blob/main/sdk/python/LICENSE)
[![python](https://img.shields.io/badge/python-3.9%2B-2ea44f)](https://github.com/egeominotti/bunqueue/tree/main/sdk/python)
[![conformance](https://img.shields.io/badge/protocol-conformant%2017%2F17-d3156d)](https://github.com/egeominotti/bunqueue/tree/main/sdk/conformance)

[Documentation](https://bunqueue.dev/guide/sdks/) · [Protocol spec](https://github.com/egeominotti/bunqueue/blob/main/docs/protocol.md) · [Server](https://github.com/egeominotti/bunqueue) · [Changelog](https://github.com/egeominotti/bunqueue/blob/main/sdk/python/CHANGELOG.md)

</div>

---

Python client for [bunqueue](https://github.com/egeominotti/bunqueue), the
high-performance job queue server for Bun. Talks the native TCP protocol
(msgpack, pipelined) with the same core API as the TypeScript SDK, including
opt-in ACK batching and dependency-free structured transport telemetry.
Connection pooling remains TypeScript-only for now.

The bunqueue **server** runs on Bun (or as a compiled binary / Docker). This
SDK lets any Python service produce and consume jobs on it: *one queue, any
language*.

## Install

```bash
pip install bunqueue-client
# dependency: msgpack; import name: bunqueue
```

Requires Python ≥ 3.9 and a running bunqueue server (`bunx bunqueue start`).

## Quick start

```python
from bunqueue import Queue, Worker

# Producer
queue = Queue("emails", host="localhost", port=6789)
job = queue.add("send", {"to": "user@example.com"}, priority=5, attempts=3)
print(job.id)

# Consumer
def process(job):
    job.update_progress(50)
    return {"sent": True}

worker = Worker("emails", process, concurrency=10)
worker.on("completed", lambda job, result: print(job.id, result))
worker.run()  # blocking; or worker.start() for a background thread
```

Job names are protocol metadata, not part of the user payload. `job.name`
comes from top-level `name`, while `job.data` preserves the submitted object,
list, scalar, or `None`, including a user-owned `"name"` key. Readers retain a
legacy `data["name"]` fallback, and scheduler templates use distinct
`jobName` and `data` fields.
The client negotiates protocol v3 and advertises `separate-job-name` in `Hello`.

All queue semantics (retry, backoff, DLQ, priorities, stall detection, cron)
run **server-side** — the worker only pulls, heartbeats, and acks.
If a broker timeout finalizes a lease before its processor returns, the
successful `applied: false` ACK/FAIL outcome is authoritative: the Worker
releases its local slot but emits no false `completed`/`failed` event and does
not increment the matching counter. ACK batching applies the same rule by
exact `ignoredIndices`, so duplicate job IDs cannot confuse lease generations;
structured `ignoredIds` without positional indices is rejected as ambiguous.
Worker-owned `job.discard()` sends the same delivery token as ACK/FAIL, so an
older processor cannot discard a newer active generation.

## Feature surface

- **Queue**: `add`, `add_bulk`, full job options (priority, delay, attempts,
  backoff, ttl, timeout, job_id, deduplication, depends_on, tags, group_id,
  lifo, remove_on_complete/fail, durable, repeat, debounce, …)
- **Query**: `get_job`, `get_job_by_custom_id`, `get_jobs` (+ per-state
  helpers), `get_state`, `get_result`, `get_progress`, `wait_for_job`
  (raises `CommandTimeoutError` on timeout, BullMQ contract),
  counts (+ per-priority), children values, job logs
- **Control**: pause/resume/drain/obliterate/clean, remove, discard,
  promote (single/bulk), retry_job / retry_jobs, move to wait/delayed,
  change priority/delay, update data, extend lock
- **DLQ**: `get_dlq`, `retry_dlq`, `purge_dlq`, DLQ config
- **Schedulers**: `upsert_job_scheduler` (cron pattern or interval),
  `add_cron` / `every` shorthands, get/list/remove
- **Admin**: rate limit with custom windows
  (`set_global_rate_limit(max_jobs, duration_ms)`), global concurrency, stall config, webhooks,
  stats/metrics/list_queues/get_workers
- **Worker**: events (`ready`, `active`, `completed`, `failed`, `progress`,
  `drained`, `error`, `closed`), pause/resume, graceful `close()` (also a
  context manager), automatic lock heartbeats (jobs longer than the lock TTL
  survive), `UnrecoverableError` to skip retries, opt-in ACK batching
  (`ack_batch={"max_size": 50, "max_delay_ms": 5}`: successful ACKs flush as
  one `ACKB` on size/delay/close; a job stays active until its batch settles;
  broker-ignored positions do not emit completion events)
- **FlowProducer**: `add` (parent/child trees), `add_bulk`, `add_chain`
  (sequential), `add_bulk_then` (fan-in), `get_flow`, atomic broker commit
- **Simple Mode** (`Bunqueue`): Queue + Worker in one object — routes,
  onion middleware, in-process retry (fixed/exponential/jitter/fibonacci/
  custom + `retry_if`), circuit breaker, batch accumulation, event triggers,
  job TTL, priority aging, cooperative cancellation (`get_signal`),
  dedup/debounce defaults, cron shorthands — 1:1 with the official client
  (TCP mode; `embedded` raises)
- **Connection**: auth token, TLS (`tls=True`, `{"ca_file": ...}`,
  `{"verify": False}`), pipelining, lazy reconnect, structured telemetry

Not applicable outside Bun (by design): embedded mode, sandboxed workers,
`QueueEvents` (in-process subscription; use webhooks or SSE/WS on the HTTP
port instead).

## Flows

```python
from bunqueue import FlowProducer

with FlowProducer(host="localhost", port=6789) as flows:
    node = flows.add(
        {
            "name": "assemble",
            "queueName": "orders",
            "opts": {"job_id": "order-42-assembly"},
            "children": [
                {
                    "name": "reserve-stock",
                    "queueName": "orders",
                    "opts": {"job_id": "order-42-stock"},
                },
                {"name": "charge-card", "queueName": "orders"},
            ],
        },
        {"queues_options": {"orders": {"attempts": 3, "backoff": 500}}},
    )
    assert node.job.id == "order-42-assembly"
    assert node.children[0].job.parent_id == node.job.id

    ids = flows.add_chain(
        [
            {"name": "extract", "queueName": "pipeline"},
            {"name": "transform", "queueName": "pipeline"},
            {"name": "load", "queueName": "pipeline"},
        ]
    )

    fan_in = flows.add_bulk_then(
        [
            {"name": "part-a", "queueName": "pipeline"},
            {"name": "part-b", "queueName": "pipeline"},
        ],
        {"name": "merge", "queueName": "pipeline"},
    )
```

`add`, `add_bulk`, `add_chain`, and `add_bulk_then` compile the full graph
locally, preallocate every ID, and send one `PUSHF` command. The broker either
persists and publishes the complete graph or creates no jobs. Public
`FlowNode` results are reconstructed from committed broker snapshots.

`opts["job_id"]` becomes both the planned ID and wire `customId`; generated
IDs are portable UUID hex strings without `:`. Atomic flows reject `repeat`,
`deduplication`/`unique_key`, and `debounce`. The planner owns `parent_id`,
`depends_on`, and `children_ids`; data keys named `name` or beginning with
`__` are reserved for immutable flow metadata and are rejected.
`queues_options` may supply scheduling and retention defaults, but not
`job_id`; identity belongs to the individual node. If supplied, `opts`,
`queues_options`, and each per-queue defaults value must be dictionaries;
malformed falsy values such as `[]`, `""`, `0`, or `False` are rejected rather
than interpreted as omitted options.

A transport timeout after `PUSHF` is ambiguous: the broker may have committed
the complete graph before the response was lost. Assign a deterministic
`opts["job_id"]` to every node and reuse the same graph on retry. If the first
call did not commit, the retry can create it; if it did, strict `PUSHF`
collision checking returns `already exists` instead of rewriting the graph or
fabricating snapshots. Treat that error as a reconciliation signal and query
the known stable IDs. Regenerated IDs can create a second graph after an
uncertain outcome.

## Errors

```python
from bunqueue import (
    BunqueueError,        # base
    ConnectionClosedError,
    CommandTimeoutError,
    CommandError,         # server answered ok=false
    AuthError,
    SerializationError,   # payload not msgpack-serializable (e.g. datetime)
    UnrecoverableError,   # raise in a processor: fail terminally, no retries
)
```

The SDK logs its otherwise-silent failure points (swallowed command errors,
raising listeners, failed registrations) at warning level on the `bunqueue`
logger; attach a handler to see them.

### Structured telemetry

Pass `on_telemetry` to `Connection` or `Queue` to bridge transport metrics to
OpenTelemetry, Prometheus, or your logger without adding an SDK dependency:

```python
queue = Queue("emails", on_telemetry=lambda event: metrics.record(event))
```

Events cover `connect`, `disconnect`, `reconnect_scheduled`, `auth`, `command`,
`command_timeout`, and `error`. Command events contain the command name,
request id, outcome, and monotonic duration. Payloads and authentication tokens
are never included. A telemetry callback that raises is isolated from the
transport and cannot fail a queue operation.

## Protocol notes

- Wire: 4-byte big-endian length prefix + standard msgpack map per message;
  requests carry a `reqId` echoed by the server (pipelining).
- Integers outside int32 are sent as float64 (exact ≤ 2^53): the server's
  msgpack decoder turns int64 into `BigInt`, which its arithmetic rejects.
  The SDK handles this automatically. Consequence: integers larger than 2^53
  (e.g. 64-bit snowflake IDs) lose precision — pass them as **strings**.
- Command maps require string keys at every nesting level. Cyclic containers
  and integers outside the float64 range raise `SerializationError` locally,
  before a pending request is registered or any bytes are written. Binary
  values and array-like lists/tuples are preserved by the wire normalizer.

## Tests

```bash
python -m venv .venv && .venv/bin/pip install -e '.[test]'
.venv/bin/python -m pytest tests/test_flow_plan_property.py tests/test_flow_commit.py \
  tests/test_flow_plan_validation.py tests/test_flow_plan_limits.py \
  tests/test_flow_plan_contract.py tests/test_flow_plan_wire_contract.py \
  --hypothesis-seed=20260730
.venv/bin/python tests/test_integration.py   # basic (8)
.venv/bin/python tests/run_e2e.py            # full e2e vs real server (112)
BUNQUEUE_SDK_SOAK_SECONDS=3600 .venv/bin/python tests/soak.py
```

Mutation testing is intentionally a final, slower quality gate and requires
Python 3.10 or later (the runtime package remains compatible with Python 3.9):

```bash
.venv/bin/pip install -e '.[test,mutation]'
.venv/bin/mutmut run       # planners plus the pure snapshot validator
.venv/bin/mutmut results
```

The integration and E2E runners spawn a real bunqueue server (`bun src/main.ts`
from the repo root). The
E2E runner emits a monotonic duration for each case so the isolated SDK gate
can rank slow tests without relying on timing assertions. Hardening covers
independent-connection idempotency and single-lease contention, fixed-seed
generated payloads, malformed mutation corpora, 1000-job bursts, and
crash/restart recovery. The opt-in soak keeps one connection alive; set
`BUNQUEUE_SDK_SOAK_BATCH` to increase load.

Maintainers should read the [runtime invariants](INVARIANTS.md), the
[module and protocol guide](CLAUDE.md), and the
[local agent rules](AGENTS.md) before changing behavior.

## License

MIT. See the [LICENSE](https://github.com/egeominotti/bunqueue/blob/main/sdk/python/LICENSE) file.
Documentation: [bunqueue.dev/guide/sdks](https://bunqueue.dev/guide/sdks/).
Issues and feature requests: [GitHub issues](https://github.com/egeominotti/bunqueue/issues).
