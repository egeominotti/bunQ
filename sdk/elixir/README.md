<div align="center">

<a href="https://bunqueue.dev">
  <img src="https://raw.githubusercontent.com/egeominotti/bunqueue/main/.github/logo.png" alt="bunqueue logo" width="110" />
</a>

# bunqueue_client (Elixir)

**The official Elixir client for [bunqueue](https://bunqueue.dev), the high performance job queue server.**

Native TCP protocol (msgpack, length-prefixed frames), OTP-safe connections owned by GenServers, verified TLS via `:ssl`.

[![license](https://img.shields.io/badge/license-MIT-1a1a2e)](https://github.com/egeominotti/bunqueue/blob/main/sdk/elixir/LICENSE)
[![elixir](https://img.shields.io/badge/elixir-1.15%2B-2ea44f)](https://elixir-lang.org/)
[![conformance](https://img.shields.io/badge/protocol-conformant%2017%2F17-d3156d)](https://github.com/egeominotti/bunqueue/tree/main/sdk/conformance)

[Documentation](https://bunqueue.dev/guide/sdks/) · [Protocol spec](https://github.com/egeominotti/bunqueue/blob/main/docs/protocol.md) · [Server](https://github.com/egeominotti/bunqueue) · [Changelog](https://github.com/egeominotti/bunqueue/blob/main/sdk/elixir/CHANGELOG.md)

</div>

---

The bunqueue server runs on Bun, distributed as a binary or a Docker image.
This client lets any Elixir service produce and consume jobs against it: one
queue, any language.

## Installation

Requires Elixir 1.15+ and OTP with `:ssl` and `:public_key`. The only runtime
serialization dependency is `msgpax`.

```elixir
def deps do
  [
    # Hex release upcoming; use a checkout as a path dependency today:
    {:bunqueue_client, path: "../bunqueue/sdk/elixir"}
  ]
end
```

## Quick start

Start a server (`bunx bunqueue start` or the Docker image), then:

```elixir
# Producer
queue = Bunqueue.queue("emails", host: "127.0.0.1", port: 6789)

{:ok, job} =
  Bunqueue.Queue.add(
    queue,
    "welcome",
    %{user_id: 42},
    attempts: 5,
    backoff: 1_000,
    jobId: "welcome-42"
  )

{:ok, fetched} = Bunqueue.Queue.get_job(queue, job.id)
{:ok, counts} = Bunqueue.Queue.get_job_counts(queue)
```

```elixir
# Worker (blocking; concurrency-bounded)
worker =
  Bunqueue.Worker.new(
    "emails",
    fn job ->
      Mailer.deliver(job.data)
      {:ok, %{delivered: true}}
    end,
    connection: [host: "127.0.0.1", port: 6789, token: System.fetch_env!("TOKEN")],
    concurrency: 16,
    batch_size: 64
  )

Bunqueue.Worker.run(worker)
```

Retries, backoff, priorities, delays, stall detection and the dead letter
queue all live in the server; the worker only pulls, heartbeats and
acknowledges.

## Producing jobs

Single-job `jobId` is automatically renamed to `customId` inside `PUSHB`.
Unknown options raise `ArgumentError`; advertised options are never silently
dropped. Integers outside int32 are recursively encoded as float64 for
JavaScript interoperability (exact up to 2^53; pass larger 64-bit identifiers
as strings).

The queue module also exposes pause/resume/drain/clean, delayed-job promotion,
DLQ retry and purge, rate and concurrency limits, and cron schedulers.
Scheduler `limit` and `tz` map to the wire fields `maxLimit` and `timezone`.

## Worker semantics

- `batch_size` is clamped to `1..1000`, and each pull is additionally bounded
  by the worker concurrency so no lease waits outside the heartbeat lifecycle.
- The worker registers before every pull, so a lazily reconnected socket is
  registered before scheduler checks.
- Active leases are renewed through an independent connection. Polling is
  capped at 30 seconds. Set `heartbeat_interval: 0` to disable job heartbeats.
- `stop/1` is idempotent and safe while `run/1` is active: it rejects new
  runs, waits for every active handler and its ACK/FAIL, then unregisters,
  closes, and releases its lifecycle process.
- Raise `Bunqueue.UnrecoverableError` to skip remaining retries and
  dead-letter a job. ACK/FAIL failures are surfaced and never counted as
  completed processing.

## Security

TLS verifies the peer and hostname by default using the OTP system CA store:

```elixir
Bunqueue.queue("secure", host: "queue.example.com", port: 6789, tls: true)
Bunqueue.queue("private", host: "queue.internal", tls: true, ca_file: "/etc/bunqueue/ca.pem")
```

`verify: false` is an explicit development-only escape hatch. It is never
enabled implicitly. Authentication uses server-side tokens: pass
`token: "..."` in connection options.

## Structured telemetry

Pass `event_handler: fn event -> ... end` in connection options. The callback
runs in an isolated lightweight process and cannot break the connection.
Events are maps with `event`, `timestamp_ms`, and `generation`, plus
event-specific fields:

- `connected`: `host`, `port`, and `transport`
- `auth`: `ok` and, on failure, `error`
- `command`: `command`, `ok`, `req_id`, and `duration_us`
- `timeout` / `error`: `command`, `error`, and `duration_us`
- `reconnect`: emitted before `connected` for later socket generations
- `close`: emitted when the connection owner terminates

```elixir
handler = fn event -> Logger.info("bunqueue", bunqueue: event) end
queue = Bunqueue.queue("observed", event_handler: handler)
```

No token, job payload, result, or TLS secret is included in telemetry.

## Flows

`Bunqueue.FlowProducer` creates children before parents, updates placeholder
parents, and rolls back created jobs best-effort after an error. It also
supports dependency chains.

## Quality assurance

Every change runs the ExUnit suite (disposable plain, authenticated, and TLS
brokers spawned per run) and the cross-language
[conformance suite](https://github.com/egeominotti/bunqueue/tree/main/sdk/conformance):

```bash
cd sdk/elixir
mix format --check-formatted
mix test
BUNQUEUE_SDK_SOAK_SECONDS=3600 mix test --include soak test/soak_test.exs

cd ../conformance
bun runner.ts --driver "cd ../elixir && mix run ../conformance/drivers/elixir.exs"
```

Hardening adds concurrent custom-id/single-lease races, generated payloads,
malformed-term fuzzing, a 512-job spike, and durable SIGKILL/restart recovery.
The tagged soak profile reuses one OTP connection; `BUNQUEUE_SDK_SOAK_BATCH`
controls stress.

## License

MIT. See the [LICENSE](https://github.com/egeominotti/bunqueue/blob/main/sdk/elixir/LICENSE) file.
Documentation: [bunqueue.dev/guide/sdks](https://bunqueue.dev/guide/sdks/).
Issues and feature requests: [GitHub issues](https://github.com/egeominotti/bunqueue/issues).
