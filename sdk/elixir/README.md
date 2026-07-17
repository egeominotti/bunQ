# bunqueue for Elixir

Official OTP-safe Elixir client for the
[bunqueue protocol](../../docs/protocol.md). The broker remains Bun-only; this
package connects to it over TCP or verified TLS.

## Requirements and installation

- Elixir 1.15 or newer
- OTP with `:ssl` and `:public_key`

```elixir
def deps do
  [{:bunqueue_client, "~> 0.1.0"}]
end
```

The only runtime serialization dependency is `msgpax ~> 2.4`.

## Queue

```elixir
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

Single-job `jobId` is automatically renamed to `customId` inside `PUSHB`.
Unknown options raise `ArgumentError`; advertised options are never silently
dropped. Integers outside int32 are recursively encoded as float64 for
JavaScript interoperability.

The queue module also exposes pause/resume/drain/clean, delayed-job promotion,
DLQ retry and purge, rate and concurrency limits, and cron schedulers. Scheduler
`limit` and `tz` map to the wire fields `maxLimit` and `timezone`.

## Worker

```elixir
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

`batch_size` is clamped to `1..1000`, and each pull is additionally bounded by
the worker concurrency so no lease waits outside the heartbeat lifecycle. The
worker registers before every pull, so a lazily reconnected socket is
registered before scheduler checks. Active leases are renewed through an
independent connection. Polling is capped at 30 seconds. `stop/1` is
idempotent and safe while `run/1` is active: it rejects new runs, waits for
every active handler and its ACK/FAIL, then unregisters, closes, and releases
its lifecycle process. Set
`heartbeat_interval: 0` to disable job heartbeats. Raise
`Bunqueue.UnrecoverableError` to skip remaining retries and dead-letter a job.
ACK/FAIL failures are surfaced and never counted as completed processing.

## TLS

TLS verifies the peer and hostname by default using the OTP system CA store:

```elixir
Bunqueue.queue("secure", host: "queue.example.com", port: 6789, tls: true)
Bunqueue.queue("private", host: "queue.internal", tls: true, ca_file: "/etc/bunqueue/ca.pem")
```

`verify: false` is an explicit development-only escape hatch. It is never
enabled implicitly.

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

## Validation

```bash
cd sdk/elixir
mix format --check-formatted
mix test
BUNQUEUE_SDK_SOAK_SECONDS=3600 mix test --include soak test/soak_test.exs

cd ../conformance
bun runner.ts --driver "cd ../elixir && mix run ../conformance/drivers/elixir.exs"
```

The ExUnit suite spawns disposable plain, authenticated, and TLS brokers; it
does not silently skip e2e coverage. The conformance runner checks all 17
protocol invariants against a real broker. Hardening adds concurrent
custom-id/single-lease races, generated payloads, malformed-term fuzzing, a
512-job spike, and durable SIGKILL/restart recovery. The tagged soak profile
reuses one OTP connection; `BUNQUEUE_SDK_SOAK_BATCH` controls stress.
