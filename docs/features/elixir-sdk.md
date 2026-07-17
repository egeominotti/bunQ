# Elixir client SDK

`sdk/elixir/` is the official OTP-native client for bunqueue server mode. It
uses the version 2 TCP protocol and MessagePack framing; the broker remains a
Bun application.

## Components

- `Bunqueue.Connection` owns one authenticated TCP or TLS socket in a
  `GenServer`, reconnects lazily, and closes a stream whose state becomes
  ambiguous after a timeout.
- `Bunqueue.Queue` and the query/control/admin modules expose produce, bulk,
  lookup, queue control, DLQ, scheduler, rate-limit, and concurrency commands.
- `Bunqueue.Worker` bounds `PULLB` by free concurrency, processes jobs with
  `Task.async_stream`, renews leases through an independent connection, and
  reports ACK or FAIL exactly once. Its lifecycle barrier rejects new runs
  during shutdown, drains active handlers through ACK/FAIL, coordinates
  concurrent `stop/1` calls, then terminates without retaining an idle process.
- `Bunqueue.FlowProducer` creates child-first dependency trees and chains, with
  best-effort rollback when a multi-job operation fails.
- `Bunqueue.Telemetry` delivers optional structured connection and command
  events in isolated lightweight processes.

All public modules are split below the repository's 300-line source limit.

## Transport invariants

- Authentication is the first frame on every socket generation.
- TLS verifies the certificate chain and hostname by default. A custom CA can
  be supplied; verification can only be disabled explicitly.
- Outgoing frames are rejected when the MessagePack body exceeds 64 MiB.
- Incoming ext type 0 becomes `nil`; other invalid extensions are rejected.
- Integers outside the signed int32 range are recursively converted to
  float64 for JavaScript interoperability.
- `PULLB`, `WaitJob`, and batch sizes are clamped to server protocol limits.
- Job payloads, results, tokens, and TLS secrets never appear in telemetry.
- Explicit connection shutdown emits a payload-free `close` event.

## Validation

The ExUnit suites cover wire encoding, option mapping, typed errors,
connection lifecycle, telemetry isolation, Queue behavior, Worker bounds and
lease handling, graceful/idempotent stop, lifecycle process cleanup, and real
broker integration. Hardening adds independent-process custom-id and
single-lease contention, fixed-seed generated payloads, malformed-term
mutations, a 512-job spike, and SIGKILL/restart visibility for a durable job.
The tagged `:soak` profile is excluded from the bounded suite and sustains one
OTP-owned connection for a configurable duration. The Elixir conformance driver
must pass all 17 shared protocol checks.

```bash
cd sdk/elixir
mix format --check-formatted
mix compile --warnings-as-errors
mix test
BUNQUEUE_SDK_SOAK_SECONDS=3600 mix test --include soak test/soak_test.exs

cd ../conformance
bun runner.ts --driver \
  "cd ../elixir && mix run ../conformance/drivers/elixir.exs"
```

The authoritative repository gate is `bun run test:sandbox:sdk`, which runs
these checks in a disposable Elixir/OTP image without external networking or
host mounts.
