<div align="center">

<a href="https://bunqueue.dev">
  <img src="https://raw.githubusercontent.com/egeominotti/bunqueue/main/.github/logo.png" alt="bunqueue logo" width="110" />
</a>

# bunqueue-client (Rust)

**The official Rust client for [bunqueue](https://bunqueue.dev), the high performance job queue server.**

Native TCP protocol (msgpack, length-prefixed frames), synchronous API, bounded threaded workers, rustls TLS with verified certificates.

[![crates.io](https://img.shields.io/crates/v/bunqueue-client?color=d3156d&label=crates.io)](https://crates.io/crates/bunqueue-client)
[![downloads](https://img.shields.io/crates/d/bunqueue-client?color=ff4f9f)](https://crates.io/crates/bunqueue-client)
[![license](https://img.shields.io/badge/license-MIT-1a1a2e)](https://github.com/egeominotti/bunqueue/blob/main/sdk/rust/LICENSE)
[![rust](https://img.shields.io/badge/rust-1.85%2B-2ea44f)](https://www.rust-lang.org/)
[![conformance](https://img.shields.io/badge/protocol-conformant%2017%2F17-d3156d)](https://github.com/egeominotti/bunqueue/tree/main/sdk/conformance)

[Documentation](https://bunqueue.dev/guide/sdks/) · [Protocol spec](https://github.com/egeominotti/bunqueue/blob/main/docs/protocol.md) · [Server](https://github.com/egeominotti/bunqueue) · [Changelog](https://github.com/egeominotti/bunqueue/blob/main/sdk/rust/CHANGELOG.md)

</div>

---

The bunqueue server runs on Bun, distributed as a binary or a Docker image.
This client lets any Rust service produce and consume jobs against it: one
queue, any language.

## Installation

```bash
cargo add bunqueue-client
```

Requires Rust 1.85+ (edition 2024). Dependencies: `rmpv` (msgpack values),
`rustls` (TLS), `serde_json`, `thiserror`.

## Quick start

Start a server (`bunx bunqueue start` or the Docker image), then:

```rust
use bunqueue_client::{ConnectionOptions, JobOptions, Queue, Value};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Producer
    let queue = Queue::new("emails", ConnectionOptions::default());
    let data = Value::Map(vec![(Value::from("to"), Value::from("user@example.com"))]);
    let job = queue.add("welcome", data, JobOptions::default())?;
    println!("queued {}", job.id());
    queue.close();
    Ok(())
}
```

```rust
use bunqueue_client::{Value, Worker, WorkerOptions};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Worker (blocking; jobs run on a bounded thread pool)
    let worker = Worker::new(
        "emails",
        |job| {
            println!("processing {}", job.id());
            Ok(Value::from("sent"))
        },
        WorkerOptions { concurrency: 8, ..Default::default() },
    );
    worker.run()?;
    Ok(())
}
```

Defaults are `localhost:6789`, so constructors need no options on a local
setup.

## Failure semantics

The processor returns `Result<Value, ProcessError>`:

```rust
use bunqueue_client::ProcessError;

// Normal failure: retried per the job's attempts/backoff settings
Err(ProcessError::retryable("transient"))

// Terminal failure: no retries, straight to the dead letter queue
Err(ProcessError::unrecoverable("malformed payload"))
```

Retries, backoff, priorities, delays, stall detection and the dead letter
queue all live in the server; the worker only pulls, heartbeats and
acknowledges. Automatic lock heartbeats renew active leases through an
independent connection, so jobs longer than the lock TTL survive; set
`heartbeat_interval: None` to disable them.

## API surface

| Area | Capabilities |
|---|---|
| Produce | `add`, `add_bulk` (custom ids preserved), the complete wire job option set: priority, delay, attempts, backoff, jobId, deduplication, dependsOn, lifo, durable, ... |
| Query | jobs, states, results, progress, `wait_for_job`, counts, job logs, children values |
| Control | pause, resume, drain, clean, obliterate, promote, retry, change priority/delay, update data |
| DLQ | get, retry, purge |
| Schedulers | cron pattern or fixed interval, execution limit, get/list/remove |
| Admin | webhooks, rate limit with duration window and broker-side TTL, workers, stats, list queues, ping |
| Flows | `FlowProducer`: parent/child trees (`add`), chains (`add_chain`), best-effort rollback on failure |
| Worker | bounded thread pool, batch pulls capped by concurrency, ACK-gated completion, reconnect-safe registration |

## Security

```rust
use bunqueue_client::{ConnectionOptions, Queue, TlsOptions};

let queue = Queue::new("emails", ConnectionOptions {
    host: "queue.example.com".into(),
    token: Some(std::env::var("BUNQUEUE_TOKEN")?),
    tls: Some(TlsOptions::default()), // system CAs, verified; or ca_file for a private CA
    ..Default::default()
});
```

Authentication uses server-side tokens (`AUTH_TOKENS`). TLS verifies
certificates by default via `rustls` with the system trust store or a custom
CA bundle. Auth tokens are redacted from `ConnectionOptions` debug output.

## Telemetry

Opt-in, structured and payload-free: connections, commands, authentication,
deadlines, reconnects, errors and worker retries.

```rust
use std::sync::Arc;
use bunqueue_client::{ConnectionOptions, Queue, TelemetryCallback, TelemetryEvent};

let telemetry: TelemetryCallback = Arc::new(|event| match event {
    TelemetryEvent::WorkerRetry { queue, message, retry_in } => {
        eprintln!("{queue}: retrying in {retry_in:?}: {message}");
    }
    TelemetryEvent::Error { operation, message } => {
        eprintln!("{operation}: {message}");
    }
    _ => {}
});
let queue = Queue::new("emails", ConnectionOptions {
    telemetry: Some(telemetry),
    ..Default::default()
});
```

Callbacks run after the connection mutex is released, so they may safely call
other SDK objects. A callback panic is isolated and never changes queue
behavior. No token, payload or result ever reaches the callback.

## Wire safety

Everything the protocol requires is enforced in the client: a recursive
int64 guard (integers outside int32 travel as float64, exact up to 2^53),
outgoing map/extension validation, ext-0 tolerance on responses, and a
64 MiB frame cap. Pass 64-bit identifiers such as snowflake IDs as strings
to avoid precision loss.

## Quality assurance

Every change runs the native suite (a real server spawned per run) and the
cross-language [conformance suite](https://github.com/egeominotti/bunqueue/tree/main/sdk/conformance):

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
BUNQUEUE_SDK_SOAK_SECONDS=3600 cargo test --test soak -- --ignored
cd ../conformance && bun runner.ts --driver \
  "cargo run --quiet --manifest-path ../rust/Cargo.toml --example conformance-driver"
```

The native tests cover wire validation, frame limits, option mapping, error
shapes, telemetry, auth, timeout/reconnect, TLS verification, worker
registration, concurrency-safe batch pulls and flow rollback. Hardening adds
idempotent retry and single-lease contention races, generated MessagePack
payloads, malformed extension fuzzing, a 512-job spike and durable
SIGKILL/restart recovery. Integration tests require `bun` and `openssl` on
`PATH`.

## License

MIT. See the [LICENSE](https://github.com/egeominotti/bunqueue/blob/main/sdk/rust/LICENSE) file.
Documentation: [bunqueue.dev/guide/sdks](https://bunqueue.dev/guide/sdks/).
Issues and feature requests: [GitHub issues](https://github.com/egeominotti/bunqueue/issues).
