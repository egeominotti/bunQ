# bunqueue-client (Rust)

Official synchronous Rust SDK for the bunqueue TCP protocol.

[![protocol](https://img.shields.io/badge/protocol-v2-d3156d)](../../docs/protocol.md)
[![conformance](https://img.shields.io/badge/conformance-17%2F17-brightgreen)](../conformance/)
[![rust](https://img.shields.io/badge/rust-1.85%2B-orange)](https://www.rust-lang.org/)

## Install

Until the crate is published, use a checkout as a path dependency:

```toml
[dependencies]
bunqueue-client = { path = "../bunqueue/sdk/rust" }
```

## Quick start

```rust
use bunqueue_client::{ConnectionOptions, JobOptions, Queue, Value};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let queue = Queue::new("emails", ConnectionOptions::default());
    let data = Value::Map(vec![(Value::from("to"), Value::from("user@example.com"))]);
    let job = queue.add("welcome", data, JobOptions::default())?;
    println!("queued {}", job.id());
    queue.close();
    Ok(())
}
```

Workers use a bounded thread pool. Return `ProcessError::retryable(...)` for
a normal failure or `ProcessError::unrecoverable(...)` to skip retries.

## Surface

- producer, bulk add, complete job option mapping;
- query, control, DLQ, schedulers, rate-limit duration/TTL, and ping;
- bounded threaded workers with ACK/FAIL and reconnect registration;
- parent/child flows and chains;
- auth-first handshake and verified TLS with system or custom CAs;
- recursive int64 guard, outgoing map/extension validation, ext-0 tolerance,
  and a 64 MiB frame cap;
- opt-in structured telemetry for connections, commands, authentication,
  deadlines, reconnects, errors, and worker retries.

## Telemetry

```rust
use std::sync::Arc;
use bunqueue_client::{
    ConnectionOptions, Queue, TelemetryCallback, TelemetryEvent,
};

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
behavior. Authentication tokens are redacted from `ConnectionOptions` debug
output.

## Quality

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
BUNQUEUE_SDK_SOAK_SECONDS=3600 cargo test --test soak -- --ignored
cd ../conformance
bun runner.ts --driver \
  "cargo run --quiet --manifest-path ../rust/Cargo.toml --example conformance-driver"
```

The native tests cover wire validation, frame limits, option mapping, error
shapes, telemetry, auth, timeout/reconnect, TLS verification, worker
registration, concurrency-safe batch pulls, and flow rollback. The conformance
command must finish with `17/17` and `VERDICT: CONFORMANT`. Native integration
tests require `bun` and `openssl` on `PATH`.

Hardening includes 24-way idempotent retries, 12-way single-lease contention,
generated MessagePack payloads, malformed extensions, a 512-job spike, and a
durable job surviving broker SIGKILL/restart. The ignored soak test reuses one
connection; `BUNQUEUE_SDK_SOAK_BATCH` controls its diagnostic stress level.
