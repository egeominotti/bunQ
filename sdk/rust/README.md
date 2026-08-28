<div align="center">

<a href="https://bunqueue.dev">
  <img src="https://raw.githubusercontent.com/egeominotti/bunqueue/main/.github/logo.png" alt="bunqueue logo" width="110" />
</a>

# bunqueue-client (Rust)

**The official Rust client for [bunqueue](https://bunqueue.dev), the high performance job queue server.**

Native TCP protocol (msgpack, length-prefixed frames), synchronous API, bounded threaded workers, rustls TLS with verified certificates.

[![crates.io](https://img.shields.io/crates/v/bunqueue-client?color=d3156d&label=crates.io)](https://crates.io/crates/bunqueue-client)
[![downloads](https://img.shields.io/crates/d/bunqueue-client?color=ff4f9f)](https://crates.io/crates/bunqueue-client)
[![docs.rs](https://img.shields.io/docsrs/bunqueue-client?color=1a1a2e)](https://docs.rs/bunqueue-client/latest/bunqueue_client/)
[![license](https://img.shields.io/badge/license-MIT-1a1a2e)](https://github.com/egeominotti/bunqueue/blob/main/sdk/rust/LICENSE)
[![rust](https://img.shields.io/badge/rust-1.85%2B-2ea44f)](https://www.rust-lang.org/)
[![conformance](https://img.shields.io/badge/protocol-conformant%2018%2F18-d3156d)](https://github.com/egeominotti/bunqueue/tree/main/sdk/conformance)

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

Job names are top-level protocol metadata. `Job::name()` reads that field,
while `Job::data()` returns the submitted `Value` unchanged, including a map
with its own `name`, scalar, array, or `Nil`. Legacy maps that stored the job
name inside `data` remain readable. Scheduler templates use separate
`jobName` and `data` fields.
The client negotiates protocol v3 and advertises `separate-job-name` in `Hello`.

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

`run_once()` returns the number of handler attempts that settled. If the broker
already finalized that exact lease generation (for example, by job timeout),
its acknowledged `already-finalized` no-op still settles the attempt but cannot
replace the broker's terminal state or persisted result.

## API surface

| Area       | Capabilities                                                                                                                                                        |
| ---------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Produce    | `add`, `add_bulk` (custom ids preserved), the complete wire job option set: priority, delay, attempts, backoff, jobId, deduplication, dependsOn, lifo, durable, ... |
| Query      | jobs, states, results, progress, `wait_for_job`, counts, job logs, children values                                                                                  |
| Control    | pause, resume, drain, clean, obliterate, promote, retry, change priority/delay, update data                                                                         |
| DLQ        | get, retry, purge                                                                                                                                                   |
| Schedulers | cron pattern or fixed interval, execution limit, get/list/remove                                                                                                    |
| Admin      | webhooks, rate limit with duration window and broker-side TTL, workers, stats, list queues, ping                                                                    |
| Flows      | `FlowProducer`: atomic parent/child trees (`add`) and dependency chains (`add_chain`)                                                                               |
| Worker     | bounded thread pool, batch pulls capped by concurrency, ACK-gated completion, reconnect-safe registration                                                           |

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

## Atomic flows

`FlowProducer` compiles the complete graph locally, then sends exactly one
`PUSHF` command. The broker either publishes every job with all links resolved
or publishes none. The returned `FlowNode` is built from the broker's
authoritative job snapshots.

```rust
use bunqueue_client::{
    ConnectionOptions, FlowJob, FlowProducer, JobOptions, Value,
};

fn main() -> Result<(), bunqueue_client::Error> {
    let producer = FlowProducer::new(ConnectionOptions::default());
    let flow = FlowJob {
        name: "send-summary".into(),
        queue_name: "reports".into(),
        data: Value::Map(vec![(Value::from("account"), Value::from("acme"))]),
        options: JobOptions {
            job_id: Some("summary-acme".into()),
            ..Default::default()
        },
        children: vec![FlowJob {
            name: "build-report".into(),
            queue_name: "reports".into(),
            data: Value::Map(Vec::new()),
            options: JobOptions::default(),
            children: Vec::new(),
        }],
    };

    let root = producer.add(flow)?;
    assert_eq!(root.job.id(), "summary-acme");
    assert_eq!(root.children.len(), 1);
    producer.close();
    Ok(())
}
```

Children run before their parent. For a linear dependency chain, every step
after the first depends only on the immediately preceding step:

```rust
use bunqueue_client::{
    ChainStep, ConnectionOptions, FlowProducer, JobOptions, Value,
};

fn main() -> Result<(), bunqueue_client::Error> {
    let producer = FlowProducer::new(ConnectionOptions::default());
    let ids = producer.add_chain(vec![
        ChainStep {
            name: "extract".into(),
            queue_name: "pipeline".into(),
            data: Value::Nil,
            options: JobOptions::default(),
        },
        ChainStep {
            name: "transform".into(),
            queue_name: "pipeline".into(),
            data: Value::Nil,
            options: JobOptions::default(),
        },
    ])?;
    assert_eq!(ids.len(), 2);
    producer.close();
    Ok(())
}
```

The planner generates cryptographically secure, colon-free IDs unless
`JobOptions.job_id` is supplied. A custom ID is also sent as the server's
`customId`, so custom-ID lookup remains available. Duplicate, empty,
colon-containing, and overlong IDs are rejected before network I/O.

Planning failures perform no network call, and a broker rejection leaves the
whole batch unpublished. A timeout or malformed response after `PUSHF` is
different: the client cannot know whether the broker committed before the
connection failed, so it returns an error and never attempts a partial
rollback. Use stable explicit `job_id` values for production flows that may be
retried. The retry commits if the first call did not; otherwise strict `PUSHF`
collision checking returns `already exists`. Treat that error as a
reconciliation signal and query the known IDs; the SDK does not fabricate the
original snapshots.

Flow topology is owned by `FlowProducer`: do not set `parent_id`,
`depends_on`, or `children_ids`. User data cannot contain `name` or a key
starting with `__`, because those fields carry canonical topology markers.
Atomic flows reject repeat, deduplication/unique-key, and debounce options.
The maximum descendant depth is 100 edges (the root is depth zero), and one
commit is limited to 10,000 jobs.

## Quality assurance

Every change runs the native suite (a real server spawned per run) and the
cross-language [conformance suite](https://github.com/egeominotti/bunqueue/tree/main/sdk/conformance):

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
cargo test --locked flow_plan_tests -- --nocapture
# cargo-mutants 26.0.0 supports Rust 1.78+, including this crate's Rust 1.85 MSRV.
cargo install cargo-mutants --version 26.0.0 --locked
cargo mutants
BUNQUEUE_SDK_SOAK_SECONDS=3600 cargo test --test soak -- --ignored
cd ../conformance && bun runner.ts --driver \
  "cargo run --quiet --manifest-path ../rust/Cargo.toml --example conformance-driver"
```

The native tests cover wire validation, frame limits, option mapping, error
shapes, telemetry, auth, timeout/reconnect, TLS verification, worker
registration, concurrency-safe batch pulls and atomic flow rejection. Hardening adds
idempotent retry and single-lease contention races, generated MessagePack
payloads, malformed extension fuzzing, shrinkable flow-plan properties, exact
snapshot validation, atomic tree/chain E2E cases, a 512-job spike and durable
SIGKILL/restart recovery. `cargo mutants` reads `.cargo/mutants.toml` and is
scoped to the pure flow planner and snapshot validator; the transport wrapper
is covered against a real broker. Integration tests require `bun` and
`openssl` on `PATH`.

Maintainers should read the [runtime invariants](INVARIANTS.md), the
[module and protocol guide](CLAUDE.md), and the
[local agent rules](AGENTS.md) before changing behavior.

## License

MIT. See the [LICENSE](https://github.com/egeominotti/bunqueue/blob/main/sdk/rust/LICENSE) file.
Documentation: [bunqueue.dev/guide/sdks](https://bunqueue.dev/guide/sdks/).
Issues and feature requests: [GitHub issues](https://github.com/egeominotti/bunqueue/issues).
