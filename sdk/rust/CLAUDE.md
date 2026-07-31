# Rust SDK development guide

Read the repository [`AGENTS.md`](../../AGENTS.md), the SDK umbrella
[`../CLAUDE.md`](../CLAUDE.md), the wire
[`protocol`](../../docs/protocol.md), and the local
[`INVARIANTS.md`](INVARIANTS.md) before changing this crate.

The crate targets Rust 1.85 and edition 2024. Keep source files at or below 300
lines, use owned values at thread boundaries, and preserve the existing
two-space/single-quote rule only for TypeScript outside this crate; Rust code is
formatted exclusively by rustfmt.

## Architecture guardrails

- `Connection` is synchronous and mutex-serialized. Do not hold its mutex while
  invoking telemetry callbacks, processor code, or another connection.
- Treat a failed round trip as stream corruption. Tear down and reconnect
  lazily; authenticate each new generation before its first normal command.
- Keep `wire::prepare_outgoing` recursive, reject non-string map keys and
  extensions, normalize incoming ext-0, and enforce the 64 MiB limit on both
  sides of the frame.
- `JobOptions` is static by design. Adding an option requires its public field,
  exact wire mapping, option test, protocol evidence, and README/change note.
- Worker jobs own their snapshot and token. Every spawned processor and
  heartbeat thread must be joined even if another thread already failed.
  Never detach work merely to make stop return sooner.
- `FlowProducer` must remain a compile-then-commit API: pure plan, one `PUSHF`,
  exact snapshot validation, then node construction. Never reintroduce
  child-first `PUSH`, placeholder parents, `UpdateParent`, or rollback loops.
- Keep flow topology fields and `__*` data markers internal. Custom `job_id`
  must equal `customId`; generated IDs use `getrandom`, never timestamps or a
  predictable counter.

## Change workflow

For a bug, first add a failing public-path regression. Use unit properties for
planner domains and real-broker tests for observable behavior. Preserve a
shrunk counterexample as a deterministic regression before fixing it.

Run while iterating:

```bash
cd sdk/rust
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test --locked flow_plan_tests -- --nocapture
cargo test --locked --test flow_atomic -- --nocapture
```

Run mutation only after behavior is stable:

```bash
cargo install cargo-mutants --version 26.0.0 --locked
cargo mutants
```

Before handoff:

```bash
cargo test --locked
cd ../conformance
bun runner.ts --driver \
  "cargo run --quiet --manifest-path ../rust/Cargo.toml --example conformance-driver"
cd ../..
bun run test:sandbox:sdk
bun run test:sandbox
```

Integration tests require `bun` and `openssl`. Never connect them to an
existing broker: use `tests/support::Server`, dynamic ports, and its disposable
database. Report the exact command, seed/counterexample, mutation survivors,
and any gate that could not run.
