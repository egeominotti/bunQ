# Rust SDK agent instructions

These instructions extend the repository-level
[`AGENTS.md`](../../AGENTS.md) for `sdk/rust/`.

## Before editing

1. Read [`INVARIANTS.md`](INVARIANTS.md) and the relevant section of
   [`../../docs/protocol.md`](../../docs/protocol.md).
2. Locate the smallest public-path test. A confirmed bug needs a RED regression
   before source changes.
3. Check `git status --short`; preserve unrelated and shared-agent edits.
4. Keep every Rust source file at or below 300 lines. Split by responsibility
   instead of appending another concern.

## Implementation rules

- Maintain Rust 1.85 compatibility and the existing public synchronous API.
- Use `Result<T, Error>`; do not panic on caller input, network data, or a
  processor result. Panics in processor threads must become an error after all
  threads are joined.
- Respect ownership boundaries: move owned `Job`, token, and raw maps into
  threads; keep shared callbacks and connection state behind `Arc`.
- Never bypass `Connection::call`, `wire::prepare_outgoing`, response
  normalization, request correlation, or frame-size checks.
- Do not silently ignore a `JobOptions` field. `job_id` mapping differs between
  `PUSH` (`jobId`) and bulk/flow input (`customId`).
- A flow change must preserve the one-command sequence:

  ```text
  plan all IDs and links -> PUSHF once -> validate ID/queue snapshots -> build nodes
  ```

  Planning errors and unsupported flow options must occur before socket use.
- Query/control/admin methods reflect server results; do not infer scheduling
  state in the client.
- Update [`README.md`](README.md), [`CHANGELOG.md`](CHANGELOG.md), and
  [`INVARIANTS.md`](INVARIANTS.md) when a public contract changes.

## Tests and evidence

Use the narrowest command first:

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test --locked flow_plan_tests -- --nocapture
cargo test --locked --test flow_atomic -- --nocapture
cargo test --locked --test flow_rollback -- --nocapture
```

The planner PBT campaign uses proptest 1.7.0 with shrinking. Replay its printed
seed with `PROPTEST_RNG_SEED=<seed>` and retain minimized cases. The mutation
gate is cargo-mutants 26.0.0:

```bash
cargo mutants
```

Review `mutants.out/outcomes.json`; report killed, survived, timeout, and
unviable counts rather than only the exit status.

Final SDK changes require:

```bash
cargo test --locked
cd ../conformance
bun runner.ts --driver \
  "cargo run --quiet --manifest-path ../rust/Cargo.toml --example conformance-driver"
cd ../..
bun run test:sandbox:sdk
bun run test:sandbox
```

Do not commit or publish from this directory unless the user explicitly
authorizes the complete release workflow.
