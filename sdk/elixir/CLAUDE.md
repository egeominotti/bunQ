# Elixir SDK development guide

The repository [`AGENTS.md`](../../AGENTS.md), SDK umbrella
[`CLAUDE.md`](../CLAUDE.md), wire
[`protocol`](../../docs/protocol.md), and local
[`INVARIANTS.md`](INVARIANTS.md) are binding.

Target Elixir 1.15 and OTP 26. Keep each source file at or below 300 lines and
let `mix format` define layout.

## OTP and protocol guardrails

- `Bunqueue.Connection` is the sole socket owner. Preserve passive raw
  `:gen_tcp`/`:ssl`, serialized GenServer calls, request correlation, lazy
  reconnect, auth-first generations, and disconnect-on-stream-error behavior.
- Keep connection, heartbeat connection, and lifecycle as distinct OTP
  processes. Handler tasks receive immutable job/token values, never socket
  ownership. Heartbeat stop messages stay tagged with the handler PID so
  unrelated mailbox traffic cannot terminate another lease.
- Worker stop is an idempotent lifecycle barrier. Do not unregister or close
  until all admitted runs and their ACK/FAIL calls have left.
- Every outgoing term passes through `Bunqueue.Wire.js_safe/1`; frame checks
  happen before send and before body allocation. Ext-0 remains the only
  tolerated incoming MessagePack extension.
- Public option maps are allowlisted. Add the exact wire mapping and tests with
  every new key; never accept and drop an option.
- Keep ordinary and scheduler job names in top-level `"name"`/`"jobName"`;
  preserve the complete user `"data"` term and decode only legacy envelopes.
- `FlowProducer` is plan-then-commit: immutable planner, one `PUSHF`, exact
  snapshot validation, node construction. Do not restore Agents for rollback,
  placeholder parents, per-node `PUSH`, or `UpdateParent`.
- Flow markers (`name`, `__*`) and topology options belong to the planner.
  Reject caller overrides before `Connection.call/2`.
- Telemetry is payload-free, optional, and delivered in an isolated process;
  callbacks cannot affect connection state.

## Working loop

Start a bug fix with a failing public-path regression. Use StreamData for
recursive/unbounded plans and small example tables for finite option and
snapshot classes. Preserve a shrunk failure as a deterministic case.

During iteration:

```bash
mix format --check-formatted
mix compile --warnings-as-errors
mix test test/flow_planner_property_test.exs test/flow_snapshots_test.exs
mix test test/flow_e2e_test.exs
```

After the planner and snapshot behavior stabilizes:

```bash
mix mutants
```

The alias runs Muex 0.8.1 separately for `flow_planner.ex` and
`flow_snapshots.ex`, each with only its relevant tests. Report both score
blocks and every survivor/equivalent.

Before handoff:

```bash
mix test
# --timeout must exceed the soak: ExUnit kills a test at 60s by default.
BUNQUEUE_SDK_SOAK_SECONDS=3600 mix test --include soak --timeout 3900000 test/soak_test.exs
cd ../conformance
bun runner.ts --driver "cd ../elixir && mix run ../conformance/drivers/elixir.exs"
cd ../..
bun run test:sandbox:sdk
bun run test:sandbox
```

Native E2E tests need `bun` and `openssl`. Always use
`Bunqueue.TestBroker`—never an existing broker—and retain the ExUnit seed for
replay. If an OTP 26, mutation, conformance, or sandbox gate cannot run, report
it explicitly.
