# Elixir SDK agent instructions

This file extends the repository [`AGENTS.md`](../../AGENTS.md) within
`sdk/elixir/`.

## Establish the contract

- Read [`INVARIANTS.md`](INVARIANTS.md) and the relevant command/response
  section of [`../../docs/protocol.md`](../../docs/protocol.md).
- Check `git status --short` and preserve other work.
- Reproduce a bug with an ExUnit test before changing `lib/`. Prefer a
  public-path E2E test when the result depends on broker state.
- Keep Elixir 1.15 / OTP 26 compatibility and source files no longer than 300
  lines.

## Implementation boundaries

- A Connection GenServer owns its socket for its entire generation. Do not call
  `:gen_tcp` or `:ssl` from Queue, Job, FlowProducer, handler tasks, or user
  callbacks.
- Preserve OTP message isolation: use specific tagged messages and monitored or
  linked processes; do not consume a caller's generic mailbox messages.
- Do not turn worker stop into an asynchronous best effort. Lifecycle admission,
  active handler completion, ACK/FAIL, unregister, and connection close happen
  in that order.
- Keep `Options` strict. Atom and string input keys may normalize to the same
  wire field; reject unknown or conflicting ownership rather than choosing one
  silently.
- Flow calls have exactly four phases:

  ```text
  validate/plan immutable terms
  -> send one PUSHF
  -> validate exact ID/queue snapshots
  -> construct returned Job nodes
  ```

  Empty chains skip the connection. `children: []` is legal for a chain step;
  non-empty and non-list values are not. Topology options are illegal even when
  empty.
- Keep docs synchronized: [`README.md`](README.md),
  [`CHANGELOG.md`](CHANGELOG.md), and [`INVARIANTS.md`](INVARIANTS.md).

## Commands and evidence

Fast flow loop:

```bash
mix format --check-formatted
MIX_ENV=test mix compile --warnings-as-errors
mix test test/flow_planner_property_test.exs test/flow_snapshots_test.exs
mix test test/flow_e2e_test.exs
```

StreamData is pinned to 1.4.0 and properties run 256 cases with shrinking.
Replay the printed ExUnit seed:

```bash
mix test test/flow_planner_property_test.exs --seed <seed>
```

Mutation uses the pinned Muex 0.8.1 dependency:

```bash
mix mutants
```

Report each campaign separately: total, killed, survived, equivalent, invalid,
timeout, and score. Do not hide a survivor by broad exclusions.

Final gate:

```bash
mix test
cd ../conformance
bun runner.ts --driver "cd ../elixir && mix run ../conformance/drivers/elixir.exs"
cd ../..
bun run test:sandbox:sdk
bun run test:sandbox
```

Use the linked `Bunqueue.TestBroker` with dynamic ports and disposable data for
all E2E cases. Close workers, queues, connections, and broker owners in cleanup.
Do not commit, push, or publish without explicit authorization for the complete
release workflow.
