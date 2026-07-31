# Python SDK agent guide

This file applies only to `sdk/python/`. Read
[`INVARIANTS.md`](./INVARIANTS.md) before changing transport, Queue, Worker, or
FlowProducer behavior.

## Scope and compatibility

- Keep changes inside this SDK unless the user explicitly expands scope.
- Preserve Python 3.9 runtime compatibility: use `typing.Optional`,
  `typing.Union`, and compatible syntax instead of newer-only language forms.
- `msgpack` remains the only runtime dependency. pytest, Hypothesis, and mutmut
  are optional development extras.
- Keep every source/test file at or below 250 lines and give it one role.
- Public names are snake_case; wire keys remain the broker's camelCase.

## Change rules

- Verify every command and response against the broker handler before editing
  the mapping.
- Preserve `_js_safe` integer normalization and `_compact` falsy-value
  semantics.
- Connection changes must maintain reader-thread/pending-future cleanup and
  the existing lock order.
- Worker changes must update lease token, heartbeat thread, concurrency slot,
  ACK/FAIL outcome, events, counters, and close behavior as one lifecycle.
- Flow creation stays planner-first with one `PUSHF`; planners perform no I/O.
  Never restore per-job `PUSH`, `UpdateParent`, or compensating cancellation.
- Start a confirmed bug with a failing focused test. Convert minimized
  Hypothesis failures into deterministic regressions.
- Update `README.md`, `CHANGELOG.md`, and `INVARIANTS.md` for public or wire
  contract changes.

## Validation

Install development tools without making them runtime dependencies:

```bash
python3 -m venv .venv
.venv/bin/pip install -e '.[test,mutation]'
```

Run pure deterministic flow checks while iterating:

```bash
.venv/bin/python -m pytest \
  tests/test_flow_plan_property.py \
  tests/test_flow_plan_validation.py \
  tests/test_flow_plan_limits.py \
  tests/test_flow_plan_contract.py \
  tests/test_flow_plan_wire_contract.py \
  tests/test_flow_commit.py \
  --hypothesis-seed=20260730
```

Replay the same Hypothesis seed or its printed `@reproduce_failure` blob. Run
mutation once at the final gate against only the pure planners and snapshot
validator; it must not start a broker. mutmut requires Python 3.10+, while the
runtime package must continue installing on 3.9:

```bash
.venv/bin/mutmut run
.venv/bin/mutmut results
```

Then run real-broker validation:

```bash
.venv/bin/python tests/test_integration.py
.venv/bin/python tests/run_e2e.py
```

Tests use dynamic ports, unique queue names, and temporary SQLite paths.
Always close/join FlowProducer, Queue, Worker, Connection, threads, and server
fixtures. Do not leave `.hypothesis/`, `mutants/`, databases, or WAL/SHM files
in the worktree.

Do not commit, push, or publish the SDK unless the user explicitly authorizes
the complete release workflow.
