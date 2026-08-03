# TypeScript SDK agent guide

This file applies only to `sdk/typescript/`. Read
[`INVARIANTS.md`](./INVARIANTS.md) before changing transport, Queue, Worker, or
FlowProducer behavior.

## Scope and runtime

- Keep changes inside this SDK unless the user explicitly expands scope.
- Preserve Node.js 20+, Bun, Deno 2+, and Workers `nodejs_compat`.
- Runtime source may use supported `node:*` builtins, but never `Bun.*`,
  `bun:*`, `deno:*`, CommonJS, or extensionless relative imports.
- `msgpackr` remains the only runtime dependency. Testing tools belong in
  `devDependencies`.
- Keep every source/test file at or below 250 lines; split by responsibility.

## Change rules

- Read the broker command type and handler before changing a wire shape.
- Preserve `undefined` omission while retaining meaningful falsy values.
- Never register pending transport state before serialization succeeds.
- Worker changes must account for lease token, heartbeat, concurrency slot,
  ACK/FAIL outcome, events, counters, and graceful close together.
- Flow creation must remain planner-first and one-command atomic. Do not add
  `PUSH`, `UpdateParent`, rollback cancellation, or an `await` inside a planner.
  IDs and reciprocal links must be resolved before `PUSHF`.
- A confirmed bug starts with a failing focused regression. Keep the minimized
  property counterexample when it represents an engine/client defect.
- Update `README.md`, `CHANGELOG.md`, and `INVARIANTS.md` when a public or
  protocol contract changes.

## Validation

Use deterministic pure tests while iterating:

```bash
bun install
bun run build
BUNQUEUE_FLOW_PBT_SEED=20260730 bun run test:property
bun run check
```

Replay fast-check with both `BUNQUEUE_FLOW_PBT_SEED` and the printed
`BUNQUEUE_FLOW_PBT_PATH`. This SDK has no mutation engine: StrykerJS was
removed because its dependency graph was the only source of the advisories the
weekly audit reported, none of it reachable from the published client. The pure
planners and snapshot validator stay covered by `bun run test:property`.

Then exercise the built package and real broker:

```bash
bun tests/integration.ts
bun tests/e2e.ts
node --experimental-strip-types tests/e2e.ts
deno run -A tests/e2e.ts
bun run test:workers
```

Use fresh server processes, dynamic ports, unique queues, and temporary
databases. Always close FlowProducer, Queue, Worker, and Connection instances.
Do not publish benchmark claims from containers or virtual machines.

Do not commit, push, or publish the SDK unless the user explicitly authorizes
the complete release workflow.
