# Go SDK agent guide

This directory is a Go 1.26.5 module. Its invariant ledger is
[INVARIANTS.md](INVARIANTS.md), and [CLAUDE.md](CLAUDE.md) maps protocol
responsibilities to files.

## Implementation guardrails

- Run `gofmt` on every changed Go file. Keep files at or below 300 lines and
  split by responsibility instead of growing multipurpose modules.
- `msgpack/v5` is the only production dependency. Rapid is test-only; Gremlins
  is an external, version-pinned QA tool.
- Reproduce bugs first. Prefer a public-path regression; pure planner and
  snapshot defects also need a focused unit/property case that demonstrates
  shrinking or a deterministic boundary.
- Never bypass `jsSafe`, compact-integer encoding, connection deadlines, or the
  independent worker pull/command/heartbeat sockets.
- Preserve typed guarantees. In particular, `ChainStep` has no `Children`
  field; do not weaken flat chains into an untyped runtime convention.
- Flow input must be rejected before `callFlow`. One valid graph means one
  `PUSHF`, reciprocal links, planner-owned topology, and an exact snapshot
  ID+queue set. There is no client rollback phase.
- When behavior changes, update [README.md](README.md),
  [CHANGELOG.md](CHANGELOG.md), [INVARIANTS.md](INVARIANTS.md), and the relevant
  module notes in [CLAUDE.md](CLAUDE.md).

## Fast feedback

```bash
gofmt -w <changed-go-files>
go test -run 'FlowPlanner|FlowProducerRejectsOwnedTopology|FlowCommit|RandomFlowID' \
  -count=1 -v ./...
go vet ./...
```

Rapid 1.3.0 writes minimized failures below `testdata/rapid/` and prints
`-rapid.seed`. Preserve both until the engine/test distinction is understood;
turn confirmed defects into stable regressions.

Install and run the pinned mutation tool without adding it to `go.mod`:

```bash
GOBIN="$(go env GOPATH)/bin" \
  go install github.com/go-gremlins/gremlins/cmd/gremlins@v0.6.0
mkdir -p build
gremlins unleash --config .gremlins.yaml
```

The configured surface is `flow_planner.go`, `flow_id.go`, and
`flow_snapshots.go`. Do not lower the 99.9% ratchets to hide a survivor; inspect
`build/gremlins.json` and classify it.

## Full confidence

```bash
go test -v ./... -count=1 -timeout 600s
go test -race -run 'Hardening|Regression|Worker' ./...
cd ../.. && bun run test:sandbox:sdk
```

Tests need fresh queues/ports and explicit cleanup. A sandbox failure or
unavailable Docker environment blocks handoff even when native tests pass.
