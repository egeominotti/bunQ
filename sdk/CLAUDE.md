# bunqueue SDKs — umbrella development guide

This directory contains every official bunqueue client and the machinery
that keeps them honest. Language-specific rules live in each SDK's own
`CLAUDE.md`; the rules here apply to ALL of them and to any new language.

```
sdk/
├── typescript/    bunqueue-client (npm) — Node, Bun, Deno, Cloudflare Workers
├── python/        bunqueue-client (PyPI) — sync + threads
├── php/           bunqueue/client (Composer) — sync + sequential worker
├── go/            github.com/egeominotti/bunqueue/sdk/go — goroutine worker
├── rust/          bunqueue-client — bounded threaded worker + rustls
├── elixir/        bunqueue_client — BEAM processes + OTP TCP/SSL
└── conformance/   the conformance suite: runner + one driver per SDK
```

## The two sources of truth (non-negotiable)

1. **`docs/protocol.md` is the wire contract.** Before writing or changing
   ANY SDK — and especially before starting a new language — read it end to
   end. Every field name, response shape, clamp, and semantic rule an SDK
   implements MUST come from that spec (which in turn defers to
   `src/domain/types/command.ts` and the server handlers). Never guess a
   field from another SDK's code without checking the spec: bugs replicate
   that way. If you discover wire behavior the spec does not cover, the fix
   is a spec PR **in the same change-set** — the spec never lags the code.

2. **`sdk/conformance/` is the certification gate.** An SDK is not done,
   and a change to an SDK is not shippable, until its driver passes the
   suite: `cd sdk/conformance && bun runner.ts --driver "<cmd>"` →
   `VERDICT: CONFORMANT`. All official drivers must stay green; a new
   language becomes "official" by adding a driver and passing, nothing else.

## Writing an SDK for a new language — the checklist

1. Read `docs/protocol.md` in full. Twice. The int64 rule (§4), the
   name-inside-data contract (§5), the response wrapping table (§6.8) and
   the worker semantics (§6.3) are where every past client bug lived.
2. Copy the structure of the closest existing SDK: `python/` for sync
   runtimes, `typescript/`/`go/` for concurrent ones, `php/` for
   request-scoped runtimes. One concern per file, ≤300 lines per file,
   single msgpack runtime dependency.
3. Implement the `jsSafe` guard FIRST (ints outside int32 → float64,
   applied recursively to every outgoing frame) and configure the msgpack
   library for smallest-form integers and ext-0 tolerance (§1, §4). This is
   not optional and it is not removable.
4. Mirror the reference clamps at the SDK surface: `batchSize → [1, 1000]`,
   poll timeout ≤ 30000, `waitForJob → [0, 600000]`, heartbeat interval
   `<= 0` (or non-finite) = disabled. Clamp with finite-guards in languages
   where NaN exists.
5. Every public method needs an e2e test against a real spawned server, in
   the SDK's own suite (mirror `php/tests/` if starting fresh).
6. Write the conformance driver (100-200 lines, references in
   `conformance/drivers/`) and iterate until 18/18.
7. Docs: SDK README with the standard header (logo, badges, link row),
   SDK-local `CLAUDE.md`, a section in `docs/src/content/docs/guide/sdks.mdx`.

## Cross-SDK rules

- **Core is untouchable.** SDK work never modifies `src/` — if the server
  looks wrong, verify against the spec and file/fix it as a separate core
  change with its own tests.
- **No silently dropped options.** If an SDK's public API accepts an option,
  it MUST reach the wire (with the exact spec name) or the API must reject
  it loudly. The "client drops a wire-supported field" class (#111) is the
  most common SDK bug; the conformance suite checks the known cases.
- **Parity by contract, not by copying.** SDKs may diverge in idiom
  (concurrency model, naming case) but never in wire behavior. When one SDK
  fixes a wire bug, check the same site in every other SDK in the same
  session — the bug class list in each SDK's CLAUDE.md exists for this.
- **Version bumps are per-SDK** (independent semver, 0.x patch bumps),
  changelogs in each SDK's `CHANGELOG.md`. The core package version never
  bumps for SDK-only changes.
- Everything (code, comments, docs, commit fragments) in English.
- Every official SDK keeps bounded race/idempotency, generated-property,
  malformed-input fuzz, crash/reconnect, and spike coverage in its native
  suite. Each also exposes an opt-in `BUNQUEUE_SDK_SOAK_SECONDS` profile.
  Database disk-full, WAL/power-loss and schema-migration injection stay in the
  broker suite; SDKs assert only the observable durable/reconnect contract.

## Gate before any SDK commit

1. The SDK's own e2e suite green (every supported runtime for TS).
2. `bun runner.ts --driver ...` → CONFORMANT for every SDK you touched.
3. Lint/format clean (`bun run check` / `php -l` / `go vet` + `gofmt`).
4. `bun run test:sandbox:sdk` passes every official SDK in isolated containers;
   review `artifacts/test-sandbox-sdk/<timestamp>/summary.md`, not only the exit
   code.
5. The repository-wide `bun run test:sandbox` gate is also green.
6. Skeptic agent review (repo rule — no exceptions).
