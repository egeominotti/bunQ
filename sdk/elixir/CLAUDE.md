# Elixir SDK development rules

The repository-level `AGENTS.md`, `sdk/CLAUDE.md`, and `docs/protocol.md` are
binding. Keep every source file at or below 300 lines.

- A `Bunqueue.Connection` GenServer exclusively owns each socket. Never expose
  it or read/write from a caller process.
- Keep `:gen_tcp` and `:ssl` in passive raw mode. TLS verification and hostname
  checks are enabled by default; custom trust uses `:ca_file`.
- Every outgoing term passes through `Bunqueue.Wire.js_safe/1`. Check the
  encoded body against 64 MiB before framing and incoming length before recv.
- A timeout or protocol/transport failure closes the socket. Reconnect only on
  a later command and authenticate first.
- Workers clamp PULLB to `1..1000`, register before pulling, retain lease tokens,
  renew active jobs through a separate connection, and only report completion
  after ACK/FAIL succeeds. Stop is an idempotent drain barrier: reject new runs,
  wait for active handlers and their ACK/FAIL, then unregister and close.
- Unknown public options raise; bulk `jobId` becomes `customId`, scheduler
  `limit` becomes `maxLimit`, and WaitJob clamps to `0..600000`.
- Telemetry must remain structured, optional, non-blocking, and must never
  expose auth tokens or job data.

After any Elixir SDK change run:

```bash
mix format --check-formatted
mix test
BUNQUEUE_SDK_SOAK_SECONDS=3600 mix test --include soak test/soak_test.exs
cd ../conformance
bun runner.ts --driver "cd ../elixir && mix run ../conformance/drivers/elixir.exs"
cd ../..
bun run test:sandbox:sdk
bun run test:sandbox
```
