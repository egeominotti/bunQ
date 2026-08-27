# bunqueue PHP SDK — development guide

PHP client for the bunqueue server. Speaks ONLY the native TCP protocol
(msgpack). Read `../CLAUDE.md` (umbrella rules) first: **`docs/protocol.md`
is the wire contract** and `../conformance/` is the certification gate —
both are mandatory for every change here. The correctness checklist is
[`INVARIANTS.md`](INVARIANTS.md).

## Non-negotiable rules

1. **Never touch the bunqueue core** (`../../src/`).
2. **Single runtime dependency: `rybakit/msgpack`** (pure PHP). Eris and
   PHPUnit are development-only; PCOV is required only by the PHP 8.4 mutation
   job. The published client has no PECL or framework requirement.
3. **`Protocol::jsSafe()` is load-bearing**: PHP ints are 64-bit; anything
   outside int32 must travel as float64 or the server crashes (BigInt
   class, protocol spec §4). NEVER remove or bypass it.
4. PSR-4 (`Bunqueue\` → `src/`), one class per file, ≤300 lines per file,
   `declare(strict_types=1)` everywhere.
5. Every public method → e2e test in `tests/e2e-*.php`; suites spawn a real
   server. Conformance driver: `../conformance/drivers/php.php` must stay
   at 18/18.
6. Everything in English.

## Module map

| File | Role |
|---|---|
| `src/Wire/Protocol.php` | Frame consts, depth-bounded `jsSafe`, recursive ext-0 normalization, `jobPayload`, `nowMs` |
| `src/Connection.php` + `ConnectionTLS.php` | Socket, framing, Auth-first, lazy reconnect, verified TLS, absolute write/read deadline and timeout teardown |
| `src/ConnectionTelemetry.php` | Optional payload-free lifecycle/command telemetry with callback isolation |
| `src/Options.php` | SDK options → wire fields (`attempts`→`maxAttempts`, dedup, debounce); unknown keys throw |
| `src/Job.php` | Job wrapper + per-id ops (progress, log, extendLock) |
| `src/Queue.php` + `QueueQuery/Control/Admin` traits | Produce, query, control, DLQ, schedulers, webhooks, monitoring |
| `src/Worker.php` + `WorkerEvents.php` | Sequential worker: `run()` / `runOnce()`, time-based heartbeats, safe registration, clamps, events and signal handlers |
| `src/Flow/Planner.php` + `SnapshotValidator.php` | Pure flow compilation, secure IDs, topology ownership and authoritative snapshot validation |
| `src/FlowProducer.php` + `FlowNode.php` | One-command `PUSHF` commit, result-tree construction, chains and `getFlow` |
| `src/Exception/*` + `UnrecoverableError.php` | Error hierarchy |
| `tests/harness.php` | Server fixture, registry, asserts, `waitUntil` |

## PHP-specific gotchas

- The worker is **single-threaded and sequential**: heartbeats fire between
  jobs (`heartbeatIfDue` renews every held lock via `JobHeartbeatB`); a
  single job longer than the lock TTL must call `$job->extendLock()`.
- First `runOnce()` must register: connection generation starts at -1 ==
  `registeredGeneration`, so the check also gates on `isConnected()`.
- `stream_socket_client` + `ssl://` verifies peers by default here — keep
  `verify_peer`/`verify_peer_name` tied together and opt-out explicit.
- PHP traces lead with the throw site: FAIL sends the FIRST lines
  (message line prepended), capped at the per-job `stackTraceLimit` or 10.
- An empty PHP array packs as a msgpack array. Keep job `name` top-level and
  pass `data` through unchanged; do not rebuild the retired name envelope.
- `FAIL`/`Progress` require an ACTIVE job (pull first, keep the token).
- A successful ACK/FAIL can carry `applied:false` after a broker timeout wins;
  release the local lease without emitting a terminal or error event.
- Test cleanup: a scheduler that reached its `limit` is removed server-side
  (`removeJobScheduler` then answers "not found").
- A flow must be fully valid before the connection closure is invoked. Do not
  reintroduce `PUSH`/`UpdateParent`/rollback, accept associative collections
  where the protocol requires lists, or trust a snapshot whose queue differs
  from the planned queue.
- Flow topology options and `name`/`__*` data markers are planner-owned.
  Reject attempts to supply them; never silently unset and continue.

## Tests

```bash
composer install
for f in $(find src tests -name '*.php'); do php -l $f; done
composer test:property
# PHP 8.4 + PCOV, Infection 0.34.1 PHAR verified as documented in README:
composer mutation
php tests/run-e2e.php     # property gate + real server/auth/race processes
BUNQUEUE_SDK_SOAK_SECONDS=3600 php tests/soak.php
cd ../conformance && bun runner.ts --driver "php drivers/php.php"
cd ../.. && bun run test:sandbox:sdk
```

Any SDK change must finish with the isolated `test:sandbox:sdk` gate.
On an Eris failure, preserve `ERIS_SEED` and the minimized example. Mutation
reports belong under `build/` and must cover only the pure `src/Flow` surface.
