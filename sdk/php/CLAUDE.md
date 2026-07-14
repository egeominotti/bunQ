# bunqueue PHP SDK — development guide

PHP client for the bunqueue server. Speaks ONLY the native TCP protocol
(msgpack). Read `../CLAUDE.md` (umbrella rules) first: **`docs/protocol.md`
is the wire contract** and `../conformance/` is the certification gate —
both are mandatory for every change here.

## Non-negotiable rules

1. **Never touch the bunqueue core** (`../../src/`).
2. **Single runtime dependency: `rybakit/msgpack`** (pure PHP). No PECL
   requirements, no framework coupling.
3. **`Protocol::jsSafe()` is load-bearing**: PHP ints are 64-bit; anything
   outside int32 must travel as float64 or the server crashes (BigInt
   class, protocol spec §4). NEVER remove or bypass it.
4. PSR-4 (`Bunqueue\` → `src/`), one class per file, ≤300 lines per file,
   `declare(strict_types=1)` everywhere.
5. Every public method → e2e test in `tests/e2e-*.php`; suites spawn a real
   server. Conformance driver: `../conformance/drivers/php.php` must stay
   at 17/17.
6. Everything in English.

## Module map

| File | Role |
|---|---|
| `src/Wire/Protocol.php` | Frame consts, `compact`, `jsSafe`, `jobPayload`, `nowMs` |
| `src/Connection.php` | Socket, framing, Auth-first, lazy reconnect, TLS (verified by default), timeout teardown |
| `src/Options.php` | SDK options → wire fields (`attempts`→`maxAttempts`, dedup, debounce); unknown keys throw |
| `src/Job.php` | Job wrapper + per-id ops (progress, log, extendLock) |
| `src/Queue.php` + `QueueQuery/Control/Admin` traits | Produce, query, control, DLQ, schedulers, webhooks, monitoring |
| `src/Worker.php` | Sequential worker: `run()` loop / `runOnce()` batch, time-based heartbeats, clamps, signal handlers |
| `src/FlowProducer.php` + `FlowNode.php` | Trees, chains, getFlow, rollback |
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
- An empty PHP array packs as msgpack **array**, not map: `jobPayload`
  always merges `name` in, so job data is never empty — keep it that way.
- `FAIL`/`Progress` require an ACTIVE job (pull first, keep the token).
- Test cleanup: a scheduler that reached its `limit` is removed server-side
  (`removeJobScheduler` then answers "not found").

## Tests

```bash
composer install
for f in $(find src tests -name '*.php'); do php -l $f; done
php tests/run-e2e.php     # 33 tests, real server + dedicated auth server
cd ../conformance && bun runner.ts --driver "php drivers/php.php"
```
