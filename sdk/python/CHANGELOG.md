# Changelog

All notable changes to `bunqueue-client` (Python SDK) are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.1] - 2026-07-08

Protocol-coherence audit against the bunqueue server. Every fix ships with a
RED→GREEN repro in `tests/e2e_audit_fixes.py`.

### Fixed

- **add_bulk dropped the custom job id.** PUSHB entries are `JobInput`
  (`customId`), not the single-PUSH `jobId` the server renames — the batch
  path now renames `jobId`→`customId`, so `get_job_by_custom_id` and idempotent
  bulk ingest work. (H1)
- **Half-open link wedge.** Enable `SO_KEEPALIVE` (~15s idle) and tear down the
  socket after 3 consecutive command timeouts so the next call reconnects,
  instead of wedging until the OS abandons the writes. The teardown is
  generation-guarded so a stale-connection timeout can't abort a fresh
  reconnect. (H2)
- **Auth race on reconnect.** `_conn_lock` is now reentrant and Auth is sent
  while holding it, flipping `_connected` only after Auth completes — a
  concurrent thread can no longer send a command ahead of the Auth frame
  (server would reject it `Not authenticated`). (H3)
- **get_flow crashed on a missing job.** A missing root/child now yields `None`
  and is skipped (partial tree) instead of raising; the catch is narrowed to
  `'not found'` so real server errors still surface, and a `visited` set guards
  against cycles now that `depth` defaults to unlimited. (H4)
- **wait_for_job returned `None` on timeout.** It now raises on non-completion:
  a `failed` job raises `CommandError`, otherwise `CommandTimeoutError` — the
  `completed` flag is no longer ignored. (M1)

### Changed

- Simple Mode cron (`Bunqueue.cron`/`every`, `upsert_job_scheduler`) now maps
  Pythonic job options snake_case→camelCase via `build_cron_job_options`
  (`attempts`→`maxAttempts`, `remove_on_complete`→`removeOnComplete`, …) so
  cron-spawned jobs honor the requested retry/cleanup policy instead of falling
  back to server defaults; also forwards `skip_missed_on_restart`. (M3)
- `retry_dlq` / `retry_jobs`: the dead `count` field is no longer sent on the
  wire (the server has no partial RetryDlq; `count` is accepted only for
  signature parity).

## [0.1.0] - initial published release

- TCP client (msgpack wire protocol): `Queue`, `Worker`, `FlowProducer`,
  `Bunqueue` Simple Mode, TLS, auth, reqId pipelining.
