# Changelog

All notable changes to `bunqueue-client` (TypeScript SDK) are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.5] - 2026-07-08

Protocol-coherence audit against the bunqueue server. Every fix ships with a
RED→GREEN repro in `tests/e2e-audit-fixes.ts`.

### Fixed

- **addBulk dropped the custom job id.** PUSHB entries are `JobInput`
  (`customId`), not the single-PUSH `jobId` the server renames — the batch
  path now renames `jobId`→`customId`, so `getJobByCustomId` and idempotent
  bulk ingest work. (H1)
- **Half-open link wedge.** Enable TCP keepalive (~15s idle) and tear down the
  socket after 3 consecutive command timeouts so the next call reconnects,
  instead of wedging until the OS abandons the writes. The teardown is
  generation-guarded so a stale-connection timeout can't abort a fresh
  reconnect. (H2)
- **getFlow crashed on a missing job.** A missing root/child now yields `null`
  and is skipped (partial tree) instead of throwing; the catch is narrowed to
  `'not found'` so real server errors still surface, and a `visited` set guards
  against cycles now that depth defaults to unlimited. (H4)
- **waitForJob returned `undefined` on timeout.** It now rejects on
  non-completion: a `failed` job throws `CommandError`, otherwise
  `CommandTimeoutError` — the `completed` flag is no longer ignored. (M1)
- **getWaitingCount / getWaiting counted prioritized jobs.** Now waiting-only,
  matching BullMQ and the Python SDK. (M2)

### Changed

- `addJobLog(id, message, level?)` accepts an optional level;
  `getJobLogs` formats entries as `[level] message` (no longer drops the level).
- `retryJobs`: the dead `count` field is no longer sent on the wire (the server
  has no partial RetryDlq; `count` is accepted only for API parity).
- Worker `FAIL` keeps the leading stack lines (`slice(0, N)`) so the error
  message is preserved on long stacks.

## [0.1.4] - initial published release

- Cross-runtime (Node/Bun/Deno) TCP client: `Queue`, `Worker`, `FlowProducer`,
  `Bunqueue` Simple Mode, msgpack wire protocol, TLS, auth, pipelining.
