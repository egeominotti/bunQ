# Changelog

All notable changes to the bunqueue Go SDK are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- Replace multi-command flow creation and rollback with a preallocated,
  reciprocal graph committed by one atomic `PUSHF`.
- Require exact ID and queue agreement in authoritative commit snapshots.
- Reject invalid queue names, reserved markers, user-owned topology options,
  and repeat/deduplication/debounce before transport.

### Added

- Rapid 1.3.0 shrinking properties for tree and chain topology, wire
  preservation, one-command atomicity, secure IDs, and invalid-input no-I/O.
- Gremlins 0.6.0 mutation gate for the pure planner, ID generator and snapshot
  validator, with 99.9% thresholds and a JSON report.
- Language-specific invariant and contributor documentation, including the
  compile-time `ChainStep` no-children guarantee.

## [0.1.0] - 2026-07-20

First tagged release: `go get github.com/egeominotti/bunqueue/sdk/go@v0.1.0`
(monorepo tag `sdk/go/v0.1.0`). Before this tag the module was only
installable as a pseudo-version.

### Added

- Optional payload-free connection telemetry through `Options.OnEvent` and
  `WorkerOptions.OnEvent`, covering connection, reconnection, authentication,
  commands, timeouts, errors and close with callback panic isolation.
- Rate-limit duration and broker-side TTL through `RateLimitOptions`.
- Scheduler `preventOverlap`, explicit `skipMissedOnRestart` booleans and
  direct `uniqueKey` forwarding.
- Add independent-connection races, 500 generated wire-property cases, a
  512-job spike, a native fuzz target, race-detector coverage, and an opt-in
  sustained profile.

### Fixed

- Recursively validate JavaScript-safe integers in typed maps, slices,
  pointers and structs instead of checking only `map[string]any` payloads;
  cyclic graphs and non-string map keys now fail with a typed connection error.
- Normalize `time.Time` payload values to JavaScript-safe Unix milliseconds.
- Normalize nested MessagePack ext type 0 values to `nil`.
- Apply the 64 MiB outgoing limit to the MessagePack body, excluding the
  four-byte frame header.
- Isolate pull, ACK/FAIL and heartbeat traffic on separate worker connections
  so long polling cannot delay completion or lock renewal.
- Treat zero, negative and non-finite heartbeat intervals as disabled, clamp
  negative poll timeouts and require successful worker registration before
  pulling.
