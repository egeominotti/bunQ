# Changelog

All notable changes to the bunqueue Go SDK are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
