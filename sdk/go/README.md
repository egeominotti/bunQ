<div align="center">

<a href="https://bunqueue.dev">
  <img src="https://raw.githubusercontent.com/egeominotti/bunqueue/main/.github/logo.png" alt="bunqueue logo" width="110" />
</a>

# bunqueue-client (Go)

**The official Go client for [bunqueue](https://bunqueue.dev), the high performance job queue server.**

Native TCP protocol (msgpack), goroutine-based worker concurrency, one runtime dependency.

[![go reference](https://pkg.go.dev/badge/github.com/egeominotti/bunqueue/sdk/go.svg)](https://pkg.go.dev/github.com/egeominotti/bunqueue/sdk/go)
[![license](https://img.shields.io/badge/license-MIT-1a1a2e)](https://github.com/egeominotti/bunqueue/blob/main/LICENSE)
[![go](https://img.shields.io/badge/go-1.26.5%2B-2ea44f)](https://github.com/egeominotti/bunqueue/tree/main/sdk/go)
[![conformance](https://img.shields.io/badge/protocol-conformant%2017%2F17-d3156d)](https://github.com/egeominotti/bunqueue/tree/main/sdk/conformance)

[Documentation](https://bunqueue.dev/guide/sdks/) · [Protocol spec](https://github.com/egeominotti/bunqueue/blob/main/docs/protocol.md) · [Server](https://github.com/egeominotti/bunqueue) · [Changelog](https://github.com/egeominotti/bunqueue/blob/main/sdk/go/CHANGELOG.md)

</div>

---

Go client for [bunqueue](https://github.com/egeominotti/bunqueue). Talks the native TCP protocol (4-byte length prefix + msgpack) with the same core API as the TypeScript, Python and PHP SDKs. The bunqueue server runs on Bun (binary or Docker); this client lets any Go service produce and consume jobs on it: one queue, any language.

## Install

```bash
go get github.com/egeominotti/bunqueue/sdk/go
```

## Quick start

```go
package main

import (
	"fmt"

	bunqueue "github.com/egeominotti/bunqueue/sdk/go"
)

func main() {
	// Producer
	queue := bunqueue.NewQueue("emails", bunqueue.Options{Host: "localhost", Port: 6789})
	defer queue.Close()
	job, err := queue.Add("welcome", map[string]any{"to": "user@example.com"},
		bunqueue.JobOptions{"attempts": 3, "backoff": 1000})
	if err != nil {
		panic(err)
	}
	fmt.Println("queued", job.ID())

	// Worker (blocking; Concurrency > 1 uses a bounded goroutine pool)
	worker := bunqueue.NewWorker("emails", func(job *bunqueue.Job) (any, error) {
		fmt.Println("processing", job.Data())
		return map[string]any{"sent": true}, nil
	}, bunqueue.WorkerOptions{
		Port: 6789, Concurrency: 8, HeartbeatIntervalS: 10,
	})
	worker.On("completed", func(args ...any) {
		fmt.Println("completed", args[0].(*bunqueue.Job).ID())
	})
	worker.Run()
}
```

Return `bunqueue.NewUnrecoverableError("...")` from a processor to skip retries and send the job straight to the dead letter queue. Panics are recovered, FAILed with their real stack, and never kill the worker.

## Surface

Queues (add, bulk, custom ids, deduplication), query (jobs, states, results, progress, `WaitForJob`), control (pause, drain, clean, obliterate, promote, retry), DLQ (`GetDlq`, `RetryDlq`, `PurgeDlq`), schedulers (cron pattern or fixed interval, execution `Limit`), webhooks, rate limit, monitoring (`GetWorkers`, `GetStats`), and flows (`FlowProducer`: parent/children trees, chains, rollback).

Rate limits accept the delivery window and broker-side expiry:

```go
queue.SetRateLimit(100, bunqueue.RateLimitOptions{
	DurationMs: 60_000,
	TTLms:      300_000,
})
```

`SchedulerRepeat.SkipMissedOnRestart` and `PreventOverlap` are pointers so
both explicit `false` and omission reach the broker correctly.

TLS: pass `TLS: &bunqueue.TLSOptions{...}` — certificates are verified by default; use `CAFile` for a private CA.

## Worker leases and telemetry

Workers use independent pull, command and heartbeat connections, so a long
poll cannot delay ACK/FAIL or lock renewal. Set a positive
`HeartbeatIntervalS` to enable automatic renewal; zero, negative, NaN and
infinite values disable it.

Connection telemetry is optional, synchronous and payload-free:

```go
queue := bunqueue.NewQueue("emails", bunqueue.Options{
	OnEvent: func(event bunqueue.TelemetryEvent) {
		log.Printf("%s command=%s duration=%s error=%s",
			event.Type, event.Command, event.Duration, event.Error)
	},
})
```

The callback receives `connected`, `reconnect`, `auth`, `command`, `timeout`,
`error` and `close` events. Callback panics are isolated from command and worker
correctness; handlers should still return quickly.

## Testing

```bash
go vet ./...
go test -v ./... -count=1
go test -race -run 'Hardening|Regression|Worker' ./...
# -timeout must exceed the soak: `go test` panics at its own 10m default.
BUNQUEUE_SDK_SOAK_SECONDS=3600 go test -run '^TestSDKSoak$' -timeout 3900s -v
go test -run '^$' -fuzz '^FuzzHardeningPortableWirePayload$' -fuzztime 60s
cd ../.. && bun run test:sandbox:sdk
```

The suite spawns real bunqueue servers (requires [Bun](https://bun.sh) and the
repo checkout) and covers the full surface: wire framing and 64 MiB
boundaries, auth, telemetry, retries, DLQ, flows, schedulers, rate-limit TTL,
unicode and 1 MiB payloads, cyclic-input rejection, recursive typed int64 and
timestamp safety, worker lease isolation, crash/restart reconnection and shared
protocol alignment. Hardening adds independent-connection idempotency and
single-lease contention, 500 generated wire cases, a 512-job spike, the Go
race detector, native fuzzing, and an opt-in sustained profile.

## License

MIT. See the [LICENSE](https://github.com/egeominotti/bunqueue/blob/main/LICENSE) file.
Documentation: [bunqueue.dev/guide/sdks](https://bunqueue.dev/guide/sdks/).
Issues and feature requests: [GitHub issues](https://github.com/egeominotti/bunqueue/issues).
