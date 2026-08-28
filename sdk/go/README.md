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
[![conformance](https://img.shields.io/badge/protocol-conformant%2018%2F18-d3156d)](https://github.com/egeominotti/bunqueue/tree/main/sdk/conformance)

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

Job names travel as top-level protocol metadata. `Job.Name()` reads that
field, while `Job.Data()` returns `any` so maps, slices, scalars, and nil remain
unchanged, including a user-owned map `name`. Legacy `data.name` envelopes are
still decoded. Scheduler templates use separate `jobName` and `data` fields.
The client negotiates protocol v3 and advertises `separate-job-name` in `Hello`.

Return `bunqueue.NewUnrecoverableError("...")` from a processor to skip retries and send the job straight to the dead letter queue. Panics are recovered, FAILed with their real stack, and never kill the worker.

## Surface

Queues (add, bulk, custom ids, deduplication), query (jobs, states, results, progress, `WaitForJob`), control (pause, drain, clean, obliterate, promote, retry), DLQ (`GetDlq`, `RetryDlq`, `PurgeDlq`), schedulers (cron pattern or fixed interval, execution `Limit`), webhooks, rate limit, monitoring (`GetWorkers`, `GetStats`), and atomic flows (`FlowProducer`: parent/children trees, chains, reconstruction).

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

## Atomic flows

The producer resolves IDs and reciprocal edges before transport and sends one
`PUSHF`. Local validation errors make zero calls; broker rejection cannot leave
a partially linked graph.

```go
flow := bunqueue.NewFlowProducer(bunqueue.Options{
	Host: "localhost",
	Port: 6789,
})
defer flow.Close()

tree, err := flow.Add(bunqueue.FlowJob{
	Name:      "publish-release",
	QueueName: "release",
	Opts:      bunqueue.JobOptions{"jobId": "release-2026-07-30"},
	Children: []bunqueue.FlowJob{
		{Name: "build", QueueName: "build"},
		{Name: "test", QueueName: "test"},
	},
})
if err != nil {
	panic(err)
}
fmt.Printf("root=%s children=%d\n", tree.Job.ID(), len(tree.Children))

ids, err := flow.AddChain([]bunqueue.ChainStep{
	{Name: "extract", QueueName: "etl"},
	{Name: "transform", QueueName: "etl"},
	{Name: "load", QueueName: "etl"},
})
if err != nil {
	panic(err)
}
fmt.Println("chain", ids)
```

`FlowJob.Children` represents tree topology. `ChainStep` deliberately has no
`Children` field, so nested chain topology is impossible at compile time.
The planner owns `parentId`, `dependsOn`, and `childrenIds`; flow data cannot
overwrite `name` or `__*`, and repeat/deduplication/debounce are rejected.
Returned snapshots must match every requested ID and queue exactly. See
[INVARIANTS.md](INVARIANTS.md#flowproducer-and-atomic-pushf).

A timeout after `PUSHF` is ambiguous: the broker may already have committed.
For a production graph that callers can retry, give every node the same stable
explicit `bunqueue.JobOptions{"jobId": stableID}` on each attempt. A retry
commits when the first call did not; otherwise strict collision checking returns
`already exists`. Treat that error as a reconciliation signal and query the
known IDs; the SDK does not fabricate the original snapshots.

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

`completed` and `failed` are broker-authoritative. A broker timeout or retired
cron generation can finalize a lease before user code returns; the resulting
late ACK/FAIL response is ignored without incrementing Worker counters or
emitting a false terminal event. Invalid outcome evidence is emitted as
`error`.

## Testing

```bash
go vet ./...
go test -v ./... -count=1
go test -run 'FlowPlanner|FlowProducerRejectsOwnedTopology|FlowCommit|RandomFlowID' \
  -count=1 -v ./...                    # Rapid properties + flow unit checks
go test -race -run 'Hardening|Regression|Worker' ./...
# -timeout must exceed the soak: `go test` panics at its own 10m default.
BUNQUEUE_SDK_SOAK_SECONDS=3600 go test -run '^TestSDKSoak$' -timeout 3900s -v
go test -run '^$' -fuzz '^FuzzHardeningPortableWirePayload$' -fuzztime 60s
cd ../.. && bun run test:sandbox:sdk
```

The flow planner, secure ID generator and pure snapshot validator also have a
mutation gate:

```bash
GOBIN="$(go env GOPATH)/bin" \
  go install github.com/go-gremlins/gremlins/cmd/gremlins@v0.6.0
mkdir -p build
gremlins unleash --config .gremlins.yaml
```

[`go.mod`](go.mod) pins Rapid 1.3.0 for shrinking property tests.
[`.gremlins.yaml`](.gremlins.yaml) enforces a
99.9% threshold; `build/gremlins.json` records the full campaign.

The suite spawns real bunqueue servers (requires [Bun](https://bun.sh) and the
repo checkout) and covers the full surface: wire framing and 64 MiB
boundaries, auth, telemetry, retries, DLQ, flows, schedulers, rate-limit TTL,
unicode and 1 MiB payloads, cyclic-input rejection, recursive typed int64 and
timestamp safety, worker lease isolation, crash/restart reconnection and shared
protocol alignment. Hardening adds independent-connection idempotency and
single-lease contention, 500 generated wire cases, a 512-job spike, the Go
race detector, native fuzzing, and an opt-in sustained profile.

Maintainers should read the [runtime invariants](INVARIANTS.md), the
[module and protocol guide](CLAUDE.md), and the
[local agent rules](AGENTS.md) before changing behavior.

## License

MIT. See the [LICENSE](https://github.com/egeominotti/bunqueue/blob/main/LICENSE) file.
Documentation: [bunqueue.dev/guide/sdks](https://bunqueue.dev/guide/sdks/).
Issues and feature requests: [GitHub issues](https://github.com/egeominotti/bunqueue/issues).
