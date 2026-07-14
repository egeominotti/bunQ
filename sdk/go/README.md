<div align="center">

<a href="https://bunqueue.dev">
  <img src="https://raw.githubusercontent.com/egeominotti/bunqueue/main/.github/logo.png" alt="bunqueue logo" width="110" />
</a>

# bunqueue-client (Go)

**The official Go client for [bunqueue](https://bunqueue.dev), the high performance job queue server.**

Native TCP protocol (msgpack), goroutine-based worker concurrency, one runtime dependency.

[![license](https://img.shields.io/badge/license-MIT-1a1a2e)](https://github.com/egeominotti/bunqueue/blob/main/LICENSE)
[![go](https://img.shields.io/badge/go-1.22%2B-2ea44f)](https://github.com/egeominotti/bunqueue/tree/main/sdk/go)

[Documentation](https://bunqueue.dev/guide/sdks/) · [Server](https://github.com/egeominotti/bunqueue) · [TypeScript SDK](https://www.npmjs.com/package/bunqueue-client) · [Python SDK](https://github.com/egeominotti/bunqueue/tree/main/sdk/python) · [PHP SDK](https://github.com/egeominotti/bunqueue/tree/main/sdk/php)

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
	}, bunqueue.WorkerOptions{Port: 6789, Concurrency: 8})
	worker.On("completed", func(args ...any) {
		fmt.Println("completed", args[0].(*bunqueue.Job).ID())
	})
	worker.Run()
}
```

Return `bunqueue.NewUnrecoverableError("...")` from a processor to skip retries and send the job straight to the dead letter queue. Panics are recovered, FAILed with their real stack, and never kill the worker.

## Surface

Queues (add, bulk, custom ids, deduplication), query (jobs, states, results, progress, `WaitForJob`), control (pause, drain, clean, obliterate, promote, retry), DLQ (`GetDlq`, `RetryDlq`, `PurgeDlq`), schedulers (cron pattern or fixed interval, execution `Limit`), webhooks, rate limit, monitoring (`GetWorkers`, `GetStats`), and flows (`FlowProducer`: parent/children trees, chains, rollback).

TLS: pass `TLS: &bunqueue.TLSOptions{...}` — certificates are verified by default; use `CAFile` for a private CA.

## Testing

```bash
go vet ./...
go test ./... -count=1
```

The e2e suite spawns a real bunqueue server (requires [Bun](https://bun.sh) and the repo checkout) and covers the full surface: wire framing, auth, retries, DLQ, flows, schedulers, unicode and 1MB payloads, int64 safety, crash + restart reconnection, and the protocol spec-alignment checks shared with the other official SDKs.

## License

MIT
