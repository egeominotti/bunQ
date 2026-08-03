package bunqueue

import (
	"sync/atomic"
	"testing"
	"time"
)

// This is deliberately a multi-command business flow rather than a protocol
// probe: bulk ingestion, concurrent processing, persisted result lookup, and
// zero-loss accounting all pass through the real broker.
func TestRealisticConcurrentInvoiceResultsAreNotCrossed(t *testing.T) {
	queue := testQueue(t, "invoices")
	var completed atomic.Int64
	worker := NewWorker(queue.Name, func(job *Job) (any, error) {
		data := asMap(job.Data())
		return map[string]any{
			"invoice": asInt(data["invoice"]),
			"total":   asInt(data["cents"]) * 2,
		}, nil
	}, WorkerOptions{
		Port: shared.port, Concurrency: 12, BatchSize: 32, PollTimeoutMs: 300,
	})
	worker.On("completed", func(...any) { completed.Add(1) })
	startWorker(t, worker)

	entries := make([]BulkEntry, 64)
	for invoice := range entries {
		entries[invoice] = BulkEntry{
			Name: "reconcile",
			Data: map[string]any{"invoice": invoice, "cents": 101 + invoice},
		}
	}
	ids, err := queue.AddBulk(entries)
	if err != nil {
		t.Fatal(err)
	}
	if !waitUntil(t, 30*time.Second, func() bool { return completed.Load() == int64(len(ids)) }) {
		t.Fatalf("only %d/%d invoices completed", completed.Load(), len(ids))
	}

	checksum := 0
	for invoice, id := range ids {
		result, err := queue.GetResult(id)
		if err != nil {
			t.Fatal(err)
		}
		fields := asMap(result)
		if got := asInt(fields["invoice"]); got != invoice {
			t.Fatalf("result %d belongs to invoice %d", invoice, got)
		}
		want := (101 + invoice) * 2
		if got := asInt(fields["total"]); got != want {
			t.Fatalf("invoice %d total = %d, want %d", invoice, got, want)
		}
		checksum += want
	}
	if checksum != 16_960 {
		t.Fatalf("persisted result checksum = %d, want 16960", checksum)
	}
}
