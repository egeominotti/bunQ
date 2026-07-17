package bunqueue

import (
	"os"
	"runtime"
	"strconv"
	"testing"
	"time"
)

func TestSDKSoak(t *testing.T) {
	rawSeconds := os.Getenv("BUNQUEUE_SDK_SOAK_SECONDS")
	if rawSeconds == "" {
		t.Skip("set BUNQUEUE_SDK_SOAK_SECONDS to run the sustained profile")
	}
	seconds, err := strconv.Atoi(rawSeconds)
	if err != nil || seconds < 1 {
		t.Fatal("BUNQUEUE_SDK_SOAK_SECONDS must be a positive integer")
	}
	batchSize := 100
	if rawBatch := os.Getenv("BUNQUEUE_SDK_SOAK_BATCH"); rawBatch != "" {
		batchSize, err = strconv.Atoi(rawBatch)
		if err != nil || batchSize < 1 {
			t.Fatal("BUNQUEUE_SDK_SOAK_BATCH must be a positive integer")
		}
	}
	queue := testQueue(t, "go-soak")
	deadline := time.Now().Add(time.Duration(seconds) * time.Second)
	var start, finish runtime.MemStats
	runtime.ReadMemStats(&start)
	iterations := 0
	jobs := 0
	for time.Now().Before(deadline) {
		entries := make([]BulkEntry, batchSize)
		for index := range entries {
			entries[index] = BulkEntry{
				Name: "soak",
				Data: map[string]any{"iteration": iterations, "index": index},
			}
		}
		ids, addErr := queue.AddBulk(entries)
		if addErr != nil || len(ids) != batchSize {
			t.Fatalf("soak add = %d / %v", len(ids), addErr)
		}
		if count, countErr := queue.Count(); countErr != nil || count != batchSize {
			t.Fatalf("soak count = %d / %v", count, countErr)
		}
		if job, getErr := queue.GetJob(ids[0]); getErr != nil || job == nil {
			t.Fatalf("soak query failed: %v", getErr)
		}
		if obliterateErr := queue.Obliterate(); obliterateErr != nil {
			t.Fatal(obliterateErr)
		}
		iterations++
		jobs += len(ids)
	}
	runtime.ReadMemStats(&finish)
	t.Logf(
		"profile=go-soak seconds=%d batch=%d iterations=%d jobs=%d heap_start=%d heap_end=%d",
		seconds, batchSize, iterations, jobs, start.HeapAlloc, finish.HeapAlloc,
	)
}
