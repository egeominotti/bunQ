package bunqueue

import (
	"bytes"
	"encoding/hex"
	"math/rand"
	"sync"
	"testing"
	"testing/quick"
	"time"

	"github.com/vmihailenco/msgpack/v5"
)

func TestHardeningConcurrentCustomIDRetriesAreIdempotent(t *testing.T) {
	name := uniqueName("idempotency-race")
	queues := make([]*Queue, 24)
	for index := range queues {
		queues[index] = NewQueue(name, Options{Port: shared.port})
		defer queues[index].Close()
	}
	defer func() { _ = queues[0].Obliterate() }()

	ids := make(chan string, len(queues))
	errs := make(chan error, len(queues))
	var group sync.WaitGroup
	for index, queue := range queues {
		group.Add(1)
		go func(attempt int, contender *Queue) {
			defer group.Done()
			job, err := contender.Add(
				"charge",
				map[string]any{"attempt": attempt},
				JobOptions{"jobId": "same-operation-id"},
			)
			if err != nil {
				errs <- err
				return
			}
			ids <- job.ID()
		}(index, queue)
	}
	group.Wait()
	close(ids)
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	unique := map[string]bool{}
	for id := range ids {
		unique[id] = true
	}
	if len(unique) != 1 {
		t.Fatalf("concurrent retries returned %d ids: %v", len(unique), unique)
	}
	if count, err := queues[0].Count(); err != nil || count != 1 {
		t.Fatalf("queue count = %d / %v, want exactly one", count, err)
	}
}

func TestHardeningSimultaneousDequeuesLeaseExactlyOnce(t *testing.T) {
	name := uniqueName("double-dequeue")
	producer := NewQueue(name, Options{Port: shared.port})
	defer producer.Close()
	defer func() { _ = producer.Obliterate() }()
	expected, err := producer.Add("only-once", map[string]any{"value": 1}, nil)
	if err != nil {
		t.Fatal(err)
	}

	contenders := make([]*Queue, 12)
	responses := make(chan map[string]any, len(contenders))
	errs := make(chan error, len(contenders))
	var group sync.WaitGroup
	for index := range contenders {
		contenders[index] = NewQueue(name, Options{Port: shared.port})
		defer contenders[index].Close()
		group.Add(1)
		go func(owner int, queue *Queue) {
			defer group.Done()
			response, callErr := queue.Connection.Call(map[string]any{
				"cmd": "PULL", "queue": name, "owner": owner, "timeout": 250,
			})
			if callErr != nil {
				errs <- callErr
				return
			}
			responses <- response
		}(index, contenders[index])
	}
	group.Wait()
	close(responses)
	close(errs)
	for callErr := range errs {
		t.Fatal(callErr)
	}
	leased := []string{}
	for response := range responses {
		if id := toIDString(asMap(response["job"])["id"]); id != "<nil>" && id != "" {
			leased = append(leased, id)
		}
	}
	if len(leased) != 1 || leased[0] != expected.ID() {
		t.Fatalf("leases = %v, want only %s", leased, expected.ID())
	}
}

func TestHardeningGeneratedPayloadsPreserveInvariants(t *testing.T) {
	queue := testQueue(t, "generated")
	random := rand.New(rand.NewSource(0xBADC0DE))
	payloads := make([]map[string]any, 64)
	entries := make([]BulkEntry, len(payloads))
	for index := range payloads {
		sample := random.Int31()
		payloads[index] = map[string]any{
			"index": index, "signed": int(sample%2_000_001) - 1_000_000,
			"flag": sample&1 == 1, "text": hex.EncodeToString([]byte{byte(sample), byte(index)}),
			"nested": []any{int(sample % 97), map[string]any{"checksum": int(sample % 1_000_003)}},
		}
		entries[index] = BulkEntry{
			Name: "generated", Data: payloads[index], Opts: JobOptions{},
		}
	}
	ids, err := queue.AddBulk(entries)
	if err != nil || len(ids) != len(entries) {
		t.Fatalf("bulk generated payloads = %d / %v", len(ids), err)
	}
	for index, id := range ids {
		job, getErr := queue.GetJob(id)
		if getErr != nil || job == nil {
			t.Fatalf("generated job %d unavailable: %v", index, getErr)
		}
		data := asMap(job.Data())
		if asInt(data["index"]) != index ||
			asInt(data["signed"]) != payloads[index]["signed"] ||
			asBool(data["flag"]) != payloads[index]["flag"] ||
			asString(data["text"]) != payloads[index]["text"] {
			t.Fatalf("generated payload %d changed: %v", index, data)
		}
		nested := asSlice(data["nested"])
		if asInt(nested[0]) != asInt(asSlice(payloads[index]["nested"])[0]) {
			t.Fatalf("generated nested payload %d changed: %v", index, nested)
		}
	}
}

func TestHardeningSpikeBurstRecoversWithoutLoss(t *testing.T) {
	queue := testQueue(t, "spike")
	entries := make([]BulkEntry, 512)
	for index := range entries {
		entries[index] = BulkEntry{
			Name: "spike", Data: map[string]any{"index": index}, Opts: JobOptions{},
		}
	}
	ids, err := queue.AddBulk(entries)
	if err != nil || len(ids) != len(entries) {
		t.Fatalf("spike accepted %d/%d jobs: %v", len(ids), len(entries), err)
	}
	if count, err := queue.Count(); err != nil || count != len(entries) {
		t.Fatalf("spike count = %d / %v", count, err)
	}
	if drained, err := queue.Drain(); err != nil || drained != len(entries) {
		t.Fatalf("spike drain = %d / %v", drained, err)
	}
	if count, err := queue.Count(); err != nil || count != 0 {
		t.Fatalf("post-spike count = %d / %v", count, err)
	}
}

func TestHardeningPropertyPortableWireRoundTrip(t *testing.T) {
	property := func(number int32, blob []byte) bool {
		command := map[string]any{
			"number": number,
			"blob":   blob,
			"text":   hex.EncodeToString(blob),
		}
		frame, err := encodeFrame(command)
		if err != nil {
			return false
		}
		var decoded map[string]any
		if msgpack.Unmarshal(frame, &decoded) != nil {
			return false
		}
		return asInt(decoded["number"]) == int(number) &&
			bytes.Equal(decoded["blob"].([]byte), blob) &&
			asString(decoded["text"]) == hex.EncodeToString(blob)
	}
	config := &quick.Config{MaxCount: 500, Rand: rand.New(rand.NewSource(0xC0FFEE))}
	if err := quick.Check(property, config); err != nil {
		t.Fatal(err)
	}
}

func FuzzHardeningPortableWirePayload(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0, 1, 2, 255})
	f.Add([]byte("unicode-🧪-payload"))
	f.Fuzz(func(t *testing.T, blob []byte) {
		frame, err := encodeFrame(map[string]any{
			"blob": blob, "hex": hex.EncodeToString(blob), "size": len(blob),
		})
		if err != nil {
			return
		}
		var decoded map[string]any
		if err := msgpack.Unmarshal(frame, &decoded); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(decoded["blob"].([]byte), blob) {
			t.Fatal("binary payload changed during fuzz round-trip")
		}
	})
}

func TestHardeningConnectionRemainsUsableAfterContention(t *testing.T) {
	queue := testQueue(t, "contention-health")
	for range 32 {
		if _, err := queue.Connection.CallTimeout(
			map[string]any{"cmd": "Ping"}, time.Second,
		); err != nil {
			t.Fatal(err)
		}
	}
}
