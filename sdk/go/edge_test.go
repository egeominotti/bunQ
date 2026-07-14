package bunqueue

import (
	"strings"
	"testing"
	"time"
)

func TestEdgeUnicodePayload(t *testing.T) {
	queue := testQueue(t, "uni")
	payload := map[string]any{
		"emoji":  "🚀🔥💯",
		"cjk":    "以呂波耳本部止",
		"rtl":    "مرحبا بالعالم",
		"nested": map[string]any{"list": []any{1, "två", map[string]any{"deep": "ключ"}}},
	}
	job, err := queue.Add("uni", payload, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !waitUntil(t, 10*time.Second, func() bool {
		fetched, _ := queue.GetJob(job.ID())
		return fetched != nil
	}) {
		t.Fatal("job not readable")
	}
	fetched, _ := queue.GetJob(job.ID())
	data := fetched.Data()
	for _, key := range []string{"emoji", "cjk", "rtl"} {
		if data[key] != payload[key] {
			t.Fatalf("%s corrupted by the msgpack roundtrip: %v", key, data[key])
		}
	}
	deep := asMap(asSlice(asMap(data["nested"])["list"])[2])["deep"]
	if deep != "ключ" {
		t.Fatalf("nested structure corrupted: %v", deep)
	}
}

func TestEdgeOneMBPayload(t *testing.T) {
	queue := testQueue(t, "big")
	blob := strings.Repeat("x", 1024*1024)
	job, err := queue.Add("blob", map[string]any{"blob": blob}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !waitUntil(t, 10*time.Second, func() bool {
		fetched, _ := queue.GetJob(job.ID())
		return fetched != nil
	}) {
		t.Fatal("job not readable")
	}
	fetched, _ := queue.GetJob(job.ID())
	if fetched.Data()["blob"] != blob {
		t.Fatal("1MB payload must survive the roundtrip")
	}
}

func TestEdgeInt64PayloadIsJsSafe(t *testing.T) {
	queue := testQueue(t, "i64")
	// 9999999999999 > int32: without jsSafe it would travel as msgpack int64
	// and hit the server-side BigInt arithmetic crash class.
	job, err := queue.Add("big-int", map[string]any{"epochMs": int64(9_999_999_999_999), "small": 42}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !waitUntil(t, 10*time.Second, func() bool {
		fetched, _ := queue.GetJob(job.ID())
		return fetched != nil
	}) {
		t.Fatal("server unhealthy after int64 traffic")
	}
	fetched, _ := queue.GetJob(job.ID())
	data := fetched.Data()
	if got := int64(asInt(data["epochMs"])); got != 9_999_999_999_999 {
		t.Fatalf("epochMs = %d, want exact value (float64 exact <= 2^53)", got)
	}
	if asInt(data["small"]) != 42 {
		t.Fatalf("small int corrupted: %v", data["small"])
	}
	if pong, err := queue.Ping(); err != nil || !pong {
		t.Fatal("connection must stay healthy after int64 traffic")
	}
}

func TestEdgeMultiQueueIsolation(t *testing.T) {
	qa := testQueue(t, "iso-a")
	qb := testQueue(t, "iso-b")
	if _, err := qa.Add("a", map[string]any{"q": "a"}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := qb.Add("b", map[string]any{"q": "b"}, nil); err != nil {
		t.Fatal(err)
	}
	if !waitUntil(t, 10*time.Second, func() bool {
		ca, _ := qa.Count()
		cb, _ := qb.Count()
		return ca == 1 && cb == 1
	}) {
		t.Fatal("both jobs must be queued")
	}
	if dropped, _ := qa.Drain(); dropped != 1 {
		t.Fatal("drain A must touch only A")
	}
	if count, _ := qb.Count(); count != 1 {
		t.Fatal("queue B must be untouched")
	}
}

func TestEdgeEmptyLongPollWaits(t *testing.T) {
	queue := testQueue(t, "empty")
	started := time.Now()
	response, err := queue.Connection.Call(map[string]any{
		"cmd": "PULLB", "queue": queue.Name, "count": 1, "timeout": 800, "owner": "go-edge",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(asSlice(response["jobs"])) != 0 {
		t.Fatal("empty queue must yield no jobs")
	}
	if elapsed := time.Since(started); elapsed < 500*time.Millisecond {
		t.Fatalf("long-poll must actually wait, returned in %v", elapsed)
	}
}

func TestEdgeCrashRestartReconnect(t *testing.T) {
	own := newTestServer(nil)
	if err := own.start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(own.stop)
	queue := NewQueue(uniqueName("crash"), Options{Port: own.port})
	t.Cleanup(queue.Close)

	// durable: bypass the 10ms write buffer — a SIGKILL right after a
	// buffered add can legitimately lose it (documented buffered-mode risk).
	if _, err := queue.Add("before", map[string]any{"i": 1}, JobOptions{"durable": true}); err != nil {
		t.Fatal(err)
	}
	if !waitUntil(t, 10*time.Second, func() bool {
		count, _ := queue.Count()
		return count == 1
	}) {
		t.Fatal("first add not visible")
	}

	own.crash()
	time.Sleep(300 * time.Millisecond)
	if err := own.start(); err != nil { // same port, same data dir
		t.Fatal(err)
	}

	// The first call after the crash may hit the torn socket: the connection
	// reconnects lazily, so a retry loop is the honest client pattern.
	if !waitUntil(t, 15*time.Second, func() bool {
		_, err := queue.Add("after", map[string]any{"i": 2}, nil)
		return err == nil
	}) {
		t.Fatal("producer must reconnect after the restart")
	}
	if !waitUntil(t, 10*time.Second, func() bool {
		count, _ := queue.Count()
		return count >= 2
	}) {
		t.Fatal("both jobs must be present (persistence + reconnect)")
	}
}
