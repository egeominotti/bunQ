package bunqueue

import (
	"testing"
	"time"
)

func TestAddGetJobRoundtrip(t *testing.T) {
	queue := testQueue(t, "rt")
	job, err := queue.Add("send-email", map[string]any{"to": "user@example.com", "n": 7}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if job.ID() == "" {
		t.Fatal("PUSH must return an id")
	}
	if !waitUntil(t, 10*time.Second, func() bool {
		fetched, _ := queue.GetJob(job.ID())
		return fetched != nil
	}) {
		t.Fatal("job not readable after the write-buffer flush")
	}
	fetched, err := queue.GetJob(job.ID())
	if err != nil || fetched == nil {
		t.Fatal("getJob failed:", err)
	}
	if fetched.Name() != "send-email" {
		t.Fatalf("name must travel inside data, got %q", fetched.Name())
	}
	data := fetched.Data()
	if data["to"] != "user@example.com" || asInt(data["n"]) != 7 {
		t.Fatalf("Data() must return the user payload, got %v", data)
	}
	missing, err := queue.GetJob("does-not-exist")
	if err != nil || missing != nil {
		t.Fatalf("not found must map to (nil, nil), got %v / %v", missing, err)
	}
}

func TestAddBulkPreservesCustomIDs(t *testing.T) {
	queue := testQueue(t, "bulk")
	ids, err := queue.AddBulk([]BulkEntry{
		{Name: "a", Data: map[string]any{"i": 1}, Opts: JobOptions{"jobId": "go-custom-1"}},
		{Name: "b", Data: map[string]any{"i": 2}},
		{Name: "c", Data: map[string]any{"i": 3}},
	})
	if err != nil || len(ids) != 3 {
		t.Fatalf("PUSHB must return every id: %v / %v", ids, err)
	}
	if !waitUntil(t, 10*time.Second, func() bool {
		found, _ := queue.GetJobByCustomID("go-custom-1")
		return found != nil
	}) {
		t.Fatal("customId must be preserved on PUSHB (jobId -> customId rename)")
	}
	if count, _ := queue.Count(); count != 3 {
		t.Fatalf("count = %d, want 3", count)
	}
}

func TestCustomJobIDIdempotent(t *testing.T) {
	queue := testQueue(t, "idem")
	first, err := queue.Add("t", map[string]any{"v": 1}, JobOptions{"jobId": "go-idem-1"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := queue.Add("t", map[string]any{"v": 2}, JobOptions{"jobId": "go-idem-1"})
	if err != nil {
		t.Fatal(err)
	}
	if first.ID() != second.ID() {
		t.Fatalf("same custom id must return the same job: %s vs %s", first.ID(), second.ID())
	}
	if count, _ := queue.Count(); count != 1 {
		t.Fatalf("count = %d, want 1 (no duplicate)", count)
	}
}

func TestDelayedThenPromote(t *testing.T) {
	queue := testQueue(t, "delay")
	job, err := queue.Add("later", map[string]any{"x": 1}, JobOptions{"delay": 60_000})
	if err != nil {
		t.Fatal(err)
	}
	if state, _ := queue.GetState(job.ID()); state != "delayed" {
		t.Fatalf("state = %q, want delayed", state)
	}
	if err := queue.Promote(job.ID()); err != nil {
		t.Fatal(err)
	}
	if !waitUntil(t, 10*time.Second, func() bool {
		state, _ := queue.GetState(job.ID())
		return state == "waiting"
	}) {
		t.Fatal("promote must move the job to waiting")
	}
}

func TestPauseResumeDrain(t *testing.T) {
	queue := testQueue(t, "ctrl")
	if err := queue.Pause(); err != nil {
		t.Fatal(err)
	}
	if paused, _ := queue.IsPaused(); !paused {
		t.Fatal("paused flag must be set")
	}
	if err := queue.Resume(); err != nil {
		t.Fatal(err)
	}
	if paused, _ := queue.IsPaused(); paused {
		t.Fatal("paused flag must clear")
	}
	if _, err := queue.AddBulk([]BulkEntry{
		{Name: "x", Data: map[string]any{"i": 1}},
		{Name: "x", Data: map[string]any{"i": 2}},
	}); err != nil {
		t.Fatal(err)
	}
	if !waitUntil(t, 10*time.Second, func() bool {
		count, _ := queue.Count()
		return count == 2
	}) {
		t.Fatal("jobs not enqueued")
	}
	if dropped, _ := queue.Drain(); dropped != 2 {
		t.Fatalf("drain = %d, want 2", dropped)
	}
	if count, _ := queue.Count(); count != 0 {
		t.Fatalf("count after drain = %d, want 0", count)
	}
}

func TestGetJobsPaginationStable(t *testing.T) {
	queue := testQueue(t, "page")
	entries := make([]BulkEntry, 30)
	for i := range entries {
		entries[i] = BulkEntry{Name: "evt", Data: map[string]any{"i": i}}
	}
	if _, err := queue.AddBulk(entries); err != nil {
		t.Fatal(err)
	}
	if !waitUntil(t, 10*time.Second, func() bool {
		jobs, _ := queue.GetJobs("waiting", 0, 30)
		return len(jobs) == 30
	}) {
		t.Fatal("jobs not all visible")
	}
	seen := map[string]bool{}
	for _, window := range [][2]int{{0, 10}, {10, 20}, {20, 30}} {
		jobs, err := queue.GetJobs("waiting", window[0], window[1])
		if err != nil {
			t.Fatal(err)
		}
		for _, job := range jobs {
			seen[job.ID()] = true
		}
	}
	if len(seen) != 30 {
		t.Fatalf("pages overlap or skip: %d unique ids, want 30", len(seen))
	}
}

func TestSchedulerLimitForwardedAsMaxLimit(t *testing.T) {
	queue := testQueue(t, "cron")
	schedID := uniqueName("sched")
	err := queue.UpsertJobScheduler(schedID,
		SchedulerRepeat{Pattern: "0 9 * * *", Limit: 3, Timezone: "UTC"},
		SchedulerTemplate{Data: map[string]any{"kind": "report"}},
	)
	if err != nil {
		t.Fatal(err)
	}
	sched, err := queue.GetJobScheduler(schedID)
	if err != nil || sched == nil {
		t.Fatal("scheduler must be readable:", err)
	}
	if asInt(sched["maxLimit"]) != 3 {
		t.Fatalf("limit must reach the wire as maxLimit, got %v", sched["maxLimit"])
	}
	if err := queue.RemoveJobScheduler(schedID); err != nil {
		t.Fatal(err)
	}
	if gone, _ := queue.GetJobScheduler(schedID); gone != nil {
		t.Fatal("deleted scheduler must map to nil")
	}
}

func TestWebhooksLifecycle(t *testing.T) {
	queue := testQueue(t, "wh")
	webhookID, err := queue.AddWebhook("https://example.com/bq-hook", []string{"job.completed"})
	if err != nil || webhookID == "" {
		t.Fatal("AddWebhook must return data.webhookId:", err)
	}
	hooks, err := queue.ListWebhooks()
	if err != nil || len(hooks) == 0 {
		t.Fatal("webhook must be listed:", err)
	}
	// Would fail if the SDK sent `webhookId` instead of `id`.
	if err := queue.SetWebhookEnabled(webhookID, false); err != nil {
		t.Fatal("SetWebhookEnabled must use the `id` field:", err)
	}
	if err := queue.RemoveWebhook(webhookID); err != nil {
		t.Fatal(err)
	}
	hooks, _ = queue.ListWebhooks()
	for _, hook := range hooks {
		if toIDString(hook["id"]) == webhookID {
			t.Fatal("webhook must be removed")
		}
	}
}

func TestMonitoringSurface(t *testing.T) {
	queue := testQueue(t, "mon")
	if _, err := queue.Add("t", map[string]any{"x": 1}, nil); err != nil {
		t.Fatal(err)
	}
	if pong, err := queue.Ping(); err != nil || !pong {
		t.Fatal("ping must answer pong:", err)
	}
	if _, err := queue.GetStats(); err != nil {
		t.Fatal("stats must decode:", err)
	}
	if !waitUntil(t, 10*time.Second, func() bool {
		queues, _ := queue.ListQueues()
		for _, name := range queues {
			if name == queue.Name {
				return true
			}
		}
		return false
	}) {
		t.Fatal("queue must be listed")
	}
	if err := queue.SetRateLimit(1000); err != nil {
		t.Fatal(err)
	}
	if err := queue.ClearRateLimit(); err != nil {
		t.Fatal(err)
	}
}
