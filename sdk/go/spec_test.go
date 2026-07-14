package bunqueue

import (
	"strings"
	"testing"
	"time"
)

func TestSpecBatchSizeClamps(t *testing.T) {
	queue := testQueue(t, "cap")
	capped := NewWorker(queue.Name, func(*Job) (any, error) { return "ok", nil },
		WorkerOptions{Port: shared.port, PollTimeoutMs: 300, BatchSize: 5000})
	floored := NewWorker(queue.Name, func(*Job) (any, error) { return "ok", nil },
		WorkerOptions{Port: shared.port, PollTimeoutMs: 300, BatchSize: -5})
	if capped.BatchSize() != 1000 {
		t.Fatalf("batchSize 5000 must clamp to 1000, got %d", capped.BatchSize())
	}
	if floored.BatchSize() != 1 {
		t.Fatalf("batchSize -5 must clamp to 1, got %d", floored.BatchSize())
	}
	startWorker(t, capped)
	if _, err := queue.Add("t", map[string]any{"x": 1}, nil); err != nil {
		t.Fatal(err)
	}
	if !waitUntil(t, 15*time.Second, func() bool {
		counts, _ := queue.GetJobCounts()
		return counts["completed"] >= 1
	}) {
		t.Fatal("clamped worker must still pull (PULLB count within the server cap)")
	}
	floored.Close()
}

func TestSpecHeartbeatDisabled(t *testing.T) {
	queue := testQueue(t, "hb0")
	worker := NewWorker(queue.Name, func(*Job) (any, error) { return "ok", nil },
		WorkerOptions{Port: shared.port, PollTimeoutMs: 300, DisableHeartbeat: true})
	if worker.HeartbeatIntervalS() != 0 {
		t.Fatalf("DisableHeartbeat must store 0, got %v", worker.HeartbeatIntervalS())
	}
	negative := NewWorker(queue.Name, func(*Job) (any, error) { return "ok", nil },
		WorkerOptions{Port: shared.port, PollTimeoutMs: 300, HeartbeatIntervalS: -1})
	if negative.HeartbeatIntervalS() != 0 {
		t.Fatalf("negative interval must disable, got %v", negative.HeartbeatIntervalS())
	}
	startWorker(t, worker)
	if _, err := queue.Add("t", map[string]any{"x": 1}, nil); err != nil {
		t.Fatal(err)
	}
	if !waitUntil(t, 15*time.Second, func() bool {
		counts, _ := queue.GetJobCounts()
		return counts["completed"] >= 1
	}) {
		t.Fatal("worker must be fully functional without heartbeats")
	}
	negative.Close()
}

func TestSpecWaitForJobClampsTimeout(t *testing.T) {
	queue := testQueue(t, "wclamp")
	worker := NewWorker(queue.Name, func(*Job) (any, error) {
		return map[string]any{"done": true}, nil
	}, WorkerOptions{Port: shared.port, PollTimeoutMs: 300})
	startWorker(t, worker)

	job, err := queue.Add("t", map[string]any{"x": 1}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !waitUntil(t, 15*time.Second, func() bool {
		state, _ := queue.GetState(job.ID())
		return state == "completed"
	}) {
		t.Fatal("job never completed")
	}
	result, err := queue.WaitForJob(job.ID(), 700_000) // pre-clamp: server error
	if err != nil {
		t.Fatal("timeout beyond the cap must clamp, not error:", err)
	}
	if !asBool(asMap(result)["done"]) {
		t.Fatalf("result = %v, want done", result)
	}
}

func TestSpecProtocolVersionMatchesServer(t *testing.T) {
	queue := testQueue(t, "hello")
	hello, err := queue.Connection.Hello()
	if err != nil {
		t.Fatal(err)
	}
	if asInt(hello["protocolVersion"]) != ProtocolVersion {
		t.Fatalf("client v%d != server v%v", ProtocolVersion, hello["protocolVersion"])
	}
}

func TestSpecPerJobStackTraceLimitHonored(t *testing.T) {
	queue := testQueue(t, "stk30")
	worker := NewWorker(queue.Name, func(*Job) (any, error) {
		panic("BOOM-DEEP-GO") // panic carries a real (deep) stack
	}, WorkerOptions{Port: shared.port, PollTimeoutMs: 300})
	startWorker(t, worker)

	job, err := queue.Add("t", nil, JobOptions{"attempts": 1, "stackTraceLimit": 30})
	if err != nil {
		t.Fatal(err)
	}
	if !waitUntil(t, 20*time.Second, func() bool {
		state, _ := queue.GetState(job.ID())
		return state == "failed"
	}) {
		t.Fatal("job never failed")
	}
	failed, _ := queue.GetJob(job.ID())
	if failed == nil {
		t.Fatal("failed job must stay readable")
	}
	stack := failed.Stacktrace()
	if len(stack) <= 10 {
		t.Fatalf("per-job stackTraceLimit must deepen the stack, got %d lines", len(stack))
	}
	if !strings.Contains(stack[0], "BOOM-DEEP-GO") {
		t.Fatalf("message must lead the persisted stack, got %q", stack[0])
	}
}

func TestSpecAuth(t *testing.T) {
	authServer := newTestServer(map[string]string{"AUTH_TOKENS": "go-secret"})
	if err := authServer.start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(authServer.stop)

	wrong := NewQueue("auth-q", Options{Port: authServer.port, Token: "wrong"})
	t.Cleanup(wrong.Close)
	if _, err := wrong.Count(); err == nil {
		t.Fatal("wrong token must be rejected")
	} else if _, ok := err.(*AuthError); !ok {
		t.Fatalf("expected *AuthError, got %T (%v)", err, err)
	}

	valid := NewQueue("auth-q", Options{Port: authServer.port, Token: "go-secret"})
	t.Cleanup(valid.Close)
	if _, err := valid.Add("t", map[string]any{"x": 1}, nil); err != nil {
		t.Fatal("valid token must work:", err)
	}
	if count, _ := valid.Count(); count != 1 {
		t.Fatal("valid token must operate the queue")
	}
}
