package bunqueue

import (
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestWorkerProcessesJobResultAndEvents(t *testing.T) {
	queue := testQueue(t, "wk")
	var completed, active, drained atomic.Int64
	worker := NewWorker(queue.Name, func(job *Job) (any, error) {
		return map[string]any{"doubled": asInt(asMap(job.Data())["v"]) * 2}, nil
	}, WorkerOptions{Port: shared.port, PollTimeoutMs: 300})
	worker.On("active", func(...any) { active.Add(1) })
	worker.On("completed", func(...any) { completed.Add(1) })
	worker.On("drained", func(...any) { drained.Add(1) })
	startWorker(t, worker)

	job, err := queue.Add("calc", map[string]any{"v": 21}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !waitUntil(t, 15*time.Second, func() bool { return completed.Load() >= 1 }) {
		t.Fatal("job never completed")
	}
	if active.Load() != 1 || completed.Load() != 1 {
		t.Fatalf("events: active=%d completed=%d, want 1/1", active.Load(), completed.Load())
	}
	result, err := queue.GetResult(job.ID())
	if err != nil {
		t.Fatal(err)
	}
	if asInt(asMap(result)["doubled"]) != 42 {
		t.Fatalf("result = %v, want doubled 42", result)
	}
	if !waitUntil(t, 10*time.Second, func() bool { return drained.Load() >= 1 }) {
		t.Fatal("drained never fired after the backlog emptied")
	}
}

func TestWorkerTransientFailureRetries(t *testing.T) {
	queue := testQueue(t, "retry")
	var calls atomic.Int64
	worker := NewWorker(queue.Name, func(*Job) (any, error) {
		if calls.Add(1) == 1 {
			return nil, errors.New("transient glitch")
		}
		return "ok", nil
	}, WorkerOptions{Port: shared.port, PollTimeoutMs: 300})
	startWorker(t, worker)

	job, err := queue.Add("flaky", map[string]any{"x": 1}, JobOptions{"attempts": 3, "backoff": 50})
	if err != nil {
		t.Fatal(err)
	}
	if !waitUntil(t, 20*time.Second, func() bool {
		state, _ := queue.GetState(job.ID())
		return state == "completed"
	}) {
		t.Fatal("job never completed after retry")
	}
	if calls.Load() != 2 {
		t.Fatalf("processor ran %d times, want 2", calls.Load())
	}
}

func TestWorkerUnrecoverableGoesStraightToDlq(t *testing.T) {
	queue := testQueue(t, "unrec")
	var calls atomic.Int64
	worker := NewWorker(queue.Name, func(*Job) (any, error) {
		calls.Add(1)
		return nil, NewUnrecoverableError("poison payload")
	}, WorkerOptions{Port: shared.port, PollTimeoutMs: 300})
	startWorker(t, worker)

	if _, err := queue.Add("poison", map[string]any{"x": 1}, JobOptions{"attempts": 5}); err != nil {
		t.Fatal(err)
	}
	if !waitUntil(t, 20*time.Second, func() bool {
		entries, _ := queue.GetDlq(0)
		return len(entries) >= 1
	}) {
		t.Fatal("poison never reached the DLQ")
	}
	if calls.Load() != 1 {
		t.Fatalf("processor ran %d times, want 1 (no retries)", calls.Load())
	}
	if requeued, _ := queue.RetryDlq("", 0); requeued != 1 {
		t.Fatalf("retryDlq = %d, want 1", requeued)
	}
	worker.Stop() // don't reprocess the requeued poison forever
}

func TestWorkerFailStackLeadsWithMessage(t *testing.T) {
	queue := testQueue(t, "stk")
	worker := NewWorker(queue.Name, func(*Job) (any, error) {
		return nil, errors.New("BOOM-GO-STACK")
	}, WorkerOptions{Port: shared.port, PollTimeoutMs: 300})
	startWorker(t, worker)

	job, err := queue.Add("boom", nil, JobOptions{"attempts": 1})
	if err != nil {
		t.Fatal(err)
	}
	if !waitUntil(t, 20*time.Second, func() bool {
		state, _ := queue.GetState(job.ID())
		return state == "failed"
	}) {
		t.Fatal("job never failed")
	}
	failed, err := queue.GetJob(job.ID())
	if err != nil || failed == nil {
		t.Fatal("failed job must stay readable:", err)
	}
	stack := failed.Stacktrace()
	if len(stack) == 0 {
		t.Fatal("stack must be persisted on FAIL")
	}
	if !strings.Contains(stack[0], "BOOM-GO-STACK") {
		t.Fatalf("first stack line must carry the message, got %q", stack[0])
	}
}

func TestWorkerConcurrencyRespected(t *testing.T) {
	queue := testQueue(t, "conc")
	var now, peak int64
	var mu sync.Mutex
	worker := NewWorker(queue.Name, func(*Job) (any, error) {
		mu.Lock()
		now++
		if now > peak {
			peak = now
		}
		mu.Unlock()
		time.Sleep(150 * time.Millisecond)
		mu.Lock()
		now--
		mu.Unlock()
		return "ok", nil
	}, WorkerOptions{Port: shared.port, PollTimeoutMs: 300, Concurrency: 3})
	startWorker(t, worker)

	entries := make([]BulkEntry, 12)
	for i := range entries {
		entries[i] = BulkEntry{Name: "t", Data: map[string]any{"i": i}}
	}
	if _, err := queue.AddBulk(entries); err != nil {
		t.Fatal(err)
	}
	if !waitUntil(t, 30*time.Second, func() bool {
		counts, _ := queue.GetJobCounts()
		return counts["completed"] == 12
	}) {
		t.Fatal("batch never drained")
	}
	mu.Lock()
	defer mu.Unlock()
	if peak > 3 {
		t.Fatalf("concurrency exceeded: peak %d > 3", peak)
	}
}

func TestWorkerBrokerTimeoutSuppressesLateOutcomeEvents(t *testing.T) {
	queue := testQueue(t, "timeout-authority")
	var completed, failed, workerErrors atomic.Int64
	worker := NewWorker(queue.Name, func(job *Job) (any, error) {
		time.Sleep(500 * time.Millisecond)
		if asBool(asMap(job.Data())["fail"]) {
			return nil, errors.New("late processor failure")
		}
		return "late processor result", nil
	}, WorkerOptions{
		Port: shared.port, PollTimeoutMs: 300, Concurrency: 2, HeartbeatIntervalS: 0.05,
	})
	worker.On("completed", func(...any) { completed.Add(1) })
	worker.On("failed", func(...any) { failed.Add(1) })
	worker.On("error", func(...any) { workerErrors.Add(1) })
	startWorker(t, worker)

	success, err := queue.Add("late-success", map[string]any{"fail": false}, JobOptions{
		"attempts": 1, "timeout": 100,
	})
	if err != nil {
		t.Fatal(err)
	}
	failure, err := queue.Add("late-failure", map[string]any{"fail": true}, JobOptions{
		"attempts": 1, "timeout": 100,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !waitUntil(t, 15*time.Second, func() bool {
		successState, _ := queue.GetState(success.ID())
		failureState, _ := queue.GetState(failure.ID())
		return successState == "failed" && failureState == "failed"
	}) {
		t.Fatal("broker timeouts did not finalize both generations")
	}
	time.Sleep(700 * time.Millisecond)
	if completed.Load() != 0 || failed.Load() != 0 || workerErrors.Load() != 0 {
		t.Fatalf(
			"late outcomes leaked events: completed=%d failed=%d error=%d",
			completed.Load(), failed.Load(), workerErrors.Load(),
		)
	}
}

func TestWorkerRegisteredVisibleInListWorkers(t *testing.T) {
	queue := testQueue(t, "reg")
	worker := NewWorker(queue.Name, func(*Job) (any, error) { return "ok", nil },
		WorkerOptions{Port: shared.port, PollTimeoutMs: 300, Name: "go-e2e-worker"})
	startWorker(t, worker)

	// ListWorkers computes uptime from startedAt: an int64 startedAt would
	// crash the server with a BigInt arithmetic error (jsSafe guard).
	if !waitUntil(t, 10*time.Second, func() bool {
		workers, err := queue.GetWorkers()
		if err != nil {
			return false
		}
		for _, w := range workers {
			if asString(w["name"]) == "go-e2e-worker" {
				return true
			}
		}
		return false
	}) {
		t.Fatal("worker must be registered and listed")
	}
}

func TestMoveJobToFailedWithPullToken(t *testing.T) {
	queue := testQueue(t, "mtf")
	job, err := queue.Add("manual", map[string]any{"x": 1}, JobOptions{"attempts": 5})
	if err != nil {
		t.Fatal(err)
	}
	// FAIL requires an ACTIVE job: pull it first to obtain the lock token.
	pulled, err := queue.Connection.Call(map[string]any{
		"cmd": "PULL", "queue": queue.Name, "owner": "go-e2e", "timeout": 2000,
	})
	if err != nil {
		t.Fatal(err)
	}
	if toIDString(asMap(pulled["job"])["id"]) != job.ID() {
		t.Fatal("expected to pull the job we just added")
	}
	err = queue.MoveJobToFailed(job.ID(), NewUnrecoverableError("manual fail"), asString(pulled["token"]))
	if err != nil {
		t.Fatal(err)
	}
	if !waitUntil(t, 10*time.Second, func() bool {
		entries, _ := queue.GetDlq(0)
		return len(entries) >= 1
	}) {
		t.Fatal("unrecoverable moveJobToFailed must skip retries -> DLQ")
	}
}
