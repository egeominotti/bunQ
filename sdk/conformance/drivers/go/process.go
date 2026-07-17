package main

import (
	"fmt"
	"sync"
	"time"

	bunqueue "github.com/egeominotti/bunqueue/sdk/go"
)

func queueFor(name string) *bunqueue.Queue {
	if queue, ok := queues[name]; ok {
		return queue
	}
	queue := bunqueue.NewQueue(name, connection)
	queues[name] = queue
	return queue
}

func lookup() *bunqueue.Queue { return queueFor("conf-lookup") }

func deepThrow(n int) {
	if n <= 0 {
		panic("BOOM-CONFORMANCE")
	}
	deepThrow(n - 1)
}

func jobView(job *bunqueue.Job) map[string]any {
	if job == nil {
		return nil
	}
	view := map[string]any{"id": job.ID(), "name": job.Name(), "data": job.Data()}
	if stack := job.Stacktrace(); stack != nil {
		view["stacktrace"] = stack
	}
	return view
}

func toOpts(raw map[string]any) bunqueue.JobOptions {
	if raw == nil {
		return nil
	}
	return bunqueue.JobOptions(raw)
}

func processUntil(req map[string]any) error {
	queue := queueFor(str(req["queue"]))
	behavior := str(req["behavior"])
	until, _ := req["until"].(map[string]any)
	timeout := time.Duration(num(req["timeoutMs"], 20000)) * time.Millisecond

	var mu sync.Mutex
	failedOnce := map[string]bool{}
	processor := func(job *bunqueue.Job) (any, error) {
		switch behavior {
		case "unrecoverable":
			return nil, bunqueue.NewUnrecoverableError("conformance poison")
		case "deepThrow":
			deepThrow(25)
		case "failOnce":
			mu.Lock()
			first := !failedOnce[job.ID()]
			failedOnce[job.ID()] = true
			mu.Unlock()
			if first {
				return nil, fmt.Errorf("conformance transient")
			}
		}
		if result, ok := req["result"]; ok {
			return result, nil
		}
		return "ok", nil
	}

	workerOpts := bunqueue.WorkerOptions{
		Host:          connection.Host,
		Port:          connection.Port,
		Token:         connection.Token,
		PollTimeoutMs: 300,
	}
	if batch, ok := req["batchSize"]; ok {
		workerOpts.BatchSize = int(num(batch, 10)) // verbatim: the SDK must clamp
	}
	worker := bunqueue.NewWorker(str(req["queue"]), processor, workerOpts)
	worker.On("error", func(...any) {})
	worker.On("failed", func(...any) {})
	done := make(chan struct{})
	go func() {
		worker.Run()
		close(done)
	}()
	defer func() {
		worker.Stop()
		select {
		case <-done:
		case <-time.After(15 * time.Second):
		}
	}()

	reached := func() bool {
		counts, err := queue.GetJobCounts()
		if err != nil {
			return false
		}
		if want, ok := until["completed"]; ok && counts["completed"] < int(num(want, 0)) {
			return false
		}
		if want, ok := until["failed"]; ok && counts["failed"] < int(num(want, 0)) {
			return false
		}
		if want, ok := until["dlq"]; ok {
			entries, err := queue.GetDlq(0)
			if err != nil || len(entries) < int(num(want, 0)) {
				return false
			}
		}
		return true
	}

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if reached() {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("until condition not reached before timeoutMs")
}
