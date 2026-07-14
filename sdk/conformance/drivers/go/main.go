// Conformance driver for the Go SDK.
// JSON lines on stdin/stdout; see ../../README.md for the contract.
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	bunqueue "github.com/egeominotti/bunqueue/sdk/go"
)

var (
	connection = bunqueue.Options{Host: "127.0.0.1", Port: 6789}
	queues     = map[string]*bunqueue.Queue{}
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

func handle(req map[string]any) (map[string]any, error) {
	switch str(req["op"]) {
	case "connect":
		connection = bunqueue.Options{Host: str(req["host"]), Port: int(num(req["port"], 6789))}
		if token := str(req["token"]); token != "" {
			connection.Token = token
		}
		return map[string]any{}, nil
	case "add":
		job, err := queueFor(str(req["queue"])).Add(str(req["name"]), req["data"], toOpts(mapOf(req["opts"])))
		if err != nil {
			return nil, err
		}
		return map[string]any{"jobId": job.ID()}, nil
	case "addBulk":
		var entries []bunqueue.BulkEntry
		for _, raw := range sliceOf(req["entries"]) {
			entry := mapOf(raw)
			entries = append(entries, bunqueue.BulkEntry{
				Name: str(entry["name"]),
				Data: entry["data"],
				Opts: toOpts(mapOf(entry["opts"])),
			})
		}
		ids, err := queueFor(str(req["queue"])).AddBulk(entries)
		if err != nil {
			return nil, err
		}
		return map[string]any{"ids": ids}, nil
	case "getJob":
		job, err := lookup().GetJob(str(req["jobId"]))
		if err != nil {
			return nil, err
		}
		return map[string]any{"job": jobView(job)}, nil
	case "getJobByCustomId":
		job, err := queueFor(str(req["queue"])).GetJobByCustomID(str(req["customId"]))
		if err != nil {
			return nil, err
		}
		if job == nil {
			return map[string]any{"job": nil}, nil
		}
		return map[string]any{"job": map[string]any{"id": job.ID()}}, nil
	case "getState":
		state, err := lookup().GetState(str(req["jobId"]))
		if err != nil {
			return nil, err
		}
		return map[string]any{"state": state}, nil
	case "getResult":
		result, err := lookup().GetResult(str(req["jobId"]))
		if err != nil {
			return nil, err
		}
		return map[string]any{"result": result}, nil
	case "count":
		count, err := queueFor(str(req["queue"])).Count()
		if err != nil {
			return nil, err
		}
		return map[string]any{"count": count}, nil
	case "isPaused":
		paused, err := queueFor(str(req["queue"])).IsPaused()
		if err != nil {
			return nil, err
		}
		return map[string]any{"paused": paused}, nil
	case "pause":
		return map[string]any{}, queueFor(str(req["queue"])).Pause()
	case "resume":
		return map[string]any{}, queueFor(str(req["queue"])).Resume()
	case "drain":
		count, err := queueFor(str(req["queue"])).Drain()
		if err != nil {
			return nil, err
		}
		return map[string]any{"count": count}, nil
	case "promote":
		return map[string]any{}, lookup().Promote(str(req["jobId"]))
	case "upsertScheduler":
		repeat := mapOf(req["repeat"])
		return map[string]any{}, queueFor(str(req["queue"])).UpsertJobScheduler(
			str(req["schedulerId"]),
			bunqueue.SchedulerRepeat{
				Pattern:  str(repeat["pattern"]),
				EveryMs:  int(num(repeat["every"], 0)),
				Limit:    int(num(repeat["limit"], 0)),
				Timezone: str(repeat["tz"]),
			},
			bunqueue.SchedulerTemplate{},
		)
	case "getScheduler":
		scheduler, err := lookup().GetJobScheduler(str(req["schedulerId"]))
		if err != nil {
			return nil, err
		}
		if scheduler == nil {
			return map[string]any{"scheduler": nil}, nil
		}
		return map[string]any{"scheduler": scheduler}, nil
	case "removeScheduler":
		return map[string]any{}, lookup().RemoveJobScheduler(str(req["schedulerId"]))
	case "waitForJob":
		result, err := lookup().WaitForJob(str(req["jobId"]), int(num(req["timeoutMs"], 30000)))
		if err != nil {
			return nil, err
		}
		return map[string]any{"result": result}, nil
	case "getDlqCount":
		entries, err := queueFor(str(req["queue"])).GetDlq(0)
		if err != nil {
			return nil, err
		}
		return map[string]any{"count": len(entries)}, nil
	case "retryDlq":
		count, err := queueFor(str(req["queue"])).RetryDlq("", 0)
		if err != nil {
			return nil, err
		}
		return map[string]any{"count": count}, nil
	case "hello":
		hello, err := lookup().Connection.Hello()
		if err != nil {
			return nil, err
		}
		return map[string]any{"protocolVersion": hello["protocolVersion"]}, nil
	case "process":
		if err := processUntil(req); err != nil {
			return nil, err
		}
		return map[string]any{}, nil
	case "close":
		for _, queue := range queues {
			queue.Close()
		}
		os.Exit(0)
	}
	return nil, fmt.Errorf("unknown op: %v", req["op"])
}

func str(value any) string {
	s, _ := value.(string)
	return s
}

func num(value any, fallback float64) float64 {
	if f, ok := value.(float64); ok {
		return f
	}
	return fallback
}

func mapOf(value any) map[string]any {
	m, _ := value.(map[string]any)
	return m
}

func sliceOf(value any) []any {
	s, _ := value.([]any)
	return s
}

func main() {
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 1024*1024), 16*1024*1024)
	out := bufio.NewWriter(os.Stdout)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var req map[string]any
		if err := json.Unmarshal(line, &req); err != nil {
			continue
		}
		result, err := handle(req)
		answer := map[string]any{"id": req["id"], "ok": err == nil}
		if err != nil {
			answer["error"] = err.Error()
		}
		for key, value := range result {
			answer[key] = value
		}
		encoded, _ := json.Marshal(answer)
		_, _ = out.Write(append(encoded, '\n'))
		_ = out.Flush()
	}
}
