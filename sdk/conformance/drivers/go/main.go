// Conformance driver for the Go SDK.
// JSON lines on stdin/stdout; see ../../README.md for the contract.
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"

	bunqueue "github.com/egeominotti/bunqueue/sdk/go"
)

var (
	connection = bunqueue.Options{Host: "127.0.0.1", Port: 6789}
	queues     = map[string]*bunqueue.Queue{}
)

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
	case "addFlow":
		queue := str(req["queue"])
		producer := bunqueue.NewFlowProducer(connection)
		defer producer.Close()
		node, err := producer.Add(bunqueue.FlowJob{
			Name:      "parent",
			QueueName: queue,
			Data:      map[string]any{"kind": "parent"},
			Opts:      bunqueue.JobOptions{"jobId": str(req["parentId"])},
			Children: []bunqueue.FlowJob{{
				Name:      "child",
				QueueName: queue,
				Data:      map[string]any{"kind": "child"},
				Opts:      bunqueue.JobOptions{"jobId": str(req["childId"])},
			}},
		})
		if err != nil {
			return nil, err
		}
		return map[string]any{
			"parentId": node.Job.ID(),
			"childId":  node.Children[0].Job.ID(),
		}, nil
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
		template := mapOf(req["template"])
		return map[string]any{}, queueFor(str(req["queue"])).UpsertJobScheduler(
			str(req["schedulerId"]),
			bunqueue.SchedulerRepeat{
				Pattern:  str(repeat["pattern"]),
				EveryMs:  int(num(repeat["every"], 0)),
				Limit:    int(num(repeat["limit"], 0)),
				Timezone: str(repeat["tz"]),
			},
			bunqueue.SchedulerTemplate{
				Name: str(template["name"]),
				Data: template["data"],
				Opts: toOpts(mapOf(template["opts"])),
			},
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
		return map[string]any{
			"protocolVersion": hello["protocolVersion"],
			"capabilities":    hello["capabilities"],
		}, nil
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
