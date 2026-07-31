package bunqueue

import (
	"fmt"
	"strings"
	"testing"

	"pgregory.net/rapid"
)

func TestFlowProducerRejectsOwnedTopologyBeforeIOProperty(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		option := rapid.SampledFrom([]string{
			"parentId", "dependsOn", "childrenIds",
		}).Draw(t, "option")
		nullValue := rapid.Bool().Draw(t, "null-value")
		chain := rapid.Bool().Draw(t, "chain")
		var value any = []string{"user-link"}
		if nullValue {
			value = nil
		}
		calls := 0
		producer := &FlowProducer{Connection: NewConnection(Options{})}
		producer.call = func(map[string]any) (map[string]any, error) {
			calls++
			return nil, fmt.Errorf("broker I/O must not occur")
		}
		var err error
		if chain {
			_, err = producer.AddChain([]ChainStep{{
				Name: "step", QueueName: "queue", Opts: JobOptions{option: value},
			}})
		} else {
			_, err = producer.Add(FlowJob{
				Name: "job", QueueName: "queue", Opts: JobOptions{option: value},
			})
		}
		if err == nil || !strings.Contains(err.Error(), "owned by FlowProducer") {
			t.Fatalf("owned topology option %q was accepted: %v", option, err)
		}
		if calls != 0 {
			t.Fatalf("invalid topology performed %d broker calls", calls)
		}
	})
}

func TestFlowProducerRejectsInvalidQueueBeforeIO(t *testing.T) {
	for _, queue := range []string{"", "has space", "has/slash", strings.Repeat("q", 257), "queüe"} {
		calls := 0
		producer := &FlowProducer{Connection: NewConnection(Options{})}
		producer.call = func(map[string]any) (map[string]any, error) {
			calls++
			return nil, fmt.Errorf("broker I/O must not occur")
		}
		_, err := producer.Add(FlowJob{Name: "job", QueueName: queue})
		if err == nil || !strings.Contains(err.Error(), "queueName is invalid") {
			t.Fatalf("invalid queue %q was accepted: %v", queue, err)
		}
		if calls != 0 {
			t.Fatalf("invalid queue performed %d broker calls", calls)
		}
	}
}

func TestFlowCommitRejectsSnapshotFromAnotherQueue(t *testing.T) {
	calls := 0
	producer := &FlowProducer{Connection: NewConnection(Options{})}
	producer.call = func(command map[string]any) (map[string]any, error) {
		calls++
		job := command["jobs"].([]map[string]any)[0]
		raw := map[string]any{"id": job["id"], "queue": "wrong-queue"}
		for key, value := range asMap(job["input"]) {
			raw[key] = value
		}
		return map[string]any{"data": map[string]any{"jobs": []any{raw}}}, nil
	}
	_, err := producer.Add(FlowJob{Name: "job", QueueName: "expected-queue"})
	if err == nil || !strings.Contains(err.Error(), "do not match") {
		t.Fatalf("snapshot with wrong queue was accepted: %v", err)
	}
	if calls != 1 {
		t.Fatalf("commit used %d broker calls, want one", calls)
	}
}
