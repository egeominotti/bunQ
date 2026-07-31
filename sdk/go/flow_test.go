package bunqueue

import (
	"strings"
	"sync"
	"testing"
	"time"
)

func TestFlowChildrenBeforeParent(t *testing.T) {
	name := uniqueName("tree")
	queue := NewQueue(name, Options{Port: shared.port})
	flow := NewFlowProducer(Options{Port: shared.port})
	t.Cleanup(func() {
		flow.Close()
		_ = queue.Obliterate()
		queue.Close()
	})
	var mu sync.Mutex
	var order []string
	worker := NewWorker(name, func(job *Job) (any, error) {
		mu.Lock()
		order = append(order, job.Name())
		mu.Unlock()
		return map[string]any{"from": job.Name()}, nil
	}, WorkerOptions{Port: shared.port, PollTimeoutMs: 300})
	startWorker(t, worker)

	node, err := flow.Add(FlowJob{
		Name:      "parent",
		QueueName: name,
		Data:      map[string]any{"kind": "root"},
		Children: []FlowJob{
			{Name: "child-a", QueueName: name},
			{Name: "child-b", QueueName: name},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(node.Children) != 2 {
		t.Fatalf("children created = %d, want 2", len(node.Children))
	}
	if !waitUntil(t, 20*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(order) >= 3
	}) {
		t.Fatal("flow never fully processed")
	}
	mu.Lock()
	last := order[len(order)-1]
	mu.Unlock()
	if last != "parent" {
		t.Fatalf("parent must run last, order: %v", order)
	}
	values, err := queue.GetChildrenValues(node.Job.ID())
	if err != nil || len(values) != 2 {
		t.Fatalf("parent must see both children values, got %v (%v)", values, err)
	}
}

func TestFlowChainStrictOrder(t *testing.T) {
	name := uniqueName("chain")
	queue := NewQueue(name, Options{Port: shared.port})
	flow := NewFlowProducer(Options{Port: shared.port})
	t.Cleanup(func() {
		flow.Close()
		_ = queue.Obliterate()
		queue.Close()
	})
	var mu sync.Mutex
	var order []string
	worker := NewWorker(name, func(job *Job) (any, error) {
		mu.Lock()
		order = append(order, job.Name())
		mu.Unlock()
		return "ok", nil
	}, WorkerOptions{Port: shared.port, PollTimeoutMs: 300})
	startWorker(t, worker)

	ids, err := flow.AddChain([]ChainStep{
		{Name: "step-1", QueueName: name},
		{Name: "step-2", QueueName: name},
		{Name: "step-3", QueueName: name},
	})
	if err != nil || len(ids) != 3 {
		t.Fatal("chain must create 3 jobs:", err)
	}
	if !waitUntil(t, 20*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(order) >= 3
	}) {
		t.Fatal("chain never fully processed")
	}
	mu.Lock()
	got := strings.Join(order, ",")
	mu.Unlock()
	if got != "step-1,step-2,step-3" {
		t.Fatalf("chain order = %s, want strict sequence", got)
	}
}

func TestGetFlowReconstructsTree(t *testing.T) {
	name := uniqueName("gf")
	queue := NewQueue(name, Options{Port: shared.port})
	flow := NewFlowProducer(Options{Port: shared.port})
	t.Cleanup(func() {
		flow.Close()
		_ = queue.Obliterate()
		queue.Close()
	})
	if err := queue.Pause(); err != nil { // keep the tree intact while reading
		t.Fatal(err)
	}
	node, err := flow.Add(FlowJob{
		Name:      "root",
		QueueName: name,
		Children:  []FlowJob{{Name: "leaf", QueueName: name}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !waitUntil(t, 10*time.Second, func() bool {
		tree, _ := flow.GetFlow(node.Job.ID(), -1)
		return tree != nil && len(tree.Children) == 1
	}) {
		t.Fatal("tree must be readable with its child")
	}
	tree, _ := flow.GetFlow(node.Job.ID(), -1)
	if tree.Children[0].Job.Name() != "leaf" {
		t.Fatalf("child = %q, want leaf", tree.Children[0].Job.Name())
	}
	missing, err := flow.GetFlow("missing-root-id", -1)
	if err != nil || missing != nil {
		t.Fatal("missing root must map to nil without error")
	}
}

func TestFlowRejectsInvalidGraphAtomically(t *testing.T) {
	name := uniqueName("rb")
	queue := NewQueue(name, Options{Port: shared.port})
	flow := NewFlowProducer(Options{Port: shared.port})
	t.Cleanup(func() {
		flow.Close()
		_ = queue.Obliterate()
		queue.Close()
	})
	_, err := flow.Add(FlowJob{
		Name:      "parent",
		QueueName: name,
		Children: []FlowJob{
			{Name: "good-child", QueueName: name},
			{Name: "bad-child", QueueName: name, Opts: JobOptions{"bogusOption": 1}},
		},
	})
	if err == nil {
		t.Fatal("invalid option must be rejected (no silent drop)")
	}
	count, countErr := queue.Count()
	if countErr != nil || count != 0 {
		t.Fatalf("invalid graph became visible: count=%d err=%v", count, countErr)
	}
}
