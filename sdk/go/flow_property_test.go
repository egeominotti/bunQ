package bunqueue

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"pgregory.net/rapid"
)

func drawFlowTree(t *rapid.T, depth int, serial *int) FlowJob {
	index := *serial
	*serial = index + 1
	custom := rapid.Bool().Draw(t, fmt.Sprintf("custom-%d", index))
	opts := JobOptions{
		"priority": rapid.IntRange(-20, 20).Draw(t, fmt.Sprintf("priority-%d", index)),
		"lifo":     rapid.Bool().Draw(t, fmt.Sprintf("lifo-%d", index)),
	}
	if custom {
		opts["jobId"] = fmt.Sprintf(
			"custom-%d-%s",
			index,
			rapid.StringMatching(`[a-z0-9]{1,8}`).Draw(t, fmt.Sprintf("custom-id-%d", index)),
		)
	}
	node := FlowJob{
		Name:      fmt.Sprintf("job-%d", index),
		QueueName: fmt.Sprintf("queue-%d", index%4),
		Data: map[string]any{
			"value":  rapid.Int().Draw(t, fmt.Sprintf("value-%d", index)),
			"active": rapid.Bool().Draw(t, fmt.Sprintf("active-%d", index)),
		},
		Opts: opts,
	}
	if depth < 3 {
		count := rapid.IntRange(0, 3).Draw(t, fmt.Sprintf("children-%d", index))
		node.Children = make([]FlowJob, count)
		for child := range count {
			node.Children[child] = drawFlowTree(t, depth+1, serial)
		}
	}
	return node
}

func deterministicFlowPlanner() *flowPlanner {
	next := 0
	return newFlowPlanner(func() (string, error) {
		next++
		return fmt.Sprintf("generated-%d", next), nil
	})
}

func planJobsByID(jobs []map[string]any) map[string]map[string]any {
	indexed := make(map[string]map[string]any, len(jobs))
	for _, job := range jobs {
		indexed[asString(job["id"])] = job
	}
	return indexed
}

func plannerStrings(value any) []string {
	if strings, ok := value.([]string); ok {
		return strings
	}
	return asStrings(value)
}

func verifyPlannedTree(
	t *rapid.T,
	source FlowJob,
	planned *plannedFlowNode,
	jobs map[string]map[string]any,
	order map[string]int,
	parent *plannedFlowNode,
) int {
	job := jobs[planned.id]
	if job == nil || strings.Contains(planned.id, ":") {
		t.Fatalf("missing or invalid planned job %q", planned.id)
	}
	if custom, ok := source.Opts["jobId"].(string); ok && planned.id != custom {
		t.Fatalf("custom id lost: got %q want %q", planned.id, custom)
	}
	if asString(job["queue"]) != source.QueueName {
		t.Fatalf("queue mismatch: %#v", job)
	}
	input := asMap(job["input"])
	data := asMap(input["data"])
	if data["name"] != source.Name ||
		data["value"] != source.Data["value"] ||
		data["active"] != source.Data["active"] {
		t.Fatalf("wire data changed: %#v", data)
	}
	if input["priority"] != source.Opts["priority"] || input["lifo"] != source.Opts["lifo"] {
		t.Fatalf("wire options changed: %#v", input)
	}
	childIDs := make([]string, len(planned.children))
	for index, child := range planned.children {
		childIDs[index] = child.id
		if order[child.id] >= order[planned.id] {
			t.Fatalf("parent appears before child: %q -> %q", planned.id, child.id)
		}
	}
	if !slices.Equal(plannerStrings(input["dependsOn"]), childIDs) ||
		!slices.Equal(plannerStrings(input["childrenIds"]), childIDs) {
		t.Fatalf("non-canonical child links: %#v", input)
	}
	if parent == nil {
		if input["parentId"] != nil || data["__parentId"] != nil {
			t.Fatalf("root inherited user parent links: %#v", input)
		}
	} else if input["parentId"] != parent.id ||
		data["__parentId"] != parent.id ||
		data["__parentQueue"] != jobs[parent.id]["queue"] {
		t.Fatalf("child does not point back to parent: %#v", input)
	}
	if custom, ok := source.Opts["jobId"].(string); ok {
		if input["customId"] != custom {
			t.Fatalf("customId mismatch: %#v", input)
		}
	} else if input["customId"] != nil {
		t.Fatalf("generated id unexpectedly became customId: %#v", input)
	}
	count := 1
	for index, child := range planned.children {
		count += verifyPlannedTree(t, source.Children[index], child, jobs, order, planned)
	}
	return count
}

func TestFlowPlannerTopologyAndWireProperties(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		serial := 0
		source := drawFlowTree(t, 0, &serial)
		plan, err := deterministicFlowPlanner().planTree(source)
		if err != nil {
			t.Fatalf("plan tree: %v", err)
		}
		jobs := planJobsByID(plan.jobs)
		order := make(map[string]int, len(plan.jobs))
		for index, job := range plan.jobs {
			order[asString(job["id"])] = index
		}
		if count := verifyPlannedTree(t, source, plan.root, jobs, order, nil); count != len(plan.jobs) {
			t.Fatalf("planned %d reachable nodes from %d jobs", count, len(plan.jobs))
		}
	})
}

func TestFlowPlannerChainProperty(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		length := rapid.IntRange(0, 30).Draw(t, "length")
		steps := make([]ChainStep, length)
		for index := range length {
			steps[index] = ChainStep{
				Name:      fmt.Sprintf("step-%d", index),
				QueueName: fmt.Sprintf("chain-%d", index%3),
				Data:      map[string]any{"position": index},
				Opts:      JobOptions{"jobId": fmt.Sprintf("chain-id-%d", index)},
			}
		}
		plan, err := deterministicFlowPlanner().planChain(steps)
		if err != nil {
			t.Fatalf("plan chain: %v", err)
		}
		if len(plan.jobs) != length || len(plan.ids) != length {
			t.Fatalf("chain cardinality mismatch: %#v", plan)
		}
		for index, job := range plan.jobs {
			input := asMap(job["input"])
			data := asMap(input["data"])
			if input["customId"] != plan.ids[index] {
				t.Fatalf("chain custom id lost: %#v", input)
			}
			if index == 0 {
				if len(plannerStrings(input["dependsOn"])) != 0 || data["__flowParentId"] != nil {
					t.Fatalf("first chain job has a dependency: %#v", input)
				}
			} else if !slices.Equal(
				plannerStrings(input["dependsOn"]),
				[]string{plan.ids[index-1]},
			) || data["__flowParentId"] != plan.ids[index-1] {
				t.Fatalf("chain dependency mismatch: %#v", input)
			}
		}
	})
}

func TestFlowProducerUsesOneAtomicCommandProperty(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		serial := 0
		source := drawFlowTree(t, 0, &serial)
		reject := rapid.Bool().Draw(t, "reject")
		calls := 0
		producer := &FlowProducer{Connection: NewConnection(Options{})}
		producer.call = func(command map[string]any) (map[string]any, error) {
			calls++
			if command["cmd"] != "PUSHF" {
				t.Fatalf("non-atomic flow command: %#v", command)
			}
			if reject {
				return nil, errors.New("atomic flow rejected")
			}
			rawJobs := make([]any, 0)
			for _, planned := range command["jobs"].([]map[string]any) {
				raw := map[string]any{"id": planned["id"], "queue": planned["queue"]}
				for key, value := range asMap(planned["input"]) {
					raw[key] = value
				}
				rawJobs = append(rawJobs, raw)
			}
			return map[string]any{"data": map[string]any{"jobs": rawJobs}}, nil
		}
		node, err := producer.Add(source)
		if calls != 1 {
			t.Fatalf("flow used %d commands, want exactly one", calls)
		}
		if reject && err == nil {
			t.Fatal("rejected atomic commit reported success")
		}
		if !reject && (err != nil || node == nil || node.Job.Name() != source.Name) {
			t.Fatalf("successful atomic commit failed: node=%#v err=%v", node, err)
		}
	})
}

func TestFlowPlannerRejectsReservedAndNonAtomicOptions(t *testing.T) {
	for _, key := range []string{"name", "__parentId", "__childrenIds", "__custom"} {
		_, err := deterministicFlowPlanner().planTree(FlowJob{
			Name: "job", QueueName: "queue", Data: map[string]any{key: "attacker"},
		})
		if err == nil || !strings.Contains(err.Error(), "reserved") {
			t.Fatalf("reserved key %q was accepted: %v", key, err)
		}
	}
	for _, option := range []string{"repeat", "uniqueKey", "deduplication", "debounce"} {
		_, err := deterministicFlowPlanner().planTree(FlowJob{
			Name: "job", QueueName: "queue", Data: map[string]any{},
			Opts: JobOptions{option: map[string]any{"id": "unsupported"}},
		})
		if err == nil || !strings.Contains(err.Error(), "not supported") {
			t.Fatalf("non-atomic option %q was accepted: %v", option, err)
		}
	}
}
