package bunqueue

import (
	"fmt"
	"regexp"
	"strings"
)

const (
	maxFlowDepth = 100
	maxFlowJobs  = 10_000
)

var unsupportedAtomicFlowOptions = map[string]string{
	"repeat":        "repeat",
	"uniqueKey":     "deduplication",
	"dedup":         "deduplication",
	"deduplication": "deduplication",
	"debounce":      "debounce",
	"debounceId":    "debounce",
	"debounceTtl":   "debounce",
}

var atomicFlowTopologyOptions = []string{"parentId", "dependsOn", "childrenIds"}
var flowQueuePattern = regexp.MustCompile(`^[a-zA-Z0-9_\-.:]+$`)

type flowIDGenerator func() (string, error)

type plannedFlowNode struct {
	id       string
	children []*plannedFlowNode
}

type flowTreePlan struct {
	jobs []map[string]any
	root *plannedFlowNode
}

type flowChainPlan struct {
	jobs []map[string]any
	ids  []string
}

type flowPlanner struct {
	generateID flowIDGenerator
}

type flowPlanState struct {
	jobs  []map[string]any
	ids   map[string]bool
	count int
}

type plannedParent struct {
	id    string
	queue string
}

func newFlowPlanner(generateID flowIDGenerator) *flowPlanner {
	if generateID == nil {
		generateID = randomFlowID
	}
	return &flowPlanner{generateID: generateID}
}

func (p *flowPlanner) planTree(flow FlowJob) (*flowTreePlan, error) {
	state := &flowPlanState{ids: map[string]bool{}}
	root, err := p.visit(flow, nil, 0, state)
	if err != nil {
		return nil, err
	}
	return &flowTreePlan{jobs: state.jobs, root: root}, nil
}

func (p *flowPlanner) planChain(steps []ChainStep) (*flowChainPlan, error) {
	if len(steps) > maxFlowJobs {
		return nil, fmt.Errorf("flow exceeds the %d job limit", maxFlowJobs)
	}
	seen := map[string]bool{}
	ids := make([]string, len(steps))
	for index, step := range steps {
		if err := validateFlowStep(step); err != nil {
			return nil, err
		}
		id, _, err := p.allocate(step.Opts, seen)
		if err != nil {
			return nil, err
		}
		ids[index] = id
	}

	jobs := make([]map[string]any, 0, len(steps))
	for index, step := range steps {
		var dependency string
		if index > 0 {
			dependency = ids[index-1]
		}
		data, err := flowData(step.Name, step.Data, map[string]any{
			"__flowParentId": nilIfEmpty(dependency),
		})
		if err != nil {
			return nil, err
		}
		dependsOn := []string{}
		if dependency != "" {
			dependsOn = []string{dependency}
		}
		input, err := atomicFlowInput(step.Opts, data, ids[index], "", dependsOn, nil)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, plannedJob(ids[index], step.QueueName, input))
	}
	return &flowChainPlan{jobs: jobs, ids: ids}, nil
}

func (p *flowPlanner) visit(
	node FlowJob,
	parent *plannedParent,
	depth int,
	state *flowPlanState,
) (*plannedFlowNode, error) {
	if err := validateFlowNode(node, depth); err != nil {
		return nil, err
	}
	state.count++
	if state.count > maxFlowJobs {
		return nil, fmt.Errorf("flow exceeds the %d job limit", maxFlowJobs)
	}
	id, _, err := p.allocate(node.Opts, state.ids)
	if err != nil {
		return nil, err
	}

	children := make([]*plannedFlowNode, 0, len(node.Children))
	for _, child := range node.Children {
		planned, childErr := p.visit(
			child,
			&plannedParent{id: id, queue: node.QueueName},
			depth+1,
			state,
		)
		if childErr != nil {
			return nil, childErr
		}
		children = append(children, planned)
	}
	childIDs := make([]string, len(children))
	for index, child := range children {
		childIDs[index] = child.id
	}
	internal := map[string]any{}
	var parentID string
	if parent != nil {
		parentID = parent.id
		internal["__parentId"] = parent.id
		internal["__parentQueue"] = parent.queue
	}
	if len(childIDs) > 0 {
		internal["__childrenIds"] = childIDs
	}
	data, err := flowData(node.Name, node.Data, internal)
	if err != nil {
		return nil, err
	}
	input, err := atomicFlowInput(node.Opts, data, id, parentID, childIDs, childIDs)
	if err != nil {
		return nil, err
	}
	state.jobs = append(state.jobs, plannedJob(id, node.QueueName, input))
	return &plannedFlowNode{id: id, children: children}, nil
}

func validateFlowNode(node FlowJob, depth int) error {
	if node.Name == "" || len(node.Name) > 256 {
		return fmt.Errorf("flow job name must be a non-empty string of at most 256 characters")
	}
	if node.QueueName == "" ||
		len(node.QueueName) > 256 ||
		!flowQueuePattern.MatchString(node.QueueName) {
		return fmt.Errorf("flow queueName is invalid")
	}
	if depth > maxFlowDepth {
		return fmt.Errorf("flow exceeds the %d level depth limit", maxFlowDepth)
	}
	return nil
}

func validateFlowStep(step ChainStep) error {
	return validateFlowNode(FlowJob{
		Name: step.Name, QueueName: step.QueueName, Data: step.Data, Opts: step.Opts,
	}, 0)
}

func (p *flowPlanner) allocate(opts JobOptions, seen map[string]bool) (string, bool, error) {
	raw, explicit := opts["jobId"]
	explicit = explicit && raw != nil
	var id string
	var err error
	if explicit {
		var ok bool
		id, ok = raw.(string)
		if !ok {
			return "", false, fmt.Errorf("flow jobId must be a string")
		}
	} else {
		id, err = p.generateID()
		if err != nil {
			return "", false, fmt.Errorf("generate flow job id: %w", err)
		}
	}
	if id == "" || len(id) > 1_024 || strings.Contains(id, ":") {
		return "", false, fmt.Errorf(
			"flow jobId must be non-empty, at most 1024 bytes and cannot contain a colon",
		)
	}
	if seen[id] {
		return "", false, fmt.Errorf("duplicate flow job id: %s", id)
	}
	seen[id] = true
	return id, explicit, nil
}

func flowData(name string, raw map[string]any, internal map[string]any) (map[string]any, error) {
	data := make(map[string]any, len(raw)+len(internal)+1)
	for key, value := range raw {
		if key == "name" || strings.HasPrefix(key, "__") {
			return nil, fmt.Errorf("flow job data key is reserved: %s", key)
		}
		data[key] = value
	}
	data["name"] = name
	for key, value := range internal {
		data[key] = value
	}
	return data, nil
}

func atomicFlowInput(
	opts JobOptions,
	data map[string]any,
	id string,
	parentID string,
	dependsOn []string,
	childrenIDs []string,
) (map[string]any, error) {
	for _, key := range atomicFlowTopologyOptions {
		if _, present := opts[key]; present {
			return nil, fmt.Errorf("flow topology options are owned by FlowProducer")
		}
	}
	for key, kind := range unsupportedAtomicFlowOptions {
		if value, present := opts[key]; present && value != nil {
			return nil, fmt.Errorf("%s is not supported inside an atomic flow", kind)
		}
	}
	wire, err := optionsToWire(opts)
	if err != nil {
		return nil, err
	}
	delete(wire, "jobId")
	input := map[string]any{"data": data}
	for key, value := range wire {
		input[key] = value
	}
	if raw, present := opts["jobId"]; present && raw != nil {
		input["customId"] = id
	}
	if parentID != "" {
		input["parentId"] = parentID
	}
	if len(dependsOn) > 0 {
		input["dependsOn"] = dependsOn
	}
	if len(childrenIDs) > 0 {
		input["childrenIds"] = childrenIDs
	}
	return compact(input), nil
}

func plannedJob(id string, queue string, input map[string]any) map[string]any {
	return map[string]any{"id": id, "queue": queue, "input": input}
}

func nilIfEmpty(value string) any {
	if value == "" {
		return nil
	}
	return value
}
