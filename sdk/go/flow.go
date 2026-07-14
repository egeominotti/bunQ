package bunqueue

// FlowJob describes one node of a flow tree (same shape as the other SDKs).
type FlowJob struct {
	Name      string
	QueueName string
	Data      map[string]any
	Opts      JobOptions
	Children  []FlowJob
}

// ChainStep is one element of a sequential AddChain.
type ChainStep struct {
	Name      string
	QueueName string
	Data      map[string]any
	Opts      JobOptions
}

// FlowNode is a created flow node: the job plus its children nodes.
type FlowNode struct {
	Job      *Job
	Children []*FlowNode
}

// FlowProducer creates dependent job hierarchies across one or more queues.
//
// Wire contract (parity with the TS/Python/PHP SDKs): children are pushed
// BEFORE their parent; the parent carries dependsOn/childrenIds; each child
// is then linked via UpdateParent. On failure every already-created job is
// cancelled (best-effort atomic rollback).
type FlowProducer struct {
	Connection *Connection
	owns       bool
}

// NewFlowProducer opens a flow producer with its own lazy connection.
func NewFlowProducer(opts Options) *FlowProducer {
	return &FlowProducer{Connection: NewConnection(opts), owns: true}
}

// Add creates a flow tree and returns its root node.
func (f *FlowProducer) Add(flow FlowJob) (*FlowNode, error) {
	var created []string
	node, err := f.addNode(flow, nil, &created)
	if err != nil {
		f.rollback(created)
		return nil, err
	}
	return node, nil
}

// AddChain creates a sequential chain (each step depends on the previous)
// and returns the created ids in chain order.
func (f *FlowProducer) AddChain(steps []ChainStep) ([]string, error) {
	var jobIDs []string
	prevID := ""
	for _, step := range steps {
		data := map[string]any{}
		for key, value := range step.Data {
			data[key] = value
		}
		if prevID != "" {
			data["__flowParentId"] = prevID
		}
		wire, err := optionsToWire(step.Opts)
		if err != nil {
			f.rollback(jobIDs)
			return nil, err
		}
		command := map[string]any{
			"cmd":   "PUSH",
			"queue": step.QueueName,
			"data":  jobPayload(step.Name, data),
		}
		for key, value := range wire {
			command[key] = value
		}
		if prevID != "" {
			command["dependsOn"] = []string{prevID}
		}
		response, err := f.Connection.Call(compact(command))
		if err != nil {
			f.rollback(jobIDs)
			return nil, err
		}
		prevID = toIDString(response["id"])
		jobIDs = append(jobIDs, prevID)
	}
	return jobIDs, nil
}

// GetFlow reconstructs a flow tree from a root job id. depth < 0 means
// unlimited. A removed child (or root) yields nil / a partial tree instead
// of an error; a visited set guards against cycles in childrenIds.
func (f *FlowProducer) GetFlow(jobID string, depth int) (*FlowNode, error) {
	visited := map[string]bool{}
	return f.fetchNode(jobID, depth, visited)
}

// Close releases the underlying connection when this producer owns it.
func (f *FlowProducer) Close() {
	if f.owns {
		f.Connection.Close()
	}
}

// ---------------------------------------------------------------- internals

type flowParentRef struct {
	id    string
	queue string
}

func (f *FlowProducer) addNode(node FlowJob, parent *flowParentRef, created *[]string) (*FlowNode, error) {
	var childNodes []*FlowNode
	var childIDs []string
	for _, child := range node.Children {
		childNode, err := f.addNode(child, &flowParentRef{id: "pending", queue: node.QueueName}, created)
		if err != nil {
			return nil, err
		}
		childNodes = append(childNodes, childNode)
		childIDs = append(childIDs, childNode.Job.ID())
	}

	data := map[string]any{}
	for key, value := range node.Data {
		data[key] = value
	}
	if parent != nil {
		data["__parentId"] = parent.id
		data["__parentQueue"] = parent.queue
	}
	if len(childIDs) > 0 {
		data["__childrenIds"] = childIDs
	}
	payload := jobPayload(node.Name, data)

	wire, err := optionsToWire(node.Opts)
	if err != nil {
		return nil, err
	}
	command := map[string]any{"cmd": "PUSH", "queue": node.QueueName, "data": payload}
	for key, value := range wire {
		command[key] = value
	}
	if parent != nil {
		command["parentId"] = parent.id
	}
	if len(childIDs) > 0 {
		command["childrenIds"] = childIDs
		command["dependsOn"] = childIDs
	}
	response, err := f.Connection.Call(compact(command))
	if err != nil {
		return nil, err
	}
	jobID := toIDString(response["id"])
	*created = append(*created, jobID)

	// Link children to their real parent id (placeholder was 'pending').
	for _, childID := range childIDs {
		if _, err := f.Connection.Call(map[string]any{
			"cmd": "UpdateParent", "childId": childID, "parentId": jobID,
		}); err != nil {
			return nil, err
		}
	}

	job := newJob(map[string]any{"id": jobID, "queue": node.QueueName, "data": payload}, f.Connection, "")
	return &FlowNode{Job: job, Children: childNodes}, nil
}

func (f *FlowProducer) fetchNode(jobID string, depth int, visited map[string]bool) (*FlowNode, error) {
	if visited[jobID] {
		return nil, nil // cycle guard
	}
	visited[jobID] = true
	response, err := f.Connection.Call(map[string]any{"cmd": "GetJob", "id": jobID})
	if err != nil {
		if isNotFound(err) {
			return nil, nil // root or a since-removed child -> skip
		}
		return nil, err
	}
	raw := asMap(response["job"])
	if len(raw) == 0 {
		return nil, nil
	}
	job := newJob(raw, f.Connection, "")
	var children []*FlowNode
	if depth != 0 {
		nextDepth := depth - 1
		if depth < 0 {
			nextDepth = -1
		}
		for _, childID := range job.ChildrenIDs() {
			child, err := f.fetchNode(childID, nextDepth, visited)
			if err != nil {
				return nil, err
			}
			if child != nil {
				children = append(children, child)
			}
		}
	}
	return &FlowNode{Job: job, Children: children}, nil
}

func (f *FlowProducer) rollback(jobIDs []string) {
	for _, jobID := range jobIDs {
		_, _ = f.Connection.Call(map[string]any{"cmd": "Cancel", "id": jobID}) // best-effort
	}
}
