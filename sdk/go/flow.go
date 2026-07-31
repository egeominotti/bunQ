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
// Every ID and edge is resolved locally, then the complete graph is committed
// by one broker-side PUSHF operation. No partially linked flow is observable.
type FlowProducer struct {
	Connection *Connection
	owns       bool
	call       func(map[string]any) (map[string]any, error)
}

// NewFlowProducer opens a flow producer with its own lazy connection.
func NewFlowProducer(opts Options) *FlowProducer {
	connection := NewConnection(opts)
	return &FlowProducer{Connection: connection, owns: true, call: connection.Call}
}

// Add creates a flow tree and returns its root node.
func (f *FlowProducer) Add(flow FlowJob) (*FlowNode, error) {
	plan, err := newFlowPlanner(nil).planTree(flow)
	if err != nil {
		return nil, err
	}
	snapshots, err := f.commitFlow(plan.jobs)
	if err != nil {
		return nil, err
	}
	return f.buildFlowNode(plan.root, snapshots), nil
}

// AddChain creates a sequential chain (each step depends on the previous)
// and returns the created ids in chain order.
func (f *FlowProducer) AddChain(steps []ChainStep) ([]string, error) {
	if len(steps) == 0 {
		return []string{}, nil
	}
	plan, err := newFlowPlanner(nil).planChain(steps)
	if err != nil {
		return nil, err
	}
	if _, err := f.commitFlow(plan.jobs); err != nil {
		return nil, err
	}
	return plan.ids, nil
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
