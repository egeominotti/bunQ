package bunqueue

import "fmt"

func (f *FlowProducer) commitFlow(
	jobs []map[string]any,
) (map[string]map[string]any, error) {
	response, err := f.callFlow(map[string]any{"cmd": "PUSHF", "jobs": jobs})
	if err != nil {
		return nil, err
	}
	data, ok := response["data"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("invalid PUSHF response: committed job snapshots are missing")
	}
	return validateFlowSnapshots(jobs, data["jobs"])
}

func (f *FlowProducer) callFlow(command map[string]any) (map[string]any, error) {
	if f.call != nil {
		return f.call(command)
	}
	return f.Connection.Call(command)
}

func (f *FlowProducer) buildFlowNode(
	planned *plannedFlowNode,
	snapshots map[string]map[string]any,
) *FlowNode {
	children := make([]*FlowNode, len(planned.children))
	for index, child := range planned.children {
		children[index] = f.buildFlowNode(child, snapshots)
	}
	return &FlowNode{
		Job:      newJob(snapshots[planned.id], f.Connection, ""),
		Children: children,
	}
}
