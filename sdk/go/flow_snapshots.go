package bunqueue

import "fmt"

func validateFlowSnapshots(
	jobs []map[string]any,
	value any,
) (map[string]map[string]any, error) {
	rawJobs, ok := value.([]any)
	if !ok || len(rawJobs) != len(jobs) {
		return nil, fmt.Errorf("invalid PUSHF response: committed job snapshots are missing")
	}

	expected := make(map[string]string, len(jobs))
	for _, job := range jobs {
		expected[asString(job["id"])] = asString(job["queue"])
	}
	snapshots := make(map[string]map[string]any, len(rawJobs))
	for _, item := range rawJobs {
		raw, ok := item.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("invalid PUSHF response: committed job snapshot is malformed")
		}
		id, ok := raw["id"].(string)
		queue, expectedID := expected[id]
		if !ok || !expectedID || snapshots[id] != nil || raw["queue"] != queue {
			return nil, fmt.Errorf(
				"invalid PUSHF response: committed job IDs or queues do not match request",
			)
		}
		snapshots[id] = raw
		delete(expected, id)
	}
	if len(expected) != 0 {
		return nil, fmt.Errorf(
			"invalid PUSHF response: committed job IDs or queues do not match request",
		)
	}
	return snapshots, nil
}
