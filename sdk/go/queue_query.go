package bunqueue

import (
	"fmt"
	"time"
)

// GetJob fetches a job by id; a missing job returns (nil, nil).
func (q *Queue) GetJob(id string) (*Job, error) {
	response, err := q.call(map[string]any{"cmd": "GetJob", "id": id})
	if err != nil {
		if isNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return newJob(asMap(response["job"]), q.Connection, ""), nil
}

// GetJobByCustomID resolves a job by its custom id; missing returns (nil, nil).
func (q *Queue) GetJobByCustomID(customID string) (*Job, error) {
	response, err := q.call(map[string]any{
		"cmd": "GetJobByCustomId", "queue": q.Name, "customId": customID,
	})
	if err != nil {
		if isNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return newJob(asMap(response["job"]), q.Connection, ""), nil
}

// GetJobs pages jobs by state; offset = start, limit = end - start
// (mirror of the reference client's getJobsAsync).
func (q *Queue) GetJobs(state any, start, end int) ([]*Job, error) {
	limit := end - start
	if limit < 0 {
		limit = 0
	}
	response, err := q.call(map[string]any{
		"cmd": "GetJobs", "queue": q.Name, "state": state, "offset": start, "limit": limit,
	})
	if err != nil {
		return nil, err
	}
	items := asSlice(response["jobs"])
	jobs := make([]*Job, 0, len(items))
	for _, item := range items {
		jobs = append(jobs, newJob(asMap(item), q.Connection, ""))
	}
	return jobs, nil
}

// GetState returns the job's current state.
func (q *Queue) GetState(id string) (string, error) {
	response, err := q.call(map[string]any{"cmd": "GetState", "id": id})
	if err != nil {
		return "", err
	}
	return asString(response["state"]), nil
}

// GetResult returns the job's persisted result, if any.
func (q *Queue) GetResult(id string) (any, error) {
	response, err := q.call(map[string]any{"cmd": "GetResult", "id": id})
	if err != nil {
		return nil, err
	}
	return response["result"], nil
}

// GetProgress returns the job's progress and message (top-level fields).
func (q *Queue) GetProgress(id string) (float64, string, error) {
	response, err := q.call(map[string]any{"cmd": "GetProgress", "id": id})
	if err != nil {
		return 0, "", err
	}
	progress := float64(asInt(response["progress"]))
	if f, ok := response["progress"].(float64); ok {
		progress = f
	}
	return progress, asString(response["message"]), nil
}

// WaitForJob blocks until the job completes and returns its result. The
// timeout is clamped to the server's [0, 600000] window. A failed job
// returns *CommandError; any other non-completion returns *CommandTimeoutError.
func (q *Queue) WaitForJob(id string, timeoutMs int) (any, error) {
	// The server validates 0 <= timeout <= 600000: clamp instead of erroring.
	if timeoutMs > 600_000 {
		timeoutMs = 600_000
	}
	if timeoutMs < 0 {
		timeoutMs = 0
	}
	response, err := q.Connection.CallTimeout(
		compact(map[string]any{"cmd": "WaitJob", "id": id, "timeout": timeoutMs}),
		time.Duration(timeoutMs)*time.Millisecond+5*time.Second,
	)
	if err != nil {
		return nil, err
	}
	if !asBool(response["completed"]) {
		state, stateErr := q.GetState(id)
		if stateErr == nil && state == "failed" {
			return nil, &CommandError{Message: fmt.Sprintf("job %s failed before completion", id)}
		}
		return nil, &CommandTimeoutError{Message: fmt.Sprintf("waitForJob timed out after %dms", timeoutMs)}
	}
	return response["result"], nil
}

// GetJobCounts returns the per-state job counts for this queue.
func (q *Queue) GetJobCounts() (map[string]int, error) {
	response, err := q.call(map[string]any{"cmd": "GetJobCounts", "queue": q.Name})
	if err != nil {
		return nil, err
	}
	counts := map[string]int{}
	for key, value := range asMap(response["counts"]) {
		counts[key] = asInt(value)
	}
	return counts, nil
}

// Count returns how many jobs are waiting to be processed.
func (q *Queue) Count() (int, error) {
	response, err := q.call(map[string]any{"cmd": "Count", "queue": q.Name})
	if err != nil {
		return 0, err
	}
	return asInt(response["count"]), nil
}

// GetJobLogs returns the job's log lines (server formats "[level] message").
func (q *Queue) GetJobLogs(id string) ([]string, error) {
	response, err := q.call(map[string]any{"cmd": "GetLogs", "id": id})
	if err != nil {
		return nil, err
	}
	return asStrings(asMap(response["data"])["logs"]), nil
}

// GetChildrenValues returns childId => returned value for a flow parent.
func (q *Queue) GetChildrenValues(id string) (map[string]any, error) {
	response, err := q.call(map[string]any{"cmd": "GetChildrenValues", "id": id})
	if err != nil {
		return nil, err
	}
	return asMap(asMap(response["data"])["values"]), nil
}
