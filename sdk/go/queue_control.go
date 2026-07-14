package bunqueue

import "strings"

// Pause stops delivery for this queue (producers can still add).
func (q *Queue) Pause() error {
	_, err := q.call(map[string]any{"cmd": "Pause", "queue": q.Name})
	return err
}

// Resume re-enables delivery for this queue.
func (q *Queue) Resume() error {
	_, err := q.call(map[string]any{"cmd": "Resume", "queue": q.Name})
	return err
}

// IsPaused reports the queue's paused flag (top-level `paused`).
func (q *Queue) IsPaused() (bool, error) {
	response, err := q.call(map[string]any{"cmd": "IsPaused", "queue": q.Name})
	if err != nil {
		return false, err
	}
	return asBool(response["paused"]), nil
}

// Drain removes all waiting/delayed jobs and returns how many were dropped.
func (q *Queue) Drain() (int, error) {
	response, err := q.call(map[string]any{"cmd": "Drain", "queue": q.Name})
	if err != nil {
		return 0, err
	}
	return asInt(response["count"]), nil
}

// Clean removes jobs in `state` older than graceMs and returns their ids.
func (q *Queue) Clean(graceMs, limit int, state string) ([]string, error) {
	response, err := q.call(map[string]any{
		"cmd": "Clean", "queue": q.Name, "grace": graceMs, "limit": limit, "state": state,
	})
	if err != nil {
		return nil, err
	}
	return asStrings(response["ids"]), nil
}

// Obliterate wipes the queue entirely (jobs, DLQ, metadata).
func (q *Queue) Obliterate() error {
	_, err := q.call(map[string]any{"cmd": "Obliterate", "queue": q.Name})
	return err
}

// Remove cancels a job by id; false when it no longer exists.
func (q *Queue) Remove(id string) (bool, error) {
	_, err := q.call(map[string]any{"cmd": "Cancel", "id": id})
	if err != nil {
		if isNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// Discard marks a job so it will not be retried again.
func (q *Queue) Discard(id string) error {
	_, err := q.call(map[string]any{"cmd": "Discard", "id": id})
	return err
}

// Promote moves a delayed job to waiting immediately.
func (q *Queue) Promote(id string) error {
	_, err := q.call(map[string]any{"cmd": "Promote", "id": id})
	return err
}

// RetryJob re-queues a failed/completed job (server command: MoveToWait).
func (q *Queue) RetryJob(id string) error {
	_, err := q.call(map[string]any{"cmd": "MoveToWait", "id": id})
	return err
}

// ChangePriority updates a waiting job's priority.
func (q *Queue) ChangePriority(id string, priority int) error {
	_, err := q.call(map[string]any{"cmd": "ChangePriority", "id": id, "priority": priority})
	return err
}

// ChangeDelay updates a delayed job's remaining delay.
func (q *Queue) ChangeDelay(id string, delayMs int) error {
	_, err := q.call(map[string]any{"cmd": "ChangeDelay", "id": id, "delay": delayMs})
	return err
}

// UpdateJobData replaces a job's user data.
func (q *Queue) UpdateJobData(id string, data map[string]any) error {
	_, err := q.call(map[string]any{"cmd": "Update", "id": id, "data": data})
	return err
}

// MoveJobToFailed fails an ACTIVE job explicitly, mirroring the worker's FAIL
// wire: the stack and the unrecoverable flag travel too, not just the message
// (#111 silent-loss class). The job must have been pulled: pass its token.
func (q *Queue) MoveJobToFailed(id string, failure error, token string) error {
	message := failure.Error()
	// A Go error carries no stack; keep the message line (the server persists
	// the FIRST stackTraceLimit lines, and Go stacks lead with the site).
	stack := []string{errorTypeName(failure) + ": " + message}
	var unrecoverable any
	var unrec *UnrecoverableError
	if asUnrecoverable(failure, &unrec) {
		unrecoverable = true
	}
	_, err := q.call(map[string]any{
		"cmd":           "FAIL",
		"id":            id,
		"error":         message,
		"stack":         stack,
		"unrecoverable": unrecoverable,
		"token":         nonEmpty(token),
	})
	return err
}

func nonEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func errorTypeName(err error) string {
	name := strings.TrimPrefix(strings.TrimPrefix(typeName(err), "*"), "bunqueue.")
	return name
}
