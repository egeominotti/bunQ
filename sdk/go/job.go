package bunqueue

// Job is a job as seen by the client: the raw server fields plus per-id
// operations. The lock Token is set only on jobs pulled by a Worker.
type Job struct {
	Raw        map[string]any
	Token      string
	connection *Connection
}

func newJob(raw map[string]any, connection *Connection, token string) *Job {
	return &Job{Raw: raw, Token: token, connection: connection}
}

// ID returns the server-assigned job id.
func (j *Job) ID() string {
	return toIDString(j.Raw["id"])
}

// Name returns the top-level name, with legacy data-envelope compatibility.
func (j *Job) Name() string {
	if name, ok := j.Raw["name"].(string); ok {
		return name
	}
	return asString(asMap(j.Raw["data"])["name"])
}

// Data preserves modern user data and unwraps only a legacy name envelope.
func (j *Job) Data() any {
	if _, ok := j.Raw["name"].(string); ok {
		return j.Raw["data"]
	}
	data := asMap(j.Raw["data"])
	if _, ok := data["name"].(string); !ok {
		return j.Raw["data"]
	}
	out := make(map[string]any, len(data))
	for key, value := range data {
		if key != "name" {
			out[key] = value
		}
	}
	return out
}

// State returns the job state carried on the raw payload, if any.
func (j *Job) State() string {
	return asString(j.Raw["state"])
}

// AttemptsMade returns how many attempts the job has consumed.
func (j *Job) AttemptsMade() int {
	return asInt(j.Raw["attempts"])
}

// ChildrenIDs lists the ids of flow children, if any.
func (j *Job) ChildrenIDs() []string {
	return asStrings(j.Raw["childrenIds"])
}

// Stacktrace returns the persisted failure stack lines, if any.
func (j *Job) Stacktrace() []string {
	if j.Raw["stacktrace"] == nil {
		return nil
	}
	return asStrings(j.Raw["stacktrace"])
}

// UpdateProgress reports progress (the server requires the job to be ACTIVE).
func (j *Job) UpdateProgress(progress float64, message string) error {
	command := map[string]any{"cmd": "Progress", "id": j.ID(), "progress": progress}
	if message != "" {
		command["message"] = message
	}
	_, err := j.connection.Call(command)
	return err
}

// Log appends a log line to the job.
func (j *Job) Log(message string, level string) error {
	command := map[string]any{"cmd": "AddLog", "id": j.ID(), "message": message}
	if level != "" {
		command["level"] = level
	}
	_, err := j.connection.Call(command)
	return err
}

// ExtendLock renews this job's lock for durationMs more milliseconds. Call it
// from inside a long-running processor when the heartbeat interval alone is
// not enough.
func (j *Job) ExtendLock(durationMs int) error {
	_, err := j.connection.Call(map[string]any{
		"cmd":      "ExtendLock",
		"id":       j.ID(),
		"token":    j.Token,
		"duration": durationMs,
	})
	return err
}

// GetState fetches the job's current state from the server.
func (j *Job) GetState() (string, error) {
	response, err := j.connection.Call(map[string]any{"cmd": "GetState", "id": j.ID()})
	if err != nil {
		return "", err
	}
	return asString(response["state"]), nil
}

// GetResult fetches the job's persisted result, if any.
func (j *Job) GetResult() (any, error) {
	response, err := j.connection.Call(map[string]any{"cmd": "GetResult", "id": j.ID()})
	if err != nil {
		return nil, err
	}
	return response["result"], nil
}
