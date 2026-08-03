package bunqueue

// Queue produces jobs and operates a named queue over TCP. Query, control
// and admin areas live in queue_query.go / queue_control.go / queue_admin.go
// (same layout as the other official SDKs).
type Queue struct {
	Name       string
	Connection *Connection
	owns       bool
}

// BulkEntry is one job in an AddBulk batch.
type BulkEntry struct {
	Name string
	Data any
	Opts JobOptions
}

// NewQueue opens a queue handle with its own lazy connection.
func NewQueue(name string, opts Options) *Queue {
	return &Queue{Name: name, Connection: NewConnection(opts), owns: true}
}

// NewQueueWithConnection shares an existing connection (caller keeps ownership).
func NewQueueWithConnection(name string, connection *Connection) *Queue {
	return &Queue{Name: name, Connection: connection, owns: false}
}

// Add pushes one job. Opts accepts the full wire option set with the same
// names as the TypeScript client (attempts, jobId, deduplication, ...).
func (q *Queue) Add(name string, data any, opts JobOptions) (*Job, error) {
	wire, err := optionsToWire(opts)
	if err != nil {
		return nil, err
	}
	payload := jobPayload(name, data)
	command := map[string]any{"cmd": "PUSH", "queue": q.Name}
	for key, value := range payload {
		command[key] = value
	}
	for key, value := range wire {
		command[key] = value
	}
	response, err := q.Connection.Call(command)
	if err != nil {
		return nil, err
	}
	raw := map[string]any{"id": toIDString(response["id"]), "queue": q.Name}
	for key, value := range payload {
		raw[key] = value
	}
	return newJob(raw, q.Connection, ""), nil
}

// AddBulk pushes many jobs in one PUSHB round-trip and returns their ids.
func (q *Queue) AddBulk(entries []BulkEntry) ([]string, error) {
	inputs := make([]any, 0, len(entries))
	for _, entry := range entries {
		wire, err := optionsToWire(entry.Opts)
		if err != nil {
			return nil, err
		}
		// PUSHB entries are typed JobInput, whose custom-id field is
		// `customId` — unlike single PUSH, which renames jobId server-side.
		if id, present := wire["jobId"]; present {
			wire["customId"] = id
			delete(wire, "jobId")
		}
		input := jobPayload(entry.Name, entry.Data)
		for key, value := range wire {
			input[key] = value
		}
		inputs = append(inputs, input)
	}
	response, err := q.Connection.Call(map[string]any{
		"cmd":   "PUSHB",
		"queue": q.Name,
		"jobs":  inputs,
	})
	if err != nil {
		return nil, err
	}
	return asStrings(response["ids"]), nil
}

// Close releases the underlying connection when this queue owns it.
func (q *Queue) Close() {
	if q.owns {
		q.Connection.Close()
	}
}

// call is the shared dispatcher used by the area files.
func (q *Queue) call(command map[string]any) (map[string]any, error) {
	return q.Connection.Call(compact(command))
}
