package bunqueue

// ---------------------------------------------------------------------- dlq

// GetDlq lists dead-letter entries for this queue.
func (q *Queue) GetDlq(count int) ([]map[string]any, error) {
	command := map[string]any{"cmd": "Dlq", "queue": q.Name}
	if count > 0 {
		command["count"] = count
	}
	response, err := q.call(command)
	if err != nil {
		return nil, err
	}
	items := asSlice(response["jobs"])
	out := make([]map[string]any, 0, len(items))
	for _, item := range items {
		out = append(out, asMap(item))
	}
	return out, nil
}

// RetryDlq re-queues DLQ entries; jobID targets one, count bounds a sweep.
func (q *Queue) RetryDlq(jobID string, count int) (int, error) {
	command := map[string]any{"cmd": "RetryDlq", "queue": q.Name}
	if jobID != "" {
		command["jobId"] = jobID
	}
	if count > 0 {
		command["count"] = count
	}
	response, err := q.call(command)
	if err != nil {
		return 0, err
	}
	return asInt(response["count"]), nil
}

// PurgeDlq drops every DLQ entry and returns how many were removed.
func (q *Queue) PurgeDlq() (int, error) {
	response, err := q.call(map[string]any{"cmd": "PurgeDlq", "queue": q.Name})
	if err != nil {
		return 0, err
	}
	return asInt(response["count"]), nil
}

// ----------------------------------------------------------------- schedulers

// SchedulerRepeat configures a recurring scheduler: exactly one of Pattern
// (cron) or EveryMs must be set. Limit travels as wire maxLimit (#111 class).
type SchedulerRepeat struct {
	Pattern        string
	EveryMs        int
	Limit          int
	Timezone       string
	Immediately    bool
	SkipIfNoWorker bool
	// Pointer booleans preserve explicit false separately from omission.
	SkipMissedOnRestart *bool
	PreventOverlap      *bool
}

// SchedulerTemplate is the job template a scheduler stamps on every fire.
type SchedulerTemplate struct {
	Name string
	Data any
	Opts JobOptions
}

// UpsertJobScheduler creates or updates a recurring scheduler.
func (q *Queue) UpsertJobScheduler(schedulerID string, repeat SchedulerRepeat, template SchedulerTemplate) error {
	name := template.Name
	if name == "" {
		name = schedulerID
	}
	command := map[string]any{
		"cmd":     "Cron",
		"name":    schedulerID,
		"jobName": name,
		"queue":   q.Name,
		"data":    template.Data,
	}
	if repeat.Pattern != "" {
		command["schedule"] = repeat.Pattern
	}
	if repeat.EveryMs > 0 {
		command["repeatEvery"] = repeat.EveryMs
	}
	if repeat.Limit > 0 {
		command["maxLimit"] = repeat.Limit
	}
	if repeat.Timezone != "" {
		command["timezone"] = repeat.Timezone
	}
	if repeat.Immediately {
		command["immediately"] = true
	}
	if repeat.SkipIfNoWorker {
		command["skipIfNoWorker"] = true
	}
	if repeat.SkipMissedOnRestart != nil {
		command["skipMissedOnRestart"] = *repeat.SkipMissedOnRestart
	}
	if repeat.PreventOverlap != nil {
		command["preventOverlap"] = *repeat.PreventOverlap
	}
	if priority, ok := template.Opts["priority"]; ok {
		command["priority"] = priority
	}
	if uniqueKey, ok := template.Opts["uniqueKey"]; ok {
		command["uniqueKey"] = uniqueKey
	}
	if dedup, ok := template.Opts["deduplication"]; ok {
		fields := asMap(dedup)
		if _, present := command["uniqueKey"]; !present {
			command["uniqueKey"] = fields["id"]
		}
		inner := compact(map[string]any{
			"ttl": fields["ttl"], "extend": fields["extend"], "replace": fields["replace"],
		})
		if len(inner) > 0 {
			command["dedup"] = inner
		}
	}
	if jobOptions := cronJobOptions(template.Opts); jobOptions != nil {
		command["jobOptions"] = jobOptions
	}
	_, err := q.call(command)
	return err
}

// RemoveJobScheduler deletes a scheduler by id.
func (q *Queue) RemoveJobScheduler(schedulerID string) error {
	_, err := q.call(map[string]any{"cmd": "CronDelete", "name": schedulerID})
	return err
}

// GetJobScheduler fetches one scheduler; a missing one returns (nil, nil).
// Note: a scheduler that reached its execution limit is REMOVED server-side.
func (q *Queue) GetJobScheduler(schedulerID string) (map[string]any, error) {
	response, err := q.call(map[string]any{"cmd": "CronGet", "name": schedulerID})
	if err != nil {
		if isNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return asMap(response["cron"]), nil
}

// GetJobSchedulers lists every scheduler on the server.
func (q *Queue) GetJobSchedulers() ([]map[string]any, error) {
	response, err := q.call(map[string]any{"cmd": "CronList"})
	if err != nil {
		return nil, err
	}
	items := asSlice(response["crons"])
	out := make([]map[string]any, 0, len(items))
	for _, item := range items {
		out = append(out, asMap(item))
	}
	return out, nil
}

// ------------------------------------------------------------------ webhooks

// AddWebhook registers a webhook and returns its id (wrapped in data).
func (q *Queue) AddWebhook(url string, events []string) (string, error) {
	response, err := q.call(map[string]any{"cmd": "AddWebhook", "url": url, "events": events})
	if err != nil {
		return "", err
	}
	return asString(asMap(response["data"])["webhookId"]), nil
}

// RemoveWebhook deletes a webhook (wire field: webhookId).
func (q *Queue) RemoveWebhook(webhookID string) error {
	_, err := q.call(map[string]any{"cmd": "RemoveWebhook", "webhookId": webhookID})
	return err
}

// ListWebhooks returns every registered webhook (wrapped in data.webhooks).
func (q *Queue) ListWebhooks() ([]map[string]any, error) {
	response, err := q.call(map[string]any{"cmd": "ListWebhooks"})
	if err != nil {
		return nil, err
	}
	items := asSlice(asMap(response["data"])["webhooks"])
	out := make([]map[string]any, 0, len(items))
	for _, item := range items {
		out = append(out, asMap(item))
	}
	return out, nil
}

// SetWebhookEnabled toggles a webhook. The wire field is `id`, NOT webhookId.
func (q *Queue) SetWebhookEnabled(webhookID string, enabled bool) error {
	_, err := q.call(map[string]any{"cmd": "SetWebhookEnabled", "id": webhookID, "enabled": enabled})
	return err
}

// ---------------------------------------------------------------- rate limit

// RateLimitOptions configures the delivery window and optional broker-side
// expiry. Non-positive/non-finite values degrade to server defaults.
type RateLimitOptions struct {
	DurationMs float64
	TTLms      float64
}

// SetRateLimit caps delivery. With no options it means limit per second.
func (q *Queue) SetRateLimit(limit int, options ...RateLimitOptions) error {
	command := map[string]any{"cmd": "RateLimit", "queue": q.Name, "limit": limit}
	if len(options) > 0 {
		command["duration"] = options[0].DurationMs
		command["ttl"] = options[0].TTLms
	}
	_, err := q.call(command)
	return err
}

// ClearRateLimit removes the queue's rate limit.
func (q *Queue) ClearRateLimit() error {
	_, err := q.call(map[string]any{"cmd": "RateLimitClear", "queue": q.Name})
	return err
}

// ---------------------------------------------------------------- monitoring

// GetWorkers lists the registered workers (wrapped in data.workers).
func (q *Queue) GetWorkers() ([]map[string]any, error) {
	response, err := q.call(map[string]any{"cmd": "ListWorkers"})
	if err != nil {
		return nil, err
	}
	items := asSlice(asMap(response["data"])["workers"])
	out := make([]map[string]any, 0, len(items))
	for _, item := range items {
		out = append(out, asMap(item))
	}
	return out, nil
}

// GetStats returns the server stats block (top-level `stats`).
func (q *Queue) GetStats() (map[string]any, error) {
	response, err := q.call(map[string]any{"cmd": "Stats"})
	if err != nil {
		return nil, err
	}
	return asMap(response["stats"]), nil
}

// ListQueues returns every queue name known to the server.
func (q *Queue) ListQueues() ([]string, error) {
	response, err := q.call(map[string]any{"cmd": "ListQueues"})
	if err != nil {
		return nil, err
	}
	return asStrings(response["queues"]), nil
}

// Ping answers true when the server replies pong.
func (q *Queue) Ping() (bool, error) {
	return q.Connection.Ping()
}
