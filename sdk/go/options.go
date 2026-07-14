package bunqueue

import "fmt"

// JobOptions carries per-job options with the same names as the TypeScript
// client (attempts, jobId, deduplication, ...). Unknown keys error instead of
// being silently dropped (the "client drops a wire-supported field" class).
type JobOptions map[string]any

var optionPassthrough = map[string]bool{
	"jobId": true, "uniqueKey": true,
	"priority": true, "delay": true, "backoff": true, "ttl": true, "timeout": true,
	"dependsOn": true, "parentId": true, "childrenIds": true, "tags": true,
	"groupId": true, "lifo": true, "removeOnComplete": true, "removeOnFail": true,
	"stallTimeout": true, "durable": true, "repeat": true, "stackTraceLimit": true,
	"keepLogs": true, "sizeLimit": true, "timestamp": true,
	"failParentOnFailure": true, "removeDependencyOnFailure": true,
	"ignoreDependencyOnFailure": true, "continueParentOnFailure": true,
}

// optionsToWire maps SDK option names to the wire PUSH field set (parity with
// the reference client's buildPushPayload).
func optionsToWire(opts JobOptions) (map[string]any, error) {
	out := map[string]any{}
	for key, value := range opts {
		if value == nil {
			continue
		}
		switch {
		case key == "attempts":
			out["maxAttempts"] = value
		case optionPassthrough[key]:
			out[key] = value
		case key == "deduplication":
			// {id, ttl, extend, replace} -> uniqueKey + dedup
			dedup := asMap(value)
			if _, present := out["uniqueKey"]; !present {
				if id, ok := dedup["id"]; ok {
					out["uniqueKey"] = id
				}
			}
			fields := compact(map[string]any{
				"ttl":     dedup["ttl"],
				"extend":  dedup["extend"],
				"replace": dedup["replace"],
			})
			if len(fields) > 0 {
				out["dedup"] = fields
			}
		case key == "debounce":
			debounce := asMap(value)
			out["debounceId"] = debounce["id"]
			out["debounceTtl"] = debounce["ttl"]
		default:
			return nil, fmt.Errorf("unknown job option: %s", key)
		}
	}
	return compact(out), nil
}

// cronJobOptions maps a scheduler template's job options to the wire
// CronJobOptions (issue #86 class: the server reads only these keys).
func cronJobOptions(jobOpts JobOptions) map[string]any {
	if len(jobOpts) == 0 {
		return nil
	}
	out := compact(map[string]any{
		"maxAttempts":  jobOpts["attempts"],
		"backoff":      jobOpts["backoff"],
		"timeout":      jobOpts["timeout"],
		"delay":        jobOpts["delay"],
		"stallTimeout": jobOpts["stallTimeout"],
	})
	if v, ok := jobOpts["removeOnComplete"].(bool); ok {
		out["removeOnComplete"] = v
	}
	if v, ok := jobOpts["removeOnFail"].(bool); ok {
		out["removeOnFail"] = v
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
