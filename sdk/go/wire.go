// Package bunqueue is the official Go client for the bunqueue job queue
// server. It speaks the native TCP protocol: 4-byte big-endian u32 length
// prefix + a standard msgpack map per frame.
package bunqueue

import (
	"fmt"
	"math"
	"strconv"
	"time"
)

// ProtocolVersion matches the version the server advertises in Hello.
const ProtocolVersion = 3

// MaxFrameSize mirrors the server-side frame cap.
const MaxFrameSize = 64 * 1024 * 1024

const (
	int32Min = math.MinInt32
	int32Max = math.MaxInt32
)

// compact removes nil-valued keys so the msgpack frame stays minimal.
func compact(command map[string]any) map[string]any {
	out := make(map[string]any, len(command))
	for key, value := range command {
		if value != nil {
			out[key] = value
		}
	}
	return out
}

// jobPayload builds protocol-owned name and data fields without changing data.
func jobPayload(name string, data any) map[string]any {
	return map[string]any{"name": name, "data": data}
}

// nowMs returns the current time in ms as float64 (already js-safe).
func nowMs() float64 {
	return float64(time.Now().UnixMilli())
}

// toIDString renders a server-sent id (string or numeric) as a string.
func toIDString(value any) string {
	switch v := value.(type) {
	case string:
		return v
	case int64:
		return strconv.FormatInt(v, 10)
	case uint64:
		return strconv.FormatUint(v, 10)
	case float64:
		if v == math.Trunc(v) {
			return strconv.FormatInt(int64(v), 10)
		}
		return strconv.FormatFloat(v, 'f', -1, 64)
	default:
		return fmt.Sprint(v)
	}
}

// ---- small decode helpers: msgpack ints arrive as int64/uint64, floats as float64.

func asInt(value any) int {
	switch v := value.(type) {
	case int64:
		return int(v)
	case uint64:
		return int(v)
	case float64:
		return int(v)
	case int:
		return v
	case int8:
		return int(v)
	case int16:
		return int(v)
	case int32:
		return int(v)
	case uint8:
		return int(v)
	case uint16:
		return int(v)
	case uint32:
		return int(v)
	case float32:
		return int(v)
	default:
		return 0
	}
}

func asString(value any) string {
	if s, ok := value.(string); ok {
		return s
	}
	return ""
}

func asBool(value any) bool {
	b, ok := value.(bool)
	return ok && b
}

func asMap(value any) map[string]any {
	if m, ok := value.(map[string]any); ok {
		return m
	}
	return map[string]any{}
}

func asSlice(value any) []any {
	if s, ok := value.([]any); ok {
		return s
	}
	return nil
}

func asStrings(value any) []string {
	items := asSlice(value)
	out := make([]string, 0, len(items))
	for _, item := range items {
		out = append(out, toIDString(item))
	}
	return out
}
