package bunqueue

import (
	"errors"
	"sync"
	"testing"
	"time"
)

type telemetryRecorder struct {
	mu     sync.Mutex
	events []TelemetryEvent
}

func (r *telemetryRecorder) record(event TelemetryEvent) {
	r.mu.Lock()
	r.events = append(r.events, event)
	r.mu.Unlock()
}

func (r *telemetryRecorder) has(kind string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, event := range r.events {
		if event.Type == kind {
			return true
		}
	}
	return false
}

func TestConnectionTelemetryLifecycle(t *testing.T) {
	recorder := &telemetryRecorder{}
	var queue *Queue
	queue = NewQueue(uniqueName("telemetry"), Options{
		Port: shared.port,
		OnEvent: func(event TelemetryEvent) {
			// Re-entry proves callbacks never run while Connection.mu is held.
			_ = queue.Connection.IsConnected()
			recorder.record(event)
		},
	})
	t.Cleanup(func() {
		_ = queue.Obliterate()
		queue.Close()
	})
	if _, err := queue.Count(); err != nil {
		t.Fatal(err)
	}
	if recorder.has("reconnect") {
		t.Fatal("first connection must not emit reconnect")
	}
	if _, err := queue.Connection.Call(map[string]any{"cmd": "DefinitelyUnknown"}); err == nil {
		t.Fatal("unknown command must fail")
	}
	_, err := queue.Connection.CallTimeout(map[string]any{
		"cmd": "PULLB", "queue": queue.Name, "count": 1, "timeout": 1000, "owner": "telemetry",
	}, 20*time.Millisecond)
	var timeoutErr *CommandTimeoutError
	if !errors.As(err, &timeoutErr) {
		t.Fatalf("expected timeout teardown, got %T (%v)", err, err)
	}
	if _, err := queue.Count(); err != nil {
		t.Fatal("command after timeout must reconnect:", err)
	}
	queue.Close()
	for _, kind := range []string{"connected", "command", "error", "timeout", "reconnect", "close"} {
		if !recorder.has(kind) {
			t.Fatalf("missing %q telemetry event", kind)
		}
	}
}

func TestConnectionTelemetryAuth(t *testing.T) {
	server := newTestServer(map[string]string{"AUTH_TOKENS": "telemetry-secret"})
	if err := server.start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(server.stop)
	recorder := &telemetryRecorder{}
	queue := NewQueue("telemetry-auth", Options{
		Port: server.port, Token: "telemetry-secret", OnEvent: recorder.record,
	})
	t.Cleanup(queue.Close)
	if _, err := queue.Count(); err != nil {
		t.Fatal(err)
	}
	if !recorder.has("auth") {
		t.Fatal("successful authentication must emit auth telemetry")
	}
}

func TestRateLimitForwardsDurationAndTTL(t *testing.T) {
	connection, socket := scriptedConnection(t)
	queue := NewQueueWithConnection("rate", connection)
	if err := queue.SetRateLimit(25, RateLimitOptions{DurationMs: 2500, TTLms: 9000}); err != nil {
		t.Fatal(err)
	}
	command := decodeWrittenCommand(t, socket)
	if asInt(command["duration"]) != 2500 || asInt(command["ttl"]) != 9000 {
		t.Fatalf("duration/ttl missing from RateLimit: %v", command)
	}
}

func TestSchedulerForwardsExplicitFalseBooleans(t *testing.T) {
	connection, socket := scriptedConnection(t)
	queue := NewQueueWithConnection("scheduler-bools", connection)
	disabled := false
	err := queue.UpsertJobScheduler(
		"scheduler-bools",
		SchedulerRepeat{
			Pattern: "0 9 * * *", SkipMissedOnRestart: &disabled, PreventOverlap: &disabled,
		},
		SchedulerTemplate{},
	)
	if err != nil {
		t.Fatal(err)
	}
	command := decodeWrittenCommand(t, socket)
	if command["skipMissedOnRestart"] != false || command["preventOverlap"] != false {
		t.Fatalf("explicit false scheduler flags were lost: %v", command)
	}
}

func TestExtZeroNormalizesRecursively(t *testing.T) {
	normalized := normalizeIncoming(map[string]any{
		"top":    jsUndefined{},
		"nested": []any{map[string]any{"value": jsUndefined{}}},
	}).(map[string]any)
	if normalized["top"] != nil {
		t.Fatalf("top ext-0 placeholder = %#v, want nil", normalized["top"])
	}
	nested := asMap(asSlice(normalized["nested"])[0])
	if nested["value"] != nil {
		t.Fatalf("nested ext-0 placeholder = %#v, want nil", nested["value"])
	}
}
