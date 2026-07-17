package bunqueue

import (
	"bytes"
	"math"
	"net"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vmihailenco/msgpack/v5"
)

type regressionAddr string

func (a regressionAddr) Network() string { return "test" }
func (a regressionAddr) String() string  { return string(a) }

type scriptedConn struct {
	mu      sync.Mutex
	read    *bytes.Reader
	written bytes.Buffer
}

func newScriptedConn(t *testing.T, reqID string) *scriptedConn {
	t.Helper()
	body, err := encodeFrame(map[string]any{"ok": true, "reqId": reqID})
	if err != nil {
		t.Fatal(err)
	}
	frame := make([]byte, 4+len(body))
	frame[0] = byte(len(body) >> 24)
	frame[1] = byte(len(body) >> 16)
	frame[2] = byte(len(body) >> 8)
	frame[3] = byte(len(body))
	copy(frame[4:], body)
	return &scriptedConn{read: bytes.NewReader(frame)}
}

func (c *scriptedConn) Read(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.read.Read(p)
}

func (c *scriptedConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.written.Write(p)
}

func (c *scriptedConn) Close() error                     { return nil }
func (c *scriptedConn) LocalAddr() net.Addr              { return regressionAddr("local") }
func (c *scriptedConn) RemoteAddr() net.Addr             { return regressionAddr("remote") }
func (c *scriptedConn) SetDeadline(time.Time) error      { return nil }
func (c *scriptedConn) SetReadDeadline(time.Time) error  { return nil }
func (c *scriptedConn) SetWriteDeadline(time.Time) error { return nil }

func scriptedConnection(t *testing.T) (*Connection, *scriptedConn) {
	t.Helper()
	socket := newScriptedConn(t, "go-1")
	return &Connection{
		opts:         Options{}.withDefaults(),
		conn:         socket,
		generation:   0,
		hasConnected: true,
	}, socket
}

func decodeWrittenCommand(t *testing.T, socket *scriptedConn) map[string]any {
	t.Helper()
	socket.mu.Lock()
	defer socket.mu.Unlock()
	frame := socket.written.Bytes()
	if len(frame) < 4 {
		t.Fatal("no framed command was written")
	}
	var command map[string]any
	if err := msgpack.Unmarshal(frame[4:], &command); err != nil {
		t.Fatal(err)
	}
	return command
}

func TestRegressionJsSafeTraversesTypedContainers(t *testing.T) {
	type payload struct {
		EpochMs int64            `msgpack:"epochMs"`
		Nested  map[string]int64 `msgpack:"nested"`
		Values  []int64          `msgpack:"values"`
	}
	value := map[string]any{
		"data": payload{
			EpochMs: 9_999_999_999_999,
			Nested:  map[string]int64{"at": 9_999_999_999_998},
			Values:  []int64{1, 9_999_999_999_997},
		},
	}
	frame, err := encodeFrame(value)
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := msgpack.Unmarshal(frame, &decoded); err != nil {
		t.Fatal(err)
	}
	data := asMap(decoded["data"])
	if _, ok := data["epochMs"].(float64); !ok {
		t.Fatalf("typed struct int64 bypassed jsSafe: %T", data["epochMs"])
	}
	if _, ok := asMap(data["nested"])["at"].(float64); !ok {
		t.Fatalf("typed map int64 bypassed jsSafe: %T", asMap(data["nested"])["at"])
	}
	values := asSlice(data["values"])
	if _, ok := values[1].(float64); !ok {
		t.Fatalf("typed slice int64 bypassed jsSafe: %T", values[1])
	}
}

func TestRegressionWorkerNumericOptionsAreSafe(t *testing.T) {
	zero := NewWorker("zero", func(*Job) (any, error) { return nil, nil },
		WorkerOptions{HeartbeatIntervalS: 0})
	if zero.heartbeatIntervalS != 0 {
		t.Fatalf("heartbeat 0 must disable, got %v", zero.heartbeatIntervalS)
	}
	for name, value := range map[string]float64{"nan": math.NaN(), "positive infinity": math.Inf(1)} {
		worker := NewWorker(name, func(*Job) (any, error) { return nil, nil },
			WorkerOptions{HeartbeatIntervalS: value})
		if worker.heartbeatIntervalS != 0 {
			t.Fatalf("%s heartbeat must disable, got %v", name, worker.heartbeatIntervalS)
		}
	}
	negativePoll := NewWorker("poll", func(*Job) (any, error) { return nil, nil },
		WorkerOptions{PollTimeoutMs: -10})
	if negativePoll.pollTimeoutMs != 0 {
		t.Fatalf("negative poll timeout must clamp to 0, got %d", negativePoll.pollTimeoutMs)
	}
}

func TestRegressionSchedulerForwardsUniqueKey(t *testing.T) {
	connection, socket := scriptedConnection(t)
	queue := NewQueueWithConnection("scheduler", connection)
	err := queue.UpsertJobScheduler(
		"scheduler-id",
		SchedulerRepeat{Pattern: "0 9 * * *"},
		SchedulerTemplate{Opts: JobOptions{"uniqueKey": "scheduler-unique"}},
	)
	if err != nil {
		t.Fatal(err)
	}
	command := decodeWrittenCommand(t, socket)
	if command["uniqueKey"] != "scheduler-unique" {
		t.Fatalf("scheduler uniqueKey was dropped: %v", command["uniqueKey"])
	}
}

func TestRegressionSchedulerExposesPreventOverlap(t *testing.T) {
	field, ok := reflect.TypeOf(SchedulerRepeat{}).FieldByName("PreventOverlap")
	if !ok {
		t.Fatal("SchedulerRepeat must expose PreventOverlap")
	}
	if field.Type.Kind() != reflect.Pointer || field.Type.Elem().Kind() != reflect.Bool {
		t.Fatalf("PreventOverlap must be *bool to preserve explicit false, got %v", field.Type)
	}
}

func TestRegressionRateLimitAcceptsWindowAndTTL(t *testing.T) {
	method, ok := reflect.TypeOf(&Queue{}).MethodByName("SetRateLimit")
	if !ok {
		t.Fatal("SetRateLimit missing")
	}
	if !method.Type.IsVariadic() {
		t.Fatal("SetRateLimit must accept optional duration/TTL options")
	}
}

func TestRegressionOutgoingFrameCapAppliesToPayloadOnly(t *testing.T) {
	connection, socket := scriptedConnection(t)
	command := map[string]any{
		"cmd":   "FrameBoundaryProbe",
		"reqId": "go-1",
		"blob":  strings.Repeat("x", MaxFrameSize-128),
	}
	for range 4 {
		frame, err := encodeFrame(compact(command))
		if err != nil {
			t.Fatal(err)
		}
		delta := MaxFrameSize - len(frame)
		if delta == 0 {
			break
		}
		command["blob"] = strings.Repeat("x", len(command["blob"].(string))+delta)
	}
	frame, err := encodeFrame(compact(command))
	if err != nil {
		t.Fatal(err)
	}
	if len(frame) != MaxFrameSize {
		t.Fatalf("could not construct boundary payload: %d", len(frame))
	}
	delete(command, "reqId")
	if _, err := connection.Call(command); err != nil {
		t.Fatalf("a %d-byte payload is legal: %v", MaxFrameSize, err)
	}
	if socket.written.Len() != MaxFrameSize+4 {
		t.Fatalf("written frame = %d, want %d", socket.written.Len(), MaxFrameSize+4)
	}
}

func TestRegressionWorkerControlTrafficIsNotBlockedByPull(t *testing.T) {
	queue := testQueue(t, "pull-isolation")
	processed := make(chan struct{}, 1)
	worker := NewWorker(queue.Name, func(*Job) (any, error) {
		time.Sleep(100 * time.Millisecond)
		return "ok", nil
	}, WorkerOptions{
		Port:               shared.port,
		Concurrency:        2,
		BatchSize:          1,
		PollTimeoutMs:      2_000,
		LockTtlMs:          500,
		HeartbeatIntervalS: 0.05,
	})
	worker.On("completed", func(...any) {
		select {
		case processed <- struct{}{}:
		default:
		}
	})
	startWorker(t, worker)
	if _, err := queue.Add("fast", map[string]any{"x": 1}, nil); err != nil {
		t.Fatal(err)
	}
	select {
	case <-processed:
	case <-time.After(time.Second):
		state := "unknown"
		jobs, _ := queue.GetJobs("active", 0, 10)
		if len(jobs) > 0 {
			state = jobs[0].State()
		}
		t.Fatalf("ACK/heartbeat blocked behind long PULL (state %s)", state)
	}
}

func TestRegressionConnectionExposesTelemetryCallback(t *testing.T) {
	field, ok := reflect.TypeOf(Options{}).FieldByName("OnEvent")
	if !ok || field.Type.Kind() != reflect.Func {
		t.Fatal("Options must expose an optional OnEvent telemetry callback")
	}
}
