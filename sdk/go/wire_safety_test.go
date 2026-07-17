package bunqueue

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/vmihailenco/msgpack/v5"
)

type cyclicPayload struct {
	Next *cyclicPayload `msgpack:"next"`
}

func TestRegressionEncodeRejectsCyclesAndNonStringMapKeys(t *testing.T) {
	if os.Getenv("BUNQUEUE_CYCLE_CHILD") == "1" {
		value := &cyclicPayload{}
		value.Next = value
		_, err := encodeFrame(map[string]any{"data": value})
		var connectionErr *ConnectionError
		if !errors.As(err, &connectionErr) {
			t.Fatalf("cycle error = %T (%v), want *ConnectionError", err, err)
		}
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestRegressionEncodeRejectsCyclesAndNonStringMapKeys$")
	child.Env = append(os.Environ(), "BUNQUEUE_CYCLE_CHILD=1", "BUNQUEUE_TEST_UNIT_ONLY=1")
	if output, err := child.CombinedOutput(); err != nil {
		if ctx.Err() != nil {
			t.Fatalf("cyclic input hung instead of returning a typed error: %v", ctx.Err())
		}
		t.Fatalf("cyclic input crashed or hung instead of returning a typed error: %v\n%s", err, output)
	}

	_, err := encodeFrame(map[string]any{
		"data": map[int]int64{1: 9_999_999_999_999},
	})
	var connectionErr *ConnectionError
	if !errors.As(err, &connectionErr) {
		t.Fatalf("non-string map-key error = %T (%v), want *ConnectionError", err, err)
	}
}

func TestRegressionEncodeNormalizesTimeToBrokerSafeMilliseconds(t *testing.T) {
	timestamp := time.Date(2026, 7, 17, 12, 34, 56, 789_000_000, time.UTC)
	frame, err := encodeFrame(map[string]any{"at": timestamp})
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := msgpack.Unmarshal(frame, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded["at"] != float64(timestamp.UnixMilli()) {
		t.Fatalf("timestamp encoded as %#v (%T), want Unix milliseconds", decoded["at"], decoded["at"])
	}
}
