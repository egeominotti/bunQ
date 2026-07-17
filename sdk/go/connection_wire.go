package bunqueue

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"reflect"
	"time"

	"github.com/vmihailenco/msgpack/v5"
)

// jsUndefined is an intermediate decoder target for msgpackr ext id 0.
// normalizeIncoming replaces it recursively with nil before data is exposed.
type jsUndefined struct{}

func init() {
	msgpack.RegisterExtDecoder(0, jsUndefined{}, func(d *msgpack.Decoder, _ reflect.Value, extLen int) error {
		skip := make([]byte, extLen)
		_, err := io.ReadFull(d.Buffered(), skip)
		return err
	})
}

// encodeFrame uses compact ints in addition to the recursive jsSafe guard.
func encodeFrame(value any) ([]byte, error) {
	safe, err := jsSafe(value)
	if err != nil {
		return nil, &ConnectionError{Message: "encode failed: " + err.Error()}
	}
	var buf bytes.Buffer
	encoder := msgpack.NewEncoder(&buf)
	encoder.UseCompactInts(true)
	if err := encoder.Encode(safe); err != nil {
		return nil, &ConnectionError{Message: "encode failed: " + err.Error()}
	}
	return buf.Bytes(), nil
}

func (c *Connection) roundTripLocked(command map[string]any, timeout time.Duration) (map[string]any, error) {
	frame, err := encodeFrame(compact(command))
	if err != nil {
		return nil, err
	}
	if len(frame) > MaxFrameSize {
		return nil, &CommandError{Message: "frame exceeds the 64MB protocol limit"}
	}
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(len(frame)))
	deadline := time.Now().Add(timeout)
	_ = c.conn.SetWriteDeadline(deadline)
	if _, err := c.conn.Write(append(header, frame...)); err != nil {
		c.closeLocked()
		return nil, &ConnectionError{Message: "socket write failed: " + err.Error()}
	}
	expected := asString(command["reqId"])
	for {
		response, err := c.readFrameLocked(deadline)
		if err != nil {
			return nil, err
		}
		reqID, present := response["reqId"]
		if !present || asString(reqID) == expected {
			return response, nil
		}
	}
}

func (c *Connection) readFrameLocked(deadline time.Time) (map[string]any, error) {
	header, err := c.readExactlyLocked(4, deadline)
	if err != nil {
		return nil, err
	}
	length := binary.BigEndian.Uint32(header)
	if length > MaxFrameSize {
		c.closeLocked()
		return nil, &ConnectionError{Message: fmt.Sprintf("oversized frame from server (%d bytes)", length)}
	}
	body, err := c.readExactlyLocked(int(length), deadline)
	if err != nil {
		return nil, err
	}
	var decoded map[string]any
	if err := msgpack.Unmarshal(body, &decoded); err != nil {
		c.closeLocked()
		return nil, &ConnectionError{Message: "malformed response frame: " + err.Error()}
	}
	return normalizeIncoming(decoded).(map[string]any), nil
}

func (c *Connection) readExactlyLocked(length int, deadline time.Time) ([]byte, error) {
	buffer := make([]byte, length)
	_ = c.conn.SetReadDeadline(deadline)
	for offset := 0; offset < length; {
		n, err := c.conn.Read(buffer[offset:])
		if err != nil {
			c.closeLocked()
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				timeoutErr := &CommandTimeoutError{Message: "command timed out (socket torn down, will reconnect)"}
				c.queueEventLocked("timeout", "", 0, timeoutErr)
				return nil, timeoutErr
			}
			return nil, &ConnectionError{Message: "connection closed by server: " + err.Error()}
		}
		offset += n
	}
	return buffer, nil
}

func normalizeIncoming(value any) any {
	switch item := value.(type) {
	case jsUndefined:
		return nil
	case map[string]any:
		for key, nested := range item {
			item[key] = normalizeIncoming(nested)
		}
		return item
	case []any:
		for index, nested := range item {
			item[index] = normalizeIncoming(nested)
		}
		return item
	default:
		return value
	}
}
