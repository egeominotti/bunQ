package bunqueue

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"reflect"
	"strconv"
	"sync"
	"time"

	"github.com/vmihailenco/msgpack/v5"
)

// TLSOptions configures the client side of a TLS connection. Certificates
// are VERIFIED by default (issue #109 class); opting out requires an
// explicit InsecureSkipVerify.
type TLSOptions struct {
	CAFile             string
	ServerName         string
	InsecureSkipVerify bool
}

// Options configures a Connection (shared by Queue, Worker, FlowProducer).
type Options struct {
	Host           string
	Port           int
	Token          string
	TLS            *TLSOptions
	ConnectTimeout time.Duration // default 10s
	CommandTimeout time.Duration // default 30s
}

func (o Options) withDefaults() Options {
	if o.Host == "" {
		o.Host = "localhost"
	}
	if o.Port == 0 {
		o.Port = 6789
	}
	if o.ConnectTimeout == 0 {
		o.ConnectTimeout = 10 * time.Second
	}
	if o.CommandTimeout == 0 {
		o.CommandTimeout = 30 * time.Second
	}
	return o
}

// Connection is a synchronous, mutex-serialized TCP connection: one frame
// out, one frame in. A read timeout tears the socket down (half-open guard)
// and the next call transparently reconnects and re-authenticates.
type Connection struct {
	opts       Options
	mu         sync.Mutex
	conn       net.Conn
	generation int
	reqCounter int
}

// NewConnection builds a lazy connection: the socket opens on the first call.
func NewConnection(opts Options) *Connection {
	return &Connection{opts: opts.withDefaults(), generation: -1}
}

// Generation increases on every successful (re)connect — used by Worker to
// re-register after a reconnect.
func (c *Connection) Generation() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.generation
}

// IsConnected reports whether a socket is currently open.
func (c *Connection) IsConnected() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.conn != nil
}

// EnsureConnected opens (and authenticates) the socket now.
func (c *Connection) EnsureConnected() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ensureConnectedLocked()
}

// Call sends a command and returns the decoded response map.
// It returns *CommandError when the server answers ok:false.
func (c *Connection) Call(command map[string]any) (map[string]any, error) {
	return c.CallTimeout(command, 0)
}

// CallTimeout is Call with a per-command response deadline (0 = default).
func (c *Connection) CallTimeout(command map[string]any, timeout time.Duration) (map[string]any, error) {
	if timeout <= 0 {
		timeout = c.opts.CommandTimeout
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.ensureConnectedLocked(); err != nil {
		return nil, err
	}
	c.reqCounter++
	command["reqId"] = "go-" + strconv.Itoa(c.reqCounter)
	response, err := c.roundTripLocked(command, timeout)
	if err != nil {
		return nil, err
	}
	if !asBool(response["ok"]) {
		message := asString(response["error"])
		if message == "" {
			message = "unknown server error"
		}
		return nil, &CommandError{Message: message}
	}
	return response, nil
}

// Hello performs protocol negotiation; the response carries protocolVersion.
func (c *Connection) Hello() (map[string]any, error) {
	return c.Call(map[string]any{
		"cmd":             "Hello",
		"protocolVersion": ProtocolVersion,
		"capabilities":    []string{"pipelining"},
	})
}

// Ping answers true when the server replies pong.
func (c *Connection) Ping() (bool, error) {
	response, err := c.Call(map[string]any{"cmd": "Ping"})
	if err != nil {
		return false, err
	}
	return asBool(asMap(response["data"])["pong"]), nil
}

// Close shuts the socket down; the next Call reconnects.
func (c *Connection) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closeLocked()
}

// ---------------------------------------------------------------- internals

func (c *Connection) closeLocked() {
	if c.conn != nil {
		_ = c.conn.Close()
		c.conn = nil
	}
}

func (c *Connection) ensureConnectedLocked() error {
	if c.conn != nil {
		return nil
	}
	address := net.JoinHostPort(c.opts.Host, strconv.Itoa(c.opts.Port))
	var conn net.Conn
	var err error
	if c.opts.TLS != nil {
		conn, err = c.dialTLS(address)
	} else {
		conn, err = net.DialTimeout("tcp", address, c.opts.ConnectTimeout)
	}
	if err != nil {
		return &ConnectionError{Message: fmt.Sprintf("connect to %s failed: %v", address, err)}
	}
	c.conn = conn
	c.generation++
	if c.opts.Token != "" {
		if err := c.authenticateLocked(); err != nil {
			return err
		}
	}
	return nil
}

func (c *Connection) dialTLS(address string) (net.Conn, error) {
	config := &tls.Config{
		ServerName:         c.opts.TLS.ServerName,
		InsecureSkipVerify: c.opts.TLS.InsecureSkipVerify,
	}
	if config.ServerName == "" {
		config.ServerName = c.opts.Host
	}
	if c.opts.TLS.CAFile != "" {
		pem, err := os.ReadFile(c.opts.TLS.CAFile)
		if err != nil {
			return nil, err
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("no certificates parsed from %s", c.opts.TLS.CAFile)
		}
		config.RootCAs = pool
	}
	dialer := &net.Dialer{Timeout: c.opts.ConnectTimeout}
	return tls.DialWithDialer(dialer, "tcp", address, config)
}

func (c *Connection) authenticateLocked() error {
	response, err := c.roundTripLocked(
		map[string]any{"cmd": "Auth", "token": c.opts.Token, "reqId": "go-auth"},
		c.opts.CommandTimeout,
	)
	if err != nil {
		return err
	}
	if !asBool(response["ok"]) {
		c.closeLocked()
		message := asString(response["error"])
		if message == "" {
			message = "authentication failed"
		}
		return &AuthError{Message: message}
	}
	return nil
}

// jsUndefined stands in for msgpackr's ext id 0 (JS `undefined`); the asX
// decode helpers treat it like an absent value.
type jsUndefined struct{}

func init() {
	// msgpackr packs JS `undefined` as ext id 0: decode it as a placeholder
	// instead of failing the whole frame with "unknown ext id".
	msgpack.RegisterExtDecoder(0, jsUndefined{}, func(d *msgpack.Decoder, v reflect.Value, extLen int) error {
		skip := make([]byte, extLen)
		_, err := io.ReadFull(d.Buffered(), skip)
		return err
	})
}

// encodeFrame marshals with compact ints: without it, Go int64 values travel
// as msgpack int64 even when tiny, and the server (msgpackr) decodes them as
// BigInt — crashing validation and JSON serialization (BigInt-killer class).
func encodeFrame(value any) ([]byte, error) {
	var buf bytes.Buffer
	encoder := msgpack.NewEncoder(&buf)
	encoder.UseCompactInts(true)
	if err := encoder.Encode(value); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (c *Connection) roundTripLocked(command map[string]any, timeout time.Duration) (map[string]any, error) {
	frame, err := encodeFrame(jsSafe(compact(command)))
	if err != nil {
		return nil, &ConnectionError{Message: "encode failed: " + err.Error()}
	}
	if len(frame)+4 > MaxFrameSize {
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
	return decoded, nil
}

func (c *Connection) readExactlyLocked(length int, deadline time.Time) ([]byte, error) {
	buffer := make([]byte, length)
	offset := 0
	_ = c.conn.SetReadDeadline(deadline)
	for offset < length {
		n, err := c.conn.Read(buffer[offset:])
		if err != nil {
			c.closeLocked()
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				// Half-open guard (issue #94 class): a timed-out socket can no
				// longer be trusted for framing — tear down, reconnect later.
				return nil, &CommandTimeoutError{Message: "command timed out (socket torn down, will reconnect)"}
			}
			return nil, &ConnectionError{Message: "connection closed by server: " + err.Error()}
		}
		offset += n
	}
	return buffer, nil
}
