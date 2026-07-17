package bunqueue

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"os"
	"strconv"
	"sync"
	"time"
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
	OnEvent        TelemetryCallback
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
	opts          Options
	mu            sync.Mutex
	conn          net.Conn
	generation    int
	hasConnected  bool
	reqCounter    int
	pendingEvents []TelemetryEvent
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
	err := c.ensureConnectedLocked()
	events := c.takeEventsLocked()
	c.mu.Unlock()
	c.deliverEvents(events)
	return err
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
	started := time.Now()
	name := commandName(command)
	c.mu.Lock()
	if err := c.ensureConnectedLocked(); err != nil {
		c.queueEventLocked("command", name, time.Since(started), err)
		c.queueEventLocked("error", name, time.Since(started), err)
		events := c.takeEventsLocked()
		c.mu.Unlock()
		c.deliverEvents(events)
		return nil, err
	}
	c.reqCounter++
	command["reqId"] = "go-" + strconv.Itoa(c.reqCounter)
	response, err := c.roundTripLocked(command, timeout)
	if err != nil {
		c.queueEventLocked("command", name, time.Since(started), err)
		c.queueEventLocked("error", name, time.Since(started), err)
		events := c.takeEventsLocked()
		c.mu.Unlock()
		c.deliverEvents(events)
		return nil, err
	}
	if !asBool(response["ok"]) {
		message := asString(response["error"])
		if message == "" {
			message = "unknown server error"
		}
		commandErr := &CommandError{Message: message}
		c.queueEventLocked("command", name, time.Since(started), commandErr)
		c.queueEventLocked("error", name, time.Since(started), commandErr)
		events := c.takeEventsLocked()
		c.mu.Unlock()
		c.deliverEvents(events)
		return nil, commandErr
	}
	c.queueEventLocked("command", name, time.Since(started), nil)
	events := c.takeEventsLocked()
	c.mu.Unlock()
	c.deliverEvents(events)
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
	c.closeLocked()
	events := c.takeEventsLocked()
	c.mu.Unlock()
	c.deliverEvents(events)
}

// ---------------------------------------------------------------- internals

func (c *Connection) closeLocked() {
	if c.conn != nil {
		_ = c.conn.Close()
		c.conn = nil
		c.queueEventLocked("close", "", 0, nil)
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
		connectionErr := &ConnectionError{Message: fmt.Sprintf("connect to %s failed: %v", address, err)}
		c.queueEventLocked("error", "", 0, connectionErr)
		return connectionErr
	}
	reconnecting := c.hasConnected
	c.conn = conn
	c.generation++
	c.hasConnected = true
	if reconnecting {
		c.queueEventLocked("reconnect", "", 0, nil)
	}
	c.queueEventLocked("connected", "", 0, nil)
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
		c.queueEventLocked("auth", "Auth", 0, err)
		return err
	}
	if !asBool(response["ok"]) {
		c.closeLocked()
		message := asString(response["error"])
		if message == "" {
			message = "authentication failed"
		}
		authErr := &AuthError{Message: message}
		c.queueEventLocked("auth", "Auth", 0, authErr)
		return authErr
	}
	c.queueEventLocked("auth", "Auth", 0, nil)
	return nil
}
