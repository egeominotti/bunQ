package bunqueue

import (
	"fmt"
	"math"
	"math/rand"
	"os"
	"sync"
	"time"
)

// Processor handles one job; return a result to ACK or an error to FAIL.
// Return (or panic with) *UnrecoverableError to skip retries straight to the
// DLQ.
type Processor func(job *Job) (any, error)

// WorkerOptions configures a Worker. Concurrency > 1 runs jobs on a bounded
// goroutine pool (Go, unlike PHP, has real in-process concurrency).
type WorkerOptions struct {
	Host               string
	Port               int
	Token              string
	TLS                *TLSOptions
	Concurrency        int     // default 4
	BatchSize          int     // default 10, clamped to [1, 1000] (server PULLB cap)
	PollTimeoutMs      int     // default 5000, capped at 30000
	LockTtlMs          int     // default 30000
	HeartbeatIntervalS float64 // positive seconds enable heartbeats; zero/negative/non-finite disables
	DisableHeartbeat   bool    // explicit off, even when HeartbeatIntervalS is positive
	Name               string
	OnEvent            TelemetryCallback
}

// Worker pulls jobs over TCP and runs a processor with bounded concurrency.
// All queue semantics (retry, backoff, DLQ, priorities) live in the server.
type Worker struct {
	Queue      string
	WorkerID   string
	Connection *Connection

	processor           Processor
	commandConnection   *Connection
	heartbeatConnection *Connection
	concurrency         int
	batchSize           int
	pollTimeoutMs       int
	lockTtlMs           int
	heartbeatIntervalS  float64
	name                string

	mu                   sync.Mutex
	active               map[string]string // job id -> lock token
	listeners            map[string][]func(...any)
	stopped              bool
	wasBusy              bool
	registeredGeneration int
	processed            int
	failed               int

	slots       chan struct{}
	inFlight    sync.WaitGroup
	stopHb      chan struct{}
	heartbeatWG sync.WaitGroup
	hbStarted   bool
}

const workerMaxStackLines = 10 // server persists the FIRST stackTraceLimit lines (default 10)

var workerBackoff = []time.Duration{500 * time.Millisecond, time.Second, 2 * time.Second, 5 * time.Second}

// NewWorker builds a worker; call Run() to start the blocking pull loop.
func NewWorker(queue string, processor Processor, opts WorkerOptions) *Worker {
	concurrency := opts.Concurrency
	if concurrency < 1 {
		concurrency = 4
	}
	// The server rejects PULLB count > 1000: clamp, never wedge the loop.
	batchSize := opts.BatchSize
	if batchSize == 0 {
		batchSize = 10
	}
	batchSize = min(max(1, batchSize), 1000)
	pollTimeout := opts.PollTimeoutMs
	if pollTimeout < 0 {
		pollTimeout = 0
	} else if pollTimeout == 0 {
		pollTimeout = 5000
	}
	pollTimeout = min(pollTimeout, 30_000)
	lockTtl := opts.LockTtlMs
	if lockTtl == 0 {
		lockTtl = 30_000
	}
	heartbeat := opts.HeartbeatIntervalS
	if heartbeat <= 0 || math.IsNaN(heartbeat) || math.IsInf(heartbeat, 0) || opts.DisableHeartbeat {
		heartbeat = 0 // disabled: no ticker at all (never a 0-interval storm)
	}
	hostname, _ := os.Hostname()
	workerID := fmt.Sprintf("go-%s-%d-%08x", hostname, os.Getpid(), rand.Uint32())
	name := opts.Name
	if name == "" {
		name = workerID
	}
	connectionOptions := Options{
		Host: opts.Host, Port: opts.Port, Token: opts.Token, TLS: opts.TLS, OnEvent: opts.OnEvent,
	}
	return &Worker{
		Queue:                queue,
		WorkerID:             workerID,
		Connection:           NewConnection(connectionOptions),
		commandConnection:    NewConnection(connectionOptions),
		heartbeatConnection:  NewConnection(connectionOptions),
		processor:            processor,
		concurrency:          concurrency,
		batchSize:            batchSize,
		pollTimeoutMs:        pollTimeout,
		lockTtlMs:            lockTtl,
		heartbeatIntervalS:   heartbeat,
		name:                 name,
		active:               map[string]string{},
		listeners:            map[string][]func(...any){},
		registeredGeneration: -1,
		slots:                make(chan struct{}, concurrency),
	}
}

// BatchSize exposes the clamped batch size (server PULLB cap is 1000).
func (w *Worker) BatchSize() int { return w.batchSize }

// HeartbeatIntervalS exposes the effective heartbeat interval (0 = disabled).
func (w *Worker) HeartbeatIntervalS() float64 { return w.heartbeatIntervalS }

// On registers an event callback: ready, active, completed, failed, error,
// drained, closed. Callbacks run on worker goroutines; a panicking callback
// never kills the loop.
func (w *Worker) On(event string, fn func(...any)) *Worker {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.listeners[event] = append(w.listeners[event], fn)
	return w
}

// Run blocks pulling and processing jobs until Stop() is called.
func (w *Worker) Run() {
	w.mu.Lock()
	w.stopped = false
	w.mu.Unlock()
	if err := w.safeRegister(); err != nil {
		w.emit("error", err)
	}
	w.startHeartbeat()
	w.emit("ready")
	backoffIdx := 0
	for !w.isStopped() {
		if err := w.pollOnce(); err != nil {
			w.emit("error", err)
			time.Sleep(workerBackoff[min(backoffIdx, len(workerBackoff)-1)])
			backoffIdx++
			continue
		}
		backoffIdx = 0
	}
	w.inFlight.Wait()
	w.Close()
}

// Stop asks the pull loop to exit after the in-flight jobs settle.
func (w *Worker) Stop() {
	w.mu.Lock()
	w.stopped = true
	w.mu.Unlock()
}

// Close unregisters (when registered), stops heartbeats and drops the socket.
func (w *Worker) Close() {
	w.mu.Lock()
	w.stopped = true
	if w.hbStarted {
		close(w.stopHb)
		w.hbStarted = false
	}
	registered := w.registeredGeneration >= 0
	w.registeredGeneration = -1
	w.mu.Unlock()
	w.heartbeatWG.Wait()
	if registered && w.Connection.IsConnected() {
		_, _ = w.Connection.Call(map[string]any{"cmd": "UnregisterWorker", "workerId": w.WorkerID})
	}
	w.Connection.Close()
	w.commandConnection.Close()
	w.heartbeatConnection.Close()
	w.emit("closed")
}

// ---------------------------------------------------------------- internals

func (w *Worker) isStopped() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.stopped
}

func (w *Worker) pollOnce() error {
	// Registration is per-connection server state: re-register after any
	// reconnect (generation change). isConnected covers the very first poll,
	// where both generations are still -1.
	if !w.Connection.IsConnected() || w.Connection.Generation() != w.registeredGenerationSnapshot() {
		if err := w.safeRegister(); err != nil {
			return err
		}
	}
	free := w.freeSlots()
	if free <= 0 {
		time.Sleep(20 * time.Millisecond)
		return nil
	}
	response, err := w.Connection.CallTimeout(map[string]any{
		"cmd":     "PULLB",
		"queue":   w.Queue,
		"count":   min(free, w.batchSize),
		"timeout": w.pollTimeoutMs,
		"owner":   w.WorkerID,
		"lockTtl": w.lockTtlMs,
	}, time.Duration(w.pollTimeoutMs)*time.Millisecond+10*time.Second)
	if err != nil {
		return err
	}
	jobs := asSlice(response["jobs"])
	tokens := asSlice(response["tokens"])
	if len(jobs) == 0 {
		w.mu.Lock()
		drained := w.wasBusy && len(w.active) == 0
		if drained {
			w.wasBusy = false
		}
		w.mu.Unlock()
		if drained {
			w.emit("drained")
		}
		return nil
	}
	w.mu.Lock()
	w.wasBusy = true
	w.mu.Unlock()
	for i, rawAny := range jobs {
		raw := asMap(rawAny)
		token := ""
		if i < len(tokens) {
			token = asString(tokens[i])
		}
		jobID := toIDString(raw["id"])
		w.mu.Lock()
		w.active[jobID] = token
		w.mu.Unlock()
		w.slots <- struct{}{} // bounded pool: blocks when concurrency is saturated
		w.inFlight.Add(1)
		go w.runJob(raw, token)
	}
	return nil
}

func (w *Worker) freeSlots() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.concurrency - len(w.active)
}

func (w *Worker) registeredGenerationSnapshot() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.registeredGeneration
}
