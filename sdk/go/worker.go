package bunqueue

import (
	"fmt"
	"math/rand"
	"os"
	"runtime/debug"
	"strings"
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
	HeartbeatIntervalS float64 // default 10; negative disables heartbeats
	DisableHeartbeat   bool    // explicit off (Go zero-values make 0 mean "default")
	Name               string
}

// Worker pulls jobs over TCP and runs a processor with bounded concurrency.
// All queue semantics (retry, backoff, DLQ, priorities) live in the server.
type Worker struct {
	Queue      string
	WorkerID   string
	Connection *Connection

	processor          Processor
	concurrency        int
	batchSize          int
	pollTimeoutMs      int
	lockTtlMs          int
	heartbeatIntervalS float64
	name               string

	mu                   sync.Mutex
	active               map[string]string // job id -> lock token
	listeners            map[string][]func(...any)
	stopped              bool
	wasBusy              bool
	registeredGeneration int
	processed            int
	failed               int

	slots     chan struct{}
	inFlight  sync.WaitGroup
	stopHb    chan struct{}
	hbStarted bool
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
	if pollTimeout == 0 {
		pollTimeout = 5000
	}
	pollTimeout = min(pollTimeout, 30_000)
	lockTtl := opts.LockTtlMs
	if lockTtl == 0 {
		lockTtl = 30_000
	}
	heartbeat := opts.HeartbeatIntervalS
	if heartbeat == 0 {
		heartbeat = 10
	}
	if heartbeat < 0 || opts.DisableHeartbeat {
		heartbeat = 0 // disabled: no ticker at all (never a 0-interval storm)
	}
	hostname, _ := os.Hostname()
	workerID := fmt.Sprintf("go-%s-%d-%08x", hostname, os.Getpid(), rand.Uint32())
	name := opts.Name
	if name == "" {
		name = workerID
	}
	return &Worker{
		Queue:    queue,
		WorkerID: workerID,
		Connection: NewConnection(Options{
			Host: opts.Host, Port: opts.Port, Token: opts.Token, TLS: opts.TLS,
		}),
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
		stopHb:               make(chan struct{}),
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
	w.safeRegister()
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
	if w.hbStarted {
		close(w.stopHb)
		w.hbStarted = false
	}
	registered := w.registeredGeneration >= 0
	w.mu.Unlock()
	if registered {
		_, _ = w.Connection.Call(map[string]any{"cmd": "UnregisterWorker", "workerId": w.WorkerID})
	}
	w.Connection.Close()
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
		w.safeRegister()
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

func (w *Worker) runJob(raw map[string]any, token string) {
	defer w.inFlight.Done()
	defer func() { <-w.slots }()
	job := newJob(raw, w.Connection, token)
	w.emit("active", job)
	result, procErr := w.invokeProcessor(job, raw)
	if procErr != nil {
		w.failJob(job, token, procErr, raw)
		return
	}
	_, err := w.Connection.Call(compact(map[string]any{
		"cmd": "ACK", "id": job.ID(), "token": token, "result": result,
	}))
	// Free the slot BEFORE emitting: a panicking listener must not leak it.
	w.finishJob(job.ID())
	if err != nil {
		// The ACK never reached the server: only 'error' fires (lock expiry
		// will retry the job) — never claim a completion.
		w.emit("error", err)
		return
	}
	w.mu.Lock()
	w.processed++
	w.mu.Unlock()
	w.emit("completed", job, result)
}

// invokeProcessor runs the processor converting panics into failures whose
// stack still points at the panic site (captured inside the deferred call,
// before the stack unwinds).
func (w *Worker) invokeProcessor(job *Job, raw map[string]any) (result any, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			if unrec, ok := recovered.(*UnrecoverableError); ok {
				err = &panicError{message: unrec.Message, stack: debug.Stack(), unrecoverable: true}
				return
			}
			err = &panicError{message: fmt.Sprint(recovered), stack: debug.Stack()}
		}
	}()
	return w.processor(job)
}

type panicError struct {
	message       string
	stack         []byte
	unrecoverable bool
}

func (e *panicError) Error() string { return e.message }

func (w *Worker) failJob(job *Job, token string, procErr error, raw map[string]any) {
	cap := asInt(raw["stackTraceLimit"])
	if cap <= 0 {
		cap = workerMaxStackLines
	}
	message := procErr.Error()
	// Lead with the message (Go/JS convention): the server persists the
	// FIRST stackTraceLimit lines.
	stack := []string{errorTypeName(procErr) + ": " + message}
	var unrecoverable any
	if pErr, ok := procErr.(*panicError); ok {
		stack = append([]string{"panic: " + message}, strings.Split(strings.TrimSpace(string(pErr.stack)), "\n")...)
		if pErr.unrecoverable {
			unrecoverable = true
		}
	}
	var unrec *UnrecoverableError
	if asUnrecoverable(procErr, &unrec) {
		unrecoverable = true
	}
	if len(stack) > cap {
		stack = stack[:cap]
	}
	_, err := w.Connection.Call(compact(map[string]any{
		"cmd":           "FAIL",
		"id":            job.ID(),
		"token":         token,
		"error":         message,
		"stack":         stack,
		"unrecoverable": unrecoverable,
	}))
	w.finishJob(job.ID())
	if err != nil {
		w.emit("error", err)
		return
	}
	w.mu.Lock()
	w.failed++
	w.mu.Unlock()
	w.emit("failed", job, procErr)
}

func (w *Worker) finishJob(id string) {
	w.mu.Lock()
	delete(w.active, id)
	w.mu.Unlock()
}

func (w *Worker) startHeartbeat() {
	// 0 (or negative) disables heartbeats entirely.
	if w.heartbeatIntervalS <= 0 {
		return
	}
	w.mu.Lock()
	w.hbStarted = true
	w.mu.Unlock()
	ticker := time.NewTicker(time.Duration(w.heartbeatIntervalS * float64(time.Second)))
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-w.stopHb:
				return
			case <-ticker.C:
				w.sendHeartbeat()
			}
		}
	}()
}

func (w *Worker) sendHeartbeat() {
	w.mu.Lock()
	ids := make([]any, 0, len(w.active))
	tokens := make([]any, 0, len(w.active))
	for id, token := range w.active {
		ids = append(ids, id)
		tokens = append(tokens, token)
	}
	activeCount, processed, failed := len(w.active), w.processed, w.failed
	w.mu.Unlock()
	if _, err := w.Connection.Call(map[string]any{
		"cmd": "Heartbeat", "id": w.WorkerID,
		"activeJobs": activeCount, "processed": processed, "failed": failed,
	}); err != nil {
		w.emit("error", err)
		return
	}
	if len(ids) > 0 {
		if _, err := w.Connection.Call(map[string]any{
			"cmd": "JobHeartbeatB", "ids": ids, "tokens": tokens,
		}); err != nil {
			w.emit("error", err)
		}
	}
}

// safeRegister registers the worker; the generation is marked ONLY on
// success (snapshot from BEFORE the call: if the call itself reconnected,
// the stale value forces one harmless duplicate registration).
func (w *Worker) safeRegister() {
	if err := w.Connection.EnsureConnected(); err != nil {
		w.emit("error", err)
		return
	}
	generation := w.Connection.Generation()
	hostname, _ := os.Hostname()
	if _, err := w.Connection.Call(map[string]any{
		"cmd":         "RegisterWorker",
		"name":        w.name,
		"queues":      []string{w.Queue},
		"concurrency": w.concurrency,
		"workerId":    w.WorkerID,
		"hostname":    hostname,
		"pid":         os.Getpid(),
		"startedAt":   nowMs(),
	}); err != nil {
		w.emit("error", err)
		return
	}
	w.mu.Lock()
	w.registeredGeneration = generation
	w.mu.Unlock()
}

func (w *Worker) emit(event string, args ...any) {
	w.mu.Lock()
	callbacks := append([]func(...any){}, w.listeners[event]...)
	w.mu.Unlock()
	for _, fn := range callbacks {
		func() {
			defer func() { _ = recover() }() // a panicking listener never kills the loop
			fn(args...)
		}()
	}
}
