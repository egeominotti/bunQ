"""Worker: pulls jobs over TCP and runs a processor with bounded concurrency.

Feature parity with the TS client's Worker (TCP mode): events (ready, active,
completed, failed, progress, drained, error, closed), pause/resume, graceful
close, lock heartbeats, UnrecoverableError. All queue semantics (retry,
backoff, DLQ, stall detection, priorities) live in the bunqueue server.

The loop/heartbeat internals live in :mod:`worker_runtime`.
"""

from __future__ import annotations

import logging
import os
import socket as _socket
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Dict, Optional

from .ack_batcher import AckBatcher
from .connection import Connection, TlsOption
from .errors import CommandTimeoutError, ConnectionClosedError
from .events import EventEmitter
from .job import Job
from .worker_runtime import MAX_POLL_TIMEOUT_MS, RECONNECT_BACKOFF_S, WorkerRuntime

logger = logging.getLogger("bunqueue")

Processor = Callable[[Job], Any]


class Worker(EventEmitter, WorkerRuntime):
    """Consumes jobs from one queue and processes them.

    Mirrors the TS Worker: ``autorun=True`` starts a background loop at
    construction; use ``run()`` for a blocking loop instead.
    """

    def __init__(
        self,
        queue: str,
        processor: Processor,
        *,
        host: str = "localhost",
        port: int = 6789,
        token: Optional[str] = None,
        tls: TlsOption = None,
        concurrency: int = 4,
        batch_size: int = 10,
        poll_timeout_ms: int = 5000,
        lock_ttl_ms: int = 30000,
        heartbeat_interval_s: float = 10.0,
        autorun: bool = True,
        name: Optional[str] = None,
        ack_batch: Optional[Dict[str, Any]] = None,
    ) -> None:
        super().__init__()
        if concurrency < 1:
            raise ValueError("concurrency must be >= 1")
        self.queue = queue
        self.processor = processor
        self.concurrency = concurrency
        # The server rejects PULLB count > 1000 — an unclamped batch_size
        # would wedge the poll loop in a permanent error cycle.
        self.batch_size = max(1, min(batch_size, 1000))
        self.poll_timeout_ms = min(poll_timeout_ms, MAX_POLL_TIMEOUT_MS)
        self.lock_ttl_ms = lock_ttl_ms
        self.heartbeat_interval_s = heartbeat_interval_s
        self.worker_id = f"py-{_socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:8]}"
        self.name = name or self.worker_id

        self.connection = Connection(host=host, port=port, token=token, tls=tls)

        # Opt-in ACKB batching, ack_batch={"max_size": 50, "max_delay_ms": 5}
        # (TS parity); the dict is the opt-in, {"enabled": False} disables.
        self._ack_batcher: Optional[AckBatcher] = None
        if ack_batch and ack_batch.get("enabled", True):
            self._ack_batcher = AckBatcher(
                self.connection,
                max_size=int(ack_batch.get("max_size", 50)),
                max_delay_ms=float(ack_batch.get("max_delay_ms", 5)),
            )

        self._active: Dict[str, str] = {}  # job id -> lock token
        self._cancelled: Dict[str, Optional[str]] = {}  # job id -> reason
        self._active_lock = threading.Lock()
        self._stop = threading.Event()
        self._paused = threading.Event()
        self._ready = threading.Event()
        self._closed = False
        self._loop_started = False  # run() actually entered its loop
        self._was_busy = False  # for 'drained' edge detection
        self._executor: Optional[ThreadPoolExecutor] = None
        self._run_thread: Optional[threading.Thread] = None
        self._processed = 0
        self._failed = 0
        self._registered_generation = -1

        if autorun:
            self.start()

    def on(self, event: str, listener) -> "Worker":  # type: ignore[override]
        """Like EventEmitter.on, but replays 'ready' to late listeners.

        With autorun the loop starts inside the constructor, so a listener
        attached right after construction could otherwise miss the event.
        """
        if event == "ready" and self._ready.is_set():
            try:
                listener()
            except Exception:  # noqa: BLE001 - listener errors must not propagate
                logger.warning("replayed 'ready' listener raised", exc_info=True)
        super().on(event, listener)
        return self

    # -------------------------------------------------------------- lifecycle

    def start(self) -> None:
        """Run the worker in a background thread."""
        if self._closed or self._run_thread is not None:
            return
        self._run_thread = threading.Thread(target=self.run, daemon=True)
        self._run_thread.start()

    def run(self) -> None:
        """Blocking main loop; returns after :meth:`close`.

        If the loop is already running (autorun / start()), this joins it
        instead of starting a second loop, so blocking semantics hold.
        """
        if self._run_thread is not None and threading.current_thread() is not self._run_thread:
            self._run_thread.join()
            return
        self._loop_started = True
        self._executor = ThreadPoolExecutor(
            max_workers=self.concurrency, thread_name_prefix="bunqueue-job"
        )
        self._register()
        self._ready.set()
        self.emit("ready")
        # 0 (or negative) disables heartbeats — Event.wait(0) returns
        # immediately, so the loop would busy-spin flooding the server.
        if self.heartbeat_interval_s > 0:
            heartbeat = threading.Thread(target=self._heartbeat_loop, daemon=True)
            heartbeat.start()
        backoff_idx = 0
        try:
            while not self._stop.is_set():
                if self._paused.is_set():
                    self._stop.wait(0.05)
                    continue
                try:
                    self._poll_once()
                    backoff_idx = 0
                except (ConnectionClosedError, CommandTimeoutError) as exc:
                    self.emit("error", exc)
                    delay = RECONNECT_BACKOFF_S[min(backoff_idx, len(RECONNECT_BACKOFF_S) - 1)]
                    backoff_idx += 1
                    self._stop.wait(delay)
        finally:
            self._shutdown()

    def pause(self) -> None:
        self._paused.set()

    def resume(self) -> None:
        self._paused.clear()

    def is_running(self) -> bool:
        return self._run_thread is not None and not self._closed

    def is_paused(self) -> bool:
        return self._paused.is_set()

    def is_closed(self) -> bool:
        return self._closed

    def wait_until_ready(self, timeout: float = 10.0) -> None:
        if not self._ready.wait(timeout):
            raise TimeoutError("worker did not become ready in time")

    def close(self, timeout: Optional[float] = None) -> bool:
        """Graceful shutdown: stop pulling, wait for in-flight jobs.

        Returns True once fully shut down; False when ``timeout`` expires with
        the loop thread still draining (the thread reference is kept alive so
        state stays honest and a later ``close()`` can join it again).
        """
        self._stop.set()
        thread = self._run_thread
        if thread is not None:
            thread.join(timeout)
            if thread.is_alive():
                return False  # still draining; do NOT null the live thread
            self._run_thread = None
            return True
        if not self._loop_started and not self._closed:
            # autorun=False and run() never called: no loop will ever reach
            # _shutdown, so settle the closed state here.
            self._closed = True
            self.connection.close()
            self.emit("closed")
        return True

    # `stop` kept as an alias (pre-1.0 SDK name)
    stop = close

    def __enter__(self) -> "Worker":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    # ------------------------------------------------------ cooperative cancel

    def cancel_job(self, job_id: str, reason: Optional[str] = None) -> bool:
        """Mark a locally-active job as cancelled (cooperative, no TCP command).

        The processor must poll :meth:`is_job_cancelled` and abort itself.
        Returns True if the job is currently active on this worker.
        """
        with self._active_lock:
            if job_id not in self._active:
                return False
            self._cancelled[job_id] = reason
        self.emit("cancelled", {"jobId": job_id, "reason": reason or "cancelled"})
        return True

    def cancel_all_jobs(self, reason: Optional[str] = None) -> None:
        with self._active_lock:
            active = list(self._active.keys())
            for job_id in active:
                self._cancelled[job_id] = reason
        for job_id in active:
            self.emit("cancelled", {"jobId": job_id, "reason": reason or "cancelled"})

    def is_job_cancelled(self, job_id: str) -> bool:
        with self._active_lock:
            return job_id in self._cancelled

    def _shutdown(self) -> None:
        if self._executor is not None:
            self._executor.shutdown(wait=True)
            self._executor = None
        # Flush AFTER the executor drained: the last in-flight jobs buffer
        # their ACKs during shutdown(wait=True); send them before closing.
        if self._ack_batcher is not None:
            self._ack_batcher.flush()
        self._safe_call({"cmd": "UnregisterWorker", "workerId": self.worker_id})
        self.connection.close()
        already_closed = self._closed
        self._closed = True
        if not already_closed:
            self.emit("closed")
