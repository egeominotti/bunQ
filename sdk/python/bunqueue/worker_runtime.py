"""Worker runtime mixin: poll loop body, job execution, heartbeats, registry.

Split from :mod:`worker` to keep files small; ``Worker`` composes this mixin
with the lifecycle surface (start/run/pause/close).
"""

from __future__ import annotations

import logging
import os
import socket as _socket
import time
import traceback
from typing import Any, Dict, Optional

from .ack_batcher import AckItem
from .ack_outcome import transition_was_applied
from .errors import BunqueueError, UnrecoverableError
from .job import Job
from .wire import _compact

logger = logging.getLogger("bunqueue")

MAX_POLL_TIMEOUT_MS = 30000
# The server persists the FIRST `stackTraceLimit` lines (default 10) of the
# stack we send. A Python traceback ends with the raise site, so we send the
# LAST lines — but no more than the server keeps, or the raise site would be
# truncated away by the server-side first-N cap.
MAX_STACK_LINES = 10
RECONNECT_BACKOFF_S = (0.5, 1.0, 2.0, 5.0)


class WorkerRuntime:
    """Mixin: requires the attributes initialized in ``Worker.__init__``."""

    def _poll_once(self) -> None:
        with self._active_lock:
            free = self.concurrency - len(self._active)
        if free <= 0:
            self._stop.wait(0.05)
            return

        # The registration is per-connection server state: after a reconnect
        # the server no longer knows this worker (ListWorkers, skipIfNoWorker
        # crons). Detect the new connection generation and re-register.
        if (
            self.connection.connected
            and self.connection.generation != self._registered_generation
        ):
            self._register()

        response = self.connection.call(
            {
                "cmd": "PULLB",
                "queue": self.queue,
                "count": min(free, self.batch_size),
                "timeout": self.poll_timeout_ms,
                "owner": self.worker_id,
                "lockTtl": self.lock_ttl_ms,
            },
            timeout=self.poll_timeout_ms / 1000 + 10,
        )
        jobs = response.get("jobs") or []
        tokens = response.get("tokens") or []
        if not jobs:
            with self._active_lock:
                idle = not self._active
            if self._was_busy and idle:
                self._was_busy = False
                self.emit("drained")
            return

        self._was_busy = True
        assert self._executor is not None
        for raw, tok in zip(jobs, tokens):
            job_id = str(raw.get("id"))
            with self._active_lock:
                self._active[job_id] = tok
            self._executor.submit(self._run_job, raw, tok)

    def _run_job(self, raw: Dict[str, Any], token: str) -> None:
        job = Job(raw, self.connection, token, on_progress=lambda j, p: self.emit("progress", j, p))
        self.emit("active", job)
        try:
            result = self.processor(job)
        except BaseException as exc:  # noqa: BLE001 - fail the job, never the worker
            # Send at most as many trailing lines as the server will keep
            # (its cap keeps the FIRST N): honor a per-job stackTraceLimit.
            cap = raw.get("stackTraceLimit")
            cap = cap if isinstance(cap, int) and cap > 0 else MAX_STACK_LINES
            stack = traceback.format_exc().splitlines()[-cap:]
            failure_applied = self._safe_transition(
                _compact(
                    {
                        "cmd": "FAIL",
                        "id": job.id,
                        "error": str(exc) or exc.__class__.__name__,
                        "token": token,
                        "stack": stack,
                        "unrecoverable": True if isinstance(exc, UnrecoverableError) else None,
                    }
                )
            )
            self._finish_job(job.id)
            # Count only a broker-applied failure. A transport/protocol failure
            # emits only 'error'; an already-finalized generation emits neither.
            if failure_applied is True:
                self._failed += 1
                self.emit("failed", job, exc)
            return
        if self._ack_batcher is not None:
            # Defer the ACK into a batch; the job stays active (lock renewed by
            # the heartbeat loop) until the ACKB settles. _on_ack_settled frees
            # the slot FIRST so a raising listener can never leak it and
            # permanently shrink the worker's effective concurrency.
            self._ack_batcher.add(
                AckItem(
                    id=job.id,
                    token=token,
                    result=result,
                    on_settled=lambda err, applied, job=job, result=result: self._on_ack_settled(
                        job, result, err, applied
                    ),
                )
            )
            return
        ack: Dict[str, Any] = {"cmd": "ACK", "id": job.id, "token": token}
        if result is not None:
            ack["result"] = result
        acked = self._safe_transition(ack)
        # Free the slot BEFORE emitting (same rationale as the batched path).
        self._finish_job(job.id)
        # Count only a broker-applied ACK. Transport/protocol failure emits an
        # error; an exact already-finalized generation emits neither outcome.
        if acked is True:
            self._processed += 1
            self.emit("completed", job, result)

    def _on_ack_settled(
        self, job: Job, result: Any, err: Optional[BaseException], applied: bool
    ) -> None:
        """Settle callback for a batched ACK: free the slot, then report.

        A failed ACKB emits ``error``; a position the broker reports as
        already finalized emits neither ``completed`` nor ``error``."""
        self._finish_job(job.id)
        if err is not None:
            self.emit("error", err)
        elif applied:
            self._processed += 1
            self.emit("completed", job, result)

    def _finish_job(self, job_id: str) -> None:
        with self._active_lock:
            self._active.pop(job_id, None)
            self._cancelled.pop(job_id, None)

    def _heartbeat_loop(self) -> None:
        while not self._stop.wait(self.heartbeat_interval_s):
            with self._active_lock:
                ids = list(self._active.keys())
                tokens = [self._active[i] for i in ids]
                active_count = len(ids)
            self._safe_call(
                {
                    "cmd": "Heartbeat",
                    "id": self.worker_id,
                    "activeJobs": active_count,
                    "processed": self._processed,
                    "failed": self._failed,
                }
            )
            if ids:
                self._safe_call({"cmd": "JobHeartbeatB", "ids": ids, "tokens": tokens})

    def _register(self) -> None:
        """Register the worker; mark the generation ONLY on success.

        A failed RegisterWorker must leave ``_registered_generation`` stale so
        the next poll iteration retries: marking it unconditionally would leave
        the server unaware of this worker (ListWorkers, skipIfNoWorker crons,
        Discussion #103 class) until the next reconnect.

        The generation is snapshotted BEFORE the call: if the call itself
        triggers a reconnect, the stale snapshot forces one harmless duplicate
        registration (server-side upsert) instead of ever missing one.
        """
        generation = self.connection.generation
        try:
            self.connection.call(
                {
                    "cmd": "RegisterWorker",
                    "name": self.name,
                    "queues": [self.queue],
                    "concurrency": self.concurrency,
                    "workerId": self.worker_id,
                    "hostname": _socket.gethostname(),
                    "pid": os.getpid(),
                    "startedAt": int(time.time() * 1000),
                }
            )
        except BunqueueError as exc:
            logger.warning("RegisterWorker failed (will retry next poll): %s", exc)
            self.emit("error", exc)
            return
        self._registered_generation = generation

    def _safe_call(self, command: Dict[str, Any]) -> bool:
        """Fire a command, swallowing wire failures into an 'error' event.

        Returns True only when the command reached the server, so callers can
        gate success-side effects (counters, 'completed'/'failed' emits) on
        the wire outcome, mirroring the TypeScript worker's safeCall."""
        try:
            self.connection.call(command)
            return True
        except BunqueueError as exc:
            logger.warning("swallowed %s failure: %s", command.get("cmd"), exc)
            self.emit("error", exc)
            return False

    def _safe_transition(self, command: Dict[str, Any]) -> Optional[bool]:
        """Send ACK/FAIL and distinguish applied, ignored, and error outcomes."""
        try:
            response = self.connection.call(command)
            return transition_was_applied(response)
        except BunqueueError as exc:
            logger.warning("swallowed %s failure: %s", command.get("cmd"), exc)
            self.emit("error", exc)
            return None
