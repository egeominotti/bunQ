"""Worker runtime mixin: poll loop body, job execution, heartbeats, registry.

Split from :mod:`worker` to keep files small; ``Worker`` composes this mixin
with the lifecycle surface (start/run/pause/close).
"""

from __future__ import annotations

import os
import socket as _socket
import time
import traceback
from typing import Any, Dict

from .errors import BunqueueError, UnrecoverableError
from .job import Job
from .wire import _compact

MAX_POLL_TIMEOUT_MS = 30000
MAX_STACK_LINES = 20
RECONNECT_BACKOFF_S = (0.5, 1.0, 2.0, 5.0)


class WorkerRuntime:
    """Mixin: requires the attributes initialized in ``Worker.__init__``."""

    def _poll_once(self) -> None:
        with self._active_lock:
            free = self.concurrency - len(self._active)
        if free <= 0:
            self._stop.wait(0.05)
            return

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
            self._failed += 1
            stack = traceback.format_exc().splitlines()[-MAX_STACK_LINES:]
            self._safe_call(
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
            self.emit("failed", job, exc)
        else:
            self._processed += 1
            ack: Dict[str, Any] = {"cmd": "ACK", "id": job.id, "token": token}
            if result is not None:
                ack["result"] = result
            self._safe_call(ack)
            self.emit("completed", job, result)
        finally:
            with self._active_lock:
                self._active.pop(job.id, None)
                self._cancelled.pop(job.id, None)

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
        self._safe_call(
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

    def _safe_call(self, command: Dict[str, Any]) -> None:
        try:
            self.connection.call(command)
        except BunqueueError as exc:
            self.emit("error", exc)
