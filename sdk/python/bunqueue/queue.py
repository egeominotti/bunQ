"""Queue: producer + control API over the bunqueue TCP protocol.

Feature parity with the TypeScript client's Queue (TCP mode). Query and
admin surfaces live in :mod:`queue_query` / :mod:`queue_admin` mixins.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence

from .connection import Connection, TlsOption, _compact
from .job import Job
from .options import job_options, job_payload
from .queue_admin import QueueAdminOps
from .queue_query import QueueQueryOps


class Queue(QueueQueryOps, QueueAdminOps):
    """Producer and management client for one named queue."""

    def __init__(
        self,
        name: str,
        host: str = "localhost",
        port: int = 6789,
        token: Optional[str] = None,
        tls: TlsOption = None,
        connection: Optional[Connection] = None,
        command_timeout: float = 10.0,
        prefix_key: str = "",
    ) -> None:
        self.name = prefix_key + name
        self.connection = connection or Connection(
            host=host, port=port, token=token, tls=tls, command_timeout=command_timeout
        )
        self._owns_connection = connection is None

    # --------------------------------------------------------------- produce

    def add(self, name: str, data: Any = None, **options: Any) -> Job:
        """Add a job; returns a Job stub carrying the assigned id."""
        payload = job_payload(name, data)
        opts = job_options(**options)
        response = self.connection.call({"cmd": "PUSH", "queue": self.name, "data": payload, **opts})
        return Job({"id": response.get("id"), "queue": self.name, "data": payload, **opts}, self.connection)

    def add_bulk(self, jobs: Sequence[Dict[str, Any]]) -> List[str]:
        """Add many jobs in one round-trip.

        Each entry: ``{"name": str, "data": Any, ...options}`` with the same
        option names accepted by :meth:`add`.
        """
        inputs = []
        for entry in jobs:
            entry = dict(entry)
            name = entry.pop("name")
            data = entry.pop("data", None)
            opts = job_options(**entry)
            # PUSHB entries are typed JobInput, whose custom-id field is
            # `customId` — unlike single PUSH which renames `jobId`->`customId`
            # server-side. Without this rename the batch custom id is dropped
            # (idempotency/getJobByCustomId broken).
            if "jobId" in opts:
                opts["customId"] = opts.pop("jobId")
            inputs.append({"data": job_payload(name, data), **opts})
        response = self.connection.call({"cmd": "PUSHB", "queue": self.name, "jobs": inputs})
        return [str(job_id) for job_id in response.get("ids", [])]

    # --------------------------------------------------------------- control

    def pause(self) -> None:
        self.connection.call({"cmd": "Pause", "queue": self.name})

    def resume(self) -> None:
        self.connection.call({"cmd": "Resume", "queue": self.name})

    def is_paused(self) -> bool:
        response = self.connection.call({"cmd": "IsPaused", "queue": self.name})
        return bool(response.get("paused"))

    def drain(self) -> None:
        self.connection.call({"cmd": "Drain", "queue": self.name})

    def obliterate(self) -> None:
        self.connection.call({"cmd": "Obliterate", "queue": self.name})

    def clean(self, grace_ms: int, limit: Optional[int] = None, state: Optional[str] = None) -> int:
        response = self.connection.call(
            _compact({"cmd": "Clean", "queue": self.name, "grace": grace_ms, "state": state, "limit": limit})
        )
        return int(response.get("count", 0))

    def remove(self, job_id: str) -> None:
        self.connection.call({"cmd": "Cancel", "id": job_id})

    # `cancel` kept as an alias of remove (bunqueue-native name)
    cancel = remove

    def discard(self, job_id: str) -> None:
        self.connection.call({"cmd": "Discard", "id": job_id})

    def promote_job(self, job_id: str) -> None:
        self.connection.call({"cmd": "Promote", "id": job_id})

    def promote_jobs(self, count: Optional[int] = None) -> int:
        response = self.connection.call(
            _compact({"cmd": "PromoteJobs", "queue": self.name, "count": count})
        )
        return int(response.get("count", 0))

    def retry_job(self, job_id: str) -> None:
        """BullMQ contract: failed -> waiting (MoveToWait over TCP)."""
        self.connection.call({"cmd": "MoveToWait", "id": job_id})

    def retry_jobs(self, state: str = "failed", count: Optional[int] = None) -> int:
        # `count` caps how many DLQ entries are retried (server >= 2.8.29);
        # older servers ignore it and retry the whole DLQ (forward-compatible).
        if state == "failed":
            response = self.connection.call(
                _compact({"cmd": "RetryDlq", "queue": self.name, "count": count})
            )
        elif state == "completed":
            response = self.connection.call({"cmd": "RetryCompleted", "queue": self.name})
        else:
            raise ValueError("state must be 'failed' or 'completed'")
        return int(response.get("count", 0))

    def retry_completed(self, job_id: Optional[str] = None) -> int:
        response = self.connection.call(
            _compact({"cmd": "RetryCompleted", "queue": self.name, "id": job_id})
        )
        return int(response.get("count", 0))

    # ----------------------------------------------------- job state changes

    def update_job_progress(self, job_id: str, progress: int, message: Optional[str] = None) -> None:
        self.connection.call(
            _compact({"cmd": "Progress", "id": job_id, "progress": progress, "message": message})
        )

    def update_job_data(self, job_id: str, data: Any) -> None:
        self.connection.call({"cmd": "Update", "id": job_id, "data": data})

    def change_job_priority(self, job_id: str, priority: int, lifo: Optional[bool] = None) -> None:
        self.connection.call(
            _compact({"cmd": "ChangePriority", "id": job_id, "priority": priority, "lifo": lifo})
        )

    def change_job_delay(self, job_id: str, delay_ms: int) -> None:
        self.connection.call({"cmd": "ChangeDelay", "id": job_id, "delay": delay_ms})

    def move_job_to_wait(self, job_id: str) -> None:
        self.connection.call({"cmd": "MoveToWait", "id": job_id})

    def move_job_to_delayed(self, job_id: str, delay_ms: int) -> None:
        self.connection.call({"cmd": "MoveToDelayed", "id": job_id, "delay": delay_ms})

    def extend_job_lock(self, job_id: str, token: str, duration_ms: int) -> None:
        self.connection.call(
            {"cmd": "ExtendLock", "id": job_id, "token": token, "duration": duration_ms}
        )

    def move_job_to_completed(self, job_id: str, result: Any = None, token: Optional[str] = None) -> None:
        self.connection.call(_compact({"cmd": "ACK", "id": job_id, "result": result, "token": token}))

    def move_job_to_failed(self, job_id: str, error: str, token: Optional[str] = None) -> None:
        self.connection.call(_compact({"cmd": "FAIL", "id": job_id, "error": error, "token": token}))

    # ------------------------------------------------------------- lifecycle

    def ping(self) -> bool:
        return self.connection.ping()

    def wait_until_ready(self) -> None:
        self.connection.connect()

    def close(self) -> None:
        if self._owns_connection:
            self.connection.close()

    def disconnect(self) -> None:
        """BullMQ-parity alias for :meth:`close`."""
        self.close()

    def __enter__(self) -> "Queue":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()
