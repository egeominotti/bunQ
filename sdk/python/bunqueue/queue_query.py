"""Query operations mixin for Queue (jobs, states, counts, logs, children)."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence, Union

from .connection import _compact
from .errors import CommandError, CommandTimeoutError
from .job import Job


def _values_payload(response: Dict[str, Any]) -> Dict[str, Any]:
    """Unwrap {data: {values: {...}}} (server) or top-level values."""
    data = response.get("data")
    if isinstance(data, dict) and "values" in data:
        return dict(data.get("values") or {})
    return dict(response.get("values") or {})


class QueueQueryOps:
    """Mixin: requires ``self.name`` and ``self.connection``."""

    # ------------------------------------------------------------------ jobs

    def get_job(self, job_id: str) -> Optional[Job]:
        try:
            response = self.connection.call({"cmd": "GetJob", "id": job_id})
        except CommandError as exc:
            if "not found" in str(exc).lower():
                return None
            raise
        raw = response.get("job")
        return Job(raw, self.connection) if raw else None

    def get_job_by_custom_id(self, custom_id: str) -> Optional[Job]:
        try:
            response = self.connection.call({"cmd": "GetJobByCustomId", "customId": custom_id})
        except CommandError as exc:
            if "not found" in str(exc).lower():
                return None
            raise
        raw = response.get("job")
        return Job(raw, self.connection) if raw else None

    def get_jobs(
        self,
        state: Optional[Union[str, Sequence[str]]] = None,
        start: int = 0,
        end: int = 1000,
    ) -> List[Job]:
        """Mirror TS getJobsAsync: offset=start, limit=end-start."""
        command = _compact(
            {
                "cmd": "GetJobs",
                "queue": self.name,
                "state": list(state) if isinstance(state, (list, tuple)) else state,
                "offset": start,
                "limit": max(end - start, 0),
            }
        )
        response = self.connection.call(command)
        return [Job(raw, self.connection) for raw in response.get("jobs", [])]

    def get_waiting(self, start: int = 0, end: int = 1000) -> List[Job]:
        return self.get_jobs("waiting", start, end)

    def get_delayed(self, start: int = 0, end: int = 1000) -> List[Job]:
        return self.get_jobs("delayed", start, end)

    def get_active(self, start: int = 0, end: int = 1000) -> List[Job]:
        return self.get_jobs("active", start, end)

    def get_completed(self, start: int = 0, end: int = 1000) -> List[Job]:
        return self.get_jobs("completed", start, end)

    def get_failed(self, start: int = 0, end: int = 1000) -> List[Job]:
        return self.get_jobs("failed", start, end)

    def get_prioritized(self, start: int = 0, end: int = 1000) -> List[Job]:
        return self.get_jobs("prioritized", start, end)

    def get_waiting_children(self, start: int = 0, end: int = 1000) -> List[Job]:
        return self.get_jobs("waiting-children", start, end)

    # ----------------------------------------------------------------- state

    def get_state(self, job_id: str) -> str:
        response = self.connection.call({"cmd": "GetState", "id": job_id})
        return str(response.get("state"))

    def get_result(self, job_id: str) -> Any:
        response = self.connection.call({"cmd": "GetResult", "id": job_id})
        return response.get("result")

    def get_progress(self, job_id: str) -> Dict[str, Any]:
        response = self.connection.call({"cmd": "GetProgress", "id": job_id})
        return {"progress": response.get("progress"), "message": response.get("message")}

    def wait_for_job(self, job_id: str, timeout_ms: int = 30000) -> Any:
        """Block until the job completes; returns its result.

        The server's ``WaitJob`` waiter resolves only on completion, answering
        ``{ok:true, completed:false}`` (no result) otherwise — so returning
        ``None`` would be indistinguishable from a genuine ``None`` result. On
        non-completion we probe the job state: a ``failed`` job raises
        :class:`CommandError` (it will not complete), everything else raises
        :class:`CommandTimeoutError`."""
        response = self.connection.call(
            {"cmd": "WaitJob", "id": job_id, "timeout": timeout_ms},
            timeout=timeout_ms / 1000 + 5,
        )
        if not response.get("completed"):
            try:
                state = self.get_state(job_id)
            except CommandError:
                state = None
            if state == "failed":
                raise CommandError(f"job {job_id} failed before completion")
            raise CommandTimeoutError(f"waitUntilFinished timed out after {timeout_ms}ms")
        return response.get("result")

    def wait_job_until_finished(
        self, job_id: str, queue_events: Any = None, ttl_ms: Optional[int] = None
    ) -> Any:
        """BullMQ-parity alias for :meth:`wait_for_job` (`queue_events` unused over TCP)."""
        del queue_events
        return self.wait_for_job(job_id, ttl_ms if ttl_ms is not None else 30000)

    # -------------------------------------------------------------- children

    def get_children_values(self, job_id: str) -> Dict[str, Any]:
        response = self.connection.call({"cmd": "GetChildrenValues", "id": job_id})
        return _values_payload(response)

    def get_failed_children_values(self, job_id: str) -> Dict[str, Any]:
        response = self.connection.call({"cmd": "GetFailedChildrenValues", "id": job_id})
        return _values_payload(response)

    def get_ignored_children_failures(self, job_id: str) -> Dict[str, Any]:
        response = self.connection.call({"cmd": "GetIgnoredChildrenFailures", "id": job_id})
        return _values_payload(response)

    def remove_child_dependency(self, job_id: str) -> None:
        self.connection.call({"cmd": "RemoveChildDependency", "id": job_id})

    def remove_unprocessed_children(self, job_id: str) -> None:
        self.connection.call({"cmd": "RemoveUnprocessedChildren", "id": job_id})

    def get_deduplication_job_id(self, deduplication_id: str) -> Optional[str]:
        """Id of the job currently holding this deduplication key (customId)."""
        job = self.get_job_by_custom_id(deduplication_id)
        return job.id if job else None

    # ---------------------------------------------------------------- counts

    def get_job_counts(self) -> Dict[str, int]:
        response = self.connection.call({"cmd": "GetJobCounts", "queue": self.name})
        return dict(response.get("counts", {}))

    def get_waiting_count(self) -> int:
        return self.get_job_counts().get("waiting", 0)

    def get_active_count(self) -> int:
        return self.get_job_counts().get("active", 0)

    def get_completed_count(self) -> int:
        return self.get_job_counts().get("completed", 0)

    def get_failed_count(self) -> int:
        return self.get_job_counts().get("failed", 0)

    def get_delayed_count(self) -> int:
        return self.get_job_counts().get("delayed", 0)

    def get_prioritized_count(self) -> int:
        return self.get_job_counts().get("prioritized", 0)

    def get_waiting_children_count(self) -> int:
        return self.get_job_counts().get("waiting-children", 0)

    def count(self) -> int:
        response = self.connection.call({"cmd": "Count", "queue": self.name})
        return int(response.get("count", 0))

    def get_counts_per_priority(self) -> Dict[str, int]:
        response = self.connection.call({"cmd": "GetCountsPerPriority", "queue": self.name})
        return dict(response.get("counts") or response.get("data") or {})

    # ------------------------------------------------------------------ logs

    def add_job_log(self, job_id: str, message: str, level: Optional[str] = None) -> None:
        self.connection.call(_compact({"cmd": "AddLog", "id": job_id, "message": message, "level": level}))

    def get_job_logs(self, job_id: str, start: Optional[int] = None, end: Optional[int] = None) -> List[Any]:
        response = self.connection.call(_compact({"cmd": "GetLogs", "id": job_id, "start": start, "end": end}))
        data = response.get("data")
        if isinstance(data, dict):
            return list(data.get("logs") or [])
        return list(response.get("logs") or [])

    def clear_job_logs(self, job_id: str, keep_logs: Optional[int] = None) -> None:
        self.connection.call(_compact({"cmd": "ClearLogs", "id": job_id, "keepLogs": keep_logs}))
