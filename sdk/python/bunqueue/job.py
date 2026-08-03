"""Job wrapper: read view over the server job record + per-job operations.

Parity with the TS client's Job object (TCP mode): progress, logs, lock
management, state transitions, children values.
"""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional

from .connection import Connection, _compact


class Job:
    """A job as seen by producers (from queries) or workers (with lock token)."""

    def __init__(
        self,
        raw: Dict[str, Any],
        connection: Optional[Connection] = None,
        token: Optional[str] = None,
        on_progress: Optional[Callable[["Job", Any], None]] = None,
    ) -> None:
        self.raw = raw
        self.token = token
        self._conn = connection
        self._on_progress = on_progress

    # ------------------------------------------------------------ properties

    @property
    def id(self) -> str:
        return str(self.raw.get("id"))

    @property
    def queue(self) -> str:
        return str(self.raw.get("queue"))

    @property
    def data(self) -> Any:
        data = self.raw.get("data")
        if self._top_level_name() is not None:
            return data
        if isinstance(data, dict) and isinstance(data.get("name"), str):
            user_data = dict(data)
            user_data.pop("name", None)
            return user_data
        return data

    @property
    def name(self) -> Optional[str]:
        """Job name, with compatibility for pre-name-field data envelopes."""
        modern = self._top_level_name()
        if modern is not None:
            return modern
        data = self.raw.get("data")
        if isinstance(data, dict) and isinstance(data.get("name"), str):
            return data["name"]
        return None

    def _top_level_name(self) -> Optional[str]:
        value = self.raw.get("name")
        return str(value) if value is not None else None

    @property
    def priority(self) -> int:
        return int(self.raw.get("priority") or 0)

    @property
    def attempts(self) -> int:
        return int(self.raw.get("attempts") or 0)

    @property
    def max_attempts(self) -> int:
        return int(self.raw.get("maxAttempts") or 0)

    @property
    def created_at(self) -> Optional[int]:
        value = self.raw.get("createdAt")
        return int(value) if value is not None else None

    @property
    def started_at(self) -> Optional[int]:
        value = self.raw.get("startedAt")
        return int(value) if value is not None else None

    @property
    def completed_at(self) -> Optional[int]:
        value = self.raw.get("completedAt")
        return int(value) if value is not None else None

    @property
    def custom_id(self) -> Optional[str]:
        value = self.raw.get("customId")
        return str(value) if value is not None else None

    @property
    def tags(self) -> List[str]:
        return list(self.raw.get("tags") or [])

    @property
    def group_id(self) -> Optional[str]:
        value = self.raw.get("groupId")
        return str(value) if value is not None else None

    @property
    def parent_id(self) -> Optional[str]:
        value = self.raw.get("parentId")
        return str(value) if value is not None else None

    @property
    def children_ids(self) -> List[str]:
        return [str(c) for c in (self.raw.get("childrenIds") or [])]

    @property
    def depends_on(self) -> List[str]:
        return [str(c) for c in (self.raw.get("dependsOn") or [])]

    @property
    def progress(self) -> int:
        return int(self.raw.get("progress") or 0)

    @property
    def stacktrace(self) -> Optional[List[str]]:
        value = self.raw.get("stacktrace")
        return list(value) if value else None

    @property
    def state(self) -> Optional[str]:
        value = self.raw.get("state")
        return str(value) if value is not None else None

    # ------------------------------------------------------------ operations

    def _call(self, command: Dict[str, Any]) -> Dict[str, Any]:
        if self._conn is None:
            raise RuntimeError("Job is not bound to a connection")
        return self._conn.call(_compact(command))

    def update_progress(self, progress: int, message: Optional[str] = None) -> None:
        """Report processing progress (0-100) to the server."""
        self._call({"cmd": "Progress", "id": self.id, "progress": progress, "message": message})
        if self._on_progress is not None:
            self._on_progress(self, progress)

    def log(self, message: str, level: Optional[str] = None) -> None:
        """Append a log row to the job (AddLog)."""
        self._call({"cmd": "AddLog", "id": self.id, "message": message, "level": level})

    def heartbeat(self) -> None:
        """Renew this job's lock (stall detection)."""
        self._call({"cmd": "JobHeartbeat", "id": self.id, "token": self.token})

    def extend_lock(self, duration_ms: int) -> None:
        self._call({"cmd": "ExtendLock", "id": self.id, "token": self.token, "duration": duration_ms})

    def get_state(self) -> str:
        return str(self._call({"cmd": "GetState", "id": self.id}).get("state"))

    def get_children_values(self) -> Dict[str, Any]:
        response = self._call({"cmd": "GetChildrenValues", "id": self.id})
        data = response.get("data")
        if isinstance(data, dict) and "values" in data:
            return dict(data.get("values") or {})
        return dict(response.get("values") or {})

    def remove(self) -> None:
        self._call({"cmd": "Cancel", "id": self.id})

    def discard(self) -> None:
        self._call({"cmd": "Discard", "id": self.id, "token": self.token})

    def promote(self) -> None:
        self._call({"cmd": "Promote", "id": self.id})

    def retry(self) -> None:
        """Move the job to waiting, forwarding a Worker lease when present."""
        self._call({"cmd": "MoveToWait", "id": self.id, "token": self.token})

    def update_data(self, data: Any) -> None:
        self._call({"cmd": "Update", "id": self.id, "data": data})

    def change_priority(self, priority: int, lifo: Optional[bool] = None) -> None:
        self._call({"cmd": "ChangePriority", "id": self.id, "priority": priority, "lifo": lifo})

    def change_delay(self, delay_ms: int) -> None:
        self._call(
            {"cmd": "ChangeDelay", "id": self.id, "delay": delay_ms, "token": self.token}
        )

    def move_to_delayed(self, delay_ms: int) -> None:
        self._call(
            {"cmd": "MoveToDelayed", "id": self.id, "delay": delay_ms, "token": self.token}
        )

    def __repr__(self) -> str:  # pragma: no cover
        return f"Job(id={self.id!r}, queue={self.queue!r}, name={self.name!r})"
