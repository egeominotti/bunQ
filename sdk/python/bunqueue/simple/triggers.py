"""Event triggers: when a job completes/fails, create another job (mirror of
triggers.ts)."""

from __future__ import annotations

from typing import Any, Dict, List

from ..queue import Queue
from ..worker import Worker


class TriggerManager:
    def __init__(self, queue: Queue, worker: Worker) -> None:
        self._queue = queue
        self._worker = worker
        self._rules: List[Dict[str, Any]] = []
        self._active = False

    def add(self, rule: Dict[str, Any]) -> None:
        """Rule keys: on, create, data(result, job), event ('completed'
        default | 'failed'), condition(result, job), opts (job options)."""
        self._rules.append(rule)
        self._ensure_active()

    def _ensure_active(self) -> None:
        if self._active:
            return
        self._active = True
        self._worker.on("completed", lambda job, result: self._fire("completed", job, result))
        self._worker.on("failed", lambda job, error: self._fire("failed", job, error))

    def _fire(self, event: str, job: Any, result_or_error: Any) -> None:
        for rule in self._rules:
            if rule["on"] != job.name:
                continue
            if rule.get("event", "completed") != event:
                continue
            condition = rule.get("condition")
            if condition and not condition(result_or_error, job):
                continue
            data = rule["data"](result_or_error, job)
            try:
                self._queue.add(rule["create"], data, **(rule.get("opts") or {}))
            except Exception:  # noqa: BLE001 - trigger add is fire-and-forget
                pass
