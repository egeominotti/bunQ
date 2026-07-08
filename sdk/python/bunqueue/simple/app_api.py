"""Public API surface of Bunqueue Simple Mode (mixin): middleware, queue
operations, cron shorthands, subsystem accessors, events and control.

Split from :mod:`app` to keep files small; semantics mirror bunqueue.ts.
"""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional

from ..job import Job
from .cancellation import CancelSignal


class BunqueueApi:
    """Mixin: requires the attributes initialized in ``Bunqueue.__init__``."""

    # ---------------------------------------------------------- middleware

    def use(self, middleware: Callable[[Job, Callable[[], Any]], Any]) -> "Bunqueue":
        self._middlewares.append(middleware)
        return self

    # ---------------------------------------------------------- queue ops

    def add(self, name: str, data: Any = None, **opts: Any) -> Job:
        merged = self._merger.merge(name, {**self._default_job_options, **opts}, data)
        return self.queue.add(name, data, **merged)

    def add_bulk(self, jobs: List[Dict[str, Any]]) -> List[str]:
        entries = []
        for entry in jobs:
            entry = dict(entry)
            opts = {k: v for k, v in entry.items() if k not in ("name", "data")}
            merged = self._merger.merge(
                entry["name"], {**self._default_job_options, **opts}, entry.get("data")
            )
            entries.append({"name": entry["name"], "data": entry.get("data"), **merged})
        return self.queue.add_bulk(entries)

    def get_job(self, job_id: str) -> Optional[Job]:
        return self.queue.get_job(job_id)

    def get_job_counts(self) -> Dict[str, int]:
        return self.queue.get_job_counts()

    def count(self) -> int:
        return self.queue.count()

    # ---------------------------------------------------------- cron

    def cron(self, scheduler_id: str, pattern: str, data: Any = None, **opts: Any) -> None:
        self.queue.upsert_job_scheduler(
            scheduler_id,
            {"pattern": pattern, "tz": opts.get("timezone")},
            {"name": scheduler_id, "data": data, "opts": opts.get("job_opts")},
        )

    def every(self, scheduler_id: str, interval_ms: int, data: Any = None, **opts: Any) -> None:
        self.queue.upsert_job_scheduler(
            scheduler_id,
            {"every": interval_ms},
            {"name": scheduler_id, "data": data, "opts": opts.get("job_opts")},
        )

    def remove_cron(self, scheduler_id: str) -> None:
        self.queue.remove_job_scheduler(scheduler_id)

    def list_crons(self) -> List[Dict[str, Any]]:
        return self.queue.get_job_schedulers()

    # ---------------------------------------------------------- subsystems

    def cancel(self, job_id: str, grace_period_ms: float = 0) -> None:
        self._cancellation.cancel(job_id, grace_period_ms)

    def is_cancelled(self, job_id: str) -> bool:
        return self._cancellation.is_cancelled(job_id)

    def get_signal(self, job_id: str) -> Optional[CancelSignal]:
        return self._cancellation.get_signal(job_id)

    def get_circuit_state(self) -> str:
        return self._cb.current_state if self._cb else "closed"

    def reset_circuit(self) -> None:
        if self._cb:
            self._cb.reset()

    def trigger(self, rule: Dict[str, Any]) -> "Bunqueue":
        self._trigger_mgr.add(rule)
        return self

    def set_default_ttl(self, ttl_ms: float) -> None:
        if self._ttl_checker:
            self._ttl_checker.set_default_ttl(ttl_ms)

    def set_name_ttl(self, name: str, ttl_ms: float) -> None:
        if self._ttl_checker:
            self._ttl_checker.set_name_ttl(name, ttl_ms)

    # ---------------------------------------------------------- events/control

    def on(self, event: str, listener: Callable[..., None]) -> "Bunqueue":
        self.worker.on(event, listener)
        return self

    def once(self, event: str, listener: Callable[..., None]) -> "Bunqueue":
        self.worker.once(event, listener)
        return self

    def off(self, event: str, listener: Callable[..., None]) -> "Bunqueue":
        self.worker.off(event, listener)
        return self

    def pause(self) -> None:
        self.queue.pause()
        self.worker.pause()

    def resume(self) -> None:
        self.queue.resume()
        self.worker.resume()

    def close(self, force: bool = False) -> None:
        if self._ager:
            self._ager.destroy()
        if self._cb:
            self._cb.destroy()
        if self._batch_acc:
            self._batch_acc.destroy()
        self._cancellation.destroy_all()
        self.worker.close(timeout=0 if force else None)
        self.queue.close()

    def is_running(self) -> bool:
        return self.worker.is_running()

    def is_paused(self) -> bool:
        return self.worker.is_paused()

    def is_closed(self) -> bool:
        return self.worker.is_closed()
