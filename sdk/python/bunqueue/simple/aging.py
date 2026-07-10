"""Priority aging: boost old waiting jobs so they never starve (mirror of
aging.ts, adapted to the TCP query surface)."""

from __future__ import annotations

import logging
import threading
import time
from typing import Any, Dict, Optional

from ..queue import Queue

logger = logging.getLogger("bunqueue")


class PriorityAger:
    def __init__(self, config: Dict[str, Any], queue: Queue) -> None:
        self._config = config
        self._queue = queue
        self._timer: Optional[threading.Timer] = None
        self._stopped = False

    def start(self) -> None:
        self._schedule()

    def _schedule(self) -> None:
        if self._stopped:
            return
        interval = float(self._config.get("interval") or 60000)
        self._timer = threading.Timer(interval / 1000.0, self._tick)
        self._timer.daemon = True
        self._timer.start()

    def _tick(self) -> None:
        try:
            self._boost_old_jobs()
        except Exception:  # noqa: BLE001 - best-effort background task
            logger.warning("priority aging tick failed", exc_info=True)
        finally:
            self._schedule()

    def _boost_old_jobs(self) -> None:
        min_age = float(self._config.get("min_age") or self._config.get("minAge") or 60000)
        boost = int(self._config.get("boost") or 1)
        max_priority = int(
            self._config.get("max_priority") or self._config.get("maxPriority") or 100
        )
        max_scan = int(self._config.get("max_scan") or self._config.get("maxScan") or 100)

        jobs = self._queue.get_waiting(0, max_scan) + self._queue.get_jobs(
            "prioritized", 0, max_scan
        )
        now = time.time() * 1000
        for job in jobs:
            created = job.created_at or now
            age = now - created
            if age >= min_age and job.priority < max_priority:
                new_priority = min(job.priority + boost, max_priority)
                try:
                    self._queue.change_job_priority(job.id, new_priority)
                except Exception:  # noqa: BLE001 - job may have been processed
                    pass

    def destroy(self) -> None:
        self._stopped = True
        if self._timer:
            self._timer.cancel()
            self._timer = None
