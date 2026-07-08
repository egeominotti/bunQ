"""Client-side rate limiter for Simple Mode (mirror of rate-gate.ts).

Throttles job starts to ``max`` per ``duration`` ms window, optionally per
group (``group_key`` names a field of job.data). A job whose window is full
waits, holding its concurrency slot, until the window frees — the same
observable semantics as the official client's worker limiter.
"""

from __future__ import annotations

import threading
import time
from typing import Any, Dict, List


class RateGate:
    def __init__(self, options: Dict[str, Any]) -> None:
        self._max = int(options["max"])
        self._duration = float(options.get("duration") or 1000)
        self._group_key = options.get("group_key") or options.get("groupKey")
        self._windows: Dict[str, List[float]] = {}
        self._lock = threading.Lock()

    def group_for(self, data: Any) -> str:
        """Resolve the rate-limit group for a job's data."""
        if not self._group_key:
            return ""
        if isinstance(data, dict):
            value = data.get(self._group_key)
            if value is not None:
                return str(value)
        return ""

    def acquire(self, group: str) -> None:
        """Block until the group's window has room, then record the start."""
        while True:
            now = time.monotonic() * 1000
            with self._lock:
                self._prune_locked(now)
                window = [t for t in self._windows.get(group, []) if now - t < self._duration]
                if len(window) < self._max:
                    window.append(now)
                    self._windows[group] = window
                    return
                self._windows[group] = window
                oldest = window[0]
            time.sleep(max((oldest + self._duration - now) / 1000.0, 0.01))

    def _prune_locked(self, now: float) -> None:
        """Evict fully-expired groups (high-cardinality group_key safety)."""
        stale = [
            group
            for group, window in self._windows.items()
            if not window or now - window[-1] >= self._duration
        ]
        for group in stale:
            del self._windows[group]
