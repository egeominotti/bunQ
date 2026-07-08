"""Circuit breaker for worker protection (mirror of circuitBreaker.ts).

CLOSED -> failures >= threshold -> OPEN (worker paused)
                                     |
        <- success --- HALF-OPEN <- reset timeout expires
"""

from __future__ import annotations

import threading
from typing import Any, Dict, Optional

from ..worker import Worker


class WorkerCircuitBreaker:
    def __init__(self, config: Dict[str, Any], worker: Worker) -> None:
        self._config = config
        self._worker = worker
        self._state = "closed"
        self._failures = 0
        self._timer: Optional[threading.Timer] = None
        self._lock = threading.Lock()

    @property
    def current_state(self) -> str:
        return self._state

    def is_open(self) -> bool:
        return self._state == "open"

    def on_success(self) -> None:
        with self._lock:
            if self._state == "half-open":
                self._state = "closed"
                self._failures = 0
                callback = self._config.get("on_close") or self._config.get("onClose")
            elif self._state == "closed":
                self._failures = 0
                callback = None
            else:
                callback = None
        if callback:
            callback()

    def on_failure(self) -> None:
        with self._lock:
            self._failures += 1
            threshold = int(self._config.get("threshold") or 5)
            should_open = self._state == "half-open" or self._failures >= threshold
        if should_open:
            self._open()

    def _open(self) -> None:
        with self._lock:
            self._state = "open"
            failures = self._failures
            if self._timer:
                self._timer.cancel()
            reset_timeout = float(
                self._config.get("reset_timeout") or self._config.get("resetTimeout") or 30000
            )
            self._timer = threading.Timer(reset_timeout / 1000.0, self._half_open)
            self._timer.daemon = True
            self._timer.start()
        on_open = self._config.get("on_open") or self._config.get("onOpen")
        if on_open:
            on_open(failures)
        self._worker.pause()

    def _half_open(self) -> None:
        with self._lock:
            self._state = "half-open"
        on_half_open = self._config.get("on_half_open") or self._config.get("onHalfOpen")
        if on_half_open:
            on_half_open()
        self._worker.resume()

    def reset(self) -> None:
        with self._lock:
            self._state = "closed"
            self._failures = 0
            if self._timer:
                self._timer.cancel()
                self._timer = None
        if self._worker.is_paused():
            self._worker.resume()

    def destroy(self) -> None:
        with self._lock:
            if self._timer:
                self._timer.cancel()
                self._timer = None
