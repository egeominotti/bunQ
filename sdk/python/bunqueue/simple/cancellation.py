"""Graceful job cancellation (mirror of cancellation.ts).

Python has no AbortController; :class:`CancelSignal` exposes the same
``aborted`` surface, checked cooperatively by processors and the middleware
chain.
"""

from __future__ import annotations

import threading
from typing import Dict, Optional


class CancelSignal:
    """Minimal AbortSignal equivalent: ``signal.aborted`` flips once."""

    def __init__(self) -> None:
        self._event = threading.Event()

    @property
    def aborted(self) -> bool:
        return self._event.is_set()

    def abort(self) -> None:
        self._event.set()


class CancellationManager:
    def __init__(self) -> None:
        self._signals: Dict[str, CancelSignal] = {}
        self._timers: Dict[str, threading.Timer] = {}
        self._lock = threading.Lock()

    def register(self, job_id: str) -> CancelSignal:
        signal = CancelSignal()
        with self._lock:
            self._signals[job_id] = signal
        return signal

    def unregister(self, job_id: str) -> None:
        with self._lock:
            self._signals.pop(job_id, None)
            timer = self._timers.pop(job_id, None)
        if timer:
            timer.cancel()

    def cancel(self, job_id: str, grace_period_ms: float = 0) -> None:
        with self._lock:
            signal = self._signals.get(job_id)
        if signal is None:
            return
        if grace_period_ms > 0:
            timer = threading.Timer(grace_period_ms / 1000.0, signal.abort)
            timer.daemon = True
            with self._lock:
                self._timers[job_id] = timer
            timer.start()
        else:
            signal.abort()

    def is_cancelled(self, job_id: str) -> bool:
        with self._lock:
            signal = self._signals.get(job_id)
        return signal.aborted if signal else False

    def get_signal(self, job_id: str) -> Optional[CancelSignal]:
        with self._lock:
            return self._signals.get(job_id)

    def destroy_all(self) -> None:
        with self._lock:
            signals = list(self._signals.values())
            timers = list(self._timers.values())
            self._signals.clear()
            self._timers.clear()
        for timer in timers:
            timer.cancel()
        for signal in signals:
            signal.abort()
