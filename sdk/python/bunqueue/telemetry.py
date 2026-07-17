"""Dependency-free structured telemetry for transport and command lifecycle."""

from __future__ import annotations

import time
from typing import Any, Callable, Dict, Optional

TelemetryEvent = Dict[str, Any]
TelemetryHandler = Callable[[TelemetryEvent], None]


class Telemetry:
    """Safely dispatch structured events to an optional application callback."""

    def __init__(self, handler: Optional[TelemetryHandler] = None) -> None:
        self._handler = handler

    @staticmethod
    def now_ms() -> float:
        return time.monotonic() * 1000

    def emit(self, event_type: str, **fields: Any) -> None:
        if self._handler is None:
            return
        try:
            self._handler({"type": event_type, **fields})
        except Exception:
            # Observability must never break connection or worker correctness.
            pass
