"""Minimal thread-safe event emitter (mirrors Node's EventEmitter subset)."""

from __future__ import annotations

import threading
from typing import Any, Callable, Dict, List

Listener = Callable[..., None]


class EventEmitter:
    """on/once/off/emit with the semantics the TS client exposes."""

    def __init__(self) -> None:
        self._listeners: Dict[str, List[Listener]] = {}
        self._once: Dict[str, List[Listener]] = {}
        self._lock = threading.Lock()

    def on(self, event: str, listener: Listener) -> "EventEmitter":
        with self._lock:
            self._listeners.setdefault(event, []).append(listener)
        return self

    def once(self, event: str, listener: Listener) -> "EventEmitter":
        with self._lock:
            self._once.setdefault(event, []).append(listener)
        return self

    def off(self, event: str, listener: Listener) -> "EventEmitter":
        with self._lock:
            for registry in (self._listeners, self._once):
                if event in registry and listener in registry[event]:
                    registry[event].remove(listener)
        return self

    def remove_all_listeners(self, event: str = None) -> None:
        with self._lock:
            if event is None:
                self._listeners.clear()
                self._once.clear()
            else:
                self._listeners.pop(event, None)
                self._once.pop(event, None)

    def emit(self, event: str, *args: Any) -> None:
        with self._lock:
            listeners = list(self._listeners.get(event, ()))
            once = self._once.pop(event, [])
        for listener in listeners + once:
            try:
                listener(*args)
            except Exception:  # noqa: BLE001 - listeners must not kill the emitter
                pass
