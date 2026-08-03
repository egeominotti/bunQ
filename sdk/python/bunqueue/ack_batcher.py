"""Batches completed-job ACKs into ACKB round-trips for higher throughput.

Opt-in (see the Worker ``ack_batch`` option; mirrors the TypeScript SDK's
AckBatcher). Items flush when the buffer reaches ``max_size`` or after
``max_delay_ms``, whichever comes first; :meth:`flush` drains the rest on
shutdown. Each item's ``on_settled`` fires after the batch is applied, ignored,
or errored, so the worker keeps a job "active", and its lock renewed, until
the server has returned authoritative positional evidence.
"""

from __future__ import annotations

import logging
import threading
from typing import Any, Callable, List, Optional

from .ack_outcome import ignored_ack_indices
from .wire import _compact

logger = logging.getLogger("bunqueue")

# Called once the batch settles: transport/protocol error and applied outcome.
SettleCallback = Callable[[Optional[BaseException], bool], None]


class AckItem:
    """One buffered ACK: job id, lock token, result, settle callback."""

    __slots__ = ("id", "token", "result", "on_settled")

    def __init__(
        self, id: str, token: str, result: Any, on_settled: SettleCallback
    ) -> None:
        self.id = id
        self.token = token
        self.result = result
        self.on_settled = on_settled


class AckBatcher:
    """Thread-safe ACK buffer flushed as a single ACKB command."""

    def __init__(self, connection: Any, max_size: int = 50, max_delay_ms: float = 5) -> None:
        self._connection = connection
        self._max_size = max(1, max_size)
        self._max_delay_s = max_delay_ms / 1000.0
        self._buffer: List[AckItem] = []
        self._lock = threading.Lock()
        self._timer: Optional[threading.Timer] = None

    def add(self, item: AckItem) -> None:
        with self._lock:
            self._buffer.append(item)
            full = len(self._buffer) >= self._max_size
            if not full and self._timer is None:
                self._timer = threading.Timer(self._max_delay_s, self.flush)
                self._timer.daemon = True  # don't keep the process alive
                self._timer.start()
        if full:
            self.flush()

    def flush(self) -> None:
        """Send the buffered ACKs as one ACKB (no-op when empty)."""
        with self._lock:
            if self._timer is not None:
                self._timer.cancel()
                self._timer = None
            batch, self._buffer = self._buffer, []
        if not batch:
            return
        # Capture the wire outcome first; settle callbacks run OUTSIDE the try
        # so a throwing callback can neither be re-invoked with an error
        # (double settle) nor starve the remaining items of their callback.
        error: Optional[BaseException] = None
        ignored_indices = set()
        try:
            response = self._connection.call(
                _compact(
                    {
                        "cmd": "ACKB",
                        "ids": [item.id for item in batch],
                        "tokens": [item.token for item in batch],
                        "results": [item.result for item in batch],
                    }
                )
            )
            ignored_indices = ignored_ack_indices(response, [item.id for item in batch])
        except BaseException as exc:  # noqa: BLE001 - settle every item, always
            error = exc
        for index, item in enumerate(batch):
            try:
                item.on_settled(error, error is None and index not in ignored_indices)
            except Exception:  # noqa: BLE001
                # A callback error (e.g. a raising listener in the worker) must
                # never prevent the other items from settling.
                logger.warning("ACKB settle callback for job %s raised", item.id, exc_info=True)
