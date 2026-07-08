"""Batch processing: accumulate N jobs, process them together (mirror of
batch.ts). Each job's processor call blocks until its batch flushes, so the
worker concurrency must be >= batch size for full batches — same as the
original."""

from __future__ import annotations

import threading
from typing import Any, Callable, Dict, List, Optional

from ..job import Job


class _Entry:
    __slots__ = ("job", "event", "result", "error")

    def __init__(self, job: Job) -> None:
        self.job = job
        self.event = threading.Event()
        self.result: Any = None
        self.error: Optional[BaseException] = None


class BatchAccumulator:
    def __init__(self, config: Dict[str, Any]) -> None:
        self._size = int(config["size"])
        self._timeout_ms = float(config.get("timeout") or 5000)
        self._processor: Callable[[List[Job]], List[Any]] = config["processor"]
        self._buffer: List[_Entry] = []
        self._timer: Optional[threading.Timer] = None
        self._lock = threading.Lock()

    def build_processor(self) -> Callable[[Job], Any]:
        """Processor that buffers jobs; blocks each caller until flush."""

        def process(job: Job) -> Any:
            entry = _Entry(job)
            with self._lock:
                self._buffer.append(entry)
                if len(self._buffer) >= self._size:
                    self._flush_locked()
                elif self._timer is None:
                    self._timer = threading.Timer(self._timeout_ms / 1000.0, self.flush)
                    self._timer.daemon = True
                    self._timer.start()
            entry.event.wait()
            if entry.error is not None:
                raise entry.error
            return entry.result

        return process

    def flush(self) -> None:
        with self._lock:
            self._flush_locked()

    def _flush_locked(self) -> None:
        if self._timer:
            self._timer.cancel()
            self._timer = None
        batch = self._buffer[:]
        self._buffer.clear()
        if not batch:
            return
        # Run the batch processor outside the lock via a dedicated thread so
        # a size-triggered flush doesn't execute inside a caller's lock scope.
        thread = threading.Thread(target=self._run_batch, args=(batch,), daemon=True)
        thread.start()

    def _run_batch(self, batch: List[_Entry]) -> None:
        try:
            results = self._processor([entry.job for entry in batch])
        except BaseException as exc:  # noqa: BLE001 - every entry fails together
            for entry in batch:
                entry.error = exc
                entry.event.set()
            return
        for i, entry in enumerate(batch):
            entry.result = results[i] if results and i < len(results) else None
            entry.event.set()

    def destroy(self) -> None:
        with self._lock:
            if self._timer:
                self._timer.cancel()
                self._timer = None
            if self._buffer:
                self._flush_locked()
