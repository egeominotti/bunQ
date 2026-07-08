"""Bunqueue Simple Mode: Queue + Worker in one object (mirror of bunqueue.ts).

TCP mode only: the ``embedded`` option of the original requires the in-process
Bun runtime and raises here with a clear error.

Processing pipeline per job (same order as the original):
Job -> Circuit Breaker -> TTL check -> CancelSignal -> Retry -> Middleware -> Processor
"""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional

from ..job import Job
from ..queue import Queue
from ..worker import Worker
from .aging import PriorityAger
from .batch import BatchAccumulator
from .cancellation import CancellationManager, CancelSignal
from .circuit_breaker import WorkerCircuitBreaker
from .dedup_debounce import DedupDebounceMerger
from .rate_gate import RateGate
from .retry import execute_with_retry
from .triggers import TriggerManager
from .ttl import TtlChecker

Processor = Callable[[Job], Any]


from .app_api import BunqueueApi


class Bunqueue(BunqueueApi):
    """All-in-one Queue + Worker with routes, middleware, cron, batch, retry,
    circuit breaker, TTL, priority aging, cancellation, dedup/debounce."""

    def __init__(self, name: str, **opts: Any) -> None:
        if opts.get("embedded"):
            raise ValueError(
                "embedded mode requires the Bun runtime and the official bunqueue "
                "package; this SDK is TCP-only — connect to a bunqueue server instead"
            )
        modes = [m for m in (opts.get("processor"), opts.get("routes"), opts.get("batch")) if m]
        if len(modes) == 0:
            raise ValueError('Bunqueue requires "processor", "routes", or "batch"')
        if len(modes) > 1:
            raise ValueError('Bunqueue: use only one of "processor", "routes", or "batch"')

        self.name = name
        self._middlewares: List[Callable[[Job, Callable[[], Any]], Any]] = []
        self._retry_config: Optional[Dict[str, Any]] = opts.get("retry")
        self._ttl_checker = TtlChecker(opts["ttl"]) if opts.get("ttl") else None
        self._merger = DedupDebounceMerger(opts.get("deduplication"), opts.get("debounce"))
        self._cancellation = CancellationManager()
        self._default_job_options: Dict[str, Any] = dict(opts.get("default_job_options") or {})

        if opts.get("batch"):
            self._batch_acc: Optional[BatchAccumulator] = BatchAccumulator(opts["batch"])
            self._base_processor: Processor = self._batch_acc.build_processor()
        else:
            self._batch_acc = None
            routes = opts.get("routes")
            self._base_processor = (
                self._build_route_processor(routes) if routes else opts["processor"]
            )

        connection = {
            "host": opts.get("host", "localhost"),
            "port": opts.get("port", 6789),
            "token": opts.get("token"),
            "tls": opts.get("tls"),
        }
        self.queue = Queue(name, **connection)
        self.worker = Worker(
            name,
            self._process_job,
            host=connection["host"],
            port=connection["port"],
            token=connection["token"],
            tls=connection["tls"],
            concurrency=opts.get("concurrency", 4),
            batch_size=opts.get("batch_size", 10),
            poll_timeout_ms=opts.get("poll_timeout_ms", 5000),
            heartbeat_interval_s=opts.get("heartbeat_interval_s", 10.0),
            autorun=opts.get("autorun", True),
        )

        if opts.get("dlq"):
            self.queue.set_dlq_config(opts["dlq"])
        limiter = opts.get("rate_limit") or opts.get("limiter")
        self._rate_gate = RateGate(limiter) if limiter else None

        self._cb = (
            WorkerCircuitBreaker(opts["circuit_breaker"], self.worker)
            if opts.get("circuit_breaker")
            else None
        )
        self._trigger_mgr = TriggerManager(self.queue, self.worker)
        self._ager = (
            PriorityAger(opts["priority_aging"], self.queue)
            if opts.get("priority_aging")
            else None
        )
        if self._ager:
            self._ager.start()

    # ---------------------------------------------------------- processing

    def _build_route_processor(self, routes: Dict[str, Processor]) -> Processor:
        def process(job: Job) -> Any:
            handler = routes.get(job.name or "")
            if handler is None:
                raise ValueError(f'No route for job "{job.name}" in queue "{self.name}"')
            return handler(job)

        return process

    def _process_job(self, job: Job) -> Any:
        if self._rate_gate is not None:
            self._rate_gate.acquire(self._rate_gate.group_for(job.data))
        if self._cb is not None and self._cb.is_open():
            raise RuntimeError("Circuit breaker is open")
        timestamp = job.created_at or 0
        if self._ttl_checker is not None and self._ttl_checker.is_expired(
            job.name or "", timestamp
        ):
            import time

            raise RuntimeError(f"Job expired (age: {int(time.time() * 1000 - timestamp)}ms)")
        signal = self._cancellation.register(job.id)
        try:
            run = lambda: self._run_middleware_chain(job, signal)  # noqa: E731
            if self._retry_config:
                result = execute_with_retry(run, self._retry_config)
            else:
                result = run()
        except BaseException:
            if self._cb is not None:
                self._cb.on_failure()
            raise
        else:
            if self._cb is not None:
                self._cb.on_success()
            return result
        finally:
            self._cancellation.unregister(job.id)

    def _run_middleware_chain(self, job: Job, signal: CancelSignal) -> Any:
        if not self._middlewares:
            return self._base_processor(job)
        index = 0

        def next_fn() -> Any:
            nonlocal index
            if signal.aborted:
                raise RuntimeError("Job cancelled")
            if index < len(self._middlewares):
                middleware = self._middlewares[index]
                index += 1
                return middleware(job, next_fn)
            return self._base_processor(job)

        return next_fn()
