"""bunqueue Python SDK — TCP client for the bunqueue job queue server.

Feature parity with the TypeScript client (TCP mode): Queue, Worker,
FlowProducer, Job, plus the full management/command surface.

Usage::

    from bunqueue import Queue, Worker

    queue = Queue("emails", host="localhost", port=6789)
    queue.add("send", {"to": "user@example.com"}, priority=5)

    def process(job):
        job.update_progress(50)
        return {"sent": True}

    Worker("emails", process, concurrency=10).run()
"""

from .connection import Connection
from .errors import (
    AuthError,
    BunqueueError,
    CommandError,
    CommandTimeoutError,
    ConnectionClosedError,
    UnrecoverableError,
)
from .events import EventEmitter
from .flow import FlowNode, FlowProducer
from .job import Job
from .queue import Queue
from .worker import Worker

__version__ = "0.1.0"

__all__ = [
    "AuthError",
    "BunqueueError",
    "CommandError",
    "CommandTimeoutError",
    "Connection",
    "ConnectionClosedError",
    "EventEmitter",
    "FlowNode",
    "FlowProducer",
    "Job",
    "Queue",
    "UnrecoverableError",
    "Worker",
    "__version__",
]
