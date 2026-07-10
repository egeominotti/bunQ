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

import logging as _logging

from .connection import Connection
from .errors import (
    AuthError,
    BunqueueError,
    CommandError,
    CommandTimeoutError,
    ConnectionClosedError,
    SerializationError,
    UnrecoverableError,
)
from .events import EventEmitter
from .flow import FlowNode, FlowProducer
from .job import Job
from .queue import Queue
from .simple import Bunqueue, CancellationManager, CancelSignal
from .worker import Worker

# Library logging convention: a NullHandler on the package logger so the SDK
# stays silent unless the application configures logging. Internal warnings
# (swallowed command failures, raising listeners, background-task errors) go
# to logging.getLogger("bunqueue").
_logging.getLogger("bunqueue").addHandler(_logging.NullHandler())

__version__ = "0.1.2"

__all__ = [
    "AuthError",
    "Bunqueue",
    "BunqueueError",
    "CancelSignal",
    "CancellationManager",
    "CommandError",
    "CommandTimeoutError",
    "Connection",
    "ConnectionClosedError",
    "EventEmitter",
    "FlowNode",
    "FlowProducer",
    "Job",
    "Queue",
    "SerializationError",
    "UnrecoverableError",
    "Worker",
    "__version__",
]
