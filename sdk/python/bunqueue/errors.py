"""Exception hierarchy for the bunqueue Python SDK."""


class BunqueueError(Exception):
    """Base class for all bunqueue SDK errors."""


class ConnectionClosedError(BunqueueError):
    """The TCP connection is closed or was lost mid-command."""


class CommandTimeoutError(BunqueueError):
    """No response received for a command within the timeout."""


class CommandError(BunqueueError):
    """The server answered with ok=false; message carries the server error."""


class SerializationError(BunqueueError):
    """A command payload cannot be encoded within the wire constraints.

    Raised before anything is written when msgpack serialization fails or the
    encoded payload exceeds the protocol frame cap. Serialization failures
    retain the original error as ``__cause__``.
    """


class AuthError(BunqueueError):
    """Authentication with the server failed."""


class UnrecoverableError(BunqueueError):
    """Raise inside a Worker processor to fail the job terminally.

    Skips all remaining retry attempts and sends the job straight to the
    DLQ (mirrors ``UnrecoverableError`` in the TypeScript client).
    """
