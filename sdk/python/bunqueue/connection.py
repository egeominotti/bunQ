"""TCP connection to a bunqueue server.

Wire protocol: each message is a 4-byte big-endian length prefix followed by a
standard msgpack map. Requests carry a ``reqId`` string; the server echoes it
back, which allows pipelining (many in-flight commands on one socket).

Thread-safe: any thread may call :meth:`Connection.call`. A single daemon
reader thread demultiplexes responses to their pending futures.
"""

from __future__ import annotations

import socket
import struct
import threading
import time
from concurrent.futures import Future
from concurrent.futures import TimeoutError as FutureTimeoutError
from typing import Any, Dict, Optional

import msgpack

from .errors import AuthError, CommandError, CommandTimeoutError, ConnectionClosedError
from .wire import (
    MAX_FRAME_SIZE,
    PROTOCOL_VERSION,
    TlsOption,
    _build_ssl_context,
    _compact,
    _js_safe,
)

__all__ = ["Connection", "TlsOption", "_compact", "PROTOCOL_VERSION", "MAX_FRAME_SIZE"]


class Connection:
    """A single pipelined TCP connection to a bunqueue server."""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 6789,
        token: Optional[str] = None,
        tls: TlsOption = None,
        connect_timeout: float = 5.0,
        command_timeout: float = 10.0,
    ) -> None:
        self.host = host
        self.port = port
        self.token = token
        self.tls = tls
        self.connect_timeout = connect_timeout
        self.command_timeout = command_timeout

        self._sock: Optional[socket.socket] = None
        self._connected = False
        self._closed = False
        # Reentrant: connect() holds this while sending Auth via _send(), which
        # may itself call _teardown() on a failed send — both need the lock.
        self._conn_lock = threading.RLock()  # serializes connect/teardown
        self._write_lock = threading.Lock()
        self._pending_lock = threading.Lock()
        self._pending: Dict[str, Future] = {}
        self._req_counter = 0
        self._generation = 0
        self._failed_attempts = 0
        self._next_attempt_at = 0.0
        # Half-open recovery (#94): after this many consecutive command timeouts
        # the socket is presumed dead and torn down so the next call reconnects.
        self._max_command_timeouts = 3
        self._consecutive_timeouts = 0

    # ------------------------------------------------------------- lifecycle

    @property
    def generation(self) -> int:
        """Monotonic counter bumped on every successful (re)connect.

        Consumers holding per-connection server state (e.g. a Worker
        registration) compare it between operations and re-establish that
        state after a reconnect.
        """
        return self._generation

    def connect(self) -> None:
        """Open the socket (and authenticate) if not already connected."""
        with self._conn_lock:
            if self._connected:
                return
            if self._closed:
                raise ConnectionClosedError("connection closed by client")
            # Fast-fail inside the reconnect backoff window: without this a
            # producer calling add() against a downed server pays the full
            # connect timeout on EVERY call (reconnect storm).
            now = time.monotonic()
            if now < self._next_attempt_at:
                remaining = int((self._next_attempt_at - now) * 1000)
                raise ConnectionClosedError(f"server unreachable, retry in {remaining}ms")

            try:
                raw = socket.create_connection(
                    (self.host, self.port), timeout=self.connect_timeout
                )
            except OSError as exc:
                self._failed_attempts += 1
                backoff = min(0.5 * (2 ** (self._failed_attempts - 1)), 5.0)
                self._next_attempt_at = time.monotonic() + backoff
                raise ConnectionClosedError(f"connect failed: {exc}") from exc
            self._failed_attempts = 0
            self._next_attempt_at = 0.0
            raw.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            self._enable_keepalive(raw)
            ctx = _build_ssl_context(self.tls, self.host)
            if ctx is not None:
                raw = ctx.wrap_socket(raw, server_hostname=self.host)
            raw.settimeout(None)  # reader thread blocks on recv

            self._sock = raw
            self._generation += 1
            self._consecutive_timeouts = 0
            reader = threading.Thread(target=self._read_loop, args=(raw,), daemon=True)
            reader.start()

            # Authenticate as the FIRST command, still holding the connection
            # lock and BEFORE flipping _connected — otherwise a concurrent
            # thread that observes _connected==True could send a command ahead
            # of the Auth frame, which the server rejects ('Not authenticated').
            if self.token:
                try:
                    self._send({"cmd": "Auth", "token": self.token})
                except CommandError as exc:
                    self._teardown()
                    raise AuthError(str(exc)) from exc
                except (CommandTimeoutError, ConnectionClosedError) as exc:
                    self._teardown()
                    raise ConnectionClosedError(f"auth failed: {exc}") from exc

            self._connected = True

    def close(self) -> None:
        """Close the connection permanently; pending commands fail."""
        self._closed = True
        self._teardown()

    @property
    def connected(self) -> bool:
        return self._connected

    # ------------------------------------------------------------- commands

    def call(self, command: Dict[str, Any], timeout: Optional[float] = None) -> Dict[str, Any]:
        """Send a command and wait for its response.

        Raises :class:`CommandError` when the server answers ``ok=false``.
        Reconnects lazily if the connection was lost.
        """
        if not self._connected:
            self.connect()
        return self._send(command, timeout)

    def _send(self, command: Dict[str, Any], timeout: Optional[float] = None) -> Dict[str, Any]:
        """Frame + write a command and await its response on the open socket.

        Assumes the socket is up (used both by :meth:`call` and, during
        :meth:`connect`, to send the initial Auth before ``_connected`` flips).
        """
        gen = self._generation  # snapshot: a timeout must not tear down a newer conn
        with self._pending_lock:
            self._req_counter = (self._req_counter + 1) & 0x7FFFFFFF
            req_id = str(self._req_counter)
            fut: Future = Future()
            self._pending[req_id] = fut

        payload = msgpack.packb(
            _js_safe({**_compact(command), "reqId": req_id}), use_bin_type=True
        )
        frame = struct.pack(">I", len(payload)) + payload

        sock = self._sock
        try:
            if sock is None:
                raise OSError("socket gone")
            with self._write_lock:
                sock.sendall(frame)
        except OSError as exc:
            with self._pending_lock:
                self._pending.pop(req_id, None)
            self._teardown()
            raise ConnectionClosedError(f"send failed: {exc}") from exc

        try:
            response = fut.result(timeout if timeout is not None else self.command_timeout)
        except FutureTimeoutError:
            with self._pending_lock:
                self._pending.pop(req_id, None)
            self._note_timeout(gen)
            raise CommandTimeoutError(f"no response for {command.get('cmd')} within timeout") from None

        self._consecutive_timeouts = 0
        if isinstance(response, BaseException):
            raise response
        if not response.get("ok"):
            raise CommandError(str(response.get("error", "unknown server error")))
        return response

    def _note_timeout(self, gen: int) -> None:
        """Tear down after repeated timeouts so a dead/half-open link recovers.

        On a link the peer dropped without FIN/RST, every command times out
        while the socket still looks connected. Counting consecutive timeouts
        and forcing a teardown lets the next :meth:`call` reconnect instead of
        wedging until the OS abandons the writes (~tcp_retries2, minutes).

        ``gen`` is the connection generation at send time: a timeout from an
        already-replaced connection must not tear down (or miscount against)
        the current one."""
        if gen != self._generation:
            return
        self._consecutive_timeouts += 1
        if self._consecutive_timeouts >= self._max_command_timeouts:
            self._teardown()

    @staticmethod
    def _enable_keepalive(raw: socket.socket) -> None:
        """Enable TCP keepalive (~15s idle) to surface half-open links fast."""
        try:
            raw.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        except OSError:
            return
        for name, value in (
            ("TCP_KEEPIDLE", 15),   # Linux: idle before first probe
            ("TCP_KEEPINTVL", 5),   # Linux: interval between probes
            ("TCP_KEEPCNT", 3),     # Linux: dead after this many failed probes
            ("TCP_KEEPALIVE", 15),  # macOS: idle before first probe
        ):
            code = getattr(socket, name, None)
            if code is not None:
                try:
                    raw.setsockopt(socket.IPPROTO_TCP, code, value)
                except OSError:
                    pass

    def ping(self) -> bool:
        try:
            response = self.call({"cmd": "Ping"})
        except (ConnectionClosedError, CommandTimeoutError, CommandError):
            return False
        data = response.get("data")
        return bool(isinstance(data, dict) and data.get("pong"))

    def hello(self) -> Dict[str, Any]:
        """Protocol negotiation; returns server name/version/protocolVersion."""
        return self.call(
            {"cmd": "Hello", "protocolVersion": PROTOCOL_VERSION, "capabilities": ["pipelining"]}
        )

    # ------------------------------------------------------------- internals

    def _read_loop(self, sock: socket.socket) -> None:
        buffer = bytearray()
        try:
            while True:
                chunk = sock.recv(65536)
                if not chunk:
                    break
                buffer.extend(chunk)
                while len(buffer) >= 4:
                    (length,) = struct.unpack_from(">I", buffer)
                    if length > MAX_FRAME_SIZE:
                        raise ConnectionClosedError("frame exceeds max size")
                    if len(buffer) < 4 + length:
                        break
                    frame = bytes(buffer[4 : 4 + length])
                    del buffer[: 4 + length]
                    self._dispatch(frame)
        except (OSError, ConnectionClosedError):
            pass
        finally:
            # Only tear down if this reader's socket is still current: a stale
            # reader from a previous connection must not kill a fresh one.
            if self._sock is sock:
                self._teardown()

    def _dispatch(self, frame: bytes) -> None:
        try:
            message = msgpack.unpackb(frame, raw=False)
        except Exception:
            return  # unparseable frame: skip; stream desync ends via recv error
        req_id = message.get("reqId") if isinstance(message, dict) else None
        if req_id is None:
            return  # server-push messages are not supported yet
        with self._pending_lock:
            fut = self._pending.pop(str(req_id), None)
        if fut is not None and not fut.done():
            fut.set_result(message)

    def _teardown(self) -> None:
        with self._conn_lock:
            self._connected = False
            sock, self._sock = self._sock, None
        if sock is not None:
            try:
                sock.close()
            except OSError:
                pass
        with self._pending_lock:
            pending, self._pending = self._pending, {}
        for fut in pending.values():
            if not fut.done():
                fut.set_result(ConnectionClosedError("connection lost"))
