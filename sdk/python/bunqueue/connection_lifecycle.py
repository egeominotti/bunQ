"""Connection establishment, authentication, backoff, and close lifecycle."""

from __future__ import annotations

import socket
import threading
import time

from .errors import AuthError, CommandError, CommandTimeoutError, ConnectionClosedError
from .transport import enable_keepalive
from .wire import _build_ssl_context


class ConnectionLifecycle:
    @property
    def generation(self) -> int:
        """Monotonic counter bumped on every successful reconnect."""
        return self._generation

    def connect(self) -> None:
        """Open the socket and authenticate before publishing it as connected."""
        with self._conn_lock:
            if self._connected:
                return
            if self._closed:
                raise ConnectionClosedError("connection closed by client")
            now = time.monotonic()
            if now < self._next_attempt_at:
                remaining = int((self._next_attempt_at - now) * 1000)
                raise ConnectionClosedError(f"server unreachable, retry in {remaining}ms")

            started_at = self._telemetry.now_ms()
            try:
                raw = socket.create_connection(
                    (self.host, self.port), timeout=self.connect_timeout
                )
            except OSError as exc:
                self._note_connect_failure()
                self._telemetry.emit("error", operation="connect", error=str(exc))
                raise ConnectionClosedError(f"connect failed: {exc}") from exc
            raw.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            enable_keepalive(raw)
            context = _build_ssl_context(self.tls, self.host)
            if context is not None:
                try:
                    raw = context.wrap_socket(raw, server_hostname=self.host)
                except OSError as exc:
                    try:
                        raw.close()
                    except OSError:
                        pass
                    self._note_connect_failure()
                    self._telemetry.emit("error", operation="tls", error=str(exc))
                    raise ConnectionClosedError(f"TLS handshake failed: {exc}") from exc

            self._failed_attempts = 0
            self._next_attempt_at = 0.0
            raw.settimeout(None)
            self._sock = raw
            self._generation += 1
            self._consecutive_timeouts = 0
            threading.Thread(target=self._read_loop, args=(raw,), daemon=True).start()

            if self.token:
                try:
                    self._send({"cmd": "Auth", "token": self.token})
                except CommandError as exc:
                    self._telemetry.emit("auth", ok=False)
                    self._teardown()
                    raise AuthError(str(exc)) from exc
                except (CommandTimeoutError, ConnectionClosedError) as exc:
                    self._telemetry.emit("auth", ok=False)
                    self._teardown()
                    raise ConnectionClosedError(f"auth failed: {exc}") from exc
                self._telemetry.emit("auth", ok=True)

            self._connected = True
            self._telemetry.emit(
                "connect",
                host=self.host,
                port=self.port,
                generation=self._generation,
                duration_ms=self._telemetry.now_ms() - started_at,
            )

    def _note_connect_failure(self) -> None:
        self._failed_attempts += 1
        backoff = min(0.5 * (2 ** (self._failed_attempts - 1)), 5.0)
        self._next_attempt_at = time.monotonic() + backoff
        self._telemetry.emit(
            "reconnect_scheduled",
            host=self.host,
            port=self.port,
            attempt=self._failed_attempts,
            delay_ms=backoff * 1000,
        )

    def close(self) -> None:
        self._closed = True
        self._teardown()

    @property
    def connected(self) -> bool:
        return self._connected
