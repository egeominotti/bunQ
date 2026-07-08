"""Shared e2e harness: real-server fixture + tiny test runner."""

from __future__ import annotations

import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import traceback
import uuid
from typing import Callable, Dict, List, Optional, Tuple

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class Server:
    """Spawns `bun src/main.ts` on a random port; waits until accepting TCP."""

    def __init__(self, extra_env: Optional[Dict[str, str]] = None) -> None:
        self.port = free_port()
        self.data_dir = tempfile.mkdtemp(prefix="bunqueue-e2e-")
        self.proc: Optional[subprocess.Popen] = None
        self.extra_env = extra_env or {}

    def start(self) -> "Server":
        env = {
            **os.environ,
            "TCP_PORT": str(self.port),
            "HTTP_PORT": str(free_port()),
            "BUNQUEUE_DATA_PATH": os.path.join(self.data_dir, "bunq.db"),
            **self.extra_env,
        }
        self.proc = subprocess.Popen(
            ["bun", "src/main.ts"],
            cwd=REPO_ROOT,
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        deadline = time.time() + 15
        while time.time() < deadline:
            try:
                with socket.create_connection(("127.0.0.1", self.port), timeout=0.5):
                    return self
            except OSError:
                time.sleep(0.1)
        self.stop()  # don't leak the spawned process on startup timeout
        raise RuntimeError("bunqueue server did not start within 15s")

    def stop(self) -> None:
        if self.proc is not None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.proc.kill()
            self.proc = None
        shutil.rmtree(self.data_dir, ignore_errors=True)


# ------------------------------------------------------------------ registry

TESTS: List[Tuple[str, Callable[[Server], None]]] = []


def test(fn: Callable[[Server], None]) -> Callable[[Server], None]:
    TESTS.append((f"{fn.__module__}.{fn.__name__}", fn))
    return fn


def unique_name(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def wait_until(predicate: Callable[[], bool], timeout: float = 15.0, interval: float = 0.1) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


def run_registered(server: Server) -> int:
    failed = 0
    for name, fn in TESTS:
        try:
            fn(server)
            print(f"PASS {name}")
        except Exception:  # noqa: BLE001
            failed += 1
            print(f"FAIL {name}")
            traceback.print_exc()
    return failed
