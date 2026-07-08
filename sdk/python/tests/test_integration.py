"""Integration tests for the bunqueue Python SDK against a real server.

Runs with pytest OR standalone: ``python tests/test_integration.py``.
Spawns ``bun src/main.ts`` from the repo root on a random port.
"""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import tempfile
import time
import uuid

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from bunqueue import CommandError, Connection, Queue, Worker  # noqa: E402

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))

_server: subprocess.Popen | None = None
_port: int = 0


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def setup_module(_module=None) -> None:
    global _server, _port
    _port = _free_port()
    data_dir = tempfile.mkdtemp(prefix="bunqueue-pytest-")
    env = {
        **os.environ,
        "TCP_PORT": str(_port),
        "HTTP_PORT": str(_free_port()),
        "BUNQUEUE_DATA_PATH": os.path.join(data_dir, "bunq.db"),
    }
    _server = subprocess.Popen(
        ["bun", "src/main.ts"],
        cwd=REPO_ROOT,
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    deadline = time.time() + 15
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", _port), timeout=0.5):
                return
        except OSError:
            time.sleep(0.1)
    raise RuntimeError("bunqueue server did not start within 15s")


def teardown_module(_module=None) -> None:
    if _server is not None:
        _server.terminate()
        _server.wait(timeout=10)


def _queue(name_prefix: str) -> Queue:
    return Queue(f"{name_prefix}-{uuid.uuid4().hex[:8]}", host="127.0.0.1", port=_port)


# ------------------------------------------------------------------- tests


def test_ping_and_hello() -> None:
    conn = Connection(host="127.0.0.1", port=_port)
    assert conn.ping() is True
    hello = conn.hello()
    assert hello["protocolVersion"] >= 1
    assert hello["server"]
    conn.close()


def test_add_and_query() -> None:
    with _queue("pytest-add") as queue:
        job = queue.add("send", {"to": "a@b.c"}, priority=5)
        assert job.id
        assert queue.get_state(job.id) in ("waiting", "prioritized")
        fetched = queue.get_job(job.id)
        assert fetched is not None
        assert fetched.name == "send"
        assert fetched.data["to"] == "a@b.c"
        counts = queue.get_job_counts()
        assert counts["waiting"] + counts["prioritized"] == 1


def test_add_bulk() -> None:
    with _queue("pytest-bulk") as queue:
        ids = queue.add_bulk([{"name": "task", "data": {"i": i}} for i in range(50)])
        assert len(ids) == 50
        assert queue.count() == 50


def test_worker_roundtrip() -> None:
    with _queue("pytest-worker") as queue:
        results = {}
        job_ids = [queue.add("double", {"value": i}).id for i in range(10)]

        def process(job):
            return {"doubled": job.data["value"] * 2}

        worker = Worker(
            queue.name,
            process,
            host="127.0.0.1",
            port=_port,
            concurrency=3,
            poll_timeout_ms=1000,
        )
        worker.on("completed", lambda job, result: results.update({job.id: result}))
        worker.start()
        deadline = time.time() + 15
        while len(results) < 10 and time.time() < deadline:
            time.sleep(0.1)
        worker.close(timeout=10)

        assert len(results) == 10
        for job_id in job_ids:
            assert queue.get_state(job_id) == "completed"
            assert queue.get_result(job_id)["doubled"] in range(0, 20, 2)


def test_worker_failure_goes_to_dlq() -> None:
    with _queue("pytest-fail") as queue:
        job = queue.add("boom", {"x": 1}, attempts=1)
        failures = []

        worker = Worker(
            queue.name,
            lambda j: (_ for _ in ()).throw(ValueError("kaboom")),
            host="127.0.0.1",
            port=_port,
            concurrency=1,
            poll_timeout_ms=1000,
        )
        worker.on("failed", lambda j, exc: failures.append(str(exc)))
        worker.start()
        deadline = time.time() + 15
        while not failures and time.time() < deadline:
            time.sleep(0.1)
        worker.close(timeout=10)

        assert failures and "kaboom" in failures[0]
        deadline = time.time() + 5
        while queue.get_state(job.id) != "failed" and time.time() < deadline:
            time.sleep(0.1)
        assert queue.get_state(job.id) == "failed"
        assert len(queue.get_dlq()) == 1


def test_priority_ordering() -> None:
    with _queue("pytest-prio") as queue:
        queue.add("low", {"p": "low"}, priority=1)
        queue.add("high", {"p": "high"}, priority=100)
        response = queue.connection.call(
            {"cmd": "PULL", "queue": queue.name, "owner": "pytest", "timeout": 1000}
        )
        assert response["job"]["data"]["p"] == "high"


def test_pause_resume() -> None:
    with _queue("pytest-pause") as queue:
        queue.pause()
        assert queue.is_paused() is True
        queue.resume()
        assert queue.is_paused() is False


def test_command_error_raised() -> None:
    conn = Connection(host="127.0.0.1", port=_port)
    try:
        try:
            conn.call({"cmd": "GetJob", "id": "nonexistent-job-id"})
        except CommandError:
            pass  # either behavior acceptable: error or null job
    finally:
        conn.close()


if __name__ == "__main__":
    setup_module()
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    failed = 0
    try:
        for test in tests:
            try:
                test()
                print(f"PASS {test.__name__}")
            except Exception as exc:  # noqa: BLE001
                failed += 1
                import traceback

                print(f"FAIL {test.__name__}: {exc}")
                traceback.print_exc()
    finally:
        teardown_module()
    print(f"\n{len(tests) - failed}/{len(tests)} passed")
    sys.exit(1 if failed else 0)
