"""E2E repro tests for the 2026-07-14 spec-alignment audit (RED pre-fix).

Covers: batch_size server cap, heartbeat_interval_s 0 = disabled (no busy
loop), FAIL stack truncation direction (raise site must survive the server's
first-N cap), Simple Mode cron()/every() execution ``limit`` (wire maxLimit),
wait_for_job timeout clamp, and the protocol version advertised by hello().
"""

from __future__ import annotations

import time

from harness import Server, test, unique_name, wait_until

from bunqueue import Bunqueue, Queue, Worker
from bunqueue.wire import PROTOCOL_VERSION


@test
def spec_batch_size_clamps_to_server_max(server: Server) -> None:
    """batch_size > 1000 must clamp: the server rejects PULLB count > 1000."""
    with Queue(unique_name("cap"), port=server.port) as queue:
        done: list = []
        worker = Worker(
            queue.name,
            lambda j: done.append(j.id) or "ok",
            port=server.port,
            poll_timeout_ms=300,
            concurrency=1200,
            batch_size=1200,
        )
        try:
            assert worker.batch_size == 1000, f"batch_size not clamped: {worker.batch_size}"
            worker.start()
            queue.add("t", {"x": 1})
            assert wait_until(lambda: len(done) >= 1, 15), "job never processed (PULLB rejected?)"
        finally:
            worker.close(timeout=10)


@test
def spec_heartbeat_zero_disables_heartbeats(server: Server) -> None:
    """heartbeat_interval_s=0 must disable heartbeats, not busy-loop them."""
    with Queue(unique_name("hb0"), port=server.port) as queue:
        counts = {"hb": 0}
        worker = Worker(
            queue.name,
            lambda j: "ok",
            port=server.port,
            poll_timeout_ms=300,
            heartbeat_interval_s=0,
            autorun=False,
        )
        orig_call = worker.connection.call

        def counting_call(command, **kwargs):
            if isinstance(command, dict) and command.get("cmd") == "Heartbeat":
                counts["hb"] += 1
            return orig_call(command, **kwargs)

        worker.connection.call = counting_call  # type: ignore[method-assign]
        try:
            worker.start()
            worker.wait_until_ready()
            queue.add("t", {"x": 1})
            assert wait_until(lambda: queue.get_job_counts().get("completed", 0) >= 1, 15)
            time.sleep(0.7)  # a 0-interval loop would have sent hundreds by now
            assert counts["hb"] == 0, f"heartbeat storm with interval 0: {counts['hb']} sent"
        finally:
            worker.close(timeout=10)


@test
def spec_fail_stack_keeps_raise_site(server: Server) -> None:
    """The persisted stack (server keeps the FIRST stackTraceLimit lines of
    what we send) must still contain the raise site, which in a Python
    traceback is at the BOTTOM — so the SDK must send at most that many
    trailing lines."""
    with Queue(unique_name("stk"), port=server.port) as queue:

        def deep(n: int) -> int:
            if n <= 0:
                raise RuntimeError("BOOM-SPEC-STACK")
            return deep(n - 1)

        worker = Worker(queue.name, lambda j: deep(20), port=server.port, poll_timeout_ms=300)
        try:
            worker.start()
            job = queue.add("t", {}, attempts=1)
            assert wait_until(lambda: queue.get_state(job.id) == "failed", 20)
            failed = queue.get_job(job.id)
            assert failed is not None, "failed job must stay readable"
            stack = failed.stacktrace or []
            assert stack, "stack must be persisted on FAIL"
            assert any("BOOM-SPEC-STACK" in line for line in stack), (
                f"persisted stack lost the raise site: {stack}"
            )
        finally:
            worker.close(timeout=10)


@test
def spec_simple_cron_limit_forwarded(server: Server) -> None:
    """Simple Mode cron()/every() must forward limit= (wire maxLimit)."""
    app = Bunqueue(
        unique_name("lim"),
        port=server.port,
        poll_timeout_ms=300,
        processor=lambda j: "ok",
    )
    cron_id = unique_name("limc")
    every_id = unique_name("lime")
    try:
        app.cron(cron_id, "0 9 * * *", {"t": 1}, limit=3)
        sched = app.queue.get_job_scheduler(cron_id)
        assert sched is not None and sched.get("maxLimit") == 3, f"cron limit dropped: {sched}"
        app.every(every_id, 60_000, {"t": 1}, limit=2)
        sched2 = app.queue.get_job_scheduler(every_id)
        assert sched2 is not None and sched2.get("maxLimit") == 2, f"every limit dropped: {sched2}"
    finally:
        app.remove_cron(cron_id)
        app.remove_cron(every_id)
        app.close()


@test
def spec_wait_for_job_clamps_timeout(server: Server) -> None:
    """timeout_ms beyond the server's 600000 cap must clamp, not error."""
    with Queue(unique_name("wclamp"), port=server.port) as queue:
        worker = Worker(queue.name, lambda j: {"done": True}, port=server.port, poll_timeout_ms=300)
        try:
            worker.start()
            job = queue.add("t", {"x": 1})
            assert wait_until(lambda: queue.get_state(job.id) == "completed", 15)
            result = queue.wait_for_job(job.id, timeout_ms=700_000)
            assert result == {"done": True}, f"unexpected result: {result}"
        finally:
            worker.close(timeout=10)


@test
def spec_protocol_version_matches_server(server: Server) -> None:
    with Queue(unique_name("hello"), port=server.port) as queue:
        hello = queue.connection.hello()
        assert hello.get("protocolVersion") == PROTOCOL_VERSION, (
            f"client v{PROTOCOL_VERSION} != server v{hello.get('protocolVersion')}"
        )
