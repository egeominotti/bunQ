"""E2E repro tests for the SDK-audit fixes (RED before the fix, GREEN after).

0.1.1 findings: H1 addBulk custom-id, H2 keepalive + command timeout teardown,
H3 auth-first ordering under concurrency, H4 getFlow not-found, M1 waitForJob
timeout, M3 cron job-option mapping.

0.1.2 findings: M1 pending-future leak on serialization failure, M2 TLS
handshake error mapping + backoff, M3 Worker close edges + context manager,
M5 register false-success, H2 ACKB error settling.
"""

from __future__ import annotations

import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, Optional

from harness import Server, test, unique_name, wait_until

from bunqueue import Connection, FlowProducer, Queue, Worker
from bunqueue.errors import (
    BunqueueError,
    CommandError,
    CommandTimeoutError,
    ConnectionClosedError,
    SerializationError,
)


# ------------------------------------------------------------------ H1 addBulk
@test
def test_h1_addbulk_preserves_custom_id(server: Server) -> None:
    q = Queue(unique_name("h1"), port=server.port)
    try:
        q.add_bulk([{"name": "t", "data": {"x": 1}, "job_id": "cid-h1-xyz"}])
        found = wait_until(lambda: q.get_job_by_custom_id("cid-h1-xyz") is not None, 10)
        assert found, "add_bulk must preserve the custom job id (customId on PUSHB)"
        # idempotency: re-adding the same custom id must not create a duplicate
        q.add_bulk([{"name": "t", "data": {"x": 2}, "job_id": "cid-h1-xyz"}])
        time.sleep(0.3)
        matches = [j for j in q.get_jobs() if j.custom_id == "cid-h1-xyz"]
        assert len(matches) <= 1, f"custom id must dedupe in bulk, got {len(matches)}"
    finally:
        q.obliterate()
        q.close()


# --------------------------------------------------------------- H2 keepalive
@test
def test_h2_keepalive_enabled(server: Server) -> None:
    q = Queue(unique_name("h2"), port=server.port)
    try:
        q.wait_until_ready()
        sock = q.connection._sock
        assert sock is not None, "socket must be open after connect"
        # macOS returns a non-1 truthy value (e.g. 8) when keepalive is on.
        val = sock.getsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE)
        assert val != 0, "SO_KEEPALIVE must be enabled to detect half-open links"
    finally:
        q.close()


def _blackhole() -> tuple[socket.socket, int]:
    """A listener that accepts connections then never replies (half-open sim)."""
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", 0))
    srv.listen(8)
    port = srv.getsockname()[1]
    accepted: list = []

    def accept_loop() -> None:
        while True:
            try:
                conn, _ = srv.accept()
                accepted.append(conn)  # hold the socket open, never respond
            except OSError:
                return

    threading.Thread(target=accept_loop, daemon=True).start()
    return srv, port


def run_h2_timeout_teardown() -> int:
    """After repeated command timeouts on a dead link the connection must
    tear down so the next call reconnects (instead of wedging forever)."""
    srv, port = _blackhole()
    conn = Connection(host="127.0.0.1", port=port, command_timeout=0.3)
    try:
        # _max_command_timeouts consecutive timeouts (3) must tear down. A 4th
        # call would reconnect (counter resets), so stop at the threshold.
        for _ in range(conn._max_command_timeouts):
            try:
                conn.call({"cmd": "Ping"})
            except BunqueueError:
                pass
        assert not conn.connected, "connection must tear down after repeated timeouts"
        print("PASS e2e_audit_fixes.h2_timeout_teardown")
        return 0
    except Exception as exc:  # noqa: BLE001
        print(f"FAIL e2e_audit_fixes.h2_timeout_teardown: {exc}")
        return 1
    finally:
        conn.close()
        srv.close()


# ------------------------------------------------------------------ H3 auth race
def run_h3_auth_race() -> int:
    """Concurrent commands at boot on a shared tokened connection must never
    race ahead of Auth (server would answer 'Not authenticated')."""
    server = Server(extra_env={"AUTH_TOKENS": "secret-token"}).start()
    failed = 0
    try:
        for attempt in range(5):
            q = Queue(unique_name("h3"), port=server.port, token="secret-token")
            errors: list[str] = []

            def do_add(i: int, queue: Queue = q, sink: list = errors) -> None:
                try:
                    queue.add("t", {"i": i})
                except Exception as exc:  # noqa: BLE001
                    sink.append(str(exc))

            with ThreadPoolExecutor(max_workers=40) as ex:
                list(ex.map(do_add, range(40)))
            q.close()
            if errors:
                failed += 1
                print(f"FAIL e2e_audit_fixes.h3_auth_race (attempt {attempt}): {errors[:2]}")
                break
        if not failed:
            print("PASS e2e_audit_fixes.h3_auth_race")
    finally:
        server.stop()
    return failed


# ------------------------------------------------------------------ H4 getFlow
@test
def test_h4_get_flow_not_found_returns_none(server: Server) -> None:
    flow = FlowProducer(port=server.port)
    try:
        assert flow.get_flow("does-not-exist-h4") is None, "get_flow(missing) must return None, not raise"
    finally:
        flow.close()


@test
def test_h4_get_flow_survives_removed_child(server: Server) -> None:
    name = unique_name("h4tree")
    q = Queue(name, port=server.port)
    flow = FlowProducer(port=server.port)
    try:
        q.pause()
        node = flow.add(
            {
                "name": "root",
                "queueName": name,
                "children": [{"name": "child", "queueName": name}],
            }
        )
        child_id = node.children[0].job.id
        q.remove(child_id)  # child gone; root.childrenIds still lists it
        tree = flow.get_flow(node.job.id)  # must not raise on the missing child
        assert tree is not None, "get_flow must return the partial tree"
    finally:
        q.obliterate()
        flow.close()
        q.close()


# ------------------------------------------------------------------ M1 waitForJob
@test
def test_m1_wait_for_job_raises_on_timeout(server: Server) -> None:
    q = Queue(unique_name("m1"), port=server.port)
    try:
        job = q.add("t", {"x": 1})  # no worker → never completes
        raised = False
        try:
            q.wait_for_job(job.id, timeout_ms=500)
        except BunqueueError:
            raised = True
        assert raised, "wait_for_job must raise on timeout, not silently return None"
    finally:
        q.obliterate()
        q.close()


@test
def test_m1_wait_for_job_failed_raises_command_error(server: Server) -> None:
    name = unique_name("m1f")
    q = Queue(name, port=server.port)

    def boom(_job: object) -> None:
        raise RuntimeError("processor boom")

    worker = Worker(name, boom, port=server.port, poll_timeout_ms=300)
    try:
        job = q.add("t", {"x": 1})
        assert wait_until(lambda: q.get_state(job.id) == "failed", 15), "job must reach failed"
        kind = None
        try:
            q.wait_for_job(job.id, timeout_ms=500)
        except CommandTimeoutError:
            kind = "timeout"
        except CommandError:
            kind = "failed"
        assert kind == "failed", f"failed job must raise CommandError, got {kind}"
    finally:
        worker.close(timeout=10)
        q.obliterate()
        q.close()


# --------------------------------------------------------------- M3 cron opts
def run_m3_cron_option_mapping() -> int:
    """The cron job-option template must be mapped snake->camel (attempts->
    maxAttempts, etc.) or the server silently drops it and uses JOB_DEFAULTS."""
    try:
        from bunqueue.options import build_cron_job_options

        out = build_cron_job_options(
            {
                "attempts": 5,
                "backoff": 2000,
                "timeout": 60000,
                "delay": 100,
                "stall_timeout": 1000,
                "remove_on_complete": True,
                "remove_on_fail": False,
            }
        )
        assert out is not None
        assert out["maxAttempts"] == 5, out
        assert out["backoff"] == 2000, out
        assert out["timeout"] == 60000, out
        assert out["delay"] == 100, out
        assert out["stallTimeout"] == 1000, out
        assert out["removeOnComplete"] is True, out
        assert out["removeOnFail"] is False, out
        assert "attempts" not in out and "stall_timeout" not in out, out
        assert build_cron_job_options(None) is None
        assert build_cron_job_options({}) is None
        print("PASS e2e_audit_fixes.m3_cron_option_mapping")
        return 0
    except Exception as exc:  # noqa: BLE001
        print(f"FAIL e2e_audit_fixes.m3_cron_option_mapping: {exc}")
        return 1


# ------------------------------------------------- 0.1.2 M1 serialization leak
@test
def test_012_serialization_error_no_pending_leak(server: Server) -> None:
    """An unserializable payload must raise SerializationError (chained) and
    leave no orphaned entry in the pending-futures map."""
    import datetime

    q = Queue(unique_name("ser"), port=server.port)
    try:
        raised: Optional[BaseException] = None
        try:
            q.add("t", {"when": datetime.datetime.now()})  # msgpack rejects datetime
        except SerializationError as exc:
            raised = exc
        assert raised is not None, "datetime payload must raise SerializationError"
        assert raised.__cause__ is not None, "original msgpack error must be chained"
        assert len(q.connection._pending) == 0, "failed pack must not leak a pending future"
        job = q.add("t", {"ok": 1})  # the connection must still be usable
        assert job.id
    finally:
        q.obliterate()
        q.close()


# ------------------------------------------------------ 0.1.2 M2 TLS handshake
@test
def test_012_tls_handshake_failure_maps_and_backs_off(server: Server) -> None:
    """TLS against a plain-TCP server: the handshake failure must surface as
    ConnectionClosedError (not raw ssl.SSLError) and count into the backoff."""
    conn = Connection(host="127.0.0.1", port=server.port, tls=True, connect_timeout=2)
    try:
        raised = False
        try:
            conn.connect()
        except ConnectionClosedError:
            raised = True
        assert raised, "TLS handshake failure must raise ConnectionClosedError"
        assert conn._failed_attempts >= 1, "handshake failure must count as a failed attempt"
        assert conn._next_attempt_at > 0, "handshake failure must arm the reconnect backoff"
    finally:
        conn.close()


# --------------------------------------------------- 0.1.2 M3 worker close edges
@test
def test_012_close_without_run_settles_state(server: Server) -> None:
    """close() with autorun=False and run() never called must still mark the
    worker closed and close its connection."""
    closed_events = []
    worker = Worker(unique_name("q"), lambda j: None, port=server.port, autorun=False)
    worker.on("closed", lambda: closed_events.append(1))
    assert worker.close(timeout=1) is True
    assert worker.is_closed(), "worker must be closed even though run() never started"
    assert worker.connection._closed, "connection must be closed"
    assert closed_events == [1]
    assert worker.close(timeout=1) is True  # idempotent, no double 'closed'
    assert closed_events == [1]


@test
def test_012_worker_context_manager(server: Server) -> None:
    with Queue(unique_name("q"), port=server.port) as queue:
        job = queue.add("t", {"v": 1})
        with Worker(queue.name, lambda j: "ok", port=server.port, poll_timeout_ms=300) as worker:
            assert wait_until(lambda: queue.get_state(job.id) == "completed", 15)
        assert worker.is_closed(), "__exit__ must close the worker"


@test
def test_012_close_timeout_keeps_thread_state(server: Server) -> None:
    """An expired close(timeout) must report False and keep the live thread
    reference so a later close() can join the still-draining loop."""
    with Queue(unique_name("q"), port=server.port) as queue:
        release = threading.Event()

        def process(job: Any) -> str:
            release.wait(15)
            return "ok"

        job = queue.add("t", {})
        worker = Worker(queue.name, process, port=server.port, poll_timeout_ms=300)
        try:
            assert wait_until(lambda: queue.get_state(job.id) == "active", 15)
            assert worker.close(timeout=0.2) is False, "expired close must report still-draining"
            assert worker._run_thread is not None, "must not null the thread while alive"
        finally:
            release.set()
        assert worker.close(timeout=15) is True
        assert worker.is_closed()


# ------------------------------------------------ 0.1.2 M5 register false-success
@test
def test_012_register_only_marks_on_success(server: Server) -> None:
    """A failed RegisterWorker must NOT mark the generation as registered, so
    the next poll iteration retries (Discussion #103 class)."""
    worker = Worker(unique_name("q"), lambda j: None, port=server.port, autorun=False)

    class FailingConnection:
        generation = 7
        connected = True

        def call(self, command: Dict[str, Any], timeout: Optional[float] = None) -> None:
            raise CommandError("register rejected")

        def close(self) -> None:
            pass

    real = worker.connection
    worker.connection = FailingConnection()  # type: ignore[assignment]
    worker._register()
    assert worker._registered_generation == -1, "failed registration must stay unregistered"
    worker.connection = real
    worker.close(timeout=1)


# ------------------------------------------------ 0.1.2 not-found narrowing
@test
def test_012_not_found_narrowing_rethrows_connection_errors(server: Server) -> None:
    """get_job/get_job_by_custom_id/get_job_scheduler map only a 'not found'
    CommandError to None; connection loss must surface, never become None."""
    q = Queue(unique_name("nf"), port=server.port)
    try:
        # genuine not-found still maps to None (server: 'Job not found', 'Cron job not found')
        assert q.get_job("missing-id-nf") is None
        assert q.get_job_scheduler("missing-sched-nf") is None

        class DownConnection:
            def call(self, command: Dict[str, Any], timeout: Optional[float] = None) -> None:
                raise ConnectionClosedError("connection lost")

        real = q.connection
        q.connection = DownConnection()  # type: ignore[assignment]
        probes = (
            lambda: q.get_job("x"),
            lambda: q.get_job_by_custom_id("x"),
            lambda: q.get_job_scheduler("x"),
        )
        for probe in probes:
            raised = False
            try:
                probe()
            except ConnectionClosedError:
                raised = True
            assert raised, "connection loss must rethrow, not masquerade as a missing job"
        q.connection = real
    finally:
        q.close()


# -------------------------------------------- 0.1.2 scheduler top-level fields
@test
def test_012_scheduler_template_priority_and_dedup_top_level(server: Server) -> None:
    """Template priority/deduplication must travel as TOP-LEVEL Cron fields
    (the server ignores them inside jobOptions), so spawned jobs carry them."""
    name = unique_name("cron111")
    sched = f"sched-{name}"
    q = Queue(name, port=server.port)
    try:
        q.pause()  # keep the spawned job visible in the backlog
        q.upsert_job_scheduler(
            sched,
            {"every": 60000, "immediately": True},
            {
                "name": "tick",
                "data": {"n": 1},
                "opts": {"priority": 7, "deduplication": {"id": f"dedup-{name}"}},
            },
        )
        assert wait_until(lambda: len(q.get_jobs()) >= 1, 15), "immediate spawn expected"
        job = q.get_jobs()[0]
        assert job.priority == 7, f"template priority must reach the spawned job: {job.priority}"
        # the scheduler stamps the dedup key on the job's uniqueKey (raw field)
        unique_key = job.raw.get("uniqueKey")
        assert unique_key == f"dedup-{name}", f"uniqueKey must reach the job: {unique_key}"
    finally:
        q.remove_job_scheduler(sched)
        q.obliterate()
        q.close()


# ---------------------------------------------- 0.1.2 move_to_failed exception
@test
def test_012_move_to_failed_persists_stack_and_unrecoverable(server: Server) -> None:
    """move_job_to_failed(exc) must send stack + unrecoverable on the FAIL
    wire (worker-path parity), not just the message; strings stay unchanged."""
    from bunqueue import UnrecoverableError

    name = unique_name("mtf")
    q = Queue(name, port=server.port)
    try:
        job = q.add("t", {}, attempts=5, backoff=50)
        pulled = q.connection.call({"cmd": "PULL", "queue": name, "owner": "e2e", "timeout": 1000})
        assert pulled.get("job"), "job must be pulled to fail it with a token"
        try:
            raise UnrecoverableError("terminal boom")
        except UnrecoverableError as exc:
            q.move_job_to_failed(job.id, exc, token=pulled.get("token"))
        assert wait_until(lambda: q.get_state(job.id) == "failed", 15), (
            "unrecoverable exception must skip the remaining retries"
        )
        fetched = q.get_job(job.id)
        assert fetched is not None and fetched.stacktrace, "stack must be persisted"
        assert any(
            "terminal boom" in line or "UnrecoverableError" in line for line in fetched.stacktrace
        ), fetched.stacktrace
    finally:
        q.obliterate()
        q.close()


# --------------------------------------------------- 0.1.2 H2 ACKB error settle
@test
def test_012_ack_batcher_settles_error_per_job(server: Server) -> None:
    """When the ACKB command fails, every buffered item must settle exactly
    once with the error (no completed, no starved callbacks)."""
    from bunqueue.ack_batcher import AckBatcher, AckItem

    class BoomConnection:
        def call(self, command: Dict[str, Any], timeout: Optional[float] = None) -> None:
            raise CommandError("boom")

    settled: list = []
    batcher = AckBatcher(BoomConnection(), max_size=10, max_delay_ms=60000)
    batcher.add(AckItem("1", "t1", None, lambda err: settled.append(err)))

    def throwing_callback(err: Optional[BaseException]) -> None:
        settled.append(err)
        raise RuntimeError("listener boom")  # must not starve item 3

    batcher.add(AckItem("2", "t2", None, throwing_callback))
    batcher.add(AckItem("3", "t3", {"r": 1}, lambda err: settled.append(err)))
    batcher.flush()
    assert len(settled) == 3, f"all items must settle exactly once, got {len(settled)}"
    assert all(isinstance(err, CommandError) for err in settled), settled
    batcher.flush()  # empty flush is a no-op
    assert len(settled) == 3


def run_standalone_audit_tests() -> int:
    """The audit tests that need their own fixtures (not the shared server)."""
    failed = 0
    failed += run_m3_cron_option_mapping()
    failed += run_h2_timeout_teardown()
    failed += run_h3_auth_race()
    return failed
