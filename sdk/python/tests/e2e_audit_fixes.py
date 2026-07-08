"""E2E repro tests for the SDK-audit fixes (RED before the fix, GREEN after).

Covers the confirmed findings: H1 addBulk custom-id, H2 keepalive + command
timeout teardown, H3 auth-first ordering under concurrency, H4 getFlow
not-found, M1 waitForJob timeout, M3 cron job-option mapping.
"""

from __future__ import annotations

import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from harness import Server, test, unique_name, wait_until

from bunqueue import Connection, FlowProducer, Queue, Worker
from bunqueue.errors import BunqueueError, CommandError, CommandTimeoutError


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


def run_standalone_audit_tests() -> int:
    """The audit tests that need their own fixtures (not the shared server)."""
    failed = 0
    failed += run_m3_cron_option_mapping()
    failed += run_h2_timeout_teardown()
    failed += run_h3_auth_race()
    return failed
