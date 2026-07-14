"""E2E: edge cases — payload limits, unicode, pipelining, bulk frames,
isolation, empty long-poll, stable pagination, crash + restart reconnect,
idempotent custom ids, per-job results under concurrency, option extremes.

Mirrors the TypeScript SDK's e2e-edge.ts so both SDKs prove the same wire
behavior at the boundaries.
"""

from __future__ import annotations

import threading
import time

from harness import Server, test, unique_name, wait_until

from bunqueue import Queue, Worker


@test
def edge_2mb_payload_roundtrip(server: Server) -> None:
    with Queue(unique_name("big"), port=server.port) as queue:
        blob = "x" * (2 * 1024 * 1024)
        job = queue.add("blob", {"blob": blob})
        fetched = queue.get_job(job.id)
        assert fetched is not None and fetched.data.get("blob") == blob, "2MB payload must survive"


@test
def edge_unicode_nested_payload(server: Server) -> None:
    with Queue(unique_name("uni"), port=server.port) as queue:
        payload = {
            "emoji": "🚀🔥💯",
            "cjk": "以呂波耳本部止",
            "rtl": "مرحبا بالعالم",
            "nested": {"list": [1, "två", {"deep": "ключ"}], "null": None, "bool": True},
        }
        job = queue.add("uni", payload)
        fetched = queue.get_job(job.id)
        assert fetched is not None
        for key in ("emoji", "cjk", "rtl"):
            assert fetched.data.get(key) == payload[key], f"{key} corrupted by msgpack roundtrip"
        assert fetched.data.get("nested", {}).get("list", [None])[-1] == {"deep": "ключ"}


@test
def edge_200_concurrent_adds_pipeline(server: Server) -> None:
    """200 adds from 8 threads pipeline on one socket without cross-talk."""
    with Queue(unique_name("pipe"), port=server.port) as queue:
        ids: list = []
        lock = threading.Lock()

        def producer(base: int) -> None:
            for i in range(25):
                job = queue.add("evt", {"n": base + i})
                with lock:
                    ids.append(job.id)

        threads = [threading.Thread(target=producer, args=(t * 25,)) for t in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert len(ids) == 200 and len(set(ids)) == 200, "every add must get a unique id"
        assert wait_until(lambda: queue.count() == 200, 15)


@test
def edge_add_bulk_1000_one_frame(server: Server) -> None:
    with Queue(unique_name("bulk"), port=server.port) as queue:
        ids = queue.add_bulk([{"name": "evt", "data": {"i": i}} for i in range(1000)])
        assert len(ids) == 1000 and len(set(ids)) == 1000
        assert wait_until(lambda: queue.count() == 1000, 15)


@test
def edge_multi_queue_isolation(server: Server) -> None:
    with Queue(unique_name("iso-a"), port=server.port) as qa:
        with Queue(unique_name("iso-b"), port=server.port) as qb:
            qa.add("a", {"q": "a"})
            qb.add("b", {"q": "b"})
            assert wait_until(lambda: qa.count() == 1 and qb.count() == 1, 10)
            done: list = []
            worker = Worker(
                qa.name, lambda j: done.append(j.data["q"]) or "ok", port=server.port, poll_timeout_ms=300
            )
            try:
                worker.start()
                assert wait_until(lambda: len(done) == 1, 15)
                time.sleep(0.5)
                assert done == ["a"], f"worker on queue A must never see queue B: {done}"
                assert qb.count() == 1, "queue B untouched"
            finally:
                worker.close(timeout=10)


@test
def edge_empty_long_poll_returns_no_jobs(server: Server) -> None:
    with Queue(unique_name("empty"), port=server.port) as queue:
        started = time.monotonic()
        response = queue.connection.call(
            {"cmd": "PULLB", "queue": queue.name, "count": 1, "timeout": 800, "owner": "edge-test"}
        )
        elapsed = time.monotonic() - started
        assert not response.get("jobs"), "empty queue must yield no jobs"
        assert elapsed >= 0.5, f"long-poll must actually wait, returned in {elapsed:.2f}s"


@test
def edge_get_jobs_pagination_stable(server: Server) -> None:
    with Queue(unique_name("page"), port=server.port) as queue:
        queue.add_bulk([{"name": "evt", "data": {"i": i}} for i in range(30)])
        assert wait_until(lambda: len(queue.get_jobs(["waiting"], 0, 30)) == 30, 15)
        page1 = queue.get_jobs(["waiting"], 0, 10)
        page2 = queue.get_jobs(["waiting"], 10, 20)
        page3 = queue.get_jobs(["waiting"], 20, 30)
        ids = [j.id for j in page1 + page2 + page3]
        assert len(ids) == 30 and len(set(ids)) == 30, "pages must not overlap or skip"


@test
def edge_worker_survives_server_crash_restart(server: Server) -> None:
    """Kill the server mid-run, restart on the same port: producer and worker
    reconnect on their own and drain the remaining jobs (zero manual steps)."""
    own = Server()
    own.start()
    try:
        queue = Queue(unique_name("crash"), port=own.port)
        done: set = set()
        worker = Worker(queue.name, lambda j: done.add(j.data["i"]) or "ok", port=own.port, poll_timeout_ms=300)
        worker.on("error", lambda e: None)  # reconnect noise expected
        worker.start()
        for i in range(5):
            queue.add("evt", {"i": i}, attempts=3)
        assert wait_until(lambda: len(done) == 5, 15)

        assert own.proc is not None
        own.proc.kill()
        own.proc.wait(timeout=10)
        own.proc = None
        time.sleep(0.5)
        own.start()  # same port, same data dir

        for i in range(5, 10):
            queue.add("evt", {"i": i}, attempts=3)
        assert wait_until(lambda: len(done) == 10, 30), f"drained only {sorted(done)}"
        worker.close(timeout=10)
        queue.close()
    finally:
        own.stop()


@test
def edge_duplicate_custom_ids_idempotent(server: Server) -> None:
    with Queue(unique_name("dup"), port=server.port) as queue:
        queue.add_bulk(
            [
                {"name": "evt", "data": {"v": 1}, "job_id": "edge-dup-1"},
                {"name": "evt", "data": {"v": 2}, "job_id": "edge-dup-1"},
                {"name": "evt", "data": {"v": 3}, "job_id": "edge-dup-2"},
            ]
        )
        assert wait_until(lambda: queue.count() == 2, 15), "duplicate custom id must dedupe"
        first = queue.get_job_by_custom_id("edge-dup-1")
        assert first is not None and first.data.get("v") == 1, "first write wins on dedupe"


@test
def edge_results_preserved_per_job_under_concurrency(server: Server) -> None:
    with Queue(unique_name("res"), port=server.port) as queue:
        worker = Worker(
            queue.name, lambda j: {"double": j.data["i"] * 2}, port=server.port, concurrency=8, poll_timeout_ms=300
        )
        try:
            worker.start()
            jobs = [queue.add("calc", {"i": i}) for i in range(20)]
            assert wait_until(
                lambda: queue.get_job_counts().get("completed", 0) >= 20, 20
            )
            for i, job in enumerate(jobs):
                assert queue.get_result(job.id) == {"double": i * 2}, f"result mixed up for job {i}"
        finally:
            worker.close(timeout=10)


@test
def edge_batch_size_zero_clamps_to_one(server: Server) -> None:
    with Queue(unique_name("bz"), port=server.port) as queue:
        done: list = []
        worker = Worker(
            queue.name, lambda j: done.append(j.id) or "ok", port=server.port, batch_size=0, poll_timeout_ms=300
        )
        try:
            assert worker.batch_size == 1, f"batch_size 0 must clamp to 1, got {worker.batch_size}"
            worker.start()
            queue.add("t", {"x": 1})
            assert wait_until(lambda: len(done) >= 1, 15)
        finally:
            worker.close(timeout=10)


@test
def edge_per_job_stack_trace_limit_honored(server: Server) -> None:
    """A job asking for a deeper stack (stack_trace_limit=30) must get more
    than the 10-line default AND keep the raise site."""
    with Queue(unique_name("stk30"), port=server.port) as queue:

        def deep(n: int) -> int:
            if n <= 0:
                raise RuntimeError("BOOM-DEEP-30")
            return deep(n - 1)

        worker = Worker(queue.name, lambda j: deep(25), port=server.port, poll_timeout_ms=300)
        try:
            worker.start()
            job = queue.add("t", {}, attempts=1, stack_trace_limit=30)
            assert wait_until(lambda: queue.get_state(job.id) == "failed", 20)
            failed = queue.get_job(job.id)
            assert failed is not None
            stack = failed.stacktrace or []
            assert len(stack) > 10, f"per-job stack_trace_limit ignored: {len(stack)} lines"
            assert any("BOOM-DEEP-30" in line for line in stack), "raise site truncated away"
        finally:
            worker.close(timeout=10)
