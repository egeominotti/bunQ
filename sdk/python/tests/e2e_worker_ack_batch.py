"""E2E: Worker ACK batching, close-time flush, and registration."""

from __future__ import annotations

import time

from harness import Server, test, unique_name, wait_until

from bunqueue import Queue, Worker


@test
def ack_batching_completes_jobs(server: Server) -> None:
    """Opt-in ACKB batching: all jobs complete, per-job completed events fire."""
    with Queue(unique_name("q"), port=server.port) as queue:
        completed = []
        queue.add_bulk([{"name": "t", "data": {"i": i}} for i in range(20)])
        worker = Worker(
            queue.name,
            lambda j: j.data["i"] * 2,
            port=server.port,
            concurrency=4,
            poll_timeout_ms=300,
            ack_batch={"max_size": 5, "max_delay_ms": 20},
        )
        worker.on("completed", lambda j, r: completed.append(r))
        try:
            assert wait_until(lambda: queue.get_job_counts().get("completed", 0) == 20, 30)
            assert wait_until(lambda: len(completed) == 20, 10), f"events: {len(completed)}"
        finally:
            worker.close(timeout=10)
        assert sorted(completed) == [i * 2 for i in range(20)]
        jobs = queue.get_jobs("completed", 0, 25)
        assert len(jobs) == 20


@test
def ack_batching_suppresses_only_timed_out_position(server: Server) -> None:
    with Queue(unique_name("q"), port=server.port) as queue:
        completed = []
        errors = []

        def process(job):
            time.sleep(0.5)
            return job.data["kind"]

        worker = Worker(
            queue.name,
            process,
            port=server.port,
            concurrency=2,
            poll_timeout_ms=300,
            heartbeat_interval_s=0.05,
            ack_batch={"max_size": 2, "max_delay_ms": 60_000},
        )
        worker.on("completed", lambda job, result: completed.append((job.id, result)))
        worker.on("error", errors.append)
        worker.start()
        try:
            timed = queue.add("timed", {"kind": "timed"}, attempts=1, timeout=100)
            live = queue.add("live", {"kind": "live"}, attempts=1, timeout=2_000)
            assert wait_until(
                lambda: queue.get_state(timed.id) == "failed"
                and queue.get_state(live.id) == "completed",
                15,
            )
            assert completed == [(live.id, "live")]
            assert errors == []
        finally:
            worker.close(timeout=10)


@test
def ack_batching_flushes_on_close(server: Server) -> None:
    """A partial batch (below max_size, long max_delay) must flush on close."""
    with Queue(unique_name("q"), port=server.port) as queue:
        job = queue.add("t", {"v": 7})
        worker = Worker(
            queue.name,
            lambda j: j.data["v"],
            port=server.port,
            poll_timeout_ms=300,
            ack_batch={"max_size": 100, "max_delay_ms": 60000},
        )
        try:
            batcher = worker._ack_batcher
            assert batcher is not None
            assert wait_until(lambda: len(batcher._buffer) == 1, 15), "ACK must be buffered"
            assert queue.get_state(job.id) == "active", "job stays active until the batch settles"
        finally:
            assert worker.close(timeout=10) is True
        assert wait_until(lambda: queue.get_state(job.id) == "completed", 10), (
            "close() must flush buffered ACKs"
        )
        assert queue.get_result(job.id) == 7


@test
def worker_registration_visible(server: Server) -> None:
    with Queue(unique_name("q"), port=server.port) as queue:
        worker = Worker(
            queue.name,
            lambda j: "ok",
            port=server.port,
            name="e2e-worker",
            poll_timeout_ms=300,
        )
        worker.start()
        worker.wait_until_ready()
        try:
            workers = queue.get_workers()
            assert any(w.get("name") == "e2e-worker" for w in workers), f"not registered: {workers}"
        finally:
            worker.close(timeout=10)
