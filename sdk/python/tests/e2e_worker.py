"""E2E: worker — events, concurrency, pause/resume, unrecoverable, locks."""

from __future__ import annotations

import threading
import time

from harness import Server, test, unique_name, wait_until

from bunqueue import Queue, UnrecoverableError, Worker


@test
def worker_events_lifecycle(server: Server) -> None:
    with Queue(unique_name("q"), port=server.port) as queue:
        seen = {"ready": 0, "active": [], "completed": [], "drained": 0, "closed": 0}
        worker = Worker(queue.name, lambda j: j.data["v"] * 2, port=server.port, poll_timeout_ms=300)
        worker.on("ready", lambda: seen.__setitem__("ready", seen["ready"] + 1))
        worker.on("active", lambda j: seen["active"].append(j.id))
        worker.on("completed", lambda j, r: seen["completed"].append((j.id, r)))
        worker.on("drained", lambda: seen.__setitem__("drained", seen["drained"] + 1))
        worker.on("closed", lambda: seen.__setitem__("closed", seen["closed"] + 1))
        worker.start()
        worker.wait_until_ready()
        assert seen["ready"] == 1

        job = queue.add("t", {"v": 21})
        assert wait_until(lambda: len(seen["completed"]) == 1, 15)
        assert seen["completed"][0] == (job.id, 42)
        assert job.id in seen["active"]
        assert wait_until(lambda: seen["drained"] >= 1, 10)

        worker.close(timeout=10)
        assert seen["closed"] == 1
        assert worker.is_closed()


@test
def concurrency_respected(server: Server) -> None:
    with Queue(unique_name("q"), port=server.port) as queue:
        peak = {"now": 0, "max": 0}
        lock = threading.Lock()

        def process(job):
            with lock:
                peak["now"] += 1
                peak["max"] = max(peak["max"], peak["now"])
            time.sleep(0.15)
            with lock:
                peak["now"] -= 1
            return "ok"

        queue.add_bulk([{"name": "t", "data": {"i": i}} for i in range(12)])
        worker = Worker(queue.name, process, port=server.port, concurrency=3, poll_timeout_ms=300)
        worker.start()
        try:
            assert wait_until(lambda: queue.get_job_counts().get("completed", 0) == 12, 30)
        finally:
            worker.close(timeout=10)
        assert peak["max"] <= 3, f"concurrency exceeded: {peak['max']}"
        assert peak["max"] >= 2, f"no parallelism observed: {peak['max']}"


@test
def pause_and_resume_worker(server: Server) -> None:
    with Queue(unique_name("q"), port=server.port) as queue:
        done = []
        worker = Worker(queue.name, lambda j: done.append(j.id), port=server.port, poll_timeout_ms=300)
        worker.start()
        worker.wait_until_ready()
        worker.pause()
        assert worker.is_paused()
        time.sleep(0.5)  # let in-flight polls settle
        queue.add("t", {})
        time.sleep(1.0)
        assert len(done) == 0, "paused worker must not process"
        worker.resume()
        assert wait_until(lambda: len(done) == 1, 15)
        worker.close(timeout=10)


@test
def unrecoverable_error_skips_retries(server: Server) -> None:
    with Queue(unique_name("q"), port=server.port) as queue:
        attempts = []

        def process(job):
            attempts.append(1)
            raise UnrecoverableError("fatal, do not retry")

        job = queue.add("t", {}, attempts=5, backoff=50)
        worker = Worker(queue.name, process, port=server.port, poll_timeout_ms=300)
        worker.start()
        try:
            assert wait_until(lambda: queue.get_state(job.id) == "failed", 15)
            time.sleep(1.0)  # would-be retry window
            assert len(attempts) == 1, f"retried {len(attempts)} times despite unrecoverable"
        finally:
            worker.close(timeout=10)


@test
def normal_error_retries(server: Server) -> None:
    with Queue(unique_name("q"), port=server.port) as queue:
        attempts = []

        def process(job):
            attempts.append(job.attempts)
            if len(attempts) < 3:
                raise RuntimeError("transient")
            return "recovered"

        job = queue.add("t", {}, attempts=5, backoff=100)
        worker = Worker(queue.name, process, port=server.port, poll_timeout_ms=300)
        worker.start()
        try:
            assert wait_until(lambda: queue.get_state(job.id) == "completed", 20)
        finally:
            worker.close(timeout=10)
        assert len(attempts) == 3
        assert queue.get_result(job.id) == "recovered"


@test
def progress_event_and_stack_on_failure(server: Server) -> None:
    with Queue(unique_name("q"), port=server.port) as queue:
        progress_events = []

        def process(job):
            job.update_progress(50, "halfway")
            raise ValueError("with stack")

        job = queue.add("t", {}, attempts=1)
        worker = Worker(queue.name, process, port=server.port, poll_timeout_ms=300)
        worker.on("progress", lambda j, p: progress_events.append((j.id, p)))
        worker.start()
        try:
            assert wait_until(lambda: queue.get_state(job.id) == "failed", 15)
        finally:
            worker.close(timeout=10)
        assert (job.id, 50) in progress_events
        fetched = queue.get_job(job.id)
        assert fetched is not None and fetched.stacktrace, "stacktrace not persisted"
        assert any("with stack" in line or "ValueError" in line for line in fetched.stacktrace)


@test
def long_job_survives_lock_ttl(server: Server) -> None:
    """Job longer than lockTtl must survive via JobHeartbeatB renewal."""
    with Queue(unique_name("q"), port=server.port) as queue:
        job = queue.add("slow", {}, attempts=1)

        def process(j):
            time.sleep(3.5)
            return "survived"

        worker = Worker(
            queue.name,
            process,
            port=server.port,
            poll_timeout_ms=300,
            lock_ttl_ms=1500,
            heartbeat_interval_s=0.5,
        )
        worker.start()
        try:
            assert wait_until(lambda: queue.get_state(job.id) == "completed", 20)
            assert queue.get_result(job.id) == "survived"
        finally:
            worker.close(timeout=10)


@test
def cooperative_cancel(server: Server) -> None:
    """cancel_job marks a local active job; the processor aborts cooperatively."""
    with Queue(unique_name("q"), port=server.port) as queue:
        holder = {}
        cancelled_events = []

        def process(job):
            for _ in range(200):
                if holder["worker"].is_job_cancelled(job.id):
                    raise RuntimeError("aborted by cancel")
                time.sleep(0.05)
            return "never"

        job = queue.add("long", {}, attempts=1)
        worker = Worker(queue.name, process, port=server.port, poll_timeout_ms=300)
        holder["worker"] = worker
        worker.on("cancelled", lambda data: cancelled_events.append(data))

        assert wait_until(lambda: queue.get_state(job.id) == "active", 15)
        assert worker.cancel_job(job.id, "user asked") is True
        assert worker.cancel_job("not-active-id") is False
        assert wait_until(lambda: queue.get_state(job.id) == "failed", 15)
        worker.close(timeout=10)

        assert cancelled_events and cancelled_events[0]["jobId"] == job.id
        assert "aborted by cancel" in str(queue.get_dlq())


@test
def worker_registration_visible(server: Server) -> None:
    with Queue(unique_name("q"), port=server.port) as queue:
        worker = Worker(queue.name, lambda j: "ok", port=server.port, name="e2e-worker", poll_timeout_ms=300)
        worker.start()
        worker.wait_until_ready()
        try:
            workers = queue.get_workers()
            assert any(w.get("name") == "e2e-worker" for w in workers), f"not registered: {workers}"
        finally:
            worker.close(timeout=10)
