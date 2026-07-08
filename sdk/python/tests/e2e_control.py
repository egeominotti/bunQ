"""E2E: control surface — delay/promote, moves, updates, clean, retry."""

from __future__ import annotations

import time

from harness import Server, test, unique_name, wait_until

from bunqueue import Queue, Worker


@test
def delayed_then_promote(server: Server) -> None:
    with Queue(unique_name("q"), port=server.port) as queue:
        job = queue.add("t", {}, delay=60000)
        assert queue.get_state(job.id) == "delayed"
        queue.promote_job(job.id)
        assert queue.get_state(job.id) in ("waiting", "prioritized")


@test
def promote_jobs_bulk(server: Server) -> None:
    with Queue(unique_name("q"), port=server.port) as queue:
        for i in range(3):
            queue.add("t", {"i": i}, delay=60000)
        # PromoteJobs scans persisted delayed jobs: wait for write-buffer flush
        assert wait_until(lambda: len(queue.get_delayed()) == 3, 10)
        promoted = queue.promote_jobs()
        assert promoted == 3, f"promoted {promoted}"
        assert queue.get_waiting_count() == 3


@test
def move_to_delayed_and_change_delay(server: Server) -> None:
    with Queue(unique_name("q"), port=server.port) as queue:
        job = queue.add("t", {})
        queue.move_job_to_delayed(job.id, 60000)
        assert queue.get_state(job.id) == "delayed"
        queue.change_job_delay(job.id, 1)
        assert wait_until(lambda: queue.get_state(job.id) in ("waiting", "prioritized"), 10)


@test
def update_data_and_priority(server: Server) -> None:
    with Queue(unique_name("q"), port=server.port) as queue:
        job = queue.add("t", {"v": 1})
        queue.update_job_data(job.id, {"name": "t", "v": 2})
        fetched = queue.get_job(job.id)
        assert fetched is not None and fetched.data["v"] == 2
        queue.change_job_priority(job.id, 99)
        fetched = queue.get_job(job.id)
        assert fetched is not None and fetched.priority == 99


@test
def remove_and_discard(server: Server) -> None:
    with Queue(unique_name("q"), port=server.port) as queue:
        job = queue.add("t", {})
        queue.remove(job.id)
        assert queue.get_job(job.id) is None or queue.get_state(job.id) not in (
            "waiting",
            "prioritized",
        )
        assert queue.count() == 0


@test
def drain_removes_waiting(server: Server) -> None:
    with Queue(unique_name("q"), port=server.port) as queue:
        queue.add_bulk([{"name": "t", "data": {"i": i}} for i in range(10)])
        assert queue.count() == 10
        queue.drain()
        assert queue.count() == 0


@test
def obliterate_wipes_queue(server: Server) -> None:
    with Queue(unique_name("q"), port=server.port) as queue:
        queue.add_bulk([{"name": "t", "data": {}} for _ in range(5)])
        queue.pause()
        queue.obliterate()
        assert queue.count() == 0
        assert queue.is_paused() is False  # obliterate resets state


@test
def retry_failed_job(server: Server) -> None:
    with Queue(unique_name("q"), port=server.port) as queue:
        job = queue.add("boom", {}, attempts=1)
        worker = Worker(
            queue.name,
            lambda j: (_ for _ in ()).throw(RuntimeError("nope")),
            port=server.port,
            poll_timeout_ms=500,
        )
        worker.start()
        try:
            assert wait_until(lambda: queue.get_state(job.id) == "failed", 15)
        finally:
            worker.close(timeout=10)
        # failed -> waiting (BullMQ retry contract)
        queue.retry_job(job.id)
        assert queue.get_state(job.id) in ("waiting", "prioritized")


@test
def clean_completed(server: Server) -> None:
    with Queue(unique_name("q"), port=server.port) as queue:
        done = []
        worker = Worker(queue.name, lambda j: "ok", port=server.port, poll_timeout_ms=500)
        worker.on("completed", lambda j, r: done.append(j.id))
        worker.start()
        try:
            for i in range(4):
                queue.add("t", {"i": i})
            assert wait_until(lambda: len(done) == 4, 15)
        finally:
            worker.close(timeout=10)
        time.sleep(0.2)
        removed = queue.clean(0, state="completed")
        assert removed == 4, f"cleaned {removed}"


@test
def lifo_ordering(server: Server) -> None:
    """LIFO applies among lifo jobs: newest first (mixed falls back to FIFO)."""
    with Queue(unique_name("q"), port=server.port) as queue:
        queue.add("first", {"o": 1}, lifo=True)
        queue.add("second", {"o": 2}, lifo=True)
        pulled = queue.connection.call(
            {"cmd": "PULL", "queue": queue.name, "owner": "e2e", "timeout": 1000}
        )
        assert pulled["job"]["data"]["o"] == 2, f"lifo not respected: {pulled['job']['data']}"
