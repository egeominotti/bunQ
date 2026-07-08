"""E2E: query surface — custom ids, states, counts, logs, progress, children."""

from __future__ import annotations

import time

from harness import Server, test, unique_name

from bunqueue import CommandError, Queue


@test
def custom_id_idempotent(server: Server) -> None:
    with Queue(unique_name("q"), port=server.port) as queue:
        first = queue.add("task", {"v": 1}, job_id="my-custom-id")
        second = queue.add("task", {"v": 2}, job_id="my-custom-id")
        assert first.id == second.id  # idempotent re-add returns same job
        fetched = queue.get_job_by_custom_id("my-custom-id")
        assert fetched is not None and fetched.id == first.id


@test
def get_jobs_by_state(server: Server) -> None:
    from harness import wait_until

    with Queue(unique_name("q"), port=server.port) as queue:
        queue.add_bulk([{"name": "t", "data": {"i": i}} for i in range(5)])
        queue.add("later", {}, delay=60000)
        # GetJobs reads persisted rows: allow the 10ms write buffer to flush
        assert wait_until(lambda: len(queue.get_waiting()) == 5, 10), queue.get_waiting()
        assert wait_until(lambda: len(queue.get_delayed()) == 1, 10)
        assert queue.get_waiting_count() == 5
        assert queue.get_delayed_count() == 1
        both = queue.get_jobs(["waiting", "delayed"])
        assert len(both) == 6, f"multi-state returned {len(both)}"


@test
def counts_per_priority(server: Server) -> None:
    with Queue(unique_name("q"), port=server.port) as queue:
        queue.add("a", {}, priority=1)
        queue.add("b", {}, priority=1)
        queue.add("c", {}, priority=5)
        counts = queue.get_counts_per_priority()
        assert counts, f"empty counts: {counts}"
        total = sum(int(v) for v in counts.values())
        assert total == 3


@test
def progress_roundtrip(server: Server) -> None:
    """Progress requires the job to be active: pull it first."""
    with Queue(unique_name("q"), port=server.port) as queue:
        job = queue.add("t", {})
        queue.connection.call({"cmd": "PULL", "queue": queue.name, "owner": "e2e", "timeout": 1000})
        queue.update_job_progress(job.id, 42, "almost")
        progress = queue.get_progress(job.id)
        assert progress["progress"] == 42
        assert progress["message"] == "almost"


@test
def job_logs(server: Server) -> None:
    with Queue(unique_name("q"), port=server.port) as queue:
        job = queue.add("t", {})
        queue.add_job_log(job.id, "step one")
        queue.add_job_log(job.id, "step two")
        logs = queue.get_job_logs(job.id)
        assert len(logs) == 2, f"expected 2 logs, got {logs}"
        queue.clear_job_logs(job.id)
        assert len(queue.get_job_logs(job.id)) == 0


@test
def tags_and_group_roundtrip(server: Server) -> None:
    with Queue(unique_name("q"), port=server.port) as queue:
        job = queue.add("t", {"x": 1}, tags=["alpha", "beta"], group_id="g1", priority=7)
        fetched = queue.get_job(job.id)
        assert fetched is not None
        assert fetched.tags == ["alpha", "beta"]
        assert fetched.group_id == "g1"
        assert fetched.priority == 7
        assert fetched.name == "t"


@test
def wait_for_job_returns_result(server: Server) -> None:
    from bunqueue import Worker

    with Queue(unique_name("q"), port=server.port) as queue:
        worker = Worker(queue.name, lambda j: {"answer": 42}, port=server.port, poll_timeout_ms=500)
        worker.start()
        try:
            job = queue.add("t", {})
            result = queue.wait_for_job(job.id, timeout_ms=10000)
            assert result == {"answer": 42}, f"got {result}"
            job2 = queue.add("t2", {})
            assert queue.wait_job_until_finished(job2.id, ttl_ms=10000) == {"answer": 42}
        finally:
            worker.close(timeout=10)


@test
def nonexistent_job_state(server: Server) -> None:
    with Queue(unique_name("q"), port=server.port) as queue:
        try:
            state = queue.get_state("no-such-id")
            assert state in ("unknown", "None", "not_found"), f"unexpected state {state}"
        except CommandError:
            pass  # error response is acceptable too
