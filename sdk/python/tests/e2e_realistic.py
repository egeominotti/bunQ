"""E2E: realistic production scenarios against a real server.

A flaky order pipeline with retries and poison jobs landing in the DLQ
(zero-loss accounting), a limit-bounded Simple Mode interval digest, a
batched-ACK throughput worker, and a long job that outlives its lock TTL
thanks to heartbeat lock renewal. The invoice burst verifies that concurrent
processing never crosses persisted results.
"""

from __future__ import annotations

import threading
import time

from harness import Server, test, unique_name, wait_until

from bunqueue import Bunqueue, Queue, UnrecoverableError, Worker


@test
def realistic_order_pipeline_retries_poison_dlq(server: Server) -> None:
    with Queue(unique_name("orders"), port=server.port) as queue:
        invocations: dict = {}
        completed_ids: set = set()
        lock = threading.Lock()

        def process(job):
            with lock:
                n = invocations.get(job.id, 0) + 1
                invocations[job.id] = n
            if job.data.get("poison"):
                raise UnrecoverableError("malformed order payload")
            if job.data.get("i", 1) % 10 == 0 and n == 1:
                raise RuntimeError("transient upstream glitch")
            return {"charged": True}

        worker = Worker(queue.name, process, port=server.port, concurrency=8, poll_timeout_ms=300)
        worker.on("completed", lambda j, r: completed_ids.add(j.id))
        worker.on("failed", lambda j, e: None)  # poison + transient failures expected
        worker.on("error", lambda e: None)
        try:
            worker.start()
            queue.add_bulk(
                [
                    {"name": "order", "data": {"i": i}, "attempts": 3, "backoff": 50}
                    for i in range(100)
                ]
            )
            for _ in range(3):
                queue.add("order", {"poison": True}, attempts=3, backoff=50)
            assert wait_until(lambda: len(completed_ids) >= 100, 60), (
                f"only {len(completed_ids)}/100 orders completed"
            )
            assert wait_until(lambda: len(queue.get_dlq()) >= 3, 30), "poison must reach the DLQ"
            assert len(completed_ids) == 100, "every order completed (and only once: id set)"
            assert len(queue.get_dlq()) == 3, "exactly the 3 poison orders in the DLQ"
            retried = sum(1 for n in invocations.values() if n > 1)
            assert retried >= 10, f"transient glitches really were retried (got {retried})"
        finally:
            worker.close(timeout=15)


@test
def realistic_simple_limit_bounded_digest(server: Server) -> None:
    fired = {"n": 0}

    def process(job):
        fired["n"] += 1
        return "sent"

    app = Bunqueue(unique_name("digest"), port=server.port, poll_timeout_ms=300, processor=process)
    sched_id = unique_name("dig")
    try:
        app.every(sched_id, 300, {"kind": "digest"}, limit=3)
        assert wait_until(lambda: fired["n"] >= 3, 20), f"digest fired {fired['n']}/3 times"
        time.sleep(1.5)  # ~5 more fires would land here without the limit
        assert fired["n"] == 3, f"scheduler must stop at its limit, fired {fired['n']}"
    finally:
        try:
            app.remove_cron(sched_id)
        except Exception:
            pass  # limit-reached crons are removed server-side
        app.close()


@test
def realistic_batched_ack_throughput(server: Server) -> None:
    """400-job burst through a worker with ACKB batching: zero loss."""
    with Queue(unique_name("burst"), port=server.port) as queue:
        completed = {"n": 0}
        errors: list = []
        worker = Worker(
            queue.name,
            lambda j: "ok",
            port=server.port,
            concurrency=32,
            batch_size=100,
            poll_timeout_ms=300,
            ack_batch={"max_size": 50, "max_delay_ms": 5},
        )
        worker.on("completed", lambda j, r: completed.__setitem__("n", completed["n"] + 1))
        worker.on("error", lambda e: errors.append(str(e)))
        try:
            worker.start()
            for off in range(0, 400, 200):
                queue.add_bulk([{"name": "evt", "data": {"i": i}} for i in range(off, off + 200)])
            assert wait_until(lambda: completed["n"] >= 400, 60), (
                f"only {completed['n']}/400 drained"
            )
            assert completed["n"] == 400, f"exactly 400 completions, got {completed['n']}"
            assert not errors, f"no worker errors expected, got: {errors[:3]}"
        finally:
            worker.close(timeout=15)


@test
def realistic_heartbeat_keeps_long_job_leased(server: Server) -> None:
    with Queue(unique_name("long"), port=server.port) as queue:
        invocations = {"n": 0}

        def crunch(job):
            invocations["n"] += 1
            time.sleep(5.5)  # far beyond the 2s lock TTL
            return "done"

        worker = Worker(
            queue.name,
            crunch,
            port=server.port,
            poll_timeout_ms=300,
            lock_ttl_ms=2000,
            heartbeat_interval_s=1.0,
        )
        try:
            worker.start()
            job = queue.add("crunch", {"size": "xl"}, attempts=3)
            assert wait_until(lambda: queue.get_state(job.id) == "completed", 30)
            assert invocations["n"] == 1, (
                f"lock renewal must prevent a stall retry, ran {invocations['n']} times"
            )
        finally:
            worker.close(timeout=15)


@test
def realistic_concurrent_invoice_results_are_not_crossed(server: Server) -> None:
    with Queue(unique_name("invoices"), port=server.port) as queue:
        worker = Worker(
            queue.name,
            lambda job: {
                "invoice": job.data["invoice"],
                "total": job.data["cents"] * 2,
            },
            port=server.port,
            concurrency=12,
            batch_size=32,
            poll_timeout_ms=300,
        )
        try:
            worker.start()
            jobs = queue.add_bulk(
                [
                    {
                        "name": "reconcile",
                        "data": {"invoice": invoice, "cents": 101 + invoice},
                    }
                    for invoice in range(64)
                ]
            )
            assert wait_until(
                lambda: queue.get_job_counts().get("completed") == len(jobs), 30
            ), "the invoice burst did not fully complete"

            checksum = 0
            for invoice, job_id in enumerate(jobs):
                result = queue.get_result(job_id)
                assert result["invoice"] == invoice, "result crossed between invoices"
                assert result["total"] == (101 + invoice) * 2, "invoice amount changed"
                checksum += result["total"]
            assert checksum == 16_960, "not every persisted result was counted exactly once"
        finally:
            worker.close(timeout=15)
