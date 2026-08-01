"""E2E: admin — schedulers, rate limits, configs, webhooks, monitoring, DLQ."""

from __future__ import annotations

import time

from harness import Server, test, unique_name, wait_until

from bunqueue import Queue, Worker


@test
def scheduler_every_spawns_jobs(server: Server) -> None:
    with Queue(unique_name("cron"), port=server.port) as queue:
        scheduler_id = unique_name("tick")
        queue.every(scheduler_id, 300, {"kind": "tick"})
        try:
            schedulers = queue.get_job_schedulers()
            assert any(s["name"] == scheduler_id for s in schedulers), f"missing: {schedulers}"
            single = queue.get_job_scheduler(scheduler_id)
            assert single is not None and single["queue"] == queue.name
            assert wait_until(lambda: queue.count() >= 1, 15), "cron did not spawn jobs"
        finally:
            queue.remove_job_scheduler(scheduler_id)
        count_after_delete = queue.count()
        time.sleep(1.0)
        assert queue.count() <= count_after_delete + 1, "scheduler kept firing after delete"


@test
def scheduler_cron_pattern(server: Server) -> None:
    with Queue(unique_name("cron2"), port=server.port) as queue:
        scheduler_id = unique_name("daily")
        queue.upsert_job_scheduler(
            scheduler_id,
            {"pattern": "0 9 * * *", "tz": "Europe/Rome"},
            {"name": "report", "data": {"type": "daily"}},
        )
        try:
            single = queue.get_job_scheduler(scheduler_id)
            assert single is not None
            assert single.get("schedule") == "0 9 * * *"
            assert single.get("nextRun", 0) > 0
            assert queue.get_job_schedulers_count() == 1
        finally:
            queue.remove_job_scheduler(scheduler_id)
        assert queue.get_job_scheduler(scheduler_id) is None
        assert queue.get_job_schedulers_count() == 0


@test
def dlq_flow_and_retry(server: Server) -> None:
    with Queue(unique_name("dlq"), port=server.port) as queue:
        job = queue.add("boom", {}, attempts=1)
        worker = Worker(
            queue.name,
            lambda j: (_ for _ in ()).throw(RuntimeError("dead")),
            port=server.port,
            poll_timeout_ms=300,
        )
        worker.start()
        try:
            assert wait_until(lambda: len(queue.get_dlq()) == 1, 15)
        finally:
            worker.close(timeout=10)
        entries = queue.get_dlq()
        assert entries[0].get("jobId") == job.id or entries[0].get("id") == job.id, entries
        retried = queue.retry_dlq()
        assert retried == 1
        assert queue.get_state(job.id) in ("waiting", "prioritized")
        assert len(queue.get_dlq()) == 0


@test
def purge_dlq(server: Server) -> None:
    with Queue(unique_name("dlqp"), port=server.port) as queue:
        queue.add("boom", {}, attempts=1)
        worker = Worker(
            queue.name,
            lambda j: (_ for _ in ()).throw(RuntimeError("dead")),
            port=server.port,
            poll_timeout_ms=300,
        )
        worker.start()
        try:
            assert wait_until(lambda: len(queue.get_dlq()) == 1, 15)
        finally:
            worker.close(timeout=10)
        assert queue.purge_dlq() == 1
        assert len(queue.get_dlq()) == 0


@test
def stall_and_dlq_config(server: Server) -> None:
    with Queue(unique_name("cfg"), port=server.port) as queue:
        queue.set_stall_config({"stallInterval": 20000, "maxStalls": 5, "gracePeriod": 3000})
        config = queue.get_stall_config()
        assert config.get("stallInterval") == 20000, config
        assert config.get("maxStalls") == 5

        queue.set_dlq_config({"autoRetry": True, "maxEntries": 500})
        dlq_config = queue.get_dlq_config()
        assert dlq_config.get("maxEntries") == 500, dlq_config


@test
def rate_limit_and_concurrency(server: Server) -> None:
    with Queue(unique_name("rate"), port=server.port) as queue:
        queue.set_global_rate_limit(100)
        queue.remove_global_rate_limit()
        queue.set_global_concurrency(2)
        queue.remove_global_concurrency()  # no exception = commands accepted


@test
def custom_rate_limit_duration_reaches_broker(server: Server) -> None:
    with Queue(unique_name("rate-window"), port=server.port) as queue:
        queue.set_global_rate_limit(7, 12345)
        response = queue.connection.call({"cmd": "GetQueueLimits", "queue": queue.name})
        limits = (response.get("data") or {}).get("limits") or {}
        rate_limit = limits.get("rateLimit") or {}
        assert rate_limit.get("duration") == 12345, rate_limit


@test
def global_concurrency_enforced(server: Server) -> None:
    with Queue(unique_name("gconc"), port=server.port) as queue:
        queue.set_global_concurrency(1)
        queue.add_bulk([{"name": "t", "data": {"i": i}} for i in range(4)])
        response = queue.connection.call(
            {"cmd": "PULLB", "queue": queue.name, "count": 4, "owner": "e2e", "lockTtl": 30000}
        )
        assert len(response.get("jobs") or []) <= 1, "global concurrency not enforced"
        queue.remove_global_concurrency()


@test
def webhooks_crud(server: Server) -> None:
    with Queue(unique_name("wh"), port=server.port) as queue:
        webhook_url = "https://example.com/bunqueue-hook"
        webhook_id = queue.add_webhook(webhook_url, ["job.completed"], queue=queue.name)
        hooks = queue.list_webhooks()
        assert any(h.get("url") == webhook_url for h in hooks), hooks
        assert webhook_id, "webhook id returned"
        queue.set_webhook_enabled(webhook_id, False)
        hooks = queue.list_webhooks()
        disabled = next(h for h in hooks if h.get("id") == webhook_id)
        assert disabled.get("enabled") is False, disabled
        queue.remove_webhook(webhook_id)
        hooks = queue.list_webhooks()
        assert not any(h.get("id") == webhook_id for h in hooks)


@test
def stats_metrics_queues(server: Server) -> None:
    with Queue(unique_name("mon"), port=server.port) as queue:
        queue.add("t", {})
        stats = queue.get_stats()
        assert "waiting" in stats and "uptime" in stats, stats
        metrics = queue.get_metrics()
        assert "totalPushed" in metrics, metrics
        queues = queue.list_queues()
        names = [q.get("name") if isinstance(q, dict) else q for q in queues]
        assert queue.name in names, queues
