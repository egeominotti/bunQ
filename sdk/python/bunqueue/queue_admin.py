"""Admin operations mixin for Queue: DLQ, configs, rate limits, schedulers,
webhooks, monitoring. Mirrors the TS client's management surface (TCP mode)."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from .connection import _compact
from .errors import CommandError


class QueueAdminOps:
    """Mixin: requires ``self.name`` and ``self.connection``."""

    # ------------------------------------------------------------------- dlq

    def get_dlq(self, count: Optional[int] = None) -> List[Dict[str, Any]]:
        response = self.connection.call(_compact({"cmd": "Dlq", "queue": self.name, "count": count}))
        return list(response.get("jobs") or response.get("data") or [])

    def retry_dlq(self, job_id: Optional[str] = None, count: Optional[int] = None) -> int:
        response = self.connection.call(
            _compact({"cmd": "RetryDlq", "queue": self.name, "jobId": job_id, "count": count})
        )
        return int(response.get("count", 0))

    def purge_dlq(self) -> int:
        response = self.connection.call({"cmd": "PurgeDlq", "queue": self.name})
        return int(response.get("count", 0))

    def set_dlq_config(self, config: Dict[str, Any]) -> None:
        self.connection.call({"cmd": "SetDlqConfig", "queue": self.name, "config": config})

    def get_dlq_config(self) -> Dict[str, Any]:
        response = self.connection.call({"cmd": "GetDlqConfig", "queue": self.name})
        return dict(response.get("config") or response.get("data") or {})

    # ----------------------------------------------------------------- stall

    def set_stall_config(self, config: Dict[str, Any]) -> None:
        self.connection.call({"cmd": "SetStallConfig", "queue": self.name, "config": config})

    def get_stall_config(self) -> Dict[str, Any]:
        response = self.connection.call({"cmd": "GetStallConfig", "queue": self.name})
        return dict(response.get("config") or response.get("data") or {})

    # ------------------------------------------------- rate limit/concurrency

    def set_global_rate_limit(self, max_jobs: int, duration_ms: Optional[int] = None) -> None:
        """`duration_ms` accepted for TS-signature parity; the wire carries only `limit`."""
        del duration_ms
        self.connection.call({"cmd": "RateLimit", "queue": self.name, "limit": max_jobs})

    def remove_global_rate_limit(self) -> None:
        self.connection.call({"cmd": "RateLimitClear", "queue": self.name})

    def set_global_concurrency(self, concurrency: int) -> None:
        self.connection.call({"cmd": "SetConcurrency", "queue": self.name, "limit": concurrency})

    def remove_global_concurrency(self) -> None:
        self.connection.call({"cmd": "ClearConcurrency", "queue": self.name})

    # -------------------------------------------------------------- scheduler

    def upsert_job_scheduler(
        self,
        scheduler_id: str,
        repeat: Dict[str, Any],
        template: Optional[Dict[str, Any]] = None,
    ) -> None:
        """BullMQ-style scheduler.

        ``repeat``: {"pattern": cron, "every": ms, "tz": timezone,
        "immediately": bool, "limit": n}. ``template``: {"name", "data", "opts"}.
        """
        template = template or {}
        data: Dict[str, Any] = {"name": template.get("name", scheduler_id)}
        tdata = template.get("data")
        if isinstance(tdata, dict):
            data.update(tdata)
        elif tdata is not None:
            data["payload"] = tdata
        command = _compact(
            {
                "cmd": "Cron",
                "name": scheduler_id,
                "queue": self.name,
                "data": data,
                "schedule": repeat.get("pattern"),
                "repeatEvery": repeat.get("every"),
                "timezone": repeat.get("tz"),
                "immediately": repeat.get("immediately"),
                "maxLimit": repeat.get("limit"),
                "skipIfNoWorker": repeat.get("skip_if_no_worker"),
                "preventOverlap": repeat.get("prevent_overlap"),
                "jobOptions": template.get("opts"),
            }
        )
        self.connection.call(command)

    def remove_job_scheduler(self, scheduler_id: str) -> None:
        self.connection.call({"cmd": "CronDelete", "name": scheduler_id})

    def get_job_scheduler(self, scheduler_id: str) -> Optional[Dict[str, Any]]:
        try:
            response = self.connection.call({"cmd": "CronGet", "name": scheduler_id})
        except CommandError as exc:
            if "not found" in str(exc).lower():
                return None
            raise
        cron = response.get("cron") or response.get("data")
        return dict(cron) if cron else None

    def get_job_schedulers(self) -> List[Dict[str, Any]]:
        response = self.connection.call({"cmd": "CronList"})
        crons = response.get("crons", [])
        return [dict(c) for c in crons if c.get("queue") == self.name]

    def get_job_schedulers_count(self) -> int:
        return len(self.get_job_schedulers())

    def add_cron(self, name: str, schedule: str, data: Any = None, **repeat: Any) -> None:
        """Shorthand: cron-pattern scheduler (mirrors Bunqueue.cron)."""
        self.upsert_job_scheduler(name, {"pattern": schedule, **repeat}, {"name": name, "data": data})

    def every(self, name: str, interval_ms: int, data: Any = None, **repeat: Any) -> None:
        """Shorthand: fixed-interval scheduler (mirrors Bunqueue.every)."""
        self.upsert_job_scheduler(name, {"every": interval_ms, **repeat}, {"name": name, "data": data})

    # --------------------------------------------------------------- webhooks

    def add_webhook(
        self,
        url: str,
        events: List[str],
        queue: Optional[str] = None,
        secret: Optional[str] = None,
    ) -> Optional[str]:
        response = self.connection.call(
            _compact({"cmd": "AddWebhook", "url": url, "events": events, "queue": queue, "secret": secret})
        )
        data = response.get("data") if isinstance(response.get("data"), dict) else response
        webhook_id = data.get("webhookId") or data.get("id")
        return str(webhook_id) if webhook_id else None

    def remove_webhook(self, webhook_id: str) -> None:
        self.connection.call({"cmd": "RemoveWebhook", "webhookId": webhook_id})

    def list_webhooks(self) -> List[Dict[str, Any]]:
        response = self.connection.call({"cmd": "ListWebhooks"})
        data = response.get("data")
        if isinstance(data, dict):
            return list(data.get("webhooks") or [])
        return list(response.get("webhooks") or [])

    def set_webhook_enabled(self, webhook_id: str, enabled: bool) -> None:
        # wire field is `id` (SetWebhookEnabledCommand), unlike RemoveWebhook's `webhookId`
        self.connection.call({"cmd": "SetWebhookEnabled", "id": webhook_id, "enabled": enabled})

    # ------------------------------------------------------------- monitoring

    def get_workers(self) -> List[Dict[str, Any]]:
        response = self.connection.call({"cmd": "ListWorkers"})
        data = response.get("data")
        if isinstance(data, dict):
            return list(data.get("workers") or [])
        return list(response.get("workers") or [])

    def get_workers_count(self) -> int:
        return len(self.get_workers())

    def get_stats(self) -> Dict[str, Any]:
        response = self.connection.call({"cmd": "Stats"})
        return dict(response.get("stats", {}))

    def get_metrics(self) -> Dict[str, Any]:
        response = self.connection.call({"cmd": "Metrics"})
        return dict(response.get("metrics", {}))

    def list_queues(self) -> List[Any]:
        """Returns the server's queue list (names or info dicts)."""
        response = self.connection.call({"cmd": "ListQueues"})
        return list(response.get("queues") or response.get("data") or [])
