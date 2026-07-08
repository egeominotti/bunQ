"""Job TTL: expire unprocessed jobs at pickup (mirror of ttl.ts).

Resolution order: per_name[job.name] -> default_ttl -> 0 (no TTL).
"""

from __future__ import annotations

import time
from typing import Any, Dict


class TtlChecker:
    def __init__(self, config: Dict[str, Any]) -> None:
        self._default_ttl = float(
            config.get("default_ttl") or config.get("defaultTtl") or 0
        )
        self._per_name: Dict[str, float] = dict(
            config.get("per_name") or config.get("perName") or {}
        )

    def get_ttl(self, job_name: str) -> float:
        return self._per_name.get(job_name, self._default_ttl)

    def is_expired(self, job_name: str, job_timestamp_ms: float) -> bool:
        ttl = self.get_ttl(job_name)
        if ttl <= 0:
            return False
        return (time.time() * 1000) - job_timestamp_ms > ttl

    def set_default_ttl(self, ttl_ms: float) -> None:
        self._default_ttl = ttl_ms

    def set_name_ttl(self, job_name: str, ttl_ms: float) -> None:
        self._per_name[job_name] = ttl_ms
