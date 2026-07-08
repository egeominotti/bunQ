"""Deduplication & debounce defaults merger (mirror of dedupDebounce.ts).

Jobs with the same name + data derive the same deduplication id
(``name:JSON(data)``, compact separators to match JSON.stringify).
"""

from __future__ import annotations

import json
from typing import Any, Dict, Optional


class DedupDebounceMerger:
    def __init__(
        self,
        dedup: Optional[Dict[str, Any]],
        debounce: Optional[Dict[str, Any]],
    ) -> None:
        self._dedup = dedup
        self._debounce = debounce

    @property
    def active(self) -> bool:
        return self._dedup is not None or self._debounce is not None

    def merge(self, name: str, opts: Dict[str, Any], data: Any) -> Dict[str, Any]:
        if not self.active:
            return opts
        merged = dict(opts)
        if self._dedup is not None and "deduplication" not in merged:
            data_key = (
                json.dumps(data, separators=(",", ":")) if data is not None else ""
            )
            merged["deduplication"] = {
                "id": f"{name}:{data_key}",
                "ttl": self._dedup.get("ttl", 3600000),
                "extend": self._dedup.get("extend"),
                "replace": self._dedup.get("replace"),
            }
        if self._debounce is not None and "debounce" not in merged:
            merged["debounce"] = {"id": name, "ttl": self._debounce["ttl"]}
        return merged
