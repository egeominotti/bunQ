"""Job option mapping: pythonic kwargs -> wire fields (parity with TS client)."""

from __future__ import annotations

from typing import Any, Dict, Optional, Sequence, Union

from .connection import _compact

Backoff = Union[int, Dict[str, Any]]  # ms or {"type": "fixed"|"exponential", "delay", "maxDelay"}


def job_payload(name: str, data: Any) -> Dict[str, Any]:
    """Mirror the JS SDK: the job name travels inside ``data``."""
    if data is None:
        return {"name": name}
    if isinstance(data, dict):
        return {"name": name, **data}
    return {"name": name, "payload": data}


def job_options(
    *,
    priority: Optional[int] = None,
    delay: Optional[int] = None,
    attempts: Optional[int] = None,
    backoff: Optional[Backoff] = None,
    ttl: Optional[int] = None,
    timeout: Optional[int] = None,
    job_id: Optional[str] = None,
    unique_key: Optional[str] = None,
    deduplication: Optional[Dict[str, Any]] = None,  # {"id", "ttl", "extend", "replace"}
    depends_on: Optional[Sequence[str]] = None,
    parent_id: Optional[str] = None,
    children_ids: Optional[Sequence[str]] = None,
    tags: Optional[Sequence[str]] = None,
    group_id: Optional[str] = None,
    lifo: Optional[bool] = None,
    remove_on_complete: Optional[bool] = None,
    remove_on_fail: Optional[bool] = None,
    stall_timeout: Optional[int] = None,
    durable: Optional[bool] = None,
    repeat: Optional[Dict[str, Any]] = None,  # {"every", "limit", "pattern", ...} camelCase wire keys
    debounce: Optional[Dict[str, Any]] = None,  # {"id", "ttl"}
    stack_trace_limit: Optional[int] = None,
    keep_logs: Optional[int] = None,
    size_limit: Optional[int] = None,
    timestamp: Optional[int] = None,
    fail_parent_on_failure: Optional[bool] = None,
    remove_dependency_on_failure: Optional[bool] = None,
    ignore_dependency_on_failure: Optional[bool] = None,
    continue_parent_on_failure: Optional[bool] = None,
) -> Dict[str, Any]:
    """Translate SDK options into the TCP PUSH field set (see buildPushPayload #88/#90)."""
    dedup = None
    if deduplication:
        unique_key = unique_key or deduplication.get("id")
        dedup = _compact(
            {
                "ttl": deduplication.get("ttl"),
                "extend": deduplication.get("extend"),
                "replace": deduplication.get("replace"),
            }
        ) or None
    return _compact(
        {
            "priority": priority,
            "delay": delay,
            "maxAttempts": attempts,
            "backoff": backoff,
            "ttl": ttl,
            "timeout": timeout,
            "jobId": job_id,
            "uniqueKey": unique_key,
            "dedup": dedup,
            "dependsOn": list(depends_on) if depends_on else None,
            "parentId": parent_id,
            "childrenIds": list(children_ids) if children_ids else None,
            "tags": list(tags) if tags else None,
            "groupId": group_id,
            "lifo": lifo,
            "removeOnComplete": remove_on_complete,
            "removeOnFail": remove_on_fail,
            "stallTimeout": stall_timeout,
            "durable": durable,
            "repeat": repeat,
            "debounceId": debounce.get("id") if debounce else None,
            "debounceTtl": debounce.get("ttl") if debounce else None,
            "stackTraceLimit": stack_trace_limit,
            "keepLogs": keep_logs,
            "sizeLimit": size_limit,
            "timestamp": timestamp,
            "failParentOnFailure": fail_parent_on_failure,
            "removeDependencyOnFailure": remove_dependency_on_failure,
            "ignoreDependencyOnFailure": ignore_dependency_on_failure,
            "continueParentOnFailure": continue_parent_on_failure,
        }
    )
