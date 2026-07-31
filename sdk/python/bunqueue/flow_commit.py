"""Commit and validate one atomic FlowProducer batch."""

from __future__ import annotations

from typing import Any, Dict, List

from .connection import Connection
from .flow_plan import AtomicFlowJob


def validate_flow_snapshots(
    jobs: List[AtomicFlowJob], snapshots: Any
) -> Dict[str, Dict[str, Any]]:
    if not isinstance(snapshots, list) or len(snapshots) != len(jobs):
        raise ValueError("Invalid PUSHF response: committed job snapshots are missing")
    expected = {job["id"]: job["queue"] for job in jobs}
    by_id: Dict[str, Dict[str, Any]] = {}
    for snapshot in snapshots:
        if not isinstance(snapshot, dict):
            raise ValueError("Invalid PUSHF response: job snapshot is invalid")
        job_id = snapshot.get("id")
        if (
            not isinstance(job_id, str)
            or job_id not in expected
            or job_id in by_id
            or snapshot.get("queue") != expected[job_id]
        ):
            raise ValueError(
                "Invalid PUSHF response: committed job IDs do not match the request"
            )
        by_id[job_id] = snapshot
    return by_id


def commit_flow(
    connection: Connection, jobs: List[AtomicFlowJob]
) -> Dict[str, Dict[str, Any]]:
    if not jobs:
        return {}
    response = connection.call({"cmd": "PUSHF", "jobs": jobs})
    data = response.get("data")
    snapshots = data.get("jobs") if isinstance(data, dict) else None
    return validate_flow_snapshots(jobs, snapshots)
