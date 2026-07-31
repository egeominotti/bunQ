"""Pure planner for atomic FlowProducer trees."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Set
from uuid import uuid4

from .options import job_options

MAX_FLOW_DEPTH = 100
MAX_FLOW_JOBS = 10_000
_QUEUE_RE = re.compile(r"^[a-zA-Z0-9_\-.:]+$")
FlowIdFactory = Callable[[], str]
AtomicFlowJob = Dict[str, Any]


@dataclass
class PlannedFlowNode:
    """A result-tree node whose ID is allocated before broker I/O."""

    id: str
    name: str
    queue_name: str
    data: Any
    children: List["PlannedFlowNode"]


@dataclass
class FlowPlan:
    """A closed server batch and the shape used to rebuild public results."""

    jobs: List[AtomicFlowJob]
    roots: List[PlannedFlowNode]


def portable_flow_id() -> str:
    """Return a runtime-portable job ID without the protocol's ':' separator."""
    return uuid4().hex


def assert_flow_identity(node: Any, depth: int = 0) -> None:
    if not isinstance(node, dict):
        raise ValueError("flow node must be an object")
    name = node.get("name")
    queue = node.get("queueName")
    if not isinstance(name, str) or not name or len(name) > 256:
        raise ValueError("flow job name must be a non-empty string of at most 256 characters")
    if (
        not isinstance(queue, str)
        or not queue
        or len(queue) > 256
        or _QUEUE_RE.fullmatch(queue) is None
    ):
        raise ValueError("flow queueName is invalid")
    if depth > MAX_FLOW_DEPTH:
        raise ValueError("flow exceeds the 100 level depth limit")
    if "children" in node and not isinstance(node["children"], list):
        raise ValueError("flow children must be an array")


def assert_atomic_options(opts: Dict[str, Any]) -> None:
    if not isinstance(opts, dict):
        raise ValueError("flow job opts must be an object")
    unsupported = {
        "repeat": "repeat",
        "deduplication": "deduplication",
        "unique_key": "deduplication",
        "debounce": "debounce",
    }
    for key, label in unsupported.items():
        if opts.get(key) is not None:
            raise ValueError(f"{label} is not supported inside an atomic flow")
    if any(opts.get(key) is not None for key in ("parent_id", "depends_on", "children_ids")):
        raise ValueError("flow topology options are owned by FlowProducer")


def flow_data(
    name: str, data: Any, internal: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    if isinstance(data, dict):
        for key in data:
            if key == "name" or key.startswith("__"):
                raise ValueError(f"flow job data key is reserved: {key}")
        payload = dict(data)
    elif data is None:
        payload = {}
    else:
        payload = {"payload": data}
    payload["name"] = name
    payload.update(internal or {})
    return payload


def flow_input(
    data: Dict[str, Any],
    opts: Dict[str, Any],
    *,
    parent_id: Optional[str] = None,
    depends_on: Optional[List[str]] = None,
    children_ids: Optional[List[str]] = None,
) -> Dict[str, Any]:
    assert_atomic_options(opts)
    mapped = job_options(**opts)
    mapped.pop("jobId", None)
    mapped.pop("parentId", None)
    mapped.pop("dependsOn", None)
    mapped.pop("childrenIds", None)
    if opts.get("tags") is not None:
        mapped["tags"] = list(opts["tags"])
    result: Dict[str, Any] = {"data": data, **mapped}
    if opts.get("job_id") is not None:
        result["customId"] = opts["job_id"]
    if parent_id is not None:
        result["parentId"] = parent_id
    if depends_on:
        result["dependsOn"] = list(depends_on)
    if children_ids:
        result["childrenIds"] = list(children_ids)
    return result


def allocate_flow_id(
    opts: Dict[str, Any], ids: Set[str], id_factory: FlowIdFactory
) -> str:
    value = opts.get("job_id")
    flow_id = id_factory() if value is None else value
    if (
        not isinstance(flow_id, str)
        or not flow_id
        or len(flow_id) > 1_024
        or ":" in flow_id
    ):
        raise ValueError("flow job_id must be non-empty and cannot contain a colon")
    if flow_id in ids:
        raise ValueError(f"duplicate flow job id: {flow_id}")
    ids.add(flow_id)
    return flow_id


def plan_flows(
    flows: List[Dict[str, Any]],
    options: Optional[Dict[str, Any]] = None,
    id_factory: FlowIdFactory = portable_flow_id,
) -> FlowPlan:
    """Compile trees into one fully resolved batch without broker I/O."""
    if options is not None and not isinstance(options, dict):
        raise ValueError("flow options must be an object")
    raw_queue_options = (options or {}).get("queues_options")
    queue_options = {} if raw_queue_options is None else raw_queue_options
    if not isinstance(queue_options, dict):
        raise ValueError("flow queues_options must be an object")
    for defaults in queue_options.values():
        if not isinstance(defaults, dict):
            raise ValueError("flow queue defaults must be an object")
        if "job_id" in defaults:
            raise ValueError("job_id cannot be a queue default")
    jobs: List[AtomicFlowJob] = []
    ids: Set[str] = set()
    seen: Set[int] = set()
    node_count = 0

    def visit(
        node: Dict[str, Any],
        parent: Optional[Dict[str, str]],
        depth: int,
    ) -> PlannedFlowNode:
        nonlocal node_count
        assert_flow_identity(node, depth)
        identity = id(node)
        if identity in seen:
            raise ValueError("flow contains a cycle or shared node")
        seen.add(identity)
        node_count += 1
        if node_count > MAX_FLOW_JOBS:
            raise ValueError("flow exceeds the 10000 job limit")

        queue = node["queueName"]
        raw_defaults = queue_options.get(queue)
        defaults = {} if raw_defaults is None else raw_defaults
        node_opts = node.get("opts")
        raw_opts = {} if node_opts is None else node_opts
        if not isinstance(defaults, dict) or not isinstance(raw_opts, dict):
            raise ValueError("flow job opts must be an object")
        opts = {**defaults, **raw_opts}
        assert_atomic_options(opts)
        flow_id = allocate_flow_id(opts, ids, id_factory)
        children = [
            visit(child, {"id": flow_id, "queue": queue}, depth + 1)
            for child in node.get("children") or []
        ]
        child_ids = [child.id for child in children]
        internal: Dict[str, Any] = {}
        if parent:
            internal.update({"__parentId": parent["id"], "__parentQueue": parent["queue"]})
        if child_ids:
            internal["__childrenIds"] = child_ids
        jobs.append(
            {
                "id": flow_id,
                "queue": queue,
                "input": flow_input(
                    flow_data(node["name"], node.get("data"), internal),
                    opts,
                    parent_id=parent["id"] if parent else None,
                    depends_on=child_ids,
                    children_ids=child_ids,
                ),
            }
        )
        return PlannedFlowNode(
            flow_id, node["name"], queue, node.get("data"), children
        )

    return FlowPlan(jobs, [visit(flow, None, 0) for flow in flows])
