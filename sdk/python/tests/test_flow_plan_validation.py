"""Boundary and option-preservation tests for atomic flow planners."""

from __future__ import annotations

from typing import Any, Dict

import pytest

from bunqueue.flow_plan import flow_data, plan_flows
from bunqueue.flow_plan_legacy import plan_bulk_then, plan_chain


def ids(prefix: str = "id"):
    index = 0

    def generate() -> str:
        nonlocal index
        value = f"{prefix}-{index}"
        index += 1
        return value

    return generate


@pytest.mark.parametrize(
    "node",
    [
        None,
        {"name": "", "queueName": "queue"},
        {"name": "x" * 257, "queueName": "queue"},
        {"name": "job", "queueName": ""},
        {"name": "job", "queueName": "has spaces"},
        {"name": "job", "queueName": "q" * 257},
        {"name": "job", "queueName": "queue", "children": {}},
    ],
)
def test_invalid_node_shapes_are_rejected(node: Any) -> None:
    with pytest.raises(ValueError):
        plan_flows([node], id_factory=ids())


@pytest.mark.parametrize("generated", ["", "has:colon", "x" * 1_025])
def test_invalid_generated_ids_are_rejected(generated: str) -> None:
    with pytest.raises(ValueError, match="job_id"):
        plan_flows(
            [{"name": "job", "queueName": "queue"}],
            id_factory=lambda: generated,
        )


def test_duplicate_ids_cycles_and_shared_nodes_are_rejected() -> None:
    with pytest.raises(ValueError, match="duplicate"):
        plan_flows(
            [
                {"name": "one", "queueName": "queue", "opts": {"job_id": "same"}},
                {"name": "two", "queueName": "queue", "opts": {"job_id": "same"}},
            ],
            id_factory=ids(),
        )
    cycle: Dict[str, Any] = {"name": "cycle", "queueName": "queue"}
    cycle["children"] = [cycle]
    with pytest.raises(ValueError, match="cycle or shared"):
        plan_flows([cycle], id_factory=ids())
    shared = {"name": "shared", "queueName": "queue"}
    with pytest.raises(ValueError, match="cycle or shared"):
        plan_flows(
            [
                {"name": "one", "queueName": "queue", "children": [shared]},
                {"name": "two", "queueName": "queue", "children": [shared]},
            ],
            id_factory=ids(),
        )


def test_depth_and_caller_owned_topology_are_rejected() -> None:
    deep: Dict[str, Any] = {"name": "leaf", "queueName": "queue"}
    for depth in range(101):
        deep = {
            "name": f"level-{depth}",
            "queueName": "queue",
            "children": [deep],
        }
    with pytest.raises(ValueError, match="depth limit"):
        plan_flows([deep], id_factory=ids())
    for opts in (
        {"parent_id": "parent"},
        {"depends_on": ["dependency"]},
        {"children_ids": ["child"]},
    ):
        with pytest.raises(ValueError, match="topology"):
            plan_flows([{"name": "job", "queueName": "queue", "opts": opts}])


def test_payload_wrapping_and_reserved_markers() -> None:
    assert flow_data("undefined", None) == {"name": "undefined"}
    assert flow_data("scalar", 42) == {"name": "scalar", "payload": 42}
    assert flow_data("array", [1, 2]) == {"name": "array", "payload": [1, 2]}
    assert flow_data("object", {"value": 1}, {"__marker": True}) == {
        "value": 1,
        "name": "object",
        "__marker": True,
    }
    with pytest.raises(ValueError, match="reserved"):
        flow_data("job", {"name": "override"})
    with pytest.raises(ValueError, match="reserved"):
        flow_data("job", {"__parentId": "override"})


def test_complete_supported_option_surface_is_preserved() -> None:
    options = {
        "priority": 7,
        "delay": 8,
        "attempts": 9,
        "backoff": {"type": "exponential", "delay": 10, "maxDelay": 11},
        "ttl": 12,
        "timeout": 13,
        "tags": ["one", "two"],
        "group_id": "group",
        "lifo": False,
        "remove_on_complete": False,
        "remove_on_fail": True,
        "stall_timeout": 14,
        "durable": True,
        "stack_trace_limit": 15,
        "keep_logs": 16,
        "size_limit": 17,
        "timestamp": 18,
    }
    plan = plan_flows(
        [
            {
                "name": "parent",
                "queueName": "queue",
                "opts": options,
                "children": [
                    {
                        "name": "child",
                        "queueName": "queue",
                        "opts": {"continue_parent_on_failure": True},
                    }
                ],
            }
        ],
        id_factory=ids(),
    )
    wire = plan.jobs[1]["input"]
    assert wire == {
        "data": {"name": "parent", "__childrenIds": ["id-1"]},
        "priority": 7,
        "delay": 8,
        "maxAttempts": 9,
        "backoff": options["backoff"],
        "ttl": 12,
        "timeout": 13,
        "tags": ["one", "two"],
        "groupId": "group",
        "lifo": False,
        "removeOnComplete": False,
        "removeOnFail": True,
        "stallTimeout": 14,
        "durable": True,
        "stackTraceLimit": 15,
        "keepLogs": 16,
        "sizeLimit": 17,
        "timestamp": 18,
        "dependsOn": ["id-1"],
        "childrenIds": ["id-1"],
    }
    assert plan.jobs[0]["input"]["continueParentOnFailure"] is True


def test_flat_planner_boundaries() -> None:
    fan_in = plan_bulk_then(
        [], {"name": "final", "queueName": "queue", "opts": {"job_id": "final-id"}}
    )
    assert fan_in.parallel_ids == []
    assert fan_in.final_id == "final-id"
    assert "dependsOn" not in fan_in.jobs[0]["input"]
    assert "childrenIds" not in fan_in.jobs[0]["input"]
    assert fan_in.jobs[0]["input"]["customId"] == "final-id"
    with pytest.raises(ValueError, match="duplicate"):
        plan_chain(
            [
                {"name": "one", "queueName": "queue", "opts": {"job_id": "same"}},
                {"name": "two", "queueName": "queue", "opts": {"job_id": "same"}},
            ],
            ids(),
        )
    with pytest.raises(ValueError, match="reserved"):
        plan_chain(
            [{"name": "one", "queueName": "queue", "data": {"__flowParentId": "user"}}]
        )


@pytest.mark.parametrize("queue_name", ["!queue", "queue!"])
def test_flat_planners_reject_partially_valid_queue_names(queue_name: str) -> None:
    with pytest.raises(ValueError, match="flow queueName is invalid"):
        plan_chain([{"name": "job", "queueName": queue_name}], ids())


@pytest.mark.parametrize("children", ["", 0, {}, None])
def test_flat_planners_reject_falsey_non_array_children(children: Any) -> None:
    nested = {"name": "nested", "queueName": "queue", "children": children}
    final = {"name": "final", "queueName": "queue"}
    with pytest.raises(ValueError, match="children must be an array"):
        plan_chain([nested], ids())
    with pytest.raises(ValueError, match="children must be an array"):
        plan_bulk_then([nested], final, ids())
    with pytest.raises(ValueError, match="children must be an array"):
        plan_bulk_then([], {**final, "children": children}, ids())


def test_flat_planners_accept_semantically_empty_children() -> None:
    empty = {"name": "empty", "queueName": "queue", "children": []}
    assert len(plan_chain([empty], ids()).jobs) == 1
    assert len(plan_bulk_then([empty], {"name": "final", "queueName": "queue"}, ids()).jobs) == 2
