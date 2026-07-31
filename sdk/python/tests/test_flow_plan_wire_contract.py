"""Exact public result and wire-shape contracts for atomic flow plans."""

from __future__ import annotations

from typing import Any, Callable

import pytest

from bunqueue.flow_commit import validate_flow_snapshots
from bunqueue.flow_plan import plan_flows
from bunqueue.flow_plan_legacy import plan_bulk_then, plan_chain


def ids(prefix: str) -> Callable[[], str]:
    index = 0

    def generate() -> str:
        nonlocal index
        value = f"{prefix}-{index}"
        index += 1
        return value

    return generate


def test_tree_result_preserves_every_public_node_field() -> None:
    source = {
        "name": "root",
        "queueName": "root-queue",
        "data": {"value": 1},
        "children": [
            {"name": "leaf", "queueName": "leaf-queue", "data": {"value": 2}}
        ],
    }
    plan = plan_flows([source], id_factory=ids("tree"))
    root = plan.roots[0]
    assert (root.id, root.name, root.queue_name, root.data) == (
        "tree-0",
        "root",
        "root-queue",
        {"value": 1},
    )
    assert len(root.children) == 1
    leaf = root.children[0]
    assert (leaf.id, leaf.name, leaf.queue_name, leaf.data, leaf.children) == (
        "tree-1",
        "leaf",
        "leaf-queue",
        {"value": 2},
        [],
    )


def test_chain_preserves_step_fields_and_options() -> None:
    plan = plan_chain(
        [
            {
                "name": "first",
                "queueName": "queue-a",
                "data": {"value": 1},
                "opts": {"priority": 3},
            },
            {
                "name": "second",
                "queueName": "queue-b",
                "data": {"value": 2},
                "opts": {"delay": 4},
            },
        ],
        ids("chain"),
    )
    assert plan.jobs == [
        {
            "id": "chain-0",
            "queue": "queue-a",
            "input": {
                "data": {"value": 1, "name": "first", "__flowParentId": None},
                "priority": 3,
            },
        },
        {
            "id": "chain-1",
            "queue": "queue-b",
            "input": {
                "data": {"value": 2, "name": "second", "__flowParentId": "chain-0"},
                "delay": 4,
                "dependsOn": ["chain-0"],
            },
        },
    ]


def test_fan_in_preserves_fields_options_and_reciprocal_markers() -> None:
    plan = plan_bulk_then(
        [
            {
                "name": "left",
                "queueName": "queue-left",
                "data": {"value": 1},
                "opts": {"priority": 2},
            }
        ],
        {
            "name": "final",
            "queueName": "queue-final",
            "data": {"value": 3},
            "opts": {"timeout": 5},
        },
        ids("fanin"),
    )
    assert plan.jobs == [
        {
            "id": "fanin-0",
            "queue": "queue-left",
            "input": {
                "data": {
                    "value": 1,
                    "name": "left",
                    "__parentId": "fanin-1",
                    "__parentQueue": "queue-final",
                },
                "priority": 2,
                "parentId": "fanin-1",
            },
        },
        {
            "id": "fanin-1",
            "queue": "queue-final",
            "input": {
                "data": {
                    "value": 3,
                    "name": "final",
                    "__flowParentIds": ["fanin-0"],
                    "__childrenIds": ["fanin-0"],
                },
                "timeout": 5,
                "dependsOn": ["fanin-0"],
                "childrenIds": ["fanin-0"],
            },
        },
    ]


def test_snapshot_validator_returns_the_original_snapshots_by_id() -> None:
    jobs = [{"id": "one", "queue": "queue", "input": {"data": {"name": "one"}}}]
    snapshot = {"id": "one", "queue": "queue", "state": "waiting"}
    assert validate_flow_snapshots(jobs, [snapshot]) == {"one": snapshot}


@pytest.mark.parametrize(
    ("snapshots", "message"),
    [
        ([], "Invalid PUSHF response: committed job snapshots are missing"),
        ([42], "Invalid PUSHF response: job snapshot is invalid"),
        (
            [{"id": "foreign", "queue": "queue"}],
            "Invalid PUSHF response: committed job IDs do not match the request",
        ),
    ],
)
def test_snapshot_validation_errors_are_stable(snapshots: Any, message: str) -> None:
    jobs = [{"id": "one", "queue": "queue", "input": {"data": {"name": "one"}}}]
    with pytest.raises(ValueError) as error:
        validate_flow_snapshots(jobs, snapshots)
    assert str(error.value) == message
