"""Mutation-focused observable contracts for the atomic flow planners."""

from __future__ import annotations

from typing import Any, Callable

import pytest

from bunqueue.flow_plan import plan_flows
from bunqueue.flow_plan_legacy import plan_chain


def ids(prefix: str = "contract") -> Callable[[], str]:
    index = 0

    def generate() -> str:
        nonlocal index
        value = f"{prefix}-{index}"
        index += 1
        return value

    return generate


def deep_flow() -> dict:
    node = {"name": "leaf", "queueName": "queue"}
    for depth in range(101):
        node = {"name": f"level-{depth}", "queueName": "queue", "children": [node]}
    return node


def cyclic_flow() -> dict:
    node = {"name": "cycle", "queueName": "queue"}
    node["children"] = [node]
    return node


@pytest.mark.parametrize(
    ("invoke", "message"),
    [
        (
            lambda: plan_flows([42], id_factory=ids()),
            "flow node must be an object",
        ),
        (
            lambda: plan_flows([{"name": "", "queueName": "queue"}], id_factory=ids()),
            "flow job name must be a non-empty string of at most 256 characters",
        ),
        (
            lambda: plan_flows([{"name": "job", "queueName": 42}], id_factory=ids()),
            "flow queueName is invalid",
        ),
        (
            lambda: plan_flows(
                [{"name": "job", "queueName": "queue", "children": None}],
                id_factory=ids(),
            ),
            "flow children must be an array",
        ),
        (
            lambda: plan_flows([{"name": "job", "queueName": "queue"}], []),
            "flow options must be an object",
        ),
        (
            lambda: plan_flows(
                [{"name": "job", "queueName": "queue"}],
                {"queues_options": ["bad"]},
            ),
            "flow queues_options must be an object",
        ),
        (
            lambda: plan_flows(
                [{"name": "job", "queueName": "queue"}],
                {"queues_options": {"queue": []}},
            ),
            "flow queue defaults must be an object",
        ),
        (
            lambda: plan_flows(
                [{"name": "job", "queueName": "queue"}],
                {"queues_options": {"queue": {"job_id": "shared"}}},
            ),
            "job_id cannot be a queue default",
        ),
        (
            lambda: plan_flows(
                [{"name": "job", "queueName": "queue", "opts": ["bad"]}]
            ),
            "flow job opts must be an object",
        ),
        (
            lambda: plan_flows(
                [{"name": "job", "queueName": "queue", "opts": {"repeat": {}}}]
            ),
            "repeat is not supported inside an atomic flow",
        ),
        (
            lambda: plan_flows(
                [
                    {
                        "name": "job",
                        "queueName": "queue",
                        "opts": {"deduplication": {"id": "same"}},
                    }
                ]
            ),
            "deduplication is not supported inside an atomic flow",
        ),
        (
            lambda: plan_flows(
                [{"name": "job", "queueName": "queue", "opts": {"unique_key": "same"}}]
            ),
            "deduplication is not supported inside an atomic flow",
        ),
        (
            lambda: plan_flows(
                [{"name": "job", "queueName": "queue", "opts": {"debounce": {}}}]
            ),
            "debounce is not supported inside an atomic flow",
        ),
        (
            lambda: plan_flows(
                [{"name": "job", "queueName": "queue", "opts": {"parent_id": "parent"}}]
            ),
            "flow topology options are owned by FlowProducer",
        ),
        (
            lambda: plan_flows(
                [{"name": "job", "queueName": "queue"}], id_factory=lambda: 42
            ),
            "flow job_id must be non-empty and cannot contain a colon",
        ),
        (
            lambda: plan_flows([deep_flow()], id_factory=ids()),
            "flow exceeds the 100 level depth limit",
        ),
        (
            lambda: plan_flows([cyclic_flow()], id_factory=ids()),
            "flow contains a cycle or shared node",
        ),
        (
            lambda: plan_chain(
                [
                    {
                        "name": "outer",
                        "queueName": "queue",
                        "children": [{"name": "inner", "queueName": "queue"}],
                    }
                ],
                ids(),
            ),
            "nested children are not supported by this flow method",
        ),
    ],
)
def test_validation_errors_identify_the_exact_contract(
    invoke: Callable[[], Any], message: str
) -> None:
    with pytest.raises(ValueError) as error:
        invoke()
    assert str(error.value) == message


@pytest.mark.parametrize("invalid", [[], "", 0, False])
def test_falsey_non_object_option_shapes_are_rejected(invalid: Any) -> None:
    node = {"name": "job", "queueName": "queue", "opts": invalid}
    with pytest.raises(ValueError, match="flow job opts must be an object"):
        plan_flows([node], id_factory=ids())
    with pytest.raises(ValueError) as error:
        plan_chain([node], ids())
    assert str(error.value) == "flow job opts must be an object"
    with pytest.raises(ValueError, match="flow queues_options must be an object"):
        plan_flows(
            [{"name": "job", "queueName": "queue"}],
            {"queues_options": invalid},
            ids(),
        )
