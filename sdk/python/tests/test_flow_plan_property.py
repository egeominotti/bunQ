"""Property tests for the pure atomic FlowProducer planners."""

from __future__ import annotations

from typing import Any, Dict, List

import pytest
from hypothesis import given, settings, strategies as st

from bunqueue.flow_commit import validate_flow_snapshots
from bunqueue.flow_plan import plan_flows
from bunqueue.flow_plan_legacy import plan_bulk_then, plan_chain

TOKEN = st.text(
    alphabet="abcdefghijklmnopqrstuvwxyz0123456789-_", min_size=1, max_size=12
)
DATA = st.fixed_dictionaries({"value": st.integers(), "label": TOKEN})
STEP = st.fixed_dictionaries({"name": TOKEN, "queueName": TOKEN, "data": DATA})

def tree(depth: int = 0) -> st.SearchStrategy:
    children = st.just([]) if depth >= 3 else st.lists(tree(depth + 1), max_size=3)
    return st.fixed_dictionaries(
        {"name": TOKEN, "queueName": TOKEN, "data": DATA, "children": children}
    )


def id_factory(prefix: str = "generated"):
    index = 0

    def generate() -> str:
        nonlocal index
        value = f"{prefix}-{index}"
        index += 1
        return value

    return generate


def children_of(job: Dict[str, Any]) -> List[str]:
    return list(job["input"].get("childrenIds") or [])


def dependencies_of(job: Dict[str, Any]) -> List[str]:
    return list(job["input"].get("dependsOn") or [])


@settings(max_examples=250, deadline=None, derandomize=True, database=None)
@given(st.lists(tree(), max_size=4))
def test_tree_shape_uniqueness_closure_and_reciprocity(flows) -> None:
    plan = plan_flows(flows, id_factory=id_factory())
    by_id = {job["id"]: job for job in plan.jobs}
    positions = {job["id"]: index for index, job in enumerate(plan.jobs)}
    assert len(by_id) == len(plan.jobs)

    for job in plan.jobs:
        assert ":" not in job["id"]
        children = children_of(job)
        data = job["input"]["data"]
        assert data.get("__childrenIds") == (children if children else None)
        if not children:
            assert "childrenIds" not in job["input"]
            assert "dependsOn" not in job["input"]
        for child_id in children:
            child = by_id[child_id]
            assert child_id in dependencies_of(job)
            assert child["input"]["parentId"] == job["id"]
            assert child["input"]["data"]["__parentId"] == job["id"]
            assert child["input"]["data"]["__parentQueue"] == job["queue"]
            assert positions[child_id] < positions[job["id"]]
        parent_id = job["input"].get("parentId")
        if parent_id:
            assert job["id"] in children_of(by_id[parent_id])

    def compare(source: Dict[str, Any], planned_id: str) -> None:
        planned = by_id[planned_id]
        assert planned["queue"] == source["queueName"]
        assert planned["input"]["data"]["name"] == source["name"]
        source_children = source.get("children") or []
        assert len(children_of(planned)) == len(source_children)
        for child, child_id in zip(source_children, children_of(planned)):
            compare(child, child_id)

    for source, root in zip(flows, plan.roots):
        compare(source, root.id)


FLOW_OPTIONS = st.fixed_dictionaries(
    {
        "priority": st.integers(min_value=-100, max_value=100),
        "delay": st.integers(min_value=0, max_value=10_000),
        "attempts": st.integers(min_value=1, max_value=20),
        "ttl": st.integers(min_value=0, max_value=10_000),
        "timeout": st.integers(min_value=0, max_value=10_000),
        "lifo": st.booleans(),
        "durable": st.booleans(),
        "remove_on_complete": st.booleans(),
        "remove_on_fail": st.booleans(),
        "tags": st.lists(TOKEN, max_size=4),
        "group_id": TOKEN,
        "job_id": TOKEN.map(lambda value: f"custom-{value}"),
    }
)


@settings(max_examples=200, deadline=None, derandomize=True, database=None)
@given(FLOW_OPTIONS)
def test_supported_options_and_custom_id_mapping(opts) -> None:
    plan = plan_flows(
        [{"name": "job", "queueName": "queue", "data": {"value": 1}, "opts": opts}],
        id_factory=id_factory(),
    )
    job = plan.jobs[0]
    wire = job["input"]
    assert job["id"] == opts["job_id"]
    assert wire["customId"] == opts["job_id"]
    assert "jobId" not in wire
    assert wire["maxAttempts"] == opts["attempts"]
    expected = {
        "priority": "priority",
        "delay": "delay",
        "ttl": "ttl",
        "timeout": "timeout",
        "lifo": "lifo",
        "durable": "durable",
        "removeOnComplete": "remove_on_complete",
        "removeOnFail": "remove_on_fail",
        "tags": "tags",
        "groupId": "group_id",
    }
    for wire_name, option_name in expected.items():
        assert wire[wire_name] == opts[option_name]


def test_queue_defaults_are_overridden_and_remaining_options_are_preserved() -> None:
    plan = plan_flows(
        [
            {
                "name": "parent",
                "queueName": "queue",
                "opts": {
                    "priority": 9,
                    "backoff": {"type": "fixed", "delay": 100},
                    "stall_timeout": 200,
                    "stack_trace_limit": 3,
                    "keep_logs": 4,
                    "size_limit": 5,
                    "timestamp": 6,
                },
                "children": [
                    {
                        "name": "child",
                        "queueName": "queue",
                        "opts": {"fail_parent_on_failure": True},
                    }
                ],
            }
        ],
        {"queues_options": {"queue": {"priority": 1, "attempts": 4}}},
        id_factory(),
    )
    parent = plan.jobs[-1]["input"]
    child = plan.jobs[0]["input"]
    assert parent["priority"] == 9
    assert parent["maxAttempts"] == 4
    assert parent["backoff"] == {"type": "fixed", "delay": 100}
    assert parent["stallTimeout"] == 200
    assert parent["stackTraceLimit"] == 3
    assert parent["keepLogs"] == 4
    assert parent["sizeLimit"] == 5
    assert parent["timestamp"] == 6
    assert child["failParentOnFailure"] is True


@settings(max_examples=100, deadline=None, derandomize=True, database=None)
@given(TOKEN)
def test_queue_defaults_cannot_define_per_job_identity(job_id: str) -> None:
    allocations = []

    def allocate() -> str:
        allocations.append(True)
        return "generated"

    with pytest.raises(ValueError, match="job_id cannot be a queue default"):
        plan_flows(
            [{"name": "job", "queueName": "queue"}],
            {"queues_options": {"queue": {"job_id": job_id}}},
            allocate,
        )
    assert allocations == []


@settings(max_examples=100, deadline=None, derandomize=True, database=None)
@given(TOKEN)
def test_reserved_data_is_rejected(suffix: str) -> None:
    with pytest.raises(ValueError, match="reserved"):
        plan_flows(
            [{"name": "job", "queueName": "queue", "data": {f"__{suffix}": True}}]
        )


@pytest.mark.parametrize(
    "opts",
    [
        {"repeat": {}},
        {"deduplication": {"id": "same"}},
        {"unique_key": "same"},
        {"debounce": {"id": "same"}},
    ],
)
def test_unsupported_atomic_options_are_rejected(opts) -> None:
    with pytest.raises(ValueError, match="not supported"):
        plan_flows([{"name": "job", "queueName": "queue", "opts": opts}])


@settings(max_examples=250, deadline=None, derandomize=True, database=None)
@given(st.lists(STEP, max_size=30))
def test_chain_is_unique_closed_and_ordered(steps) -> None:
    plan = plan_chain(steps, id_factory("chain"))
    assert len(set(plan.ids)) == len(plan.ids)
    for index, job in enumerate(plan.jobs):
        previous = plan.ids[index - 1] if index else None
        assert dependencies_of(job) == ([previous] if previous else [])
        assert job["input"]["data"]["__flowParentId"] == previous


@settings(max_examples=250, deadline=None, derandomize=True, database=None)
@given(st.lists(STEP, max_size=30), STEP)
def test_fan_in_is_closed_and_reciprocal(parallel, final) -> None:
    plan = plan_bulk_then(parallel, final, id_factory("fanin"))
    final_job = plan.jobs[-1]
    assert children_of(final_job) == plan.parallel_ids
    assert dependencies_of(final_job) == plan.parallel_ids
    assert final_job["input"]["data"]["__flowParentIds"] == plan.parallel_ids
    for job in plan.jobs[:-1]:
        assert job["input"]["parentId"] == plan.final_id
        assert job["input"]["data"]["__parentId"] == plan.final_id
        assert job["id"] in children_of(final_job)


@settings(max_examples=150, deadline=None, derandomize=True, database=None)
@given(st.lists(STEP, min_size=1, max_size=20))
def test_response_validation_requires_exact_ids_and_queues(steps) -> None:
    jobs = plan_chain(steps, id_factory("response")).jobs
    snapshots = [{"id": job["id"], "queue": job["queue"]} for job in reversed(jobs)]
    assert len(validate_flow_snapshots(jobs, snapshots)) == len(jobs)
    with pytest.raises(ValueError, match="missing"):
        validate_flow_snapshots(jobs, snapshots[1:])
    invalid = [{"id": "foreign", "queue": snapshots[0]["queue"]}, *snapshots[1:]]
    with pytest.raises(ValueError, match="IDs"):
        validate_flow_snapshots(jobs, invalid)
