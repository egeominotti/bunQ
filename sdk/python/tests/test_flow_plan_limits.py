"""Exact boundary tests for the pure atomic flow planners."""

from bunqueue.flow_plan import plan_flows
from bunqueue.flow_plan_legacy import plan_bulk_then, plan_chain


def ids():
    index = 0

    def generate() -> str:
        nonlocal index
        value = f"limit-{index}"
        index += 1
        return value

    return generate


def step(index: int):
    return {"name": f"job-{index}", "queueName": "queue"}


def test_exact_text_id_and_depth_limits_are_accepted() -> None:
    text_plan = plan_flows(
        [{"name": "n" * 256, "queueName": "q" * 256}],
        {},
        lambda: "i" * 1_024,
    )
    assert len(text_plan.jobs[0]["id"]) == 1_024

    deep = {"name": "leaf", "queueName": "queue"}
    for depth in range(100):
        deep = {
            "name": f"level-{depth}",
            "queueName": "queue",
            "children": [deep],
        }
    assert len(plan_flows([deep], id_factory=ids()).jobs) == 101


def test_tree_planner_enforces_the_exact_10000_job_bound() -> None:
    at_limit = [step(index) for index in range(10_000)]
    assert len(plan_flows(at_limit, id_factory=ids()).jobs) == 10_000
    try:
        plan_flows([*at_limit, step(10_000)], id_factory=ids())
        raise AssertionError("expected tree batch overflow")
    except ValueError as exc:
        assert str(exc) == "flow exceeds the 10000 job limit"


def test_flat_planners_enforce_total_batch_bounds() -> None:
    chain = [step(index) for index in range(10_000)]
    assert len(plan_chain(chain, ids()).jobs) == 10_000
    try:
        plan_chain([*chain, step(10_000)], ids())
        raise AssertionError("expected chain batch overflow")
    except ValueError as exc:
        assert str(exc) == "flow exceeds the 10000 job limit"

    parallel = [step(index) for index in range(9_999)]
    assert len(plan_bulk_then(parallel, step(9_999), ids()).jobs) == 10_000
    try:
        plan_bulk_then([*parallel, step(10_000)], step(10_001), ids())
        raise AssertionError("expected fan-in batch overflow")
    except ValueError as exc:
        assert str(exc) == "flow exceeds the 10000 job limit"
