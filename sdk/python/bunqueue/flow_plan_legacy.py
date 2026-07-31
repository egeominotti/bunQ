"""Pure atomic planners for FlowProducer chains and fan-in graphs."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Sequence, Set

from .flow_plan import (
    MAX_FLOW_JOBS,
    AtomicFlowJob,
    FlowIdFactory,
    allocate_flow_id,
    assert_atomic_options,
    assert_flow_identity,
    flow_data,
    flow_input,
    portable_flow_id,
)


@dataclass
class ChainPlan:
    jobs: List[AtomicFlowJob]
    ids: List[str]


@dataclass
class FanInPlan:
    jobs: List[AtomicFlowJob]
    parallel_ids: List[str]
    final_id: str


def _allocate_steps(
    steps: Sequence[Dict[str, Any]], id_factory: FlowIdFactory
) -> List[str]:
    if len(steps) > MAX_FLOW_JOBS:
        raise ValueError("flow exceeds the 10000 job limit")
    allocated: Set[str] = set()
    ids: List[str] = []
    for step in steps:
        assert_flow_identity(step)
        if step.get("children"):
            raise ValueError("nested children are not supported by this flow method")
        raw_opts = step.get("opts")
        opts = {} if raw_opts is None else raw_opts
        assert_atomic_options(opts)
        ids.append(allocate_flow_id(opts, allocated, id_factory))
    return ids


def plan_chain(
    steps: Sequence[Dict[str, Any]],
    id_factory: FlowIdFactory = portable_flow_id,
) -> ChainPlan:
    ids = _allocate_steps(steps, id_factory)
    jobs: List[AtomicFlowJob] = []
    for index, step in enumerate(steps):
        dependency = ids[index - 1] if index else None
        jobs.append(
            {
                "id": ids[index],
                "queue": step["queueName"],
                "input": flow_input(
                    flow_data(
                        step["name"],
                        step.get("data"),
                        {"__flowParentId": dependency},
                    ),
                    step.get("opts") or {},
                    depends_on=[dependency] if dependency else None,
                ),
            }
        )
    return ChainPlan(jobs, ids)


def plan_bulk_then(
    parallel: Sequence[Dict[str, Any]],
    final: Dict[str, Any],
    id_factory: FlowIdFactory = portable_flow_id,
) -> FanInPlan:
    steps = [*parallel, final]
    ids = _allocate_steps(steps, id_factory)
    parallel_ids = ids[:-1]
    final_id = ids[-1]
    parallel_jobs = [
        {
            "id": parallel_ids[index],
            "queue": step["queueName"],
            "input": flow_input(
                flow_data(
                    step["name"],
                    step.get("data"),
                    {
                        "__parentId": final_id,
                        "__parentQueue": final["queueName"],
                    },
                ),
                step.get("opts") or {},
                parent_id=final_id,
            ),
        }
        for index, step in enumerate(parallel)
    ]
    final_job = {
        "id": final_id,
        "queue": final["queueName"],
        "input": flow_input(
            flow_data(
                final["name"],
                final.get("data"),
                {
                    "__flowParentIds": parallel_ids,
                    "__childrenIds": parallel_ids,
                },
            ),
            final.get("opts") or {},
            depends_on=parallel_ids,
            children_ids=parallel_ids,
        ),
    }
    return FanInPlan([*parallel_jobs, final_job], parallel_ids, final_id)
