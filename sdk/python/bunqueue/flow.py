"""FlowProducer: atomic trees, chains, fan-in flows, and flow reads."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence, Set

from .connection import Connection, TlsOption
from .errors import CommandError
from .flow_commit import commit_flow
from .flow_plan import PlannedFlowNode, plan_flows
from .flow_plan_legacy import plan_bulk_then, plan_chain
from .job import Job
from .telemetry import TelemetryHandler


class FlowNode:
    """Result node built from the broker's committed job snapshot."""

    def __init__(self, job: Job, children: Optional[List["FlowNode"]] = None) -> None:
        self.job = job
        self.children = children or []


class FlowProducer:
    """Create dependent job graphs through one broker-side atomic commit."""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 6789,
        token: Optional[str] = None,
        tls: TlsOption = None,
        connection: Optional[Connection] = None,
        on_telemetry: Optional[TelemetryHandler] = None,
    ) -> None:
        self.connection = connection or Connection(
            host=host, port=port, token=token, tls=tls, on_telemetry=on_telemetry
        )
        self._owns_connection = connection is None

    def add(
        self, flow: Dict[str, Any], opts: Optional[Dict[str, Any]] = None
    ) -> FlowNode:
        """Add a tree atomically; children become runnable before their parent."""
        plan = plan_flows([flow], opts)
        snapshots = commit_flow(self.connection, plan.jobs)
        return self._build_node(plan.roots[0], snapshots)

    def add_bulk(self, flows: Sequence[Dict[str, Any]]) -> List[FlowNode]:
        """Add multiple independent trees in one atomic batch."""
        plan = plan_flows(list(flows))
        snapshots = commit_flow(self.connection, plan.jobs)
        return [self._build_node(root, snapshots) for root in plan.roots]

    def add_chain(self, steps: Sequence[Dict[str, Any]]) -> List[str]:
        """Add a sequential dependency chain in one atomic batch."""
        if not steps:
            return []
        plan = plan_chain(steps)
        commit_flow(self.connection, plan.jobs)
        return plan.ids

    def add_bulk_then(
        self, parallel: Sequence[Dict[str, Any]], final: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Add parallel jobs and their final fan-in job in one atomic batch."""
        plan = plan_bulk_then(parallel, final)
        commit_flow(self.connection, plan.jobs)
        return {"parallel_ids": plan.parallel_ids, "final_id": plan.final_id}

    def get_flow(self, job_id: str, depth: Optional[int] = None) -> Optional[FlowNode]:
        """Reconstruct a surviving flow tree from broker snapshots."""
        return self._get_flow(job_id, depth, set())

    def close(self) -> None:
        if self._owns_connection:
            self.connection.close()

    def __enter__(self) -> "FlowProducer":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def _build_node(
        self,
        node: PlannedFlowNode,
        snapshots: Dict[str, Dict[str, Any]],
    ) -> FlowNode:
        snapshot = snapshots.get(node.id)
        if snapshot is None:
            raise ValueError(f"Committed flow snapshot missing for {node.id}")
        children = [self._build_node(child, snapshots) for child in node.children]
        return FlowNode(Job(snapshot, self.connection), children)

    def _get_flow(
        self, job_id: str, depth: Optional[int], visited: Set[str]
    ) -> Optional[FlowNode]:
        if job_id in visited:
            return None
        visited.add(job_id)
        try:
            response = self.connection.call({"cmd": "GetJob", "id": job_id})
        except CommandError as exc:
            if "not found" in str(exc).lower():
                return None
            raise
        raw = response.get("job")
        if not raw:
            return None
        job = Job(raw, self.connection)
        children: List[FlowNode] = []
        if depth is None or depth > 0:
            next_depth = None if depth is None else depth - 1
            for child_id in job.children_ids:
                child = self._get_flow(child_id, next_depth, visited)
                if child:
                    children.append(child)
        return FlowNode(job, children)
