"""FlowProducer: parent/child job trees, chains, and fan-in flows.

Mirrors the TS FlowProducer (TCP mode): children are pushed BEFORE their
parent; the parent carries dependsOn/childrenIds; each child is then linked
via UpdateParent. On any failure every already-created job is cancelled
(atomic rollback).
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence

from .connection import Connection, TlsOption, _compact
from .errors import CommandError
from .job import Job
from .options import job_options, job_payload
from .telemetry import TelemetryHandler


class FlowNode:
    """Result node: the created job plus its (optional) children nodes."""

    def __init__(self, job: Job, children: Optional[List["FlowNode"]] = None) -> None:
        self.job = job
        self.children = children or []


class FlowProducer:
    """Create dependent job hierarchies across one or more queues.

    Flow tree entries: ``{"name", "queueName", "data", "opts", "children": [...]}``
    (same shape as BullMQ / the TS client).
    """

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

    # ------------------------------------------------------------------- api

    def add(self, flow: Dict[str, Any], opts: Optional[Dict[str, Any]] = None) -> FlowNode:
        """Add a flow tree. Children are processed BEFORE their parent."""
        created: List[str] = []
        try:
            return self._add_node(flow, None, created, opts or {})
        except Exception:
            self._cleanup(created)
            raise

    def add_bulk(self, flows: Sequence[Dict[str, Any]]) -> List[FlowNode]:
        results: List[FlowNode] = []
        created: List[str] = []
        try:
            for flow in flows:
                results.append(self._add_node(flow, None, created, {}))
            return results
        except Exception:
            self._cleanup(created)
            raise

    def add_chain(self, steps: Sequence[Dict[str, Any]]) -> List[str]:
        """Sequential chain: step[0] -> step[1] -> ... (each depends on the previous)."""
        job_ids: List[str] = []
        prev_id: Optional[str] = None
        try:
            for step in steps:
                data = {"__flowParentId": prev_id, **(step.get("data") or {})}
                job_id = self._push(
                    step["queueName"],
                    job_payload(step["name"], data),
                    step.get("opts") or {},
                    depends_on=[prev_id] if prev_id else None,
                )
                job_ids.append(job_id)
                prev_id = job_id
            return job_ids
        except Exception:
            self._cleanup(job_ids)
            raise

    def add_bulk_then(
        self, parallel: Sequence[Dict[str, Any]], final: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Fan-in: N independent jobs converging into one final job.

        The final job records the parallel ids as childrenIds so its processor
        can read their results via ``get_children_values``.
        """
        parallel_ids: List[str] = []
        try:
            for step in parallel:
                parallel_ids.append(
                    self._push(
                        step["queueName"],
                        job_payload(step["name"], step.get("data")),
                        step.get("opts") or {},
                    )
                )
            final_data = {"__flowParentIds": parallel_ids, **(final.get("data") or {})}
            final_id = self._push_with_children(
                final["queueName"],
                job_payload(final["name"], final_data),
                final.get("opts") or {},
                child_ids=parallel_ids,
                parent_id=None,
            )
            return {"parallel_ids": parallel_ids, "final_id": final_id}
        except Exception:
            self._cleanup(parallel_ids)
            raise

    def get_flow(self, job_id: str, depth: Optional[int] = None) -> Optional[FlowNode]:
        """Reconstruct a flow tree from a root job id (GetJob recursion).

        ``depth`` limits recursion; ``None`` means unlimited (parity with the
        TS client's ``Number.POSITIVE_INFINITY`` default). A missing job — the
        root or any child removed via removeOnComplete/cancel — yields ``None``
        and is skipped, returning the surviving partial tree instead of raising.
        A ``visited`` set guards against cycles in ``childrenIds`` so unlimited
        depth cannot recurse forever.
        """
        return self._get_flow(job_id, depth, set())

    def _get_flow(
        self, job_id: str, depth: Optional[int], visited: set
    ) -> Optional[FlowNode]:
        if job_id in visited:
            return None  # cycle guard: childrenIds already on the current path
        visited.add(job_id)
        try:
            response = self.connection.call({"cmd": "GetJob", "id": job_id})
        except CommandError as exc:
            if "not found" in str(exc).lower():
                return None  # root or a since-removed child -> skip
            raise  # a real server error must not masquerade as a missing child
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

    def close(self) -> None:
        if self._owns_connection:
            self.connection.close()

    def __enter__(self) -> "FlowProducer":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    # ------------------------------------------------------------- internals

    def _add_node(
        self,
        node: Dict[str, Any],
        parent_ref: Optional[Dict[str, str]],
        created: List[str],
        flow_opts: Dict[str, Any],
    ) -> FlowNode:
        child_nodes: List[FlowNode] = []
        child_ids: List[str] = []

        for child in node.get("children") or []:
            child_node = self._add_node(
                child, {"id": "pending", "queue": node["queueName"]}, created, flow_opts
            )
            child_nodes.append(child_node)
            child_ids.append(child_node.job.id)

        queue_defaults = (flow_opts.get("queues_options") or {}).get(node["queueName"]) or {}
        merged_opts = {**queue_defaults, **(node.get("opts") or {})}

        data: Dict[str, Any] = dict(node.get("data") or {})
        if parent_ref:
            data["__parentId"] = parent_ref["id"]
            data["__parentQueue"] = parent_ref["queue"]
        if child_ids:
            data["__childrenIds"] = child_ids

        job_id = self._push_with_children(
            node["queueName"],
            job_payload(node["name"], data),
            merged_opts,
            child_ids=child_ids,
            parent_id=parent_ref["id"] if parent_ref else None,
        )
        created.append(job_id)

        job = Job({"id": job_id, "queue": node["queueName"], "data": data}, self.connection)
        return FlowNode(job, child_nodes)

    def _push(
        self,
        queue: str,
        data: Dict[str, Any],
        opts: Dict[str, Any],
        depends_on: Optional[List[str]] = None,
    ) -> str:
        command = {
            "cmd": "PUSH",
            "queue": queue,
            "data": data,
            **job_options(**opts),
        }
        if depends_on:
            command["dependsOn"] = depends_on
        response = self.connection.call(_compact(command))
        return str(response.get("id"))

    def _push_with_children(
        self,
        queue: str,
        data: Dict[str, Any],
        opts: Dict[str, Any],
        child_ids: List[str],
        parent_id: Optional[str],
    ) -> str:
        command = {
            "cmd": "PUSH",
            "queue": queue,
            "data": data,
            **job_options(**opts),
            "parentId": parent_id,
            "childrenIds": child_ids or None,
            "dependsOn": child_ids or None,
        }
        response = self.connection.call(_compact(command))
        job_id = str(response.get("id"))
        # Link children to their real parent id (placeholder was 'pending')
        for child_id in child_ids:
            self.connection.call({"cmd": "UpdateParent", "childId": child_id, "parentId": job_id})
        return job_id

    def _cleanup(self, job_ids: List[str]) -> None:
        for job_id in job_ids:
            try:
                self.connection.call({"cmd": "Cancel", "id": job_id})
            except Exception:  # noqa: BLE001 - best-effort rollback
                pass
