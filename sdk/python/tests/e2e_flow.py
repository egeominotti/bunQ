"""E2E: FlowProducer — trees, chains, fan-in, getFlow."""

from __future__ import annotations

import threading

from harness import Server, test, unique_name, wait_until

from bunqueue import FlowProducer, Queue, Worker


@test
def flow_tree_children_before_parent(server: Server) -> None:
    qname = unique_name("flow")
    order = []
    lock = threading.Lock()

    def process(job):
        with lock:
            order.append(job.name)
        return {"step": job.name}

    with Queue(qname, port=server.port) as queue, FlowProducer(port=server.port) as flows:
        worker = Worker(qname, process, port=server.port, concurrency=4, poll_timeout_ms=300)
        worker.start()
        try:
            node = flows.add(
                {
                    "name": "parent",
                    "queueName": qname,
                    "data": {"kind": "root"},
                    "children": [
                        {"name": "child-a", "queueName": qname, "data": {}},
                        {"name": "child-b", "queueName": qname, "data": {}},
                    ],
                }
            )
            assert len(node.children) == 2
            assert wait_until(lambda: queue.get_state(node.job.id) == "completed", 20)
        finally:
            worker.close(timeout=10)

        assert order[-1] == "parent", f"parent did not run last: {order}"
        assert set(order[:2]) == {"child-a", "child-b"}
        values = queue.get_children_values(node.job.id)
        assert len(values) == 2, f"children values missing: {values}"


@test
def flow_chain_sequential(server: Server) -> None:
    qname = unique_name("chain")
    order = []
    lock = threading.Lock()

    def process(job):
        with lock:
            order.append(job.name)
        return job.name

    with Queue(qname, port=server.port) as queue, FlowProducer(port=server.port) as flows:
        worker = Worker(qname, process, port=server.port, concurrency=4, poll_timeout_ms=300)
        worker.start()
        try:
            ids = flows.add_chain(
                [
                    {"name": "step-1", "queueName": qname},
                    {"name": "step-2", "queueName": qname},
                    {"name": "step-3", "queueName": qname},
                ]
            )
            assert len(ids) == 3
            assert wait_until(lambda: queue.get_state(ids[-1]) == "completed", 20)
        finally:
            worker.close(timeout=10)
        assert order == ["step-1", "step-2", "step-3"], f"chain order broken: {order}"


@test
def flow_fan_in(server: Server) -> None:
    qname = unique_name("fanin")
    merged = {}

    def process(job):
        if job.name == "merge":
            merged["children"] = job.get_children_values()
            return {"merged": True}
        return {"part": job.name}

    with Queue(qname, port=server.port) as queue, FlowProducer(port=server.port) as flows:
        worker = Worker(qname, process, port=server.port, concurrency=4, poll_timeout_ms=300)
        worker.start()
        try:
            result = flows.add_bulk_then(
                [
                    {"name": "part-a", "queueName": qname},
                    {"name": "part-b", "queueName": qname},
                    {"name": "part-c", "queueName": qname},
                ],
                {"name": "merge", "queueName": qname},
            )
            assert len(result["parallel_ids"]) == 3
            assert wait_until(lambda: queue.get_state(result["final_id"]) == "completed", 20)
        finally:
            worker.close(timeout=10)
        assert len(merged.get("children", {})) == 3, f"fan-in values: {merged}"


@test
def get_flow_reconstructs_tree(server: Server) -> None:
    qname = unique_name("gettree")
    with Queue(qname, port=server.port) as queue, FlowProducer(port=server.port) as flows:
        queue.pause()  # keep jobs in place while we inspect
        node = flows.add(
            {
                "name": "root",
                "queueName": qname,
                "children": [
                    {
                        "name": "mid",
                        "queueName": qname,
                        "children": [{"name": "leaf", "queueName": qname}],
                    }
                ],
            }
        )
        tree = flows.get_flow(node.job.id)
        assert tree is not None
        assert len(tree.children) == 1
        assert len(tree.children[0].children) == 1
        queue.obliterate()


@test
def flow_custom_ids_and_reciprocal_links(server: Server) -> None:
    qname = unique_name("customflow")
    root_id = f"root-{qname}"
    child_id = f"child-{qname}"
    with Queue(qname, port=server.port) as queue, FlowProducer(port=server.port) as flows:
        queue.pause()
        node = flows.add(
            {
                "name": "root",
                "queueName": qname,
                "opts": {"job_id": root_id},
                "children": [
                    {
                        "name": "child",
                        "queueName": qname,
                        "opts": {"job_id": child_id},
                    }
                ],
            }
        )
        assert node.job.id == root_id
        assert node.job.custom_id == root_id
        assert node.children[0].job.id == child_id
        assert node.children[0].job.parent_id == root_id
        assert node.job.children_ids == [child_id]
        queue.obliterate()


@test
def flow_invalid_batch_commits_no_jobs(server: Server) -> None:
    qname = unique_name("rollback")
    with Queue(qname, port=server.port) as queue, FlowProducer(port=server.port) as flows:
        try:
            flows.add_chain(
                [
                    {"name": "ok-step", "queueName": qname},
                    {"name": "bad-step", "queueName": "invalid queue name!!"},
                ]
            )
            raise AssertionError("expected chain to fail on invalid queue name")
        except Exception:
            pass
        assert queue.count() == 0, "failed atomic batch created jobs"
