"""Conformance driver for the Python SDK (bunqueue-client).

JSON lines on stdin/stdout; see ../README.md for the contract.
"""

from __future__ import annotations

import json
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "python"))

from bunqueue import Queue, UnrecoverableError, Worker  # noqa: E402

CONNECTION: dict = {"host": "127.0.0.1", "port": 6789}
QUEUES: dict = {}


def queue_for(name: str) -> Queue:
    if name not in QUEUES:
        QUEUES[name] = Queue(name, **CONNECTION)
    return QUEUES[name]


def deep_throw(n: int) -> None:
    if n <= 0:
        raise RuntimeError("BOOM-CONFORMANCE")
    deep_throw(n - 1)


def process_until(req: dict) -> None:
    queue = queue_for(req["queue"])
    behavior = req["behavior"]
    until = req.get("until") or {}
    timeout_s = float(req.get("timeoutMs", 20000)) / 1000
    failed_once: set = set()

    def processor(job):
        if behavior == "unrecoverable":
            raise UnrecoverableError("conformance poison")
        if behavior == "deepThrow":
            deep_throw(25)
        if behavior == "failOnce" and job.id not in failed_once:
            failed_once.add(job.id)
            raise RuntimeError("conformance transient")
        return req.get("result", "ok")

    worker_opts: dict = {"port": CONNECTION["port"], "host": CONNECTION["host"], "poll_timeout_ms": 300}
    if CONNECTION.get("token"):
        worker_opts["token"] = CONNECTION["token"]
    if req.get("batchSize") is not None:
        worker_opts["batch_size"] = int(req["batchSize"])
    worker = Worker(req["queue"], processor, **worker_opts)
    worker.on("error", lambda e: None)
    worker.on("failed", lambda j, e: None)

    def reached() -> bool:
        counts = queue.get_job_counts()
        if "completed" in until and counts.get("completed", 0) < until["completed"]:
            return False
        if "failed" in until and counts.get("failed", 0) < until["failed"]:
            return False
        if "dlq" in until and len(queue.get_dlq()) < until["dlq"]:
            return False
        return True

    worker.start()
    try:
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            if reached():
                return
            time.sleep(0.1)
        raise RuntimeError("until condition not reached before timeoutMs")
    finally:
        worker.close(timeout=10)


_CAMEL_TO_SNAKE = {"jobId": "job_id"}


def snake_opts(opts: dict | None) -> dict:
    """The wire contract uses camelCase; this SDK's kwargs are snake_case."""
    return {_CAMEL_TO_SNAKE.get(k, k): v for k, v in (opts or {}).items()}


def job_view(job) -> dict:
    return {
        "id": job.id,
        "name": job.name,
        "data": job.data,
        "stacktrace": job.stacktrace,
    }


def handle(req: dict) -> dict:
    op = req["op"]
    if op == "connect":
        CONNECTION.clear()
        CONNECTION.update({"host": req["host"], "port": int(req["port"])})
        if req.get("token"):
            CONNECTION["token"] = req["token"]
        return {}
    if op == "add":
        job = queue_for(req["queue"]).add(req["name"], req.get("data"), **snake_opts(req.get("opts")))
        return {"jobId": job.id}
    if op == "addBulk":
        entries = [
            {"name": e["name"], "data": e.get("data"), **snake_opts(e.get("opts"))}
            for e in req["entries"]
        ]
        return {"ids": queue_for(req["queue"]).add_bulk(entries)}
    if op == "getJob":
        job = queue_for("conf-lookup").get_job(req["jobId"])
        return {"job": job_view(job) if job else None}
    if op == "getJobByCustomId":
        job = queue_for(req["queue"]).get_job_by_custom_id(req["customId"])
        return {"job": {"id": job.id} if job else None}
    if op == "getState":
        return {"state": queue_for("conf-lookup").get_state(req["jobId"])}
    if op == "getResult":
        return {"result": queue_for("conf-lookup").get_result(req["jobId"])}
    if op == "count":
        return {"count": queue_for(req["queue"]).count()}
    if op == "isPaused":
        return {"paused": queue_for(req["queue"]).is_paused()}
    if op == "pause":
        queue_for(req["queue"]).pause()
        return {}
    if op == "resume":
        queue_for(req["queue"]).resume()
        return {}
    if op == "drain":
        return {"count": queue_for(req["queue"]).drain()}
    if op == "promote":
        queue_for("conf-lookup").promote_job(req["jobId"])
        return {}
    if op == "upsertScheduler":
        repeat = req.get("repeat") or {}
        template = req.get("template") or {}
        queue_for(req["queue"]).upsert_job_scheduler(req["schedulerId"], repeat, template)
        return {}
    if op == "getScheduler":
        return {"scheduler": queue_for("conf-lookup").get_job_scheduler(req["schedulerId"])}
    if op == "removeScheduler":
        queue_for("conf-lookup").remove_job_scheduler(req["schedulerId"])
        return {}
    if op == "waitForJob":
        return {"result": queue_for("conf-lookup").wait_for_job(req["jobId"], timeout_ms=int(req["timeoutMs"]))}
    if op == "getDlqCount":
        return {"count": len(queue_for(req["queue"]).get_dlq())}
    if op == "retryDlq":
        return {"count": queue_for(req["queue"]).retry_dlq()}
    if op == "hello":
        hello = queue_for("conf-lookup").connection.hello()
        return {
            "protocolVersion": hello.get("protocolVersion"),
            "capabilities": hello.get("capabilities"),
        }
    if op == "process":
        process_until(req)
        return {}
    if op == "close":
        for queue in QUEUES.values():
            queue.close()
        sys.exit(0)
    raise RuntimeError(f"unknown op: {op}")


def main() -> None:
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        req = json.loads(line)
        try:
            result = handle(req)
            answer = {"id": req["id"], "ok": True, **result}
        except SystemExit:
            raise
        except Exception as exc:  # noqa: BLE001 - report to the runner
            answer = {"id": req["id"], "ok": False, "error": str(exc)}
        sys.stdout.write(json.dumps(answer) + "\n")
        sys.stdout.flush()


if __name__ == "__main__":
    main()
