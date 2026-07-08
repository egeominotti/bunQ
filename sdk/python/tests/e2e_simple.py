"""E2E: Simple Mode (Bunqueue) — routes, middleware, retry, circuit breaker,
batch, triggers, TTL, cancellation, dedup, cron. Mirrors bunqueue.ts semantics."""

from __future__ import annotations

import threading
import time

from harness import Server, test, unique_name, wait_until

from bunqueue import Bunqueue


@test
def simple_requires_exactly_one_mode(server: Server) -> None:
    try:
        Bunqueue(unique_name("s"), port=server.port)
        raise AssertionError("expected error with no mode")
    except ValueError:
        pass
    try:
        Bunqueue(
            unique_name("s"),
            port=server.port,
            processor=lambda j: 1,
            routes={"a": lambda j: 2},
        )
        raise AssertionError("expected error with two modes")
    except ValueError:
        pass
    try:
        Bunqueue(unique_name("s"), embedded=True, processor=lambda j: 1)
        raise AssertionError("expected error with embedded=True")
    except ValueError:
        pass


@test
def simple_routes_dispatch(server: Server) -> None:
    app = Bunqueue(
        unique_name("routes"),
        port=server.port,
        poll_timeout_ms=300,
        routes={
            "send-email": lambda job: {"channel": "email", "to": job.data["to"]},
            "send-sms": lambda job: {"channel": "sms"},
        },
    )
    results = {}
    app.on("completed", lambda job, result: results.update({job.name: result}))
    try:
        app.add("send-email", {"to": "alice"})
        app.add("send-sms", {"to": "bob"})
        assert wait_until(lambda: len(results) == 2, 15)
        assert results["send-email"]["channel"] == "email"
        assert results["send-email"]["to"] == "alice"
        assert results["send-sms"]["channel"] == "sms"
    finally:
        app.close()


@test
def simple_middleware_onion_order(server: Server) -> None:
    order: list = []
    app = Bunqueue(
        unique_name("mw"),
        port=server.port,
        poll_timeout_ms=300,
        processor=lambda job: order.append("processor") or "done",
    )

    def mw1(job, next_fn):
        order.append("mw1-in")
        result = next_fn()
        order.append("mw1-out")
        return result

    def mw2(job, next_fn):
        order.append("mw2-in")
        result = next_fn()
        order.append("mw2-out")
        return result

    app.use(mw1).use(mw2)
    done = []
    app.on("completed", lambda job, result: done.append(result))
    try:
        app.add("task", {})
        assert wait_until(lambda: len(done) == 1, 15)
        assert order == ["mw1-in", "mw2-in", "processor", "mw2-out", "mw1-out"], order
    finally:
        app.close()


@test
def simple_retry_with_retry_if(server: Server) -> None:
    attempts = []

    def flaky(job):
        attempts.append(1)
        if len(attempts) < 3:
            raise RuntimeError("503 service unavailable")
        return "recovered"

    app = Bunqueue(
        unique_name("retry"),
        port=server.port,
        poll_timeout_ms=300,
        processor=flaky,
        retry={
            "max_attempts": 5,
            "delay": 30,
            "strategy": "jitter",
            "retry_if": lambda error, attempt: "503" in str(error),
        },
    )
    done = []
    app.on("completed", lambda job, result: done.append(result))
    try:
        app.add("api-call", {})
        assert wait_until(lambda: done == ["recovered"], 15)
        assert len(attempts) == 3, f"in-process retries: {len(attempts)}"
    finally:
        app.close()


@test
def simple_circuit_breaker_opens_and_recovers(server: Server) -> None:
    opened = []
    app = Bunqueue(
        unique_name("cb"),
        port=server.port,
        poll_timeout_ms=300,
        concurrency=1,
        processor=lambda job: (_ for _ in ()).throw(RuntimeError("gateway down")),
        circuit_breaker={"threshold": 2, "reset_timeout": 800, "on_open": opened.append},
    )
    try:
        for _ in range(3):
            app.add("charge", {}, attempts=1)
        assert wait_until(lambda: app.get_circuit_state() == "open", 15)
        assert opened and opened[0] >= 2
        assert app.is_paused()
        assert wait_until(lambda: app.get_circuit_state() == "half-open", 10)
        app.reset_circuit()
        assert app.get_circuit_state() == "closed"
    finally:
        app.close()


@test
def simple_batch_accumulates(server: Server) -> None:
    batches = []

    def batch_processor(jobs):
        batches.append(len(jobs))
        return [{"inserted": True} for _ in jobs]

    app = Bunqueue(
        unique_name("batch"),
        port=server.port,
        poll_timeout_ms=300,
        concurrency=8,
        batch={"size": 4, "timeout": 1500, "processor": batch_processor},
    )
    done = []
    app.on("completed", lambda job, result: done.append(result))
    try:
        for i in range(4):
            app.add("row", {"i": i})
        assert wait_until(lambda: len(done) == 4, 15)
        assert batches and batches[0] == 4, f"size-triggered flush: {batches}"
        app.add("row", {"i": 99})  # partial batch → timeout flush
        assert wait_until(lambda: len(done) == 5, 15)
    finally:
        app.close()


@test
def simple_triggers_with_condition(server: Server) -> None:
    handled = []
    app = Bunqueue(
        unique_name("trig"),
        port=server.port,
        poll_timeout_ms=300,
        routes={
            "place-order": lambda job: {"total": job.data["total"]},
            "fraud-alert": lambda job: handled.append(("fraud", job.data["amount"])) or "ok",
            "send-receipt": lambda job: handled.append(("receipt", job.data["total"])) or "ok",
        },
    )
    app.trigger(
        {
            "on": "place-order",
            "create": "send-receipt",
            "data": lambda result, job: {"total": result["total"]},
        }
    ).trigger(
        {
            "on": "place-order",
            "create": "fraud-alert",
            "data": lambda result, job: {"amount": result["total"]},
            "condition": lambda result, job: result["total"] > 1000,
        }
    )
    try:
        app.add("place-order", {"total": 99})
        app.add("place-order", {"total": 5000})
        assert wait_until(lambda: len([h for h in handled if h[0] == "receipt"]) == 2, 20)
        frauds = [h for h in handled if h[0] == "fraud"]
        assert frauds == [("fraud", 5000)], f"conditional trigger: {handled}"
    finally:
        app.close()
