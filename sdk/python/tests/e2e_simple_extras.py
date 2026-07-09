"""E2E: Simple Mode extras — rate gate window/groups, TTL, dedup, cron."""

from __future__ import annotations

import threading
import time

from harness import Server, test, unique_name, wait_until

from bunqueue import Bunqueue


@test
def simple_rate_gate_window(server: Server) -> None:
    """Client-side limiter: no more than `max` starts per duration window."""
    starts: list = []
    lock = threading.Lock()

    def process(job):
        with lock:
            starts.append(time.monotonic())
        return "ok"

    app = Bunqueue(
        unique_name("rate"),
        port=server.port,
        poll_timeout_ms=300,
        concurrency=6,
        processor=process,
        rate_limit={"max": 2, "duration": 500},
    )
    try:
        for i in range(6):
            app.add("call", {"i": i})
        assert wait_until(lambda: len(starts) == 6, 20)
        starts.sort()
        # The limiter admits <= 2 per 500ms sliding window at ADMISSION time, but
        # `starts` records time.monotonic() inside the processor — slightly after
        # admission, with thread-scheduling jitter that under CI load can shift a
        # start across a window edge. So assert the guarantee two robust ways
        # instead of an exact per-sliding-window count (which flaked: a straddle
        # measured 3):
        #   1) Throttling actually happened: 6 jobs at 2/500ms are gated to ~3
        #      windows, so the last start is ~1s after the first. A broken/no-op
        #      limiter would release all 6 at once (span ~0).
        span = starts[-1] - starts[0]
        assert span >= 0.9, f"limiter did not throttle: 6 starts spanned {span * 1000:.0f}ms"
        #   2) Burst bound: no window holds more than max+1 (tolerating a single
        #      boundary/jitter straddle). Combined with (1) this still catches a
        #      grossly broken limiter — e.g. 3/window would finish 6 in ~2 windows
        #      and fail the span check above.
        for i, t0 in enumerate(starts):
            in_window = [t for t in starts if 0 <= (t - t0) * 1000 < 500]
            assert len(in_window) <= 3, f"window {i} had {len(in_window)} starts"
    finally:
        app.close()


@test
def simple_rate_gate_group_key(server: Server) -> None:
    """Per-group limiting: different groups do not throttle each other."""
    starts: dict = {"a": [], "b": []}
    lock = threading.Lock()

    def process(job):
        with lock:
            starts[job.data["customer"]].append(time.monotonic())
        return "ok"

    app = Bunqueue(
        unique_name("rateg"),
        port=server.port,
        poll_timeout_ms=300,
        concurrency=8,
        processor=process,
        rate_limit={"max": 1, "duration": 400, "group_key": "customer"},
    )
    try:
        for _ in range(2):
            app.add("call", {"customer": "a"})
            app.add("call", {"customer": "b"})
        assert wait_until(lambda: len(starts["a"]) + len(starts["b"]) == 4, 20)
        # each group serialized (max 1 per 400ms), but groups run independently
        for group in ("a", "b"):
            gap = abs(starts[group][1] - starts[group][0]) * 1000
            assert gap >= 350, f"group {group} not throttled: gap {gap:.0f}ms"
        first_a, first_b = min(starts["a"]), min(starts["b"])
        assert abs(first_a - first_b) * 1000 < 350, "groups throttled each other"
    finally:
        app.close()


@test
def simple_ttl_expires_jobs(server: Server) -> None:
    processed = []
    app = Bunqueue(
        unique_name("ttl"),
        port=server.port,
        poll_timeout_ms=300,
        processor=lambda job: processed.append(job.name) or "ok",
        ttl={"per_name": {"verify-otp": 200}},
    )
    failed = []
    app.on("failed", lambda job, err: failed.append((job.name, str(err))))
    try:
        app.pause()  # keep the job waiting past its TTL
        app.add("verify-otp", {"code": "123"}, attempts=1)
        time.sleep(0.5)
        app.resume()
        assert wait_until(lambda: len(failed) == 1, 15)
        assert "expired" in failed[0][1]
        assert not processed
    finally:
        app.close()


@test
def simple_cancellation_signal(server: Server) -> None:
    app_ref = {}

    def long_job(job):
        signal = app_ref["app"].get_signal(job.id)
        for _ in range(100):
            if signal is not None and signal.aborted:
                raise RuntimeError("Cancelled")
            time.sleep(0.05)
        return "never"

    app = Bunqueue(unique_name("cancel"), port=server.port, poll_timeout_ms=300, processor=long_job)
    app_ref["app"] = app
    failed = []
    app.on("failed", lambda job, err: failed.append(str(err)))
    try:
        job = app.add("video", {}, attempts=1)
        assert wait_until(lambda: app.get_signal(job.id) is not None, 15)
        app.cancel(job.id)
        assert app.is_cancelled(job.id)
        assert wait_until(lambda: any("Cancelled" in f for f in failed), 15)
    finally:
        app.close()


@test
def simple_dedup_same_name_and_data(server: Server) -> None:
    app = Bunqueue(
        unique_name("dedup"),
        port=server.port,
        poll_timeout_ms=300,
        autorun=False,  # producer-only: keep jobs waiting for the count check
        processor=lambda job: "ok",
        deduplication={"ttl": 60000},
    )
    try:
        app.add("hook", {"event": "user.created", "userId": "123"})
        app.add("hook", {"event": "user.created", "userId": "123"})  # deduplicated
        app.add("hook", {"event": "user.updated", "userId": "123"})  # different data
        assert wait_until(lambda: app.count() == 2, 10), f"count={app.count()}"
    finally:
        app.close()


@test
def simple_cron_lifecycle(server: Server) -> None:
    ticks = []
    app = Bunqueue(
        unique_name("cron"),
        port=server.port,
        poll_timeout_ms=300,
        processor=lambda job: ticks.append(job.name) or "ok",
    )
    try:
        scheduler = unique_name("hc")
        app.every(scheduler, 300, {"type": "ping"})
        crons = app.list_crons()
        assert any(c["name"] == scheduler for c in crons), crons
        assert wait_until(lambda: len(ticks) >= 1, 15)
        app.remove_cron(scheduler)
    finally:
        app.close()
