"""E2E: structured connection telemetry is complete and failure-isolated."""

from __future__ import annotations

from harness import Server, test, unique_name

from bunqueue import Queue


@test
def telemetry_reports_connection_command_and_disconnect(server: Server) -> None:
    events = []
    queue = Queue(
        unique_name("telemetry"),
        port=server.port,
        on_telemetry=events.append,
    )
    queue.add("job", {"value": 1})
    queue.close()

    assert any(event["type"] == "connect" for event in events)
    command = next(
        event
        for event in events
        if event["type"] == "command" and event["cmd"] == "PUSH"
    )
    assert command["ok"] is True
    assert command["duration_ms"] >= 0
    assert any(event["type"] == "disconnect" for event in events)


@test
def telemetry_consumer_failure_never_breaks_transport(server: Server) -> None:
    calls = []

    def raising_handler(event) -> None:
        calls.append(event["type"])
        raise RuntimeError("telemetry backend unavailable")

    queue = Queue(
        unique_name("telemetry-safe"),
        port=server.port,
        on_telemetry=raising_handler,
    )
    try:
        assert queue.ping() is True
        assert "connect" in calls
        assert "command" in calls
    finally:
        queue.close()
