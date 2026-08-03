from typing import Any, Dict, List

from bunqueue.job import Job
from bunqueue.queue import Queue


class RecordingConnection:
    def __init__(self) -> None:
        self.commands: List[Dict[str, Any]] = []

    def call(self, command: Dict[str, Any], **_kwargs: Any) -> Dict[str, Any]:
        self.commands.append(command)
        if command["cmd"] == "PUSHB":
            return {"ok": True, "ids": ["bulk-a", "bulk-b"]}
        return {"ok": True, "id": "single"}


def test_push_keeps_data_name_and_primitive_payloads_separate() -> None:
    connection = RecordingConnection()
    queue = Queue("named", connection=connection)  # type: ignore[arg-type]
    data = {"name": "customer-visible", "to": "a@b.c"}

    job = queue.add("send-email", data)
    ids = queue.add_bulk(
        [
            {"name": "object-job", "data": {"name": "user-name", "value": 1}},
            {"name": "scalar-job", "data": 42},
        ]
    )

    assert connection.commands == [
        {"cmd": "PUSH", "queue": "named", "name": "send-email", "data": data},
        {
            "cmd": "PUSHB",
            "queue": "named",
            "jobs": [
                {"name": "object-job", "data": {"name": "user-name", "value": 1}},
                {"name": "scalar-job", "data": 42},
            ],
        },
    ]
    assert (job.name, job.data) == ("send-email", data)
    assert ids == ["bulk-a", "bulk-b"]


def test_job_prefers_top_level_name_and_only_unwraps_legacy_data() -> None:
    modern = Job({"name": "modern-op", "data": {"name": "user-name", "value": 1}})
    legacy = Job({"data": {"name": "legacy-op", "value": 2}})
    scalar = Job({"name": "scalar-op", "data": False})

    assert (modern.name, modern.data) == (
        "modern-op",
        {"name": "user-name", "value": 1},
    )
    assert (legacy.name, legacy.data) == ("legacy-op", {"value": 2})
    assert (scalar.name, scalar.data) == ("scalar-op", False)


def test_worker_owned_job_mutations_forward_the_delivery_token() -> None:
    connection = RecordingConnection()
    job = Job(
        {"id": "leased-job", "queue": "leased"},
        connection=connection,  # type: ignore[arg-type]
        token="lease-token",
    )

    job.retry()
    job.change_delay(30_000)
    job.move_to_delayed(60_000)
    job.discard()

    assert connection.commands == [
        {"cmd": "MoveToWait", "id": "leased-job", "token": "lease-token"},
        {
            "cmd": "ChangeDelay",
            "id": "leased-job",
            "delay": 30_000,
            "token": "lease-token",
        },
        {
            "cmd": "MoveToDelayed",
            "id": "leased-job",
            "delay": 60_000,
            "token": "lease-token",
        },
        {"cmd": "Discard", "id": "leased-job", "token": "lease-token"},
    ]


def test_explicit_queue_moves_accept_the_delivery_token() -> None:
    connection = RecordingConnection()
    queue = Queue("leased", connection=connection)  # type: ignore[arg-type]

    queue.move_job_to_wait("leased-job", "lease-token")
    queue.change_job_delay("leased-job", 30_000, "lease-token")
    queue.move_job_to_delayed("leased-job", 60_000, "lease-token")

    assert connection.commands == [
        {"cmd": "MoveToWait", "id": "leased-job", "token": "lease-token"},
        {
            "cmd": "ChangeDelay",
            "id": "leased-job",
            "delay": 30_000,
            "token": "lease-token",
        },
        {
            "cmd": "MoveToDelayed",
            "id": "leased-job",
            "delay": 60_000,
            "token": "lease-token",
        },
    ]
