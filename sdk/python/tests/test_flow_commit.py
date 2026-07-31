"""Focused contracts for atomic FlowProducer commit and snapshots."""

import pytest

from bunqueue.flow_commit import commit_flow, validate_flow_snapshots
from bunqueue.flow_plan_legacy import plan_chain


def test_commit_sends_the_complete_plan_with_one_pushf_command() -> None:
    index = 0

    def next_id() -> str:
        nonlocal index
        value = f"commit-{index}"
        index += 1
        return value

    jobs = plan_chain(
        [
            {"name": "first", "queueName": "queue"},
            {"name": "second", "queueName": "queue"},
        ],
        next_id,
    ).jobs

    class FakeConnection:
        def __init__(self) -> None:
            self.commands = []

        def call(self, command):
            self.commands.append(command)
            return {
                "ok": True,
                "data": {
                    "jobs": [
                        {"id": job["id"], "queue": job["queue"]} for job in jobs
                    ]
                },
            }

    connection = FakeConnection()
    assert len(commit_flow(connection, jobs)) == 2
    assert connection.commands == [{"cmd": "PUSHF", "jobs": jobs}]


def test_commit_skips_transport_for_an_empty_plan() -> None:
    class FailingConnection:
        def call(self, command):
            raise AssertionError(f"unexpected transport call: {command}")

    assert commit_flow(FailingConnection(), []) == {}


def test_snapshot_validation_rejects_every_malformed_identity_set() -> None:
    jobs = [
        {"id": "one", "queue": "queue-a", "input": {"data": {"name": "one"}}},
        {"id": "two", "queue": "queue-b", "input": {"data": {"name": "two"}}},
    ]
    valid = [
        {"id": "two", "queue": "queue-b", "state": "waiting"},
        {"id": "one", "queue": "queue-a", "state": "waiting"},
    ]
    assert list(validate_flow_snapshots(jobs, valid)) == ["two", "one"]
    malformed = [
        None,
        {},
        [valid[0]],
        [valid[0], None],
        [valid[0], valid[0]],
        [valid[0], {**valid[1], "id": "foreign"}],
        [valid[0], {"id": "foreign"}],
        [valid[0], {**valid[1], "queue": "wrong"}],
        [valid[0], {**valid[1], "id": 42}],
        [valid[0], {"id": 42}],
        [valid[0], 42],
    ]
    for snapshots in malformed:
        with pytest.raises(ValueError, match="Invalid PUSHF response"):
            validate_flow_snapshots(jobs, snapshots)


def test_commit_reports_a_missing_response_envelope() -> None:
    class MissingDataConnection:
        def call(self, command):
            return {"ok": True}

    jobs = [{"id": "one", "queue": "queue", "input": {"data": {"name": "one"}}}]
    with pytest.raises(ValueError, match="committed job snapshots are missing"):
        commit_flow(MissingDataConnection(), jobs)
