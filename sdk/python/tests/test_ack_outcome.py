"""Focused ACK outcome validation, including duplicate-ID ACKB positions."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import pytest

from bunqueue.ack_batcher import AckBatcher, AckItem
from bunqueue.ack_outcome import ignored_ack_indices, transition_was_applied
from bunqueue.errors import CommandError


def test_single_transition_accepts_historical_and_ignored_outcomes() -> None:
    assert transition_was_applied({"ok": True}) is True
    assert transition_was_applied(
        {"ok": True, "data": {"applied": False, "reason": "already-finalized"}}
    ) is False


@pytest.mark.parametrize("data", [None, {}, {"applied": True}, {"applied": False}])
def test_single_transition_rejects_unknown_evidence(data: Any) -> None:
    with pytest.raises(CommandError):
        transition_was_applied({"ok": True, "data": data})


def test_ackb_uses_ignored_indices_for_duplicate_ids() -> None:
    response = {
        "ok": True,
        "data": {"ignoredIds": ["same"], "ignoredIndices": [1]},
    }
    assert ignored_ack_indices(response, ["same", "same"]) == {1}


def test_ackb_accepts_historical_response_without_data() -> None:
    assert ignored_ack_indices({"ok": True}, ["same", "same"]) == set()


def test_ackb_rejects_mismatched_or_duplicate_evidence() -> None:
    with pytest.raises(CommandError):
        ignored_ack_indices(
            {"ok": True, "data": {"ignoredIds": ["other"], "ignoredIndices": [0]}},
            ["same"],
        )
    with pytest.raises(CommandError):
        ignored_ack_indices(
            {"ok": True, "data": {"ignoredIndices": [0, 0]}},
            ["same"],
        )


def test_ackb_rejects_ambiguous_ignored_ids_only_evidence() -> None:
    with pytest.raises(CommandError, match="ignoredIndices"):
        ignored_ack_indices(
            {"ok": True, "data": {"ignoredIds": ["same"]}},
            ["same"],
        )


def test_ack_batcher_settles_ignored_position_without_error() -> None:
    class Connection:
        def call(self, _command: Dict[str, Any]) -> Dict[str, Any]:
            return {
                "ok": True,
                "data": {"ignoredIds": ["same"], "ignoredIndices": [1]},
            }

    settled: List[Tuple[Optional[BaseException], bool]] = []
    batcher = AckBatcher(Connection(), max_size=2, max_delay_ms=60_000)
    batcher.add(AckItem("same", "old", None, lambda err, applied: settled.append((err, applied))))
    batcher.add(AckItem("same", "new", None, lambda err, applied: settled.append((err, applied))))

    assert settled == [(None, True), (None, False)]


def test_ack_batcher_rejects_ids_only_evidence_for_every_position() -> None:
    class Connection:
        def call(self, _command: Dict[str, Any]) -> Dict[str, Any]:
            return {"ok": True, "data": {"ignoredIds": ["same"]}}

    settled: List[Tuple[Optional[BaseException], bool]] = []
    batcher = AckBatcher(Connection(), max_size=2, max_delay_ms=60_000)
    batcher.add(AckItem("same", "old", None, lambda err, applied: settled.append((err, applied))))
    batcher.add(AckItem("same", "new", None, lambda err, applied: settled.append((err, applied))))

    assert len(settled) == 2
    assert all(isinstance(error, CommandError) and applied is False for error, applied in settled)
