"""Validation helpers for authoritative ACK, ACKB, and FAIL outcomes."""

from __future__ import annotations

from typing import Any, Dict, List, Sequence, Set

from .errors import CommandError


def transition_was_applied(response: Dict[str, Any]) -> bool:
    """Return whether a successful single-job transition changed broker state."""
    if "data" not in response:
        return True
    data = response["data"]
    if (
        isinstance(data, dict)
        and data.get("applied") is False
        and data.get("reason") == "already-finalized"
    ):
        return False
    raise CommandError("invalid ACK/FAIL outcome from broker")


def ignored_ack_indices(response: Dict[str, Any], batch_ids: Sequence[str]) -> Set[int]:
    """Validate ACKB evidence and return ignored input positions.

    ``ignoredIndices`` is authoritative because duplicate job IDs can occupy
    different lease generations in one batch. Historical responses without a
    ``data`` field mean that every position applied; structured ignored
    evidence must include exact indices.
    """
    if "data" not in response:
        return set()
    data = response["data"]
    if not isinstance(data, dict):
        raise CommandError("invalid ACKB outcome from broker")

    has_indices = "ignoredIndices" in data
    has_ids = "ignoredIds" in data
    if not has_indices and not has_ids:
        return set()
    if not has_indices:
        raise CommandError("invalid ACKB outcome: ignoredIndices are required")

    ids = _validated_ids(data.get("ignoredIds")) if has_ids else None
    indices = _validated_indices(data.get("ignoredIndices"), len(batch_ids))
    if ids is not None:
        if len(ids) != len(indices):
            raise CommandError("mismatched ACKB ignored evidence")
        for offset, index in enumerate(indices):
            if batch_ids[index] != ids[offset]:
                raise CommandError("mismatched ACKB ignored evidence")
    return set(indices)


def _validated_ids(value: Any) -> List[str]:
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        raise CommandError("invalid ACKB ignoredIds response")
    return value


def _validated_indices(value: Any, size: int) -> List[int]:
    if not isinstance(value, list):
        raise CommandError("invalid ACKB ignoredIndices response")
    seen: Set[int] = set()
    for index in value:
        if (
            isinstance(index, bool)
            or not isinstance(index, int)
            or index < 0
            or index >= size
            or index in seen
        ):
            raise CommandError("invalid ACKB ignoredIndices response")
        seen.add(index)
    return list(value)
