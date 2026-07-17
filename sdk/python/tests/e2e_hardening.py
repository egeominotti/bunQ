"""Hardening: races, generated invariants, and malformed-input fuzzing."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict

from harness import Server, test, unique_name, wait_until

from bunqueue import Queue, SerializationError


def generated_payloads(seed: int, count: int) -> list[Dict[str, Any]]:
    state = seed & 0xFFFFFFFF
    payloads = []
    for index in range(count):
        state = (state * 1_664_525 + 1_013_904_223) & 0xFFFFFFFF
        payloads.append(
            {
                "index": index,
                "signed": state % 2_000_001 - 1_000_000,
                "flag": bool(state & 1),
                "text": f"case-{state:x}-🧪",
                "nullable": None if index % 3 == 0 else f"value-{index}",
                "nested": [state % 97, {"checksum": (state ^ index) & 0xFFFFFFFF}],
            }
        )
    return payloads


@test
def hardening_concurrent_custom_id_retries_are_idempotent(server: Server) -> None:
    name = unique_name("idempotency-race")

    def add(attempt: int) -> str:
        with Queue(name, port=server.port) as queue:
            return queue.add(
                "charge", {"attempt": attempt}, job_id="same-operation-id"
            ).id

    with ThreadPoolExecutor(max_workers=24) as pool:
        ids = list(pool.map(add, range(24)))

    with Queue(name, port=server.port) as queue:
        try:
            assert len(set(ids)) == 1, f"concurrent retries returned {set(ids)}"
            assert queue.count() == 1, "concurrent retries enqueued a duplicate"
        finally:
            queue.obliterate()


@test
def hardening_simultaneous_dequeues_lease_exactly_once(server: Server) -> None:
    name = unique_name("double-dequeue")
    with Queue(name, port=server.port) as producer:
        expected = producer.add("only-once", {"value": 1})
        contenders = [Queue(name, port=server.port) for _ in range(12)]

        def pull(index: int) -> Dict[str, Any]:
            return contenders[index].connection.call(
                {
                    "cmd": "PULL",
                    "queue": name,
                    "owner": f"contender-{index}",
                    "timeout": 250,
                }
            )

        try:
            with ThreadPoolExecutor(max_workers=12) as pool:
                responses = list(pool.map(pull, range(12)))
            leased = [response["job"] for response in responses if response.get("job")]
            assert len(leased) == 1, f"expected one lease, got {len(leased)}"
            assert str(leased[0]["id"]) == expected.id
        finally:
            for contender in contenders:
                contender.close()
            producer.obliterate()


@test
def hardening_generated_payloads_preserve_invariants(server: Server) -> None:
    payloads = generated_payloads(0xBADC0DE, 64)
    with Queue(unique_name("generated"), port=server.port) as queue:
        try:
            ids = queue.add_bulk(
                [
                    {"name": f"generated-{index % 7}", "data": payload}
                    for index, payload in enumerate(payloads)
                ]
            )
            assert len(ids) == len(payloads)
            assert wait_until(lambda: queue.count() == len(payloads))
            for index, job_id in enumerate(ids):
                fetched = queue.get_job(job_id)
                assert fetched is not None
                data = dict(fetched.data)
                assert data.pop("name") == f"generated-{index % 7}"
                assert data == payloads[index], f"payload {index} changed: {data}"
        finally:
            queue.obliterate()


@test
def hardening_mutation_fuzz_is_typed_and_connection_recovers(server: Server) -> None:
    invalid: list[Any] = []
    for depth in range(1, 13):
        value: Any = 10 ** (1_000 + depth)
        for _level in range(depth):
            value = {"nested": [value]}
        invalid.append(value)
    cycle: Dict[str, Any] = {}
    cycle["self"] = cycle
    invalid.extend([cycle, {1: "non-string key"}])

    with Queue(unique_name("mutation-fuzz"), port=server.port) as queue:
        for payload in invalid:
            try:
                queue.connection.call({"cmd": "Ping", "payload": payload})
            except SerializationError:
                pass
            else:
                raise AssertionError("malformed mutation was accepted")
        assert not queue.connection._pending
        assert queue.ping() is True
