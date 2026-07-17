"""E2E regressions for local protocol serialization validation."""

from __future__ import annotations

import struct

import msgpack

from harness import Server, test, unique_name

from bunqueue import Connection, Queue, SerializationError
from bunqueue.transport import encode_command
from bunqueue.wire import _js_safe


def assert_serialization_error(queue: Queue, payload) -> None:
    try:
        queue.connection.call({"cmd": "Ping", "payload": payload})
    except SerializationError:
        return
    except Exception as exc:
        raise AssertionError(f"expected SerializationError, got {type(exc).__name__}") from exc
    raise AssertionError("invalid payload was written to the broker")


@test
def serialization_rejects_cycles_and_non_string_map_keys(server: Server) -> None:
    queue = Queue(unique_name("serialization"), port=server.port)
    cyclic = {}
    cyclic["self"] = cyclic
    cyclic_list = []
    cyclic_tuple = (cyclic_list,)
    cyclic_list.append(cyclic_tuple)
    try:
        assert_serialization_error(queue, cyclic)
        assert_serialization_error(queue, cyclic_list)
        assert_serialization_error(queue, {1: "non-string key"})
        assert_serialization_error(queue, {"nested": [{"valid": {False: "invalid"}}]})
        assert not queue.connection._pending
        assert queue.ping() is True
    finally:
        queue.close()


@test
def serialization_reports_integer_float_overflow_as_typed(server: Server) -> None:
    queue = Queue(unique_name("serialization-int"), port=server.port)
    try:
        assert_serialization_error(queue, {"value": 10**10_000})
        assert queue.ping() is True
    finally:
        queue.close()


@test
def serialization_preserves_supported_containers_and_shared_values(
    _server: Server,
) -> None:
    shared = [2**40, b"\x00\xff"]
    source = {"tuple": (shared, bytearray(b"bin")), "again": shared}

    normalized = _js_safe(source)
    assert isinstance(normalized["tuple"], tuple)
    assert normalized["tuple"][0][0] == float(2**40)
    assert normalized["tuple"][0][1] == b"\x00\xff"
    assert normalized["tuple"][1] == bytearray(b"bin")

    frame = encode_command({"cmd": "Ping", "payload": source}, "req-1")
    (length,) = struct.unpack(">I", frame[:4])
    decoded = msgpack.unpackb(frame[4:], raw=False)
    assert length == len(frame) - 4
    assert decoded["payload"]["tuple"][0][0] == float(2**40)
    assert decoded["payload"]["tuple"][0][1] == b"\x00\xff"
    assert decoded["payload"]["again"][1] == b"\x00\xff"


@test
def serialization_rejection_precedes_pending_registration_and_write(
    _server: Server,
) -> None:
    class SinkSocket:
        def __init__(self) -> None:
            self.write_count = 0

        def sendall(self, _frame: bytes) -> None:
            self.write_count += 1

        def close(self) -> None:
            pass

    sink = SinkSocket()
    connection = Connection(command_timeout=0.01)
    connection._connected = True
    connection._sock = sink
    cyclic = []
    cyclic.append(cyclic)
    try:
        try:
            connection._send({"cmd": "Ping", "payload": cyclic})
        except SerializationError:
            pass
        else:
            raise AssertionError("cyclic payload was accepted")
        assert not connection._pending
        assert sink.write_count == 0
    finally:
        connection.close()
