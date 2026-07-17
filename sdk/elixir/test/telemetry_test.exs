defmodule Bunqueue.TelemetryTest do
  use ExUnit.Case, async: true

  alias Bunqueue.Telemetry

  test "emits structured events and isolates callback failures" do
    parent = self()
    state = %{event_handler: &send(parent, {:event, &1}), generation: 2}

    assert Telemetry.emit(state, "command", %{command: "Ping", ok: true}) == :ok
    assert_receive {:event, event}, 500
    assert event.event == "command"
    assert event.command == "Ping"
    assert event.generation == 2
    assert is_integer(event.timestamp_ms)

    assert Telemetry.emit(%{state | event_handler: fn _ -> raise "ignored" end}, "error", %{}) ==
             :ok
  end
end
