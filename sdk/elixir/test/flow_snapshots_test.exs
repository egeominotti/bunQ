defmodule Bunqueue.FlowSnapshotsTest do
  use ExUnit.Case, async: true

  alias Bunqueue.{FlowSnapshots, ProtocolError}

  test "requires an exact id and queue bijection" do
    jobs = [
      %{"id" => "one", "queue" => "queue"},
      %{"id" => "two", "queue" => "other"}
    ]

    valid = [snapshot("two", "other"), snapshot("one", "queue")]
    assert map_size(FlowSnapshots.index!(jobs, valid)) == 2

    invalid = [
      nil,
      [snapshot("one", "queue")],
      [snapshot("one", "queue"), snapshot("one", "queue")],
      [snapshot("one", "wrong"), snapshot("two", "other")],
      [snapshot("one", "queue"), snapshot("unknown", "other")],
      [snapshot("one", "queue"), "not-a-map"]
    ]

    for snapshots <- invalid do
      assert_raise ProtocolError, fn -> FlowSnapshots.index!(jobs, snapshots) end
    end
  end

  defp snapshot(id, queue) do
    %{"id" => id, "queue" => queue, "data" => %{"name" => "job"}}
  end
end
