defmodule Bunqueue.FlowSnapshots do
  @moduledoc false

  alias Bunqueue.ProtocolError

  @spec index!([map()], term()) :: %{String.t() => map()}
  def index!(jobs, snapshots) when is_list(snapshots) and length(jobs) == length(snapshots) do
    expected = Map.new(jobs, &{&1["id"], &1["queue"]})

    by_id =
      Enum.reduce(snapshots, %{}, fn
        snapshot, result when is_map(snapshot) ->
          id = snapshot["id"]
          queue = snapshot["queue"]

          if not is_binary(id) or Map.get(expected, id) != queue or Map.has_key?(result, id) do
            invalid_ids!()
          end

          Map.put(result, id, snapshot)

        _snapshot, _result ->
          raise ProtocolError, message: "Invalid PUSHF response: job snapshot is invalid"
      end)

    if map_size(by_id) != map_size(expected), do: invalid_ids!()
    by_id
  end

  def index!(_jobs, _snapshots) do
    raise ProtocolError, message: "Invalid PUSHF response: committed job snapshots are missing"
  end

  defp invalid_ids! do
    raise ProtocolError,
      message: "Invalid PUSHF response: committed job IDs do not match the request"
  end
end
