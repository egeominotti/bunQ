defmodule Bunqueue.Telemetry do
  @moduledoc false

  @spec emit(map(), String.t(), map()) :: :ok
  def emit(%{event_handler: nil}, _event, _fields), do: :ok

  def emit(state, event, fields) do
    payload =
      Map.merge(
        %{
          event: event,
          generation: state.generation,
          timestamp_ms: System.system_time(:millisecond)
        },
        fields
      )

    handler = state.event_handler

    spawn(fn ->
      try do
        handler.(payload)
      rescue
        _error -> :ok
      catch
        _kind, _reason -> :ok
      end
    end)

    :ok
  end
end
