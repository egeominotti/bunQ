defmodule Bunqueue.Job do
  @moduledoc "A typed view of a bunqueue job."

  alias Bunqueue.Connection

  @enforce_keys [:id, :queue, :data, :connection]
  defstruct [
    :id,
    :queue,
    :name,
    :data,
    :state,
    :attempts_made,
    :stacktrace,
    :token,
    :connection,
    raw: %{}
  ]

  @type t :: %__MODULE__{
          id: String.t(),
          queue: String.t(),
          name: String.t() | nil,
          data: term(),
          state: String.t() | nil,
          attempts_made: integer(),
          stacktrace: [String.t()],
          token: String.t() | nil,
          connection: GenServer.server(),
          raw: map()
        }

  @spec from_wire(map(), GenServer.server(), String.t() | nil) :: t()
  def from_wire(raw, connection, token \\ nil) do
    data = Map.get(raw, "data") || %{}

    %__MODULE__{
      id: to_string(Map.get(raw, "id", "")),
      queue: to_string(Map.get(raw, "queue", "")),
      name: if(is_map(data), do: Map.get(data, "name")),
      data: data,
      state: Map.get(raw, "state"),
      attempts_made: Map.get(raw, "attemptsMade", 0),
      stacktrace: Map.get(raw, "stacktrace") || [],
      token: token,
      connection: connection,
      raw: raw
    }
  end

  @spec update_progress(t(), number(), String.t() | nil) ::
          {:ok, map()} | {:error, Exception.t()}
  def update_progress(job, progress, message \\ nil) do
    Connection.call(job.connection, %{
      "cmd" => "Progress",
      "id" => job.id,
      "progress" => progress,
      "message" => message
    })
  end

  @spec log(t(), String.t()) :: {:ok, map()} | {:error, Exception.t()}
  def log(job, message) do
    Connection.call(job.connection, %{"cmd" => "Log", "id" => job.id, "message" => message})
  end
end
