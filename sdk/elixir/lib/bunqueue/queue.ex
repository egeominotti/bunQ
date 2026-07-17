defmodule Bunqueue.Queue do
  @moduledoc "Producing API for a named bunqueue queue."

  alias Bunqueue.{Connection, Job, Options}

  @enforce_keys [:name, :connection]
  defstruct [:name, :connection, owns_connection: false]

  @type t :: %__MODULE__{
          name: String.t(),
          connection: GenServer.server(),
          owns_connection: boolean()
        }

  @spec new(String.t(), GenServer.server(), boolean()) :: t()
  def new(name, connection, owns_connection \\ false) do
    %__MODULE__{name: name, connection: connection, owns_connection: owns_connection}
  end

  @spec add(t(), String.t(), term(), keyword() | map()) ::
          {:ok, Job.t()} | {:error, Exception.t()}
  def add(queue, name, data \\ %{}, options \\ []) do
    payload = job_payload(name, data)

    command =
      %{"cmd" => "PUSH", "queue" => queue.name, "data" => payload}
      |> Map.merge(Options.job(options))

    with {:ok, response} <- call(queue, command) do
      raw = %{"id" => response["id"], "queue" => queue.name, "data" => payload}
      {:ok, Job.from_wire(raw, queue.connection)}
    end
  end

  @spec add!(t(), String.t(), term(), keyword() | map()) :: Job.t()
  def add!(queue, name, data \\ %{}, options \\ []) do
    case add(queue, name, data, options) do
      {:ok, job} -> job
      {:error, error} -> raise error
    end
  end

  @spec add_bulk(t(), [map()]) :: {:ok, [String.t()]} | {:error, Exception.t()}
  def add_bulk(queue, entries) do
    jobs =
      Enum.map(entries, fn entry ->
        name = fetch(entry, :name)
        data = get(entry, :data, %{})
        options = get(entry, :opts, get(entry, :options, []))

        %{"data" => job_payload(name, data)}
        |> Map.merge(Options.job(options, true))
      end)

    with {:ok, response} <-
           call(queue, %{"cmd" => "PUSHB", "queue" => queue.name, "jobs" => jobs}) do
      {:ok, response["ids"] || []}
    end
  end

  @spec hello(t()) :: {:ok, map()} | {:error, Exception.t()}
  def hello(queue) do
    call(queue, %{
      "cmd" => "Hello",
      "protocolVersion" => 2,
      "capabilities" => []
    })
  end

  @spec ping(t()) :: {:ok, boolean()} | {:error, Exception.t()}
  def ping(queue) do
    with {:ok, %{"data" => data}} <- call(queue, %{"cmd" => "Ping"}) do
      {:ok, data["pong"] == true}
    end
  end

  defdelegate get_job(queue, id), to: Bunqueue.QueueQuery
  defdelegate get_job_by_custom_id(queue, custom_id), to: Bunqueue.QueueQuery
  defdelegate get_jobs(queue), to: Bunqueue.QueueQuery
  defdelegate get_jobs(queue, state), to: Bunqueue.QueueQuery
  defdelegate get_jobs(queue, state, offset), to: Bunqueue.QueueQuery
  defdelegate get_jobs(queue, state, offset, limit), to: Bunqueue.QueueQuery
  defdelegate get_state(queue, id), to: Bunqueue.QueueQuery
  defdelegate get_result(queue, id), to: Bunqueue.QueueQuery
  defdelegate get_progress(queue, id), to: Bunqueue.QueueQuery
  defdelegate get_job_counts(queue), to: Bunqueue.QueueQuery
  defdelegate count(queue), to: Bunqueue.QueueQuery
  defdelegate wait_for_job(queue, id, timeout_ms), to: Bunqueue.QueueQuery
  defdelegate get_logs(queue, id), to: Bunqueue.QueueQuery
  defdelegate get_logs(queue, id, start), to: Bunqueue.QueueQuery
  defdelegate get_logs(queue, id, start, finish), to: Bunqueue.QueueQuery
  defdelegate pause(queue), to: Bunqueue.QueueControl
  defdelegate resume(queue), to: Bunqueue.QueueControl
  defdelegate is_paused(queue), to: Bunqueue.QueueControl
  defdelegate drain(queue), to: Bunqueue.QueueControl
  defdelegate clean(queue, grace_ms, limit, state), to: Bunqueue.QueueControl
  defdelegate obliterate(queue), to: Bunqueue.QueueControl
  defdelegate cancel(queue, id), to: Bunqueue.QueueControl
  defdelegate discard(queue, id), to: Bunqueue.QueueControl
  defdelegate promote(queue, id), to: Bunqueue.QueueControl
  defdelegate promote_jobs(queue), to: Bunqueue.QueueControl
  defdelegate retry_job(queue, id), to: Bunqueue.QueueControl
  defdelegate move_to_delayed(queue, id, delay_ms), to: Bunqueue.QueueControl
  defdelegate change_priority(queue, id, priority), to: Bunqueue.QueueControl
  defdelegate change_delay(queue, id, delay_ms), to: Bunqueue.QueueControl
  defdelegate update(queue, id, data), to: Bunqueue.QueueControl
  defdelegate update_parent(queue, child_id, parent_id), to: Bunqueue.QueueControl
  defdelegate dlq(queue), to: Bunqueue.QueueAdmin
  defdelegate dlq(queue, count), to: Bunqueue.QueueAdmin
  defdelegate retry_dlq(queue), to: Bunqueue.QueueAdmin
  defdelegate retry_dlq(queue, job_id), to: Bunqueue.QueueAdmin
  defdelegate retry_dlq(queue, job_id, count), to: Bunqueue.QueueAdmin
  defdelegate purge_dlq(queue), to: Bunqueue.QueueAdmin
  defdelegate upsert_scheduler(queue, id, repeat), to: Bunqueue.QueueAdmin
  defdelegate upsert_scheduler(queue, id, repeat, template), to: Bunqueue.QueueAdmin
  defdelegate get_scheduler(queue, id), to: Bunqueue.QueueAdmin
  defdelegate list_schedulers(queue), to: Bunqueue.QueueAdmin
  defdelegate remove_scheduler(queue, id), to: Bunqueue.QueueAdmin
  defdelegate set_rate_limit(queue, limit), to: Bunqueue.QueueAdmin
  defdelegate set_rate_limit(queue, limit, options), to: Bunqueue.QueueAdmin
  defdelegate clear_rate_limit(queue), to: Bunqueue.QueueAdmin
  defdelegate set_concurrency(queue, limit), to: Bunqueue.QueueAdmin
  defdelegate clear_concurrency(queue), to: Bunqueue.QueueAdmin

  @spec close(t()) :: :ok
  def close(%{owns_connection: true, connection: connection}), do: Connection.close(connection)
  def close(_queue), do: :ok

  @doc false
  def call(queue, command, timeout \\ nil),
    do: Connection.call(queue.connection, command, timeout)

  @doc false
  def job_payload(name, data) when is_map(data) do
    normalized = Map.new(data, fn {key, value} -> {to_string(key), value} end)
    Map.merge(%{"name" => name}, normalized)
  end

  def job_payload(name, nil), do: %{"name" => name}
  def job_payload(name, data), do: %{"name" => name, "payload" => data}

  defp fetch(map, key) do
    case Map.fetch(map, key) do
      {:ok, value} -> value
      :error -> Map.fetch!(map, to_string(key))
    end
  end

  defp get(map, key, default) do
    Map.get(map, key, Map.get(map, to_string(key), default))
  end
end
