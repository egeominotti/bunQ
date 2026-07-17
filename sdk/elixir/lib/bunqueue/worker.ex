defmodule Bunqueue.Worker do
  @moduledoc """
  Lease-aware worker with bounded batch pulls and automatic job heartbeats.

  The handler may return a result, `{:ok, result}`, `{:error, reason}`, or
  raise. Raising `Bunqueue.UnrecoverableError` skips all remaining retries.
  """

  alias Bunqueue.{Connection, Job, ProtocolError, WorkerLifecycle, WorkerProcessor}

  @enforce_keys [
    :queue,
    :handler,
    :connection,
    :heartbeat_connection,
    :worker_id,
    :stats,
    :lifecycle
  ]
  defstruct [
    :queue,
    :handler,
    :connection,
    :heartbeat_connection,
    :worker_id,
    :stats,
    :lifecycle,
    concurrency: 1,
    batch_size: 1,
    poll_timeout: 1_000,
    lock_ttl: 30_000,
    heartbeat_interval: 10_000,
    stack_trace_limit: 10,
    name: nil
  ]

  @type t :: %__MODULE__{}

  @spec new(String.t(), (Job.t() -> term()), keyword()) :: t()
  def new(queue, handler, options \\ []) when is_function(handler, 1) do
    connection_options = Keyword.get(options, :connection, options)
    {:ok, connection} = Connection.start_link(connection_options)
    {:ok, heartbeat_connection} = Connection.start_link(connection_options)
    {:ok, lifecycle} = WorkerLifecycle.start_link()
    stats = :atomics.new(3, signed: false)
    worker_id = Keyword.get(options, :worker_id, unique_id())
    concurrency = options |> Keyword.get(:concurrency, 1) |> positive()

    %__MODULE__{
      queue: queue,
      handler: handler,
      connection: connection,
      heartbeat_connection: heartbeat_connection,
      worker_id: worker_id,
      stats: stats,
      lifecycle: lifecycle,
      concurrency: concurrency,
      batch_size: options |> Keyword.get(:batch_size, concurrency) |> clamp_batch(),
      poll_timeout: options |> Keyword.get(:poll_timeout, 1_000) |> clamp_poll(),
      lock_ttl: options |> Keyword.get(:lock_ttl, 30_000) |> positive(),
      heartbeat_interval: normalize_interval(Keyword.get(options, :heartbeat_interval, 10_000)),
      stack_trace_limit: options |> Keyword.get(:stack_trace_limit, 10) |> positive(),
      name: Keyword.get(options, :name, worker_id)
    }
  end

  @spec run_once(t()) :: {:ok, non_neg_integer()} | {:error, Exception.t()}
  def run_once(worker) do
    case WorkerLifecycle.enter(worker.lifecycle) do
      :ok ->
        try do
          execute_once(worker)
        after
          WorkerLifecycle.leave(worker.lifecycle)
        end

      :stopped ->
        {:ok, 0}
    end
  end

  defp execute_once(worker) do
    with :ok <- register(worker),
         {:ok, response} <- pull_batch(worker) do
      jobs = response["jobs"] || []
      tokens = response["tokens"] || []

      if length(jobs) == length(tokens) do
        results =
          jobs
          |> Enum.zip(tokens)
          |> Task.async_stream(
            fn {raw, token} -> WorkerProcessor.process(worker, raw, token) end,
            max_concurrency: worker.concurrency,
            ordered: false,
            timeout: :infinity
          )
          |> Enum.to_list()

        summarize(results)
      else
        {:error, %ProtocolError{message: "PULLB jobs/tokens length mismatch"}}
      end
    end
  end

  @spec run(t()) :: :ok | {:error, Exception.t()}
  def run(worker) do
    if stopped?(worker) do
      :ok
    else
      case run_once(worker) do
        {:ok, _count} ->
          run(worker)

        {:error, _error} ->
          Process.sleep(100)
          run(worker)
      end
    end
  end

  @spec stop(t()) :: :ok
  def stop(worker) do
    :atomics.put(worker.stats, 3, 1)

    case WorkerLifecycle.begin_stop(worker.lifecycle) do
      :owner ->
        try do
          unregister(worker)
          Connection.close(worker.connection)
          Connection.close(worker.heartbeat_connection)
        after
          WorkerLifecycle.finish_stop(worker.lifecycle)
        end

      :done ->
        :ok
    end

    :ok
  end

  defp unregister(worker) do
    if Connection.generation(worker.connection) > 0 do
      Connection.call(
        worker.connection,
        %{"cmd" => "UnregisterWorker", "workerId" => worker.worker_id},
        1_000
      )
    end
  end

  @doc false
  def pull_count(worker), do: min(worker.batch_size, worker.concurrency)

  defp register(worker) do
    command = %{
      "cmd" => "RegisterWorker",
      "workerId" => worker.worker_id,
      "name" => worker.name,
      "queues" => [worker.queue],
      "concurrency" => worker.concurrency,
      "hostname" => hostname(),
      "pid" => os_pid(),
      "startedAt" => System.system_time(:millisecond)
    }

    with {:ok, _response} <- Connection.call(worker.connection, command), do: :ok
  end

  defp pull_batch(worker) do
    command = %{
      "cmd" => "PULLB",
      "queue" => worker.queue,
      "count" => pull_count(worker),
      "timeout" => clamp_poll(worker.poll_timeout),
      "owner" => worker.worker_id,
      "lockTtl" => worker.lock_ttl
    }

    Connection.call(worker.connection, command, worker.poll_timeout + 5_000)
  end

  defp stopped?(worker), do: :atomics.get(worker.stats, 3) == 1

  defp summarize(results) do
    case Enum.find_value(results, fn
           {:ok, {:error, error}} -> error
           {:exit, reason} -> %RuntimeError{message: "worker task exited: #{inspect(reason)}"}
           _result -> nil
         end) do
      nil -> {:ok, Enum.count(results, &match?({:ok, :ok}, &1))}
      error -> {:error, error}
    end
  end

  defp clamp_batch(value), do: value |> positive() |> min(1_000)

  defp clamp_poll(value) when is_number(value),
    do: value |> trunc() |> max(0) |> min(30_000)

  defp clamp_poll(_value), do: 0
  defp normalize_interval(value) when is_number(value) and value > 0, do: trunc(value)
  defp normalize_interval(_value), do: nil
  defp positive(value) when is_integer(value) and value > 0, do: value
  defp positive(_value), do: 1
  defp hostname, do: :inet.gethostname() |> elem(1) |> to_string()
  defp os_pid, do: :os.getpid() |> to_string() |> String.to_integer()
  defp unique_id, do: "elixir-worker-#{System.unique_integer([:positive, :monotonic])}"
end
