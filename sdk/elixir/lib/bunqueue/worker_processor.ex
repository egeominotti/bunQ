defmodule Bunqueue.WorkerProcessor do
  @moduledoc false

  alias Bunqueue.{Connection, Job, UnrecoverableError}

  def process(worker, raw, token) do
    job = Job.from_wire(raw, worker.connection, token)
    heartbeat = start_job_heartbeat(worker, job)
    outcome = invoke(worker.handler, job, worker.stack_trace_limit)
    stop_job_heartbeat(heartbeat)

    result =
      case outcome do
        {:ok, value} ->
          acknowledge(worker, job, value)

        {:error, error, stack, unrecoverable} ->
          fail(worker, job, error, stack, unrecoverable)
      end

    record_result(worker, outcome, result)
  end

  defp record_result(worker, outcome, {:ok, _response}) do
    index = if match?({:ok, _}, outcome), do: 1, else: 2
    :atomics.add_get(worker.stats, index, 1)
    :ok
  end

  defp record_result(_worker, _outcome, {:error, error}), do: {:error, error}

  defp invoke(handler, job, limit) do
    try do
      case handler.(job) do
        {:ok, result} ->
          {:ok, result}

        {:error, %UnrecoverableError{} = error} ->
          {:error, error.message, [error.message], true}

        {:error, reason} ->
          {:error, error_message(reason), [error_message(reason)], false}

        result ->
          {:ok, result}
      end
    rescue
      error ->
        stack = Exception.format(:error, error, __STACKTRACE__) |> String.split("\n")
        {:error, Exception.message(error), Enum.take(stack, limit), unrecoverable?(error)}
    catch
      kind, reason ->
        message = Exception.format(kind, reason, __STACKTRACE__)
        {:error, message, message |> String.split("\n") |> Enum.take(limit), false}
    end
  end

  defp acknowledge(worker, job, result) do
    Connection.call(worker.connection, %{
      "cmd" => "ACK",
      "id" => job.id,
      "token" => job.token,
      "result" => result
    })
  end

  defp fail(worker, job, error, stack, unrecoverable) do
    Connection.call(worker.connection, %{
      "cmd" => "FAIL",
      "id" => job.id,
      "token" => job.token,
      "error" => error,
      "stack" => stack,
      "unrecoverable" => unrecoverable
    })
  end

  defp start_job_heartbeat(%{heartbeat_interval: nil}, _job), do: nil

  defp start_job_heartbeat(worker, job) do
    parent = self()

    spawn_link(fn ->
      heartbeat_loop(
        parent,
        worker.heartbeat_connection,
        job.id,
        job.token,
        worker.heartbeat_interval
      )
    end)
  end

  defp heartbeat_loop(parent, connection, id, token, interval) do
    receive do
      {:stop_heartbeat, ^parent} ->
        :ok
    after
      interval ->
        Connection.call(connection, %{
          "cmd" => "JobHeartbeatB",
          "ids" => [id],
          "tokens" => [token]
        })

        heartbeat_loop(parent, connection, id, token, interval)
    end
  end

  defp stop_job_heartbeat(nil), do: :ok

  defp stop_job_heartbeat(pid) do
    send(pid, {:stop_heartbeat, self()})
    :ok
  end

  defp unrecoverable?(%UnrecoverableError{}), do: true
  defp unrecoverable?(_error), do: false
  defp error_message(%{message: message}), do: to_string(message)
  defp error_message(reason), do: inspect(reason)
end
