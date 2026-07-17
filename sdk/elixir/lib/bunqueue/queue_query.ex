defmodule Bunqueue.QueueQuery do
  @moduledoc false

  alias Bunqueue.{CommandError, Job, Queue, TimeoutError}

  def get_job(queue, id) do
    not_found_nil(fn ->
      with {:ok, response} <- Queue.call(queue, %{"cmd" => "GetJob", "id" => id}) do
        {:ok, from_response(response["job"], queue)}
      end
    end)
  end

  def get_job_by_custom_id(queue, custom_id) do
    not_found_nil(fn ->
      command = %{"cmd" => "GetJobByCustomId", "queue" => queue.name, "customId" => custom_id}

      with {:ok, response} <- Queue.call(queue, command) do
        {:ok, from_response(response["job"], queue)}
      end
    end)
  end

  def get_jobs(queue, state \\ "waiting", offset \\ 0, limit \\ 100) do
    command = %{
      "cmd" => "GetJobs",
      "queue" => queue.name,
      "state" => state,
      "offset" => max(offset, 0),
      "limit" => max(limit, 0)
    }

    with {:ok, response} <- Queue.call(queue, command) do
      jobs = Enum.map(response["jobs"] || [], &Job.from_wire(&1, queue.connection))
      {:ok, jobs}
    end
  end

  def get_state(queue, id), do: field(queue, %{"cmd" => "GetState", "id" => id}, "state")
  def get_result(queue, id), do: field(queue, %{"cmd" => "GetResult", "id" => id}, "result")

  def get_progress(queue, id) do
    with {:ok, response} <- Queue.call(queue, %{"cmd" => "GetProgress", "id" => id}) do
      {:ok, %{progress: response["progress"], message: response["message"]}}
    end
  end

  def get_job_counts(queue) do
    field(queue, %{"cmd" => "GetJobCounts", "queue" => queue.name}, "counts", %{})
  end

  def count(queue), do: field(queue, %{"cmd" => "Count", "queue" => queue.name}, "count", 0)

  def wait_for_job(queue, id, timeout_ms) do
    timeout = timeout_ms |> normalize_timeout() |> max(0) |> min(600_000)
    command = %{"cmd" => "WaitJob", "id" => id, "timeout" => timeout}

    with {:ok, response} <- Queue.call(queue, command, timeout + 5_000) do
      if response["completed"] do
        {:ok, response["result"]}
      else
        wait_timeout(queue, id, timeout)
      end
    end
  end

  def get_logs(queue, id, start \\ nil, finish \\ nil) do
    command = compact(%{"cmd" => "GetLogs", "id" => id, "start" => start, "end" => finish})

    with {:ok, response} <- Queue.call(queue, command) do
      {:ok, get_in(response, ["data", "logs"]) || []}
    end
  end

  defp wait_timeout(queue, id, timeout) do
    case get_state(queue, id) do
      {:ok, "failed"} -> {:error, %CommandError{message: "job #{id} failed", command: "WaitJob"}}
      _ -> {:error, %TimeoutError{message: "WaitJob timed out", timeout: timeout}}
    end
  end

  defp field(queue, command, key, default \\ nil) do
    with {:ok, response} <- Queue.call(queue, command), do: {:ok, Map.get(response, key, default)}
  end

  defp from_response(nil, _queue), do: nil
  defp from_response(raw, queue), do: Job.from_wire(raw, queue.connection)

  defp not_found_nil(function) do
    case function.() do
      {:error, %CommandError{} = error} ->
        if CommandError.not_found?(error), do: {:ok, nil}, else: {:error, error}

      result ->
        result
    end
  end

  defp compact(map), do: Map.reject(map, fn {_key, value} -> is_nil(value) end)
  defp normalize_timeout(value) when is_number(value), do: trunc(value)
  defp normalize_timeout(_value), do: 0
end
