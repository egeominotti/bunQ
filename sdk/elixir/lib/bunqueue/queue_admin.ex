defmodule Bunqueue.QueueAdmin do
  @moduledoc false

  alias Bunqueue.{CommandError, Options, Queue}

  def dlq(queue, count \\ nil) do
    command = compact(%{"cmd" => "Dlq", "queue" => queue.name, "count" => count})
    with {:ok, response} <- Queue.call(queue, command), do: {:ok, response["jobs"] || []}
  end

  def retry_dlq(queue, job_id \\ nil, count \\ nil) do
    command =
      compact(%{
        "cmd" => "RetryDlq",
        "queue" => queue.name,
        "jobId" => job_id,
        "count" => count
      })

    with {:ok, response} <- Queue.call(queue, command), do: {:ok, response["count"] || 0}
  end

  def purge_dlq(queue) do
    with {:ok, response} <-
           Queue.call(queue, %{"cmd" => "PurgeDlq", "queue" => queue.name}) do
      {:ok, response["count"] || 0}
    end
  end

  def upsert_scheduler(queue, id, repeat, template \\ %{}) do
    repeat_fields = Options.scheduler(repeat)
    name = get(template, :name, id)
    payload = Queue.job_payload(name, get(template, :data, %{}))
    job_options = Options.scheduler_job(get(template, :options, []))

    command =
      %{
        "cmd" => "Cron",
        "name" => id,
        "jobName" => payload["name"],
        "queue" => queue.name,
        "data" => payload["data"]
      }
      |> Map.merge(repeat_fields)
      |> maybe_put("jobOptions", job_options)

    unit(queue, command)
  end

  def get_scheduler(queue, id) do
    case Queue.call(queue, %{"cmd" => "CronGet", "name" => id}) do
      {:ok, response} ->
        {:ok, response["cron"]}

      {:error, %CommandError{} = error} ->
        if CommandError.not_found?(error), do: {:ok, nil}, else: {:error, error}

      error ->
        error
    end
  end

  def list_schedulers(queue) do
    with {:ok, response} <- Queue.call(queue, %{"cmd" => "CronList"}) do
      {:ok, response["crons"] || []}
    end
  end

  def remove_scheduler(queue, id), do: unit(queue, %{"cmd" => "CronDelete", "name" => id})

  def set_rate_limit(queue, limit, options \\ []) do
    command = %{
      "cmd" => "RateLimit",
      "queue" => queue.name,
      "limit" => limit,
      "duration" => get(options, :duration, nil),
      "ttl" => get(options, :ttl, nil)
    }

    unit(queue, compact(command))
  end

  def clear_rate_limit(queue),
    do: unit(queue, %{"cmd" => "RateLimitClear", "queue" => queue.name})

  def set_concurrency(queue, limit),
    do: unit(queue, %{"cmd" => "SetConcurrency", "queue" => queue.name, "limit" => limit})

  def clear_concurrency(queue),
    do: unit(queue, %{"cmd" => "ClearConcurrency", "queue" => queue.name})

  defp unit(queue, command) do
    with {:ok, _response} <- Queue.call(queue, command), do: :ok
  end

  defp compact(map), do: Map.reject(map, fn {_key, value} -> is_nil(value) end)
  defp maybe_put(map, _key, empty) when empty == %{}, do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)

  defp get(map, key, default) when is_map(map),
    do: Map.get(map, key, Map.get(map, to_string(key), default))

  defp get(list, key, default) when is_list(list), do: Keyword.get(list, key, default)
end
