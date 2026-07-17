defmodule Bunqueue.Options do
  @moduledoc false

  @job_keys %{
    "priority" => "priority",
    "delay" => "delay",
    "attempts" => "maxAttempts",
    "maxAttempts" => "maxAttempts",
    "backoff" => "backoff",
    "ttl" => "ttl",
    "timeout" => "timeout",
    "jobId" => "jobId",
    "uniqueKey" => "uniqueKey",
    "dedup" => "dedup",
    "deduplication" => "dedup",
    "dependsOn" => "dependsOn",
    "parentId" => "parentId",
    "childrenIds" => "childrenIds",
    "tags" => "tags",
    "groupId" => "groupId",
    "lifo" => "lifo",
    "removeOnComplete" => "removeOnComplete",
    "removeOnFail" => "removeOnFail",
    "stallTimeout" => "stallTimeout",
    "durable" => "durable",
    "repeat" => "repeat",
    "debounceId" => "debounceId",
    "debounceTtl" => "debounceTtl",
    "stackTraceLimit" => "stackTraceLimit",
    "keepLogs" => "keepLogs",
    "sizeLimit" => "sizeLimit",
    "timestamp" => "timestamp",
    "failParentOnFailure" => "failParentOnFailure",
    "removeDependencyOnFailure" => "removeDependencyOnFailure",
    "ignoreDependencyOnFailure" => "ignoreDependencyOnFailure",
    "continueParentOnFailure" => "continueParentOnFailure"
  }

  @cron_keys %{
    "pattern" => "schedule",
    "schedule" => "schedule",
    "every" => "repeatEvery",
    "repeatEvery" => "repeatEvery",
    "limit" => "maxLimit",
    "maxLimit" => "maxLimit",
    "tz" => "timezone",
    "timezone" => "timezone",
    "immediately" => "immediately",
    "uniqueKey" => "uniqueKey",
    "dedup" => "dedup",
    "skipIfNoWorker" => "skipIfNoWorker",
    "skipMissedOnRestart" => "skipMissedOnRestart",
    "preventOverlap" => "preventOverlap",
    "priority" => "priority"
  }

  @cron_job_keys ~w(maxAttempts backoff timeout delay stallTimeout removeOnComplete removeOnFail)

  @spec job(keyword() | map(), boolean()) :: map()
  def job(options, bulk \\ false) do
    options
    |> enumerable()
    |> Enum.reduce(%{}, fn {raw_key, value}, output ->
      key = to_string(raw_key)
      wire_key = Map.get(@job_keys, key) || raise ArgumentError, "unknown job option: #{key}"
      wire_key = if bulk and wire_key == "jobId", do: "customId", else: wire_key
      Map.put(output, wire_key, value)
    end)
  end

  @spec scheduler(keyword() | map()) :: map()
  def scheduler(options) do
    options
    |> enumerable()
    |> Enum.reduce(%{}, fn {raw_key, value}, output ->
      key = to_string(raw_key)
      wire_key = Map.get(@cron_keys, key) || raise ArgumentError, "unknown repeat option: #{key}"
      Map.put(output, wire_key, value)
    end)
  end

  @spec scheduler_job(keyword() | map()) :: map()
  def scheduler_job(options) do
    mapped = job(options)

    Enum.reduce(mapped, %{}, fn {key, value}, output ->
      if key in @cron_job_keys do
        Map.put(output, key, value)
      else
        raise ArgumentError, "option #{key} is not supported in scheduler jobOptions"
      end
    end)
  end

  defp enumerable(options) when is_map(options), do: options
  defp enumerable(options) when is_list(options), do: options
  defp enumerable(nil), do: []

  defp enumerable(other),
    do: raise(ArgumentError, "options must be a map or keyword list, got: #{inspect(other)}")
end
