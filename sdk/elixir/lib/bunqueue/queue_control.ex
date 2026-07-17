defmodule Bunqueue.QueueControl do
  @moduledoc false

  alias Bunqueue.Queue

  def pause(queue), do: unit(queue, %{"cmd" => "Pause", "queue" => queue.name})
  def resume(queue), do: unit(queue, %{"cmd" => "Resume", "queue" => queue.name})

  def is_paused(queue) do
    with {:ok, response} <- Queue.call(queue, %{"cmd" => "IsPaused", "queue" => queue.name}) do
      {:ok, response["paused"] == true}
    end
  end

  def drain(queue) do
    with {:ok, response} <- Queue.call(queue, %{"cmd" => "Drain", "queue" => queue.name}) do
      {:ok, response["count"] || 0}
    end
  end

  def clean(queue, grace_ms, limit, state) do
    command = %{
      "cmd" => "Clean",
      "queue" => queue.name,
      "grace" => grace_ms,
      "limit" => limit,
      "state" => state
    }

    with {:ok, response} <- Queue.call(queue, command), do: {:ok, response["ids"] || []}
  end

  def obliterate(queue), do: unit(queue, %{"cmd" => "Obliterate", "queue" => queue.name})
  def cancel(queue, id), do: unit(queue, %{"cmd" => "Cancel", "id" => id})
  def discard(queue, id), do: unit(queue, %{"cmd" => "Discard", "id" => id})
  def promote(queue, id), do: unit(queue, %{"cmd" => "Promote", "id" => id})
  def promote_jobs(queue), do: unit(queue, %{"cmd" => "PromoteJobs", "queue" => queue.name})
  def retry_job(queue, id), do: unit(queue, %{"cmd" => "MoveToWait", "id" => id})

  def move_to_delayed(queue, id, delay_ms),
    do: unit(queue, %{"cmd" => "MoveToDelayed", "id" => id, "delay" => delay_ms})

  def change_priority(queue, id, priority),
    do: unit(queue, %{"cmd" => "ChangePriority", "id" => id, "priority" => priority})

  def change_delay(queue, id, delay_ms),
    do: unit(queue, %{"cmd" => "ChangeDelay", "id" => id, "delay" => delay_ms})

  def update(queue, id, data),
    do: unit(queue, %{"cmd" => "Update", "id" => id, "data" => data})

  def update_parent(queue, child_id, parent_id) do
    unit(queue, %{"cmd" => "UpdateParent", "childId" => child_id, "parentId" => parent_id})
  end

  defp unit(queue, command) do
    with {:ok, _response} <- Queue.call(queue, command), do: :ok
  end
end
