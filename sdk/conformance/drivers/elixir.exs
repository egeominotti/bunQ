alias Bunqueue.{Job, Queue, UnrecoverableError, Worker}

defmodule Bunqueue.Conformance.ElixirDriver do
  def run do
    IO.stream(:stdio, :line)
    |> Enum.reduce(initial_state(), fn line, state ->
      request = Jason.decode!(line)

      try do
        {fields, next} = handle(request, state)
        answer = fields |> Map.merge(%{"id" => request["id"], "ok" => true})
        IO.puts(Jason.encode!(answer))
        next
      rescue
        error ->
          IO.puts(
            Jason.encode!(%{
              "id" => request["id"],
              "ok" => false,
              "error" => Exception.message(error)
            })
          )

          state
      end
    end)
  end

  defp initial_state, do: %{connection: [host: "127.0.0.1", port: 6789], queues: %{}}

  defp handle(%{"op" => "connect"} = request, state) do
    connection =
      [host: request["host"], port: request["port"]]
      |> maybe_put(:token, request["token"])

    {%{}, %{state | connection: connection, queues: %{}}}
  end

  defp handle(%{"op" => "add"} = request, state) do
    {queue, state} = queue_for(state, request["queue"])
    job = ok!(Queue.add(queue, request["name"], request["data"], request["opts"] || %{}))
    {%{"jobId" => job.id}, state}
  end

  defp handle(%{"op" => "addBulk"} = request, state) do
    {queue, state} = queue_for(state, request["queue"])
    {%{"ids" => ok!(Queue.add_bulk(queue, request["entries"]))}, state}
  end

  defp handle(%{"op" => "getJob"} = request, state) do
    {queue, state} = queue_for(state, "conf-lookup")
    {%{"job" => job_view(ok!(Queue.get_job(queue, request["jobId"])))}, state}
  end

  defp handle(%{"op" => "getJobByCustomId"} = request, state) do
    {queue, state} = queue_for(state, request["queue"])
    job = ok!(Queue.get_job_by_custom_id(queue, request["customId"]))
    {%{"job" => if(job, do: %{"id" => job.id}, else: nil)}, state}
  end

  defp handle(%{"op" => "getState"} = request, state) do
    {queue, state} = queue_for(state, "conf-lookup")
    {%{"state" => ok!(Queue.get_state(queue, request["jobId"]))}, state}
  end

  defp handle(%{"op" => "getResult"} = request, state) do
    {queue, state} = queue_for(state, "conf-lookup")
    {%{"result" => ok!(Queue.get_result(queue, request["jobId"]))}, state}
  end

  defp handle(%{"op" => op} = request, state) when op in ~w(count isPaused pause resume drain) do
    {queue, state} = queue_for(state, request["queue"])

    fields =
      case op do
        "count" -> %{"count" => ok!(Queue.count(queue))}
        "isPaused" -> %{"paused" => ok!(Queue.is_paused(queue))}
        "pause" -> ok!(Queue.pause(queue)) && %{}
        "resume" -> ok!(Queue.resume(queue)) && %{}
        "drain" -> %{"count" => ok!(Queue.drain(queue))}
      end

    {fields, state}
  end

  defp handle(%{"op" => "promote"} = request, state) do
    {queue, state} = queue_for(state, "conf-lookup")
    ok!(Queue.promote(queue, request["jobId"]))
    {%{}, state}
  end

  defp handle(%{"op" => "upsertScheduler"} = request, state) do
    {queue, state} = queue_for(state, request["queue"])

    ok!(
      Queue.upsert_scheduler(
        queue,
        request["schedulerId"],
        request["repeat"],
        request["template"] || %{}
      )
    )

    {%{}, state}
  end

  defp handle(%{"op" => "getScheduler"} = request, state) do
    {queue, state} = queue_for(state, "conf-lookup")
    {%{"scheduler" => ok!(Queue.get_scheduler(queue, request["schedulerId"]))}, state}
  end

  defp handle(%{"op" => "removeScheduler"} = request, state) do
    {queue, state} = queue_for(state, "conf-lookup")
    ok!(Queue.remove_scheduler(queue, request["schedulerId"]))
    {%{}, state}
  end

  defp handle(%{"op" => "waitForJob"} = request, state) do
    {queue, state} = queue_for(state, "conf-lookup")
    result = ok!(Queue.wait_for_job(queue, request["jobId"], request["timeoutMs"]))
    {%{"result" => result}, state}
  end

  defp handle(%{"op" => "getDlqCount"} = request, state) do
    {queue, state} = queue_for(state, request["queue"])
    {%{"count" => length(ok!(Queue.dlq(queue)))}, state}
  end

  defp handle(%{"op" => "retryDlq"} = request, state) do
    {queue, state} = queue_for(state, request["queue"])
    {%{"count" => ok!(Queue.retry_dlq(queue))}, state}
  end

  defp handle(%{"op" => "hello"}, state) do
    {queue, state} = queue_for(state, "conf-lookup")
    hello = ok!(Queue.hello(queue))

    {%{"protocolVersion" => hello["protocolVersion"], "capabilities" => hello["capabilities"]},
     state}
  end

  defp handle(%{"op" => "process"} = request, state) do
    {queue, state} = queue_for(state, request["queue"])
    process_until(queue, request, state.connection)
    {%{}, state}
  end

  defp handle(%{"op" => "close"}, state) do
    Enum.each(state.queues, fn {_name, queue} -> Queue.close(queue) end)
    {%{}, state}
  end

  defp handle(request, _state), do: raise("unknown op: #{request["op"]}")

  defp process_until(queue, request, connection) do
    {:ok, seen} = Agent.start_link(fn -> MapSet.new() end)

    handler = fn job ->
      case request["behavior"] do
        "unrecoverable" -> raise UnrecoverableError, message: "conformance poison"
        "deepThrow" -> deep_throw(25)
        "failOnce" -> fail_once(seen, job, request["result"])
        _ -> request["result"] || "ok"
      end
    end

    worker =
      Worker.new(queue.name, handler,
        connection: connection,
        batch_size: request["batchSize"] || 10,
        poll_timeout: 300
      )

    deadline = System.monotonic_time(:millisecond) + (request["timeoutMs"] || 20_000)

    try do
      process_loop(worker, queue, request["until"] || %{}, deadline)
    after
      Worker.stop(worker)
      Agent.stop(seen)
    end
  end

  defp process_loop(worker, queue, until, deadline) do
    if reached?(queue, until) do
      :ok
    else
      if System.monotonic_time(:millisecond) >= deadline,
        do: raise("until condition not reached before timeoutMs")

      ok!(Worker.run_once(worker))
      process_loop(worker, queue, until, deadline)
    end
  end

  defp reached?(queue, until) do
    counts = ok!(Queue.get_job_counts(queue))
    dlq = if Map.has_key?(until, "dlq"), do: length(ok!(Queue.dlq(queue))), else: 0

    (counts["completed"] || 0) >= (until["completed"] || 0) and
      (counts["failed"] || 0) >= (until["failed"] || 0) and
      dlq >= (until["dlq"] || 0)
  end

  defp fail_once(seen, job, result) do
    first = Agent.get_and_update(seen, &{not MapSet.member?(&1, job.id), MapSet.put(&1, job.id)})
    if first, do: raise("conformance transient"), else: result || "ok"
  end

  defp deep_throw(0), do: raise("BOOM-CONFORMANCE")
  defp deep_throw(depth), do: deep_throw(depth - 1)

  defp job_view(nil), do: nil

  defp job_view(%Job{} = job) do
    %{"id" => job.id, "name" => job.name, "data" => job.data, "stacktrace" => job.stacktrace}
  end

  defp queue_for(state, name) do
    case Map.fetch(state.queues, name) do
      {:ok, queue} ->
        {queue, state}

      :error ->
        {:ok, connection} = Bunqueue.Connection.start_link(state.connection)
        queue = Queue.new(name, connection, true)
        {queue, put_in(state, [:queues, name], queue)}
    end
  end

  defp ok!({:ok, value}), do: value
  defp ok!(:ok), do: true
  defp ok!({:error, error}), do: raise(error)
  defp maybe_put(list, _key, nil), do: list
  defp maybe_put(list, key, value), do: Keyword.put(list, key, value)
end

Bunqueue.Conformance.ElixirDriver.run()
