defmodule Bunqueue.SoakTest do
  use ExUnit.Case

  alias Bunqueue.{Connection, Queue, TestBroker}

  @tag :soak
  test "sustained producer profile keeps one long-lived connection healthy" do
    seconds = positive_env!("BUNQUEUE_SDK_SOAK_SECONDS", nil)
    batch_size = positive_env!("BUNQUEUE_SDK_SOAK_BATCH", 100)
    broker = TestBroker.start!()
    {:ok, connection} = Connection.start_link(host: "127.0.0.1", port: broker.port)
    queue = Queue.new("elixir-sustained-soak", connection)
    deadline = System.monotonic_time(:millisecond) + seconds * 1_000

    try do
      {iterations, jobs} = run_batches(queue, batch_size, deadline, 0, 0)

      IO.puts(
        "profile=elixir-soak seconds=#{seconds} batch=#{batch_size} " <>
          "iterations=#{iterations} jobs=#{jobs}"
      )
    after
      Connection.close(connection)
      TestBroker.stop(broker)
    end
  end

  defp run_batches(queue, batch_size, deadline, iterations, jobs) do
    if System.monotonic_time(:millisecond) >= deadline do
      {iterations, jobs}
    else
      entries =
        Enum.map(0..(batch_size - 1), fn index ->
          %{name: "soak", data: %{iteration: iterations, index: index}}
        end)

      assert {:ok, ids} = Queue.add_bulk(queue, entries)
      assert length(ids) == batch_size
      assert {:ok, ^batch_size} = Queue.count(queue)
      assert {:ok, job} = Queue.get_job(queue, hd(ids))
      assert job != nil
      assert :ok = Queue.obliterate(queue)
      run_batches(queue, batch_size, deadline, iterations + 1, jobs + length(ids))
    end
  end

  defp positive_env!(name, default) do
    case System.get_env(name) do
      nil when is_integer(default) ->
        default

      nil ->
        raise "#{name} must be set for the soak profile"

      value ->
        case Integer.parse(value) do
          {integer, ""} when integer > 0 -> integer
          _ -> raise "#{name} must be a positive integer"
        end
    end
  end
end
