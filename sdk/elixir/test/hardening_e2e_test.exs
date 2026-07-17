defmodule Bunqueue.HardeningE2ETest do
  use ExUnit.Case

  alias Bunqueue.{Connection, ProtocolError, Queue, TestBroker}

  setup_all do
    broker = TestBroker.start!()
    on_exit(fn -> TestBroker.stop(broker) end)
    {:ok, broker: broker, options: [host: "127.0.0.1", port: broker.port]}
  end

  test "concurrent custom-id retries enqueue exactly once", %{options: options} do
    name = unique("idempotency-race")

    contenders =
      for _index <- 1..24 do
        {:ok, connection} = Connection.start_link(options)
        {Queue.new(name, connection), connection}
      end

    try do
      ids =
        contenders
        |> Task.async_stream(
          fn {queue, _connection} ->
            {:ok, job} =
              Queue.add(queue, "charge", %{attempt: 1}, jobId: "same-operation-id")

            job.id
          end,
          max_concurrency: 24,
          timeout: 5_000,
          ordered: false
        )
        |> Enum.map(fn {:ok, id} -> id end)

      assert ids |> MapSet.new() |> MapSet.size() == 1
      assert {:ok, 1} = Queue.count(elem(hd(contenders), 0))
    after
      Queue.obliterate(elem(hd(contenders), 0))
      Enum.each(contenders, fn {_queue, connection} -> Connection.close(connection) end)
    end
  end

  test "simultaneous dequeues lease one job to exactly one owner", %{options: options} do
    name = unique("double-dequeue")
    {:ok, producer_connection} = Connection.start_link(options)
    producer = Queue.new(name, producer_connection)
    {:ok, expected} = Queue.add(producer, "only-once", %{value: 1})

    contenders =
      for _index <- 1..12 do
        {:ok, connection} = Connection.start_link(options)
        connection
      end

    try do
      leased =
        contenders
        |> Enum.with_index()
        |> Task.async_stream(
          fn {connection, index} ->
            Connection.call(connection, %{
              "cmd" => "PULL",
              "queue" => name,
              "owner" => "contender-#{index}",
              "timeout" => 250
            })
          end,
          max_concurrency: 12,
          timeout: 5_000,
          ordered: false
        )
        |> Enum.map(fn {:ok, {:ok, response}} -> response["job"] end)
        |> Enum.reject(&is_nil/1)

      assert Enum.map(leased, & &1["id"]) == [expected.id]
    after
      Queue.obliterate(producer)
      Enum.each(contenders, &Connection.close/1)
      Connection.close(producer_connection)
    end
  end

  test "generated payloads preserve every user field", %{options: options} do
    name = unique("generated")
    {:ok, connection} = Connection.start_link(options)
    queue = Queue.new(name, connection)
    payloads = generated_payloads(64)

    entries =
      payloads
      |> Enum.with_index()
      |> Enum.map(fn {data, index} ->
        %{name: "generated-#{rem(index, 7)}", data: data}
      end)

    try do
      assert {:ok, ids} = Queue.add_bulk(queue, entries)
      assert length(ids) == length(payloads)

      ids
      |> Enum.with_index()
      |> Enum.each(fn {id, index} ->
        assert {:ok, job} = Queue.get_job(queue, id)
        assert job.name == "generated-#{rem(index, 7)}"
        assert Map.delete(job.data, "name") == Enum.at(payloads, index)
      end)
    after
      Queue.obliterate(queue)
      Connection.close(connection)
    end
  end

  test "malformed mutation corpus is typed and leaves the connection healthy", %{
    options: options
  } do
    {:ok, connection} = Connection.start_link(options)

    invalid =
      [self(), make_ref(), fn -> :invalid end, Integer.pow(10, 400)] ++
        Enum.map(1..12, fn depth ->
          Enum.reduce(1..depth, make_ref(), fn _, value -> [nested: value] end)
        end)

    try do
      Enum.each(invalid, fn payload ->
        assert {:error, %ProtocolError{}} =
                 Connection.call(connection, %{"cmd" => "Ping", "payload" => payload})
      end)

      assert {:ok, %{"data" => %{"pong" => true}}} =
               Connection.call(connection, %{"cmd" => "Ping"})
    after
      Connection.close(connection)
    end
  end

  test "512-job spike is accepted without loss and the queue recovers", %{options: options} do
    {:ok, connection} = Connection.start_link(options)
    queue = Queue.new(unique("spike"), connection)

    entries =
      Enum.map(0..511, fn index ->
        %{name: "spike", data: %{"index" => index}}
      end)

    try do
      assert {:ok, ids} = Queue.add_bulk(queue, entries)
      assert length(ids) == 512
      assert {:ok, 512} = Queue.count(queue)
      assert {:ok, 512} = Queue.drain(queue)
      assert {:ok, 0} = Queue.count(queue)
    after
      Queue.obliterate(queue)
      Connection.close(connection)
    end
  end

  test "durable job survives SIGKILL and the existing client reconnects" do
    broker = TestBroker.start!()
    options = [host: "127.0.0.1", port: broker.port, timeout: 500]
    {:ok, connection} = Connection.start_link(options)
    queue = Queue.new(unique("crash"), connection)
    {:ok, job} = Queue.add(queue, "before-crash", %{value: 1}, durable: true)
    assert {:ok, 1} = Queue.count(queue)
    TestBroker.crash(broker)
    restarted = TestBroker.restart(broker)

    try do
      assert eventually(fn -> Queue.ping(queue) == {:ok, true} end)
      assert {:ok, fetched} = Queue.get_job(queue, job.id)
      assert fetched.id == job.id
      assert {:ok, 1} = Queue.count(queue)
    after
      Queue.obliterate(queue)
      Connection.close(connection)
      TestBroker.stop(restarted)
    end
  end

  defp generated_payloads(count) do
    {payloads, _state} =
      Enum.map_reduce(0..(count - 1), 0x0BADC0DE, fn index, state ->
        state = rem(state * 1_664_525 + 1_013_904_223, 4_294_967_296)

        payload = %{
          "index" => index,
          "signed" => rem(state, 2_000_001) - 1_000_000,
          "flag" => Bitwise.band(state, 1) == 1,
          "text" => "case-#{Integer.to_string(state, 16)}-🧪",
          "nullable" => if(rem(index, 3) == 0, do: nil, else: "value-#{index}"),
          "nested" => [
            rem(state, 97),
            %{"checksum" => rem(Bitwise.bxor(state, index), 1_000_003)}
          ]
        }

        {payload, state}
      end)

    payloads
  end

  defp eventually(predicate, attempts \\ 40)
  defp eventually(_predicate, 0), do: false

  defp eventually(predicate, attempts) do
    if predicate.() do
      true
    else
      Process.sleep(50)
      eventually(predicate, attempts - 1)
    end
  end

  defp unique(prefix), do: "elixir-#{prefix}-#{System.unique_integer([:positive])}"
end
