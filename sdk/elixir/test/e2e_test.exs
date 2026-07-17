defmodule Bunqueue.E2ETest do
  use ExUnit.Case

  alias Bunqueue.{Connection, Queue, Worker}

  setup_all do
    broker = Bunqueue.TestBroker.start!()
    on_exit(fn -> Bunqueue.TestBroker.stop(broker) end)
    {:ok, options: [host: "127.0.0.1", port: broker.port]}
  end

  test "produces, queries, processes, and persists a result", %{options: options} do
    name = "elixir-e2e-#{System.unique_integer([:positive])}"
    {:ok, connection} = Connection.start_link(options)
    queue = Queue.new(name, connection)
    {:ok, job} = Queue.add(queue, "double", %{value: 21}, attempts: 1, durable: true)
    assert {:ok, fetched} = Queue.get_job(queue, job.id)
    assert fetched.data["value"] == 21

    worker =
      Worker.new(name, fn _job -> %{value: 42} end,
        connection: options,
        poll_timeout: 100,
        heartbeat_interval: 20
      )

    assert {:ok, 1} = Worker.run_once(worker)
    assert {:ok, %{"value" => 42}} = Queue.get_result(queue, job.id)
    Worker.stop(worker)
    Connection.close(connection)
  end

  test "stop waits for an active handler to acknowledge before closing", %{options: options} do
    name = "elixir-stop-#{System.unique_integer([:positive])}"
    {:ok, connection} = Connection.start_link(options)
    queue = Queue.new(name, connection)
    {:ok, job} = Queue.add(queue, "slow", %{value: 1}, attempts: 1, durable: true)
    parent = self()

    worker =
      Worker.new(
        name,
        fn _job ->
          send(parent, {:handler_started, self()})

          receive do
            :finish -> %{stopped: false}
          end
        end,
        connection: options,
        poll_timeout: 100,
        heartbeat_interval: 20
      )

    runner = Task.async(fn -> Worker.run_once(worker) end)

    handler =
      receive do
        {:handler_started, pid} -> pid
      end

    stopper = Task.async(fn -> Worker.stop(worker) end)

    try do
      refute Task.yield(stopper, 100), "stop returned while an active handler still held a lease"
    after
      send(handler, :finish)
    end

    assert Task.await(runner, 2_000) == {:ok, 1}
    assert Task.await(stopper, 2_000) == :ok
    assert {:ok, %{"stopped" => false}} = Queue.get_result(queue, job.id)
    Connection.close(connection)
  end
end
