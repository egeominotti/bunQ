defmodule Bunqueue.FlowE2ETest do
  use ExUnit.Case

  alias Bunqueue.{Connection, FlowProducer, Queue, Worker}

  setup_all do
    broker = Bunqueue.TestBroker.start!()
    on_exit(fn -> Bunqueue.TestBroker.stop(broker) end)
    {:ok, options: [host: "127.0.0.1", port: broker.port]}
  end

  test "atomic tree preserves planned ids and runs child before parent", %{options: options} do
    suffix = System.unique_integer([:positive, :monotonic])
    queue_name = "elixir-atomic-flow-#{suffix}"
    parent_id = "elixir-flow-parent-#{suffix}"
    child_id = "elixir-flow-child-#{suffix}"
    producer = FlowProducer.new(options)

    assert {:ok, node} =
             FlowProducer.add(producer, %{
               name: "parent",
               queue: queue_name,
               data: %{},
               options: [jobId: parent_id],
               children: [
                 %{
                   name: "child",
                   queue: queue_name,
                   data: %{},
                   options: [jobId: child_id]
                 }
               ]
             })

    assert node.job.id == parent_id
    assert [%{job: child}] = node.children
    assert child.id == child_id
    assert child.data["__parentId"] == parent_id
    assert child.raw["parentId"] == parent_id

    {:ok, connection} = Connection.start_link(options)
    queue = Queue.new(queue_name, connection)
    assert {:ok, %{id: ^parent_id}} = Queue.get_job_by_custom_id(queue, parent_id)
    owner = self()

    worker =
      Worker.new(
        queue_name,
        fn job ->
          send(owner, {:processed, job.name})
          job.name
        end,
        connection: options,
        concurrency: 1,
        batch_size: 1,
        poll_timeout: 100
      )

    assert {:ok, 1} = Worker.run_once(worker)
    assert_receive {:processed, "child"}
    assert {:ok, 1} = Worker.run_once(worker)
    assert_receive {:processed, "parent"}

    Worker.stop(worker)
    assert :ok = Queue.obliterate(queue)
    Connection.close(connection)
    FlowProducer.close(producer)
  end

  test "atomic chain preserves ids and runs in dependency order", %{options: options} do
    suffix = System.unique_integer([:positive, :monotonic])
    queue_name = "elixir-atomic-chain-#{suffix}"
    expected_ids = Enum.map(0..2, &"elixir-chain-#{suffix}-#{&1}")
    producer = FlowProducer.new(options)

    steps =
      expected_ids
      |> Enum.with_index()
      |> Enum.map(fn {id, index} ->
        %{name: "step-#{index}", queue: queue_name, options: [jobId: id]}
      end)

    assert {:ok, ^expected_ids} = FlowProducer.add_chain(producer, steps)
    {:ok, connection} = Connection.start_link(options)
    queue = Queue.new(queue_name, connection)
    owner = self()

    worker =
      Worker.new(
        queue_name,
        fn job ->
          send(owner, {:processed, job.name})
          :ok
        end,
        connection: options,
        concurrency: 1,
        batch_size: 1,
        poll_timeout: 100
      )

    for index <- 0..2 do
      expected_name = "step-#{index}"
      assert {:ok, 1} = Worker.run_once(worker)
      assert_receive {:processed, ^expected_name}
    end

    Worker.stop(worker)
    assert :ok = Queue.obliterate(queue)
    Connection.close(connection)
    FlowProducer.close(producer)
  end
end
