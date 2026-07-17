defmodule Bunqueue.RealisticE2ETest do
  use ExUnit.Case

  alias Bunqueue.{Connection, Queue, Worker}

  setup_all do
    broker = Bunqueue.TestBroker.start!()
    on_exit(fn -> Bunqueue.TestBroker.stop(broker) end)
    {:ok, options: [host: "127.0.0.1", port: broker.port]}
  end

  test "concurrent invoice burst preserves every persisted result", %{options: options} do
    name = "elixir-invoices-#{System.unique_integer([:positive])}"
    {:ok, connection} = Connection.start_link(options)
    queue = Queue.new(name, connection)

    entries =
      for invoice <- 0..31 do
        %{name: "reconcile", data: %{invoice: invoice, cents: 101 + invoice}}
      end

    assert {:ok, ids} = Queue.add_bulk(queue, entries)

    worker =
      Worker.new(
        name,
        fn job ->
          %{
            invoice: job.data["invoice"],
            total: job.data["cents"] * 2
          }
        end,
        connection: options,
        concurrency: 12,
        batch_size: 32,
        poll_timeout: 100
      )

    try do
      drain(worker, queue, length(ids), System.monotonic_time(:millisecond) + 30_000)
      assert {:ok, %{"completed" => completed}} = Queue.get_job_counts(queue)
      assert completed == length(ids)

      checksum =
        ids
        |> Enum.with_index()
        |> Enum.reduce(0, fn {id, invoice}, total ->
          assert {:ok, result} = Queue.get_result(queue, id)
          assert result["invoice"] == invoice
          assert result["total"] == (101 + invoice) * 2
          total + result["total"]
        end)

      assert checksum == 7_456
    after
      Worker.stop(worker)
      Queue.obliterate(queue)
      Connection.close(connection)
    end
  end

  defp drain(worker, queue, target, deadline) do
    assert {:ok, _count} = Worker.run_once(worker)
    assert {:ok, counts} = Queue.get_job_counts(queue)

    cond do
      counts["completed"] == target ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        flunk("invoice burst did not complete: #{inspect(counts)}")

      true ->
        drain(worker, queue, target, deadline)
    end
  end
end
