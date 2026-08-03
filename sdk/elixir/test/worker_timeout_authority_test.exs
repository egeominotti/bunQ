defmodule Bunqueue.WorkerTimeoutAuthorityTest do
  use ExUnit.Case

  alias Bunqueue.{Connection, Queue, Worker}

  setup_all do
    broker = Bunqueue.TestBroker.start!()
    on_exit(fn -> Bunqueue.TestBroker.stop(broker) end)
    {:ok, options: [host: "127.0.0.1", port: broker.port]}
  end

  test "a late processor success does not override the broker timeout", %{options: options} do
    assert_late_processor_outcome(options, :success)
  end

  test "a late processor failure does not override the broker timeout", %{options: options} do
    assert_late_processor_outcome(options, :failure)
  end

  defp assert_late_processor_outcome(options, outcome) do
    name = "elixir-timeout-authority-#{System.unique_integer([:positive])}"
    {:ok, connection} = Connection.start_link(options)
    queue = Queue.new(name, connection)

    {:ok, job} =
      Queue.add(queue, "slow", %{}, timeout: 60, attempts: 1, durable: true)

    worker =
      Worker.new(
        name,
        fn _job ->
          Process.sleep(250)

          case outcome do
            :success -> %{late: "result"}
            :failure -> {:error, "late processor failure"}
          end
        end,
        connection: options,
        concurrency: 1,
        batch_size: 1,
        poll_timeout: 100,
        heartbeat_interval: 0
      )

    try do
      # run_once reports the handler attempt that settled. It does not claim
      # that this late local outcome replaced the broker's terminal decision.
      assert {:ok, 1} = Worker.run_once(worker)
      assert {:ok, "failed"} = Queue.get_state(queue, job.id)
      assert {:ok, stored} = Queue.get_job(queue, job.id)
      assert String.contains?(String.downcase(stored.raw["failedReason"]), "timeout")
      assert {:ok, nil} = Queue.get_result(queue, job.id)
      assert {:ok, counts} = Queue.get_job_counts(queue)
      assert counts["completed"] == 0
      assert counts["failed"] == 1

      # Ignored ACK/FAIL replies are acknowledged no-ops, not authoritative
      # local terminal outcomes, so neither worker heartbeat counter advances.
      assert :atomics.get(worker.stats, 1) == 0
      assert :atomics.get(worker.stats, 2) == 0
    after
      Worker.stop(worker)
      Queue.obliterate(queue)
      Connection.close(connection)
    end
  end
end
