defmodule Bunqueue.JobLogTest do
  use ExUnit.Case

  alias Bunqueue.{Connection, Job, Queue, Worker}

  setup_all do
    broker = Bunqueue.TestBroker.start!()
    on_exit(fn -> Bunqueue.TestBroker.stop(broker) end)
    {:ok, options: [host: "127.0.0.1", port: broker.port]}
  end

  # Regression: Job.log/2 must speak the wire command the server routes
  # ("AddLog", like every other SDK), not "Log" which the server rejects
  # as an unknown command.
  test "Job.log appends a persisted log line", %{options: options} do
    name = "elixir-joblog-#{System.unique_integer([:positive])}"
    {:ok, connection} = Connection.start_link(options)
    queue = Queue.new(name, connection)
    {:ok, job} = Queue.add(queue, "logs", %{value: 1}, attempts: 1, durable: true)
    parent = self()

    worker =
      Worker.new(
        name,
        fn active_job ->
          send(parent, {:log_result, Job.log(active_job, "hello from elixir")})
          %{done: true}
        end,
        connection: options,
        poll_timeout: 100,
        heartbeat_interval: 20
      )

    assert {:ok, 1} = Worker.run_once(worker)

    assert_receive {:log_result, log_result}, 2_000
    assert {:ok, _} = log_result

    assert {:ok, logs} = Queue.get_logs(queue, job.id)

    messages =
      Enum.map(logs, fn
        %{"message" => message} -> message
        message when is_binary(message) -> message
      end)

    assert "hello from elixir" in messages

    Worker.stop(worker)
    Connection.close(connection)
  end
end
