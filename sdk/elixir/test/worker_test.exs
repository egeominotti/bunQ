defmodule Bunqueue.WorkerTest do
  use ExUnit.Case

  alias Bunqueue.Worker

  test "clamps PULLB count and disables non-positive heartbeat intervals" do
    worker =
      Worker.new("clamp-test", fn _job -> :ok end,
        connection: [host: "127.0.0.1", port: 1, timeout: 10],
        batch_size: 5_000,
        heartbeat_interval: 0
      )

    assert worker.batch_size == 1_000
    assert Worker.pull_count(worker) == 1
    assert worker.heartbeat_interval == nil
    assert Worker.stop(worker) == :ok
    refute Process.alive?(worker.lifecycle)
    assert Worker.stop(worker) == :ok
  end

  test "uses concurrency as the default batch size" do
    worker =
      Worker.new("concurrency-test", fn _job -> :ok end,
        connection: [host: "127.0.0.1", port: 1, timeout: 10],
        concurrency: 8
      )

    assert worker.concurrency == 8
    assert worker.batch_size == 8
    assert Worker.pull_count(worker) == 8
    Worker.stop(worker)
  end

  test "never leases more jobs than the available processing concurrency" do
    worker =
      Worker.new("lease-bound-test", fn _job -> :ok end,
        connection: [host: "127.0.0.1", port: 1, timeout: 10],
        concurrency: 4,
        batch_size: 100,
        poll_timeout: 90_000
      )

    assert Worker.pull_count(worker) == 4
    assert worker.poll_timeout == 30_000
    Worker.stop(worker)
  end

  test "stop is graceful while the run loop is active" do
    worker =
      Worker.new("concurrent-stop-test", fn _job -> :ok end,
        connection: [host: "127.0.0.1", port: 1, timeout: 10]
      )

    task = Task.async(fn -> Worker.run(worker) end)
    Process.sleep(20)
    assert Worker.stop(worker) == :ok
    assert Task.await(task, 1_000) == :ok
  end
end
