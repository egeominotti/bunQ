defmodule Bunqueue.WorkerOutcomeTest do
  use ExUnit.Case

  alias Bunqueue.{Connection, ProtocolError, WorkerProcessor}

  defmodule StubConnection do
    use GenServer

    def start_link(response), do: GenServer.start_link(__MODULE__, response)
    def init(response), do: {:ok, response}
    def handle_call(:timeout, _from, response), do: {:reply, 1_000, response}

    def handle_call({:command, _command, _timeout}, _from, response),
      do: {:reply, {:ok, response}, response}
  end

  test "counts normal responses with absent or nil terminal data as applied" do
    for response <- [%{}, %{"data" => nil}] do
      {result, stats} = process(response)

      assert result == :ok
      assert :atomics.get(stats, 1) == 1
      assert :atomics.get(stats, 2) == 0
    end
  end

  test "settles exact already-finalized evidence without incrementing counters" do
    response = %{"data" => %{"applied" => false, "reason" => "already-finalized"}}
    {result, stats} = process(response)

    assert result == :ok
    assert :atomics.get(stats, 1) == 0
    assert :atomics.get(stats, 2) == 0
  end

  test "rejects unknown non-applied terminal evidence without incrementing counters" do
    response = %{"data" => %{"applied" => false, "reason" => "unknown"}}
    {result, stats} = process(response)

    assert {:error, %ProtocolError{}} = result
    assert :atomics.get(stats, 1) == 0
    assert :atomics.get(stats, 2) == 0
  end

  test "rejects unexpected defined terminal data without incrementing counters" do
    {result, stats} = process(%{"data" => %{}})

    assert {:error, %ProtocolError{}} = result
    assert :atomics.get(stats, 1) == 0
    assert :atomics.get(stats, 2) == 0
  end

  defp process(response) do
    {:ok, connection} = StubConnection.start_link(response)
    stats = :atomics.new(3, signed: false)

    worker = %{
      connection: connection,
      handler: fn _job -> :result end,
      heartbeat_interval: nil,
      stack_trace_limit: 10,
      stats: stats
    }

    result =
      WorkerProcessor.process(
        worker,
        %{"id" => "job-1", "queue" => "strict-outcome", "name" => "job", "data" => %{}},
        "lease-token"
      )

    Connection.close(connection)
    {result, stats}
  end
end
