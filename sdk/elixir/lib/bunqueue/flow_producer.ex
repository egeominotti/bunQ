defmodule Bunqueue.FlowProducer do
  @moduledoc """
  Plans flows locally and commits each tree or chain with one atomic `PUSHF`.

  Job identifiers and reciprocal topology links are fully resolved before the
  broker sees any job. Returned nodes are built from authoritative broker
  snapshots, not reconstructed request data.
  """

  alias Bunqueue.{Connection, FlowPlanner, FlowSnapshots, Job}

  @enforce_keys [:connection]
  defstruct [:connection, owns_connection: true]

  def new(options \\ []) do
    {:ok, connection} = Connection.start_link(options)
    %__MODULE__{connection: connection}
  end

  def with_connection(connection),
    do: %__MODULE__{connection: connection, owns_connection: false}

  @spec add(t(), map()) :: {:ok, map()} | {:error, Exception.t()}
  def add(producer, flow) do
    protect(fn ->
      plan = FlowPlanner.plan_tree(flow)

      with {:ok, snapshots} <- commit(producer.connection, plan.jobs) do
        {:ok, build_node(plan.root, snapshots, producer.connection)}
      end
    end)
  end

  @spec add_chain(t(), [map()]) :: {:ok, [String.t()]} | {:error, Exception.t()}
  def add_chain(producer, steps) do
    protect(fn ->
      plan = FlowPlanner.plan_chain(steps)

      with {:ok, _snapshots} <- commit(producer.connection, plan.jobs) do
        {:ok, plan.ids}
      end
    end)
  end

  def close(%{owns_connection: true, connection: connection}), do: Connection.close(connection)
  def close(_producer), do: :ok

  defp commit(_connection, []), do: {:ok, %{}}

  defp commit(connection, jobs) do
    case Connection.call(connection, %{"cmd" => "PUSHF", "jobs" => jobs}) do
      {:ok, response} ->
        snapshots = get_in(response, ["data", "jobs"])
        {:ok, FlowSnapshots.index!(jobs, snapshots)}

      {:error, error} ->
        {:error, error}
    end
  end

  defp build_node(node, snapshots, connection) do
    %{
      job: snapshots |> Map.fetch!(node.id) |> Job.from_wire(connection),
      children: Enum.map(node.children, &build_node(&1, snapshots, connection))
    }
  end

  defp protect(fun) do
    fun.()
  rescue
    error -> {:error, error}
  end

  @type t :: %__MODULE__{
          connection: GenServer.server(),
          owns_connection: boolean()
        }
end
