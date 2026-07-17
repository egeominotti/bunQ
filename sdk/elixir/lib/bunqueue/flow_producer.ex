defmodule Bunqueue.FlowProducer do
  @moduledoc "Creates parent/child flows and chains over the public queue protocol."

  alias Bunqueue.{Connection, Queue}

  @enforce_keys [:connection]
  defstruct [:connection, owns_connection: true]

  def new(options \\ []) do
    {:ok, connection} = Connection.start_link(options)
    %__MODULE__{connection: connection}
  end

  def with_connection(connection),
    do: %__MODULE__{connection: connection, owns_connection: false}

  def add(producer, flow) do
    {:ok, created} = Agent.start_link(fn -> [] end)

    try do
      case do_add_node(producer, flow, nil, created) do
        {:ok, node} ->
          {:ok, node}

        {:error, error} ->
          rollback(producer, Agent.get(created, & &1))
          {:error, error}
      end
    rescue
      error ->
        rollback(producer, Agent.get(created, & &1))
        {:error, error}
    after
      Agent.stop(created)
    end
  end

  def add_chain(producer, steps) do
    Enum.reduce_while(steps, {:ok, []}, fn step, {:ok, ids} ->
      previous = List.last(ids)
      data = marker(get(step, :data, %{}), "__flowParentId", previous)
      options = merge_option(get(step, :options, []), "dependsOn", list(previous))
      queue = Queue.new(fetch(step, :queue), producer.connection)

      case Queue.add(queue, fetch(step, :name), data, options) do
        {:ok, job} ->
          {:cont, {:ok, ids ++ [job.id]}}

        {:error, error} ->
          rollback(producer, ids)
          {:halt, {:error, error}}
      end
    end)
  end

  def close(%{owns_connection: true, connection: connection}), do: Connection.close(connection)
  def close(_producer), do: :ok

  defp do_add_node(producer, node, parent, created) do
    children =
      Enum.map(get(node, :children, []), fn child ->
        case do_add_node(producer, child, {"pending", fetch(node, :queue)}, created) do
          {:ok, child_node} -> child_node
          {:error, error} -> raise error
        end
      end)

    child_ids = Enum.map(children, & &1.job.id)
    options = node_options(node, parent, child_ids)
    data = node_data(node, parent, child_ids)
    queue = Queue.new(fetch(node, :queue), producer.connection)

    with {:ok, job} <- Queue.add(queue, fetch(node, :name), data, options) do
      Agent.update(created, &[job.id | &1])

      case update_children(queue, children, job.id) do
        :ok -> {:ok, %{job: job, children: children}}
        {:error, error} -> {:error, error}
      end
    end
  end

  defp update_children(queue, children, parent_id) do
    Enum.reduce_while(children, :ok, fn child, :ok ->
      case Queue.update_parent(queue, child.job.id, parent_id) do
        :ok -> {:cont, :ok}
        {:error, error} -> {:halt, {:error, error}}
      end
    end)
  end

  defp node_options(node, parent, child_ids) do
    options = get(node, :options, [])
    options = merge_option(options, "childrenIds", child_ids)
    options = merge_option(options, "dependsOn", child_ids)
    if parent, do: merge_option(options, "parentId", elem(parent, 0)), else: options
  end

  defp node_data(node, parent, child_ids) do
    data = get(node, :data, %{})
    data = if parent, do: marker(data, "__parentId", elem(parent, 0)), else: data
    data = if parent, do: marker(data, "__parentQueue", elem(parent, 1)), else: data
    if child_ids == [], do: data, else: marker(data, "__childrenIds", child_ids)
  end

  defp rollback(producer, ids) do
    queue = Queue.new("", producer.connection)
    Enum.each(ids, &Queue.cancel(queue, &1))
  end

  defp marker(data, _key, nil), do: data
  defp marker(data, key, value) when is_map(data), do: Map.put(data, key, value)
  defp marker(data, key, value), do: %{"payload" => data, key => value}
  defp list(nil), do: []
  defp list(value), do: [value]

  defp merge_option(options, key, value) when is_map(options), do: Map.put(options, key, value)
  defp merge_option(options, key, value), do: Keyword.put(options, String.to_atom(key), value)

  defp fetch(map, key) do
    Map.get(map, key) || Map.fetch!(map, to_string(key))
  end

  defp get(map, key, default),
    do: Map.get(map, key, Map.get(map, to_string(key), default))
end
