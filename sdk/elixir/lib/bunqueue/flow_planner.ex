defmodule Bunqueue.FlowPlanner do
  @moduledoc false

  alias Bunqueue.Options

  @max_depth 100
  @max_jobs 10_000
  @queue ~r/\A[a-zA-Z0-9_\-.:]+\z/
  @topology ~w(parentId dependsOn childrenIds)

  @spec plan_tree(map()) :: map()
  def plan_tree(flow) do
    {root, state} = visit(flow, nil, 0, new_state())
    %{jobs: Enum.reverse(state.jobs), root: root}
  end

  @spec plan_chain([map()]) :: map()
  def plan_chain(steps) when is_list(steps) do
    if length(steps) > @max_jobs, do: invalid!("flow exceeds the #{@max_jobs} job limit")

    {planned, _state} =
      Enum.map_reduce(steps, new_state(), fn step, state ->
        {name, queue, data, options} = prepare_chain_step(step)
        {id, state} = allocate_id(options, state)
        {%{id: id, name: name, queue: queue, data: data, options: options}, state}
      end)

    ids = Enum.map(planned, & &1.id)

    jobs =
      planned
      |> Enum.with_index()
      |> Enum.map(fn {step, index} ->
        previous = if index > 0, do: Enum.at(ids, index - 1)

        %{
          "id" => step.id,
          "queue" => step.queue,
          "input" =>
            flow_input(
              step.options,
              flow_data(step.name, step.data, %{"__flowParentId" => previous}),
              depends_on: if(previous, do: [previous])
            )
        }
      end)

    %{jobs: jobs, ids: ids}
  end

  def plan_chain(_steps), do: invalid!("flow steps must be a list")

  defp prepare_chain_step(step) when is_map(step) do
    children = field(step, :children, [])

    unless is_list(children), do: invalid!("flow chain step children must be a list")
    if children != [], do: invalid!("flow chain steps cannot have children")
    prepare_step(step)
  end

  defp prepare_chain_step(_step), do: invalid!("flow step must be a map")

  defp visit(node, parent, depth, state) when is_map(node) do
    {name, queue, data, options} = prepare_step(node)
    validate_depth!(depth)
    children = field(node, :children, [])
    unless is_list(children), do: invalid!("flow children must be a list")

    state = %{state | count: state.count + 1}
    if state.count > @max_jobs, do: invalid!("flow exceeds the #{@max_jobs} job limit")
    {id, state} = allocate_id(options, state)

    {children, state} =
      Enum.map_reduce(children, state, fn child, child_state ->
        visit(child, {id, queue}, depth + 1, child_state)
      end)

    child_ids = Enum.map(children, & &1.id)

    internal =
      %{}
      |> maybe_parent(parent)
      |> maybe_put("__childrenIds", nonempty(child_ids))

    job = %{
      "id" => id,
      "queue" => queue,
      "input" =>
        flow_input(
          options,
          flow_data(name, data, internal),
          parent_id: if(parent, do: elem(parent, 0)),
          depends_on: nonempty(child_ids),
          children_ids: nonempty(child_ids)
        )
    }

    {%{id: id, children: children}, %{state | jobs: [job | state.jobs]}}
  end

  defp visit(_node, _parent, _depth, _state), do: invalid!("flow node must be a map")

  defp prepare_step(step) when is_map(step) do
    name = required(step, :name)
    queue = required(step, :queue)
    validate_name_queue!(name, queue)
    data = field(step, :data, %{})
    raw_options = field(step, :options, field(step, :opts, []))
    options = Options.job(raw_options, true)
    validate_options!(options)
    {name, queue, data, options}
  end

  defp prepare_step(_step), do: invalid!("flow step must be a map")

  defp validate_name_queue!(name, queue) do
    unless is_binary(name) and String.valid?(name) and String.length(name) in 1..256 do
      invalid!("flow job name must be a non-empty string of at most 256 characters")
    end

    unless is_binary(queue) and String.valid?(queue) and byte_size(queue) <= 256 and
             Regex.match?(@queue, queue) do
      invalid!("invalid flow queue: #{inspect(queue)}")
    end
  end

  defp validate_depth!(depth) when depth <= @max_depth, do: :ok
  defp validate_depth!(_depth), do: invalid!("flow exceeds the #{@max_depth} level depth limit")

  defp validate_options!(options) do
    cond do
      Map.has_key?(options, "repeat") ->
        invalid!("repeat is not supported inside an atomic flow")

      Map.has_key?(options, "uniqueKey") or Map.has_key?(options, "dedup") ->
        invalid!("deduplication is not supported inside an atomic flow")

      Map.has_key?(options, "debounceId") or Map.has_key?(options, "debounceTtl") ->
        invalid!("debounce is not supported inside an atomic flow")

      Enum.any?(@topology, &Map.has_key?(options, &1)) ->
        invalid!("flow topology options are owned by FlowProducer")

      true ->
        :ok
    end
  end

  defp allocate_id(options, state) do
    id =
      case Map.fetch(options, "customId") do
        {:ok, custom} -> custom
        :error -> random_id()
      end

    unless is_binary(id) and String.valid?(id) and String.length(id) in 1..1024 and
             not String.contains?(id, ":") do
      invalid!("flow jobId must be non-empty and cannot contain a colon")
    end

    if MapSet.member?(state.ids, id), do: invalid!("duplicate flow job id: #{id}")
    {id, %{state | ids: MapSet.put(state.ids, id)}}
  end

  defp random_id, do: :crypto.strong_rand_bytes(16) |> Base.encode16(case: :lower)

  defp flow_data(name, data, internal) do
    data =
      cond do
        is_map(data) -> normalize_data(data)
        is_nil(data) -> %{}
        true -> %{"payload" => data}
      end

    data
    |> Map.put("name", name)
    |> Map.merge(internal)
  end

  defp normalize_data(data) do
    Enum.reduce(data, %{}, fn {raw_key, value}, normalized ->
      key = data_key!(raw_key)

      if key == "name" or String.starts_with?(key, "__") do
        invalid!("flow job data key is reserved: #{key}")
      end

      if Map.has_key?(normalized, key), do: invalid!("duplicate flow job data key: #{key}")
      Map.put(normalized, key, value)
    end)
  end

  defp data_key!(key) when is_binary(key) and byte_size(key) > 0, do: key
  defp data_key!(key) when is_atom(key), do: Atom.to_string(key)
  defp data_key!(_key), do: invalid!("flow job data keys must be strings or atoms")

  defp flow_input(options, data, links) do
    options
    |> Map.drop(@topology)
    |> Map.put("data", data)
    |> maybe_put("parentId", links[:parent_id])
    |> maybe_put("dependsOn", links[:depends_on])
    |> maybe_put("childrenIds", links[:children_ids])
  end

  defp maybe_parent(data, nil), do: data

  defp maybe_parent(data, {id, queue}) do
    data
    |> Map.put("__parentId", id)
    |> Map.put("__parentQueue", queue)
  end

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)
  defp nonempty([]), do: nil
  defp nonempty(values), do: values
  defp new_state, do: %{ids: MapSet.new(), jobs: [], count: 0}

  defp required(map, key) do
    case fetch_field(map, key) do
      {:ok, value} -> value
      :error -> invalid!("flow #{key} is required")
    end
  end

  defp field(map, key, default) do
    case fetch_field(map, key) do
      {:ok, value} -> value
      :error -> default
    end
  end

  defp fetch_field(map, key) do
    case Map.fetch(map, key) do
      {:ok, value} -> {:ok, value}
      :error -> Map.fetch(map, Atom.to_string(key))
    end
  end

  defp invalid!(message), do: raise(ArgumentError, message)
end
