defmodule Bunqueue.FlowPlannerPropertyTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Bunqueue.{FlowPlanner, FlowProducer}

  property "tree plans have unique ids and reciprocal parent/child links" do
    check all(flow <- flow_tree(), max_runs: 256) do
      plan = FlowPlanner.plan_tree(flow)
      assert_tree_invariants(plan)
    end
  end

  property "custom job ids become the server customId" do
    check all(id <- string(:alphanumeric, min_length: 1, max_length: 64), max_runs: 256) do
      plan =
        FlowPlanner.plan_tree(%{
          name: "job",
          queue: "queue",
          options: [jobId: id]
        })

      assert [%{"id" => ^id, "input" => %{"customId" => ^id}}] = plan.jobs
    end
  end

  property "chains depend only on their immediately preceding step" do
    check all(steps <- list_of(step(), max_length: 30), max_runs: 256) do
      plan = FlowPlanner.plan_chain(steps)
      assert length(plan.jobs) == length(plan.ids)
      assert MapSet.size(MapSet.new(plan.ids)) == length(plan.ids)

      Enum.with_index(plan.jobs, fn job, index ->
        previous = if index > 0, do: Enum.at(plan.ids, index - 1)
        expected = if previous, do: [previous]
        input = job["input"]

        assert Map.get(input, "dependsOn") == expected
        assert Map.has_key?(input["data"], "__flowParentId")
        assert input["data"]["__flowParentId"] == previous
      end)
    end
  end

  test "rejects unsupported and user-owned options" do
    options = [
      [repeat: %{}],
      [uniqueKey: "dedup"],
      [dedup: %{id: "dedup"}],
      [debounceId: "debounce"],
      [debounceTtl: 1],
      [parentId: "parent"],
      [dependsOn: []],
      [childrenIds: []]
    ]

    for option <- options do
      assert_raise ArgumentError, fn ->
        FlowPlanner.plan_tree(%{name: "job", queue: "queue", options: option})
      end
    end
  end

  test "rejects reserved and colliding normalized data keys" do
    for key <- ["name", "__parentId", "__private"] do
      assert_raise ArgumentError, fn ->
        FlowPlanner.plan_tree(%{name: "job", queue: "queue", data: %{key => "owned"}})
      end
    end

    assert_raise ArgumentError, fn ->
      FlowPlanner.plan_tree(%{
        name: "job",
        queue: "queue",
        data: %{"value" => 2, value: 1}
      })
    end
  end

  test "chain planning accepts empty children and rejects other shapes before I/O" do
    assert %{ids: [_id]} =
             FlowPlanner.plan_chain([%{name: "job", queue: "queue", children: []}])

    assert %{ids: [_id]} =
             FlowPlanner.plan_chain([%{"name" => "job", "queue" => "queue", "children" => []}])

    cases = [
      {[%{name: "child", queue: "queue"}], ~r/cannot have children/},
      {"not-a-list", ~r/must be a list/}
    ]

    for {children, message} <- cases do
      assert_raise ArgumentError, message, fn ->
        FlowPlanner.plan_chain([%{name: "job", queue: "queue", children: children}])
      end

      assert_raise ArgumentError, message, fn ->
        FlowPlanner.plan_chain([%{"name" => "job", "queue" => "queue", "children" => children}])
      end
    end
  end

  test "FlowProducer rejects owned topology before touching its connection" do
    producer = FlowProducer.with_connection(self())

    assert {:error, %ArgumentError{}} =
             FlowProducer.add_chain(producer, [
               %{name: "job", queue: "queue", options: [dependsOn: []]}
             ])

    assert {:error, %ArgumentError{}} =
             FlowProducer.add_chain(producer, [
               %{name: "job", queue: "queue", children: [%{}]}
             ])

    refute_received _
  end

  defp flow_tree do
    tree(step(), fn child ->
      gen all(
            node <- step(),
            children <- list_of(child, max_length: 3)
          ) do
        Map.put(node, :children, children)
      end
    end)
    |> resize(16)
  end

  defp step do
    gen all(
          suffix <- positive_integer(),
          value <- integer()
        ) do
      %{
        name: "job-#{suffix}",
        queue: "queue_#{rem(suffix, 17)}",
        data: %{"value" => value}
      }
    end
  end

  defp assert_tree_invariants(plan) do
    by_id = Map.new(plan.jobs, &{&1["id"], &1})
    assert map_size(by_id) == length(plan.jobs)
    assert Enum.all?(Map.keys(by_id), &(not String.contains?(&1, ":")))

    node_ids = collect_node_ids(plan.root)
    assert length(node_ids) == length(plan.jobs)
    assert MapSet.new(node_ids) == MapSet.new(Map.keys(by_id))

    Enum.each(plan.jobs, fn job ->
      input = job["input"]
      data = input["data"]
      assert is_binary(data["name"])
      children = Map.get(input, "childrenIds", [])
      assert Map.get(input, "dependsOn", []) == children
      assert Map.get(data, "__childrenIds", []) == children

      Enum.each(children, fn child_id ->
        child = Map.fetch!(by_id, child_id)
        assert child["input"]["parentId"] == job["id"]
        assert child["input"]["data"]["__parentId"] == job["id"]
        assert child["input"]["data"]["__parentQueue"] == job["queue"]
      end)
    end)
  end

  defp collect_node_ids(node) do
    [node.id | Enum.flat_map(node.children, &collect_node_ids/1)]
  end
end
