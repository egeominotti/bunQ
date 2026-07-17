defmodule Bunqueue.WorkerLifecycle do
  @moduledoc false

  use GenServer

  def start_link, do: GenServer.start_link(__MODULE__, :ok)
  def enter(server), do: safe_call(server, :enter, :stopped)
  def leave(server), do: GenServer.cast(server, :leave)
  def begin_stop(server), do: safe_call(server, :begin_stop, :done, :infinity)

  def finish_stop(server) do
    monitor = Process.monitor(server)
    result = safe_call(server, :finish_stop, :ok)

    receive do
      {:DOWN, ^monitor, :process, ^server, _reason} -> result
    end
  end

  @impl true
  def init(:ok) do
    {:ok, %{phase: :running, active: 0, owner: nil, followers: []}}
  end

  @impl true
  def handle_call(:enter, _from, %{phase: :running} = state) do
    {:reply, :ok, %{state | active: state.active + 1}}
  end

  def handle_call(:enter, _from, state), do: {:reply, :stopped, state}

  def handle_call(:begin_stop, from, %{phase: :running, active: 0} = state) do
    {:reply, :owner, %{state | phase: :stopping, owner: from}}
  end

  def handle_call(:begin_stop, from, %{phase: :running} = state) do
    {:noreply, %{state | phase: :stopping, owner: from}}
  end

  def handle_call(:begin_stop, from, %{phase: :stopping} = state) do
    {:noreply, %{state | followers: [from | state.followers]}}
  end

  def handle_call(:begin_stop, _from, %{phase: :stopped} = state) do
    {:reply, :done, state}
  end

  def handle_call(:finish_stop, _from, state) do
    Enum.each(state.followers, &GenServer.reply(&1, :done))
    {:stop, :normal, :ok, %{state | phase: :stopped, owner: nil, followers: []}}
  end

  @impl true
  def handle_cast(:leave, %{active: active} = state) when active > 0 do
    next = %{state | active: active - 1}

    if next.phase == :stopping and next.active == 0 do
      GenServer.reply(next.owner, :owner)
      {:noreply, next}
    else
      {:noreply, next}
    end
  end

  def handle_cast(:leave, state), do: {:noreply, state}

  defp safe_call(server, message, fallback, timeout \\ 5_000) do
    GenServer.call(server, message, timeout)
  catch
    :exit, _reason -> fallback
  end
end
