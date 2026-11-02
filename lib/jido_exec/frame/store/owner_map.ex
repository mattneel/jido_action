defmodule Jido.Exec.Frame.Store.OwnerMap do
  @moduledoc """
  Owner-process continuation store for normalized Flow frames.

  Frames live in a GenServer owned (linked) by the process that creates the
  store. Workers call the store process; no ETS table is used. The store is
  runtime-scoped and released explicitly or when its owner exits. Snapshot
  externalization preserves a stable `id` and the frame records while replacing
  the pid with a portable resource reference.
  """

  @behaviour Jido.Exec.Frame.Store

  use GenServer

  defstruct [:pid, :id]

  @impl true
  def new do
    id = {:jido_frame_store, :crypto.strong_rand_bytes(16)}
    {:ok, pid} = start_owned(%{id: id, frames: %{}})
    %__MODULE__{id: id, pid: pid}
  end

  @doc false
  def load_resource(id, records) do
    {:ok, pid} = start_owned(%{id: id, frames: Map.new(records)})
    %__MODULE__{id: id, pid: pid}
  end

  defp start_owned(state) do
    owner = self()
    GenServer.start(__MODULE__, Map.put(state, :owner, owner))
  end

  @doc false
  def dump_records(%__MODULE__{pid: pid}), do: GenServer.call(pid, :dump)

  @impl true
  def put(%__MODULE__{pid: pid}, id, local), do: GenServer.call(pid, {:put, id, local})

  @impl true
  def get(%__MODULE__{pid: pid}, id), do: GenServer.call(pid, {:get, id})

  @impl true
  def delete(%__MODULE__{pid: pid}, id), do: GenServer.call(pid, {:delete, id})

  @impl true
  def release(%__MODULE__{pid: pid}) do
    if Process.alive?(pid), do: GenServer.stop(pid, :normal)
    :ok
  end

  @impl true
  def size(%__MODULE__{pid: pid}), do: GenServer.call(pid, :size)

  @impl true
  def reap(%__MODULE__{pid: pid}, keep_ids), do: GenServer.call(pid, {:reap, keep_ids})

  @impl true
  def new_id, do: {:frame, :crypto.strong_rand_bytes(16)}

  @impl GenServer
  def init(%{owner: owner} = state) do
    ref = Process.monitor(owner)
    {:ok, Map.put(state, :owner_ref, ref)}
  end

  @impl GenServer
  def handle_call({:put, id, local}, _from, state) do
    {:reply, :ok, %{state | frames: Map.put(state.frames, id, local)}}
  end

  def handle_call({:get, id}, _from, state) do
    {:reply, Map.fetch(state.frames, id), state}
  end

  def handle_call({:delete, id}, _from, state) do
    {:reply, :ok, %{state | frames: Map.delete(state.frames, id)}}
  end

  def handle_call(:size, _from, state) do
    {:reply, map_size(state.frames), state}
  end

  def handle_call(:dump, _from, state) do
    {:reply, Map.to_list(state.frames), state}
  end

  def handle_call({:reap, keep_ids}, _from, state) do
    keep = MapSet.new(keep_ids)
    {kept, dead} = Enum.split_with(state.frames, fn {id, _} -> MapSet.member?(keep, id) end)
    {:reply, {:ok, length(dead)}, %{state | frames: Map.new(kept)}}
  end

  @impl GenServer
  def handle_info({:DOWN, ref, :process, _owner, _reason}, %{owner_ref: ref} = state) do
    {:stop, :normal, state}
  end

  def handle_info(_message, state), do: {:noreply, state}
end

defimpl Runic.Workflow.State.Portable, for: Jido.Exec.Frame.Store.OwnerMap do
  alias Runic.Workflow.State.ResourceRef

  def identity(%Jido.Exec.Frame.Store.OwnerMap{id: id}),
    do: {:ok, id, Jido.Exec.Frame.Store.OwnerMap}

  def externalize(%Jido.Exec.Frame.Store.OwnerMap{id: id} = store) do
    replacement = %ResourceRef{id: id, module: Jido.Exec.Frame.Store.OwnerMap}
    records = Jido.Exec.Frame.Store.OwnerMap.dump_records(store)
    {:ok, replacement, %{id => {Jido.Exec.Frame.Store.OwnerMap, records}}}
  end
end
