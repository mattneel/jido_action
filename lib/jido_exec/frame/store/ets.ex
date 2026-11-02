defmodule Jido.Exec.Frame.Store.Ets do
  @moduledoc """
  Owner-created ETS frame store for normalized Flow continuations.

  The table is public because Flow nodes execute in TaskScope worker
  processes and write only their local frame deltas. It is owned by the
  execution task and is deleted when that task exits. Reads and writes copy
  only the selected local record, never an ancestor chain.
  """

  @behaviour Jido.Exec.Frame.Store

  defstruct [:tid, :id]

  @impl true
  def new do
    %__MODULE__{
      id: {:jido_frame_store, :crypto.strong_rand_bytes(16)},
      tid:
        :ets.new(:jido_frame_store, [
          :set,
          :public,
          read_concurrency: true,
          write_concurrency: true
        ])
    }
  end

  @doc false
  def dump_records(%__MODULE__{tid: tid}) do
    :ets.tab2list(tid)
  end

  @doc false
  def load_resource(id, records) do
    store = %__MODULE__{
      id: id,
      tid:
        :ets.new(:jido_frame_store, [
          :set,
          :public,
          read_concurrency: true,
          write_concurrency: true
        ])
    }

    Enum.each(records, fn {frame_id, local} ->
      :ets.insert(store.tid, {frame_id, local})
    end)

    store
  end

  @impl true
  def put(%__MODULE__{tid: tid}, id, local) do
    true = :ets.insert(tid, {id, local})
    :ok
  end

  @impl true
  def get(%__MODULE__{tid: tid}, id) do
    case :ets.lookup(tid, id) do
      [{^id, local}] -> {:ok, local}
      [] -> :error
    end
  end

  @impl true
  def delete(%__MODULE__{tid: tid}, id) do
    true = :ets.delete(tid, id)
    :ok
  end

  @impl true
  def release(%__MODULE__{tid: tid}) do
    :ets.delete(tid)
    :ok
  end

  @impl true
  def size(%__MODULE__{tid: tid}), do: :ets.info(tid, :size)
  def size(_), do: 0

  @impl true
  def reap(%__MODULE__{tid: tid}, keep_ids) do
    keep = MapSet.new(keep_ids)
    ids = :ets.select(tid, [{{:"$1", :_}, [], [:"$1"]}])
    dead = Enum.reject(ids, &MapSet.member?(keep, &1))
    Enum.each(dead, &:ets.delete(tid, &1))
    {:ok, length(dead)}
  end

  @impl true
  def new_id, do: {:frame, :crypto.strong_rand_bytes(16)}
end

defimpl Runic.Workflow.State.Portable, for: Jido.Exec.Frame.Store.Ets do
  alias Runic.Workflow.State.ResourceRef

  def identity(%Jido.Exec.Frame.Store.Ets{id: id}), do: {:ok, id, Jido.Exec.Frame.Store.Ets}

  def externalize(%Jido.Exec.Frame.Store.Ets{id: id} = store) do
    replacement = %ResourceRef{id: id, module: Jido.Exec.Frame.Store.Ets}
    records = Jido.Exec.Frame.Store.Ets.dump_records(store)
    {:ok, replacement, %{id => {Jido.Exec.Frame.Store.Ets, records}}}
  end
end
