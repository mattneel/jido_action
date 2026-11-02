defmodule JidoActionTest.Exec.Frame.ContinuationTest do
  use ExUnit.Case, async: true

  alias Jido.Exec.Frame
  alias Jido.Exec.Frame.Continuation
  alias Jido.Exec.Frame.Store.Ets

  setup do
    store = Ets.new()
    on_exit(fn -> if :ets.info(store.tid) != :undefined, do: Ets.release(store) end)
    {:ok, store: store}
  end

  defp root(store, input \\ %{request: :root}) do
    Frame.root(Frame.new(input), store)
  end

  test "a continuation keeps only its local level and a parent id", %{store: store} do
    parent = root(store)
    child = Frame.nest(parent, %{round: 1}, :continuation)

    assert %Continuation{parent_id: parent_id, store: ^store} = child
    assert parent_id == parent.id
    assert %{input: %{round: 1}, results: %{}} = Frame.resolver_state(child, %{})
    assert Frame.fetch_result(child, "model") == :error
    assert Frame.effects_for(child, "model") == []

    child = Frame.put_result(child, "model", %{answer: 42}, [:effect])
    assert Frame.fetch_result(child, "model") == {:ok, %{answer: 42}}
    assert Frame.effects_for(child, "model") == [:effect]
    assert {:ok, _} = Frame.merge(child)
  end

  test "completion moves child output to the parent record without embedding the parent", %{
    store: store
  } do
    parent = root(store)
    child = Frame.nest(parent, %{round: 1}, :continuation)
    child = Frame.put_result(child, "model", 1, [])
    updated = Frame.complete_nested(child, "child", :done, [:effect])

    assert %Continuation{id: new_parent_id, parent_id: nil} = updated
    assert new_parent_id != parent.id
    assert Frame.fetch_result(updated, "child") == {:ok, :done}
    assert {:ok, %{results: %{}}} = Ets.get(store, parent.id)
    assert {:ok, %{results: results}} = Ets.get(store, new_parent_id)
    assert results["child"] == :done
  end

  test "the parent id chain does not embed parent terms", %{store: store} do
    root_cont = root(store)
    one = Frame.nest(root_cont, %{round: 1}, :continuation) |> Frame.put_result("model", 1, [])
    two = Frame.nest(one, %{round: 2}, :continuation) |> Frame.put_result("model", 2, [])

    assert two.parent_id == one.id
    refute Map.has_key?(two, :parent)
    assert Frame.resolver_state(two, %{}).input == %{round: 2}
    assert :erts_debug.flat_size(two) < 100

    updated_one = Frame.complete_nested(two, "child", :second, [])
    assert updated_one.parent_id == one.parent_id
    assert updated_one.id != one.id
    assert Frame.fetch_result(updated_one, "child") == {:ok, :second}
    assert Frame.fetch_result(updated_one, "model") == {:ok, 1}
  end

  test "materialize is the only operation that walks to the legacy nested tuple", %{store: store} do
    parent = root(store)
    child = Frame.nest(parent, %{round: 1}, :continuation)
    materialized = Frame.materialize(child, store)

    assert materialized == Frame.nest(Frame.new(%{request: :root}), %{round: 1})
    assert Frame.resolver_state(materialized, %{}) == Frame.resolver_state(child, %{})
  end

  test "local updates append a new id and never mutate accepted records", %{store: store} do
    parent = root(store)
    child = Frame.nest(parent, %{round: 1}, :continuation)
    old_id = child.id
    updated = Frame.put_result(child, "model", 1, [])

    assert updated.id != old_id
    assert {:ok, %{results: %{}}} = Ets.get(store, old_id)
    assert {:ok, %{results: %{"model" => 1}}} = Ets.get(store, updated.id)
    assert {:ok, %{results: %{}}} = Ets.get(store, parent.id)
  end

  test "duplicate and stale completions append versions without overwriting history", %{
    store: store
  } do
    parent = root(store)
    child = Frame.nest(parent, %{round: 1}, :continuation) |> Frame.put_result("model", 1, [])
    first = Frame.complete_nested(child, "child", :first, [])
    second = Frame.complete_nested(child, "child", :second, [])

    assert first.id != second.id
    assert first.id != parent.id
    assert second.id != parent.id
    assert Frame.fetch_result(first, "child") == {:ok, :first}
    assert Frame.fetch_result(second, "child") == {:ok, :second}
    assert {:ok, %{results: %{}}} = Ets.get(store, parent.id)
  end

  test "parallel branches keep isolated local versions until explicit merge", %{store: store} do
    parent = root(store)
    left = Frame.nest(parent, %{round: 1}, :continuation) |> Frame.put_result("left", 1, [])
    right = Frame.nest(parent, %{round: 1}, :continuation) |> Frame.put_result("right", 2, [])

    assert left.id != right.id
    assert left.parent_id == right.parent_id
    assert {:ok, %{results: %{}}} = Ets.get(store, parent.id)
    assert {:ok, %{results: %{"left" => 1}}} = Ets.get(store, left.id)
    assert {:ok, %{results: %{"right" => 2}}} = Ets.get(store, right.id)

    assert {:ok, merged} = Frame.merge([left, right])
    assert merged.id != left.id and merged.id != right.id
    assert Frame.fetch_result(merged, "left") == {:ok, 1}
    assert Frame.fetch_result(merged, "right") == {:ok, 2}
  end

  test "the frame store is reclaimed when its owner exits" do
    parent = self()

    pid =
      spawn(fn ->
        store = Ets.new()
        send(parent, {:frame_tid, store.tid})
        receive do: (:stop -> :ok)
      end)

    tid = receive do: ({:frame_tid, tid} -> tid)
    assert :ets.info(tid) != :undefined
    ref = Process.monitor(pid)
    send(pid, :stop)
    assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 5_000
    assert :ets.info(tid) == :undefined
  end

  test "owner-map store dies on normal or crashed owner exit without leaking" do
    for exit_kind <- [:normal, :crash] do
      parent = self()

      owner =
        spawn(fn ->
          store = Jido.Exec.Frame.Store.OwnerMap.new()
          send(parent, {:owner_map_store, store})
          if exit_kind == :normal, do: receive(do: (:stop -> :ok)), else: Process.sleep(:infinity)
        end)

      store = receive do: ({:owner_map_store, store} -> store)
      assert Process.alive?(store.pid)

      ref = Process.monitor(owner)

      case exit_kind do
        :normal ->
          send(owner, :stop)
          assert_receive {:DOWN, ^ref, :process, ^owner, :normal}, 5_000

        :crash ->
          Process.exit(owner, :kill)
          assert_receive {:DOWN, ^ref, :process, ^owner, :killed}, 5_000
      end

      assert wait_for_down(store.pid, 50), "owner-map store leaked after #{exit_kind} owner exit"
    end
  end

  test "owner-map and ETS frame IDs are 128-bit random namespace values" do
    store = Jido.Exec.Frame.Store.OwnerMap.new()
    ets = Ets.new()
    root = Frame.root(Frame.new(%{root: true}), store)

    assert {:jido_frame_store, <<_::128>>} = store.id
    assert {:jido_frame_store, <<_::128>>} = ets.id
    assert {:frame, <<_::128>>} = root.id
    assert store.id != ets.id

    Jido.Exec.Frame.Store.OwnerMap.release(store)
    Ets.release(ets)
  end

  defp wait_for_down(pid, attempts) do
    cond do
      not Process.alive?(pid) -> true
      attempts == 0 -> false
      true -> Process.sleep(10) && wait_for_down(pid, attempts - 1)
    end
  end

  test "reap removes superseded frame versions and preserves live ones", %{store: store} do
    parent = root(store)
    child = Frame.nest(parent, %{round: 1}, :continuation)
    old_id = child.id
    live = Frame.put_result(child, "model", 1, [])

    assert Ets.size(store) >= 3
    assert {:ok, deleted} = Ets.reap(store, [parent.id, live.id])
    assert deleted >= 1
    assert :ets.member(store.tid, old_id) == false
    assert {:ok, _} = Ets.get(store, live.id)
  end
end
