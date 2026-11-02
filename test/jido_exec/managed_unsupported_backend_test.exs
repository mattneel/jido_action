defmodule JidoActionTest.Exec.ManagedUnsupportedBackendTest do
  use ExUnit.Case, async: false

  alias Jido.Exec

  defmodule ProbeContext do
    use Jido.Action, name: "managed_backend_probe"

    @impl true
    def run(_params, context) do
      {:ok,
       %{
         frame_mode: context[:__jido_frame_mode__],
         state_backend: context[:__jido_state_backend__],
         has_frame_store: is_map(context[:__jido_frame_store__])
       }}
    end
  end

  defmodule ProbeFlow do
    use Jido.Flow, name: "managed_backend_probe_flow"

    flow do
      step("probe", action: ProbeContext, params: %{})
      output(result("probe"))
    end
  end

  defmodule AddOne do
    use Jido.Action, name: "managed_nested_add_one"

    @impl true
    def run(%{value: value}, context) do
      :persistent_term.put(
        {__MODULE__, :probe, context[:probe_key]},
        %{
          state_backend: context[:__jido_state_backend__],
          frame_mode: context[:__jido_frame_mode__],
          has_frame_store: is_map(context[:__jido_frame_store__])
        }
      )

      {:ok, %{value: value + 1}}
    end
  end

  defmodule ChildFlow do
    use Jido.Flow, name: "managed_nested_child"

    flow do
      step("add", action: AddOne, params: %{value: input(:value)})
      output(result("add"))
    end
  end

  defmodule ParentFlow do
    use Jido.Flow, name: "managed_nested_parent"

    flow do
      step("child", action: ChildFlow, params: %{value: input(:value)})
      output(result("child"))
    end
  end

  setup do
    name = :"managed_backend_runner_#{System.unique_integer([:positive])}"
    start_supervised!({Runic.Runner, name: name})
    {:ok, runner: name}
  end

  test "managed start accepts normalized state and frame modes", %{runner: runner} do
    for backend <- [:owner_map, :graph_ets],
        frame_mode <- [:full, :continuation] do
      id = {:managed, backend, frame_mode, System.unique_integer([:positive])}
      test_pid = self()
      hook = [on_idle: fn _state -> send(test_pid, {:idle, id}) end]

      assert {:ok, _worker} =
               Exec.start(runner, id, ProbeFlow, %{}, %{},
                 state_backend: backend,
                 frame_mode: frame_mode,
                 hooks: hook
               )

      assert_receive {:idle, ^id}, 2_000

      assert {:ok,
              %{state_backend: ^backend, frame_mode: ^frame_mode, has_frame_store: has_store}} =
               Exec.result(runner, id)

      assert has_store == (frame_mode == :continuation)
      assert :ok = Runic.Runner.stop(runner, id)
    end
  end

  test "managed resume restores normalized state and continuation resources", %{runner: runner} do
    for backend <- [:owner_map, :graph_ets],
        frame_mode <- [:full, :continuation] do
      id = {:managed_resume, backend, frame_mode, System.unique_integer([:positive])}
      test_pid = self()
      hook = [on_idle: fn _state -> send(test_pid, {:idle, id}) end]

      assert {:ok, _worker} =
               Exec.start(runner, id, ProbeFlow, %{}, %{},
                 state_backend: backend,
                 frame_mode: frame_mode,
                 checkpoint_strategy: :manual,
                 hooks: hook
               )

      assert_receive {:idle, ^id}, 2_000
      assert {:ok, _} = Exec.result(runner, id)
      assert :ok = Runic.Runner.checkpoint(runner, id)
      assert :ok = Runic.Runner.stop(runner, id)

      assert {:ok, _worker} = Exec.resume(runner, id, %{}, [])
      assert {:ok, %{state_backend: ^backend, frame_mode: ^frame_mode}} = Exec.result(runner, id)
      assert :ok = Runic.Runner.stop(runner, id)
    end
  end

  test "managed nested Flow snapshots survive frame-store owner exit", %{runner: runner} do
    for backend <- [:owner_map, :graph_ets] do
      id = {:managed_owner_dead, backend, System.unique_integer([:positive])}
      parent = self()

      task =
        Task.async(fn ->
          task_pid = self()
          hook = [on_idle: fn _state -> send(task_pid, :idle) end]

          assert {:ok, _worker} =
                   Exec.start(runner, id, ParentFlow, %{value: 2}, %{probe_key: id},
                     state_backend: backend,
                     frame_mode: :continuation,
                     checkpoint_strategy: :manual,
                     hooks: hook
                   )

          assert_receive :idle, 2_000
          assert {:ok, %{value: 3}} = Exec.result(runner, id)

          assert %{
                   state_backend: ^backend,
                   frame_mode: :continuation,
                   has_frame_store: true
                 } = :persistent_term.get({AddOne, :probe, id})

          assert :ok = Runic.Runner.checkpoint(runner, id)

          {store_mod, store_state} = Runic.Runner.get_store(runner)
          store_state = Runic.Runner.Store.for_workflow(store_mod, id, store_state)
          {:ok, stream} = store_mod.stream(id, store_state)
          events = Enum.to_list(stream)

          facts =
            if store_state[:facts_table], do: :ets.tab2list(store_state.facts_table), else: []

          assert handle_count(events) == 0, "#{backend} event journal retained a runtime handle"
          assert handle_count(facts) == 0, "#{backend} fact store retained a runtime handle"

          assert :ok = Runic.Runner.stop(runner, id)
          send(parent, {:snapshot_ready, id})
        end)

      assert_receive {:snapshot_ready, ^id}, 5_000
      Task.await(task)

      # The task that owned the original frame store has exited.
      assert {:ok, _worker} = Exec.resume(runner, id, %{}, [])
      assert {:ok, %{value: 3}} = Exec.result(runner, id)
      :persistent_term.erase({AddOne, :probe, id})
      assert :ok = Runic.Runner.stop(runner, id)
    end
  end

  test "managed nested Flow Original/full remains the control", %{runner: runner} do
    id = {:managed_original_control, System.unique_integer([:positive])}
    test_pid = self()
    hook = [on_idle: fn _state -> send(test_pid, {:idle, id}) end]

    assert {:ok, _worker} =
             Exec.start(runner, id, ParentFlow, %{value: 2}, %{probe_key: id},
               state_backend: :original,
               frame_mode: :full,
               hooks: hook
             )

    assert_receive {:idle, ^id}, 2_000
    assert {:ok, %{value: 3}} = Exec.result(runner, id)

    assert %{state_backend: :original, frame_mode: :full, has_frame_store: false} =
             :persistent_term.get({AddOne, :probe, id})

    :persistent_term.erase({AddOne, :probe, id})
    assert :ok = Runic.Runner.stop(runner, id)
  end

  test "managed start accepts inherited runtime options from context", %{runner: runner} do
    id = {:managed_context, System.unique_integer([:positive])}
    test_pid = self()
    hook = [on_idle: fn _state -> send(test_pid, {:idle, id}) end]

    context = %{
      __jido_state_backend__: :owner_map,
      __jido_frame_mode__: :full
    }

    assert {:ok, _worker} = Exec.start(runner, id, ProbeFlow, %{}, context, hooks: hook)
    assert_receive {:idle, ^id}, 2_000
    assert {:ok, %{state_backend: :owner_map, frame_mode: :full}} = Exec.result(runner, id)
    assert :ok = Runic.Runner.stop(runner, id)
  end

  defp handle_count(term, acc \\ 0)
  defp handle_count(%Jido.Exec.Frame.Store.Ets{}, acc), do: acc + 1
  defp handle_count(%Jido.Exec.Frame.Store.OwnerMap{}, acc), do: acc + 1
  defp handle_count(%_{} = struct, acc), do: handle_count(Map.from_struct(struct), acc)
  defp handle_count(list, acc) when is_list(list), do: Enum.reduce(list, acc, &handle_count/2)

  defp handle_count(tuple, acc) when is_tuple(tuple),
    do: tuple |> Tuple.to_list() |> handle_count(acc)

  defp handle_count(map, acc) when is_map(map),
    do: Enum.reduce(map, acc, fn {k, v}, acc -> handle_count(k, handle_count(v, acc)) end)

  defp handle_count(_term, acc), do: acc
end
