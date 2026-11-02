defmodule JidoActionTest.Exec.StateBackendIntegrationTest do
  use ExUnit.Case, async: false
  require Runic

  alias Jido.Exec
  alias Jido.Exec.Frame
  alias Jido.Exec.Frame.Store.Ets, as: FrameStore
  alias Runic.Workflow
  alias Runic.Workflow.{CausalContext, Fact, Runnable, State}

  defmodule AddOne do
    use Jido.Action, name: "state_backend_add_one"

    @impl true
    def run(%{value: value}, _context), do: {:ok, %{value: value + 1}}
  end

  defmodule ChildFlow do
    use Jido.Flow, name: "state_backend_child"

    flow do
      step("add", action: AddOne, params: %{value: input(:value)})
      output(result("add"))
    end
  end

  defmodule ParentFlow do
    use Jido.Flow, name: "state_backend_parent"

    flow do
      step("child", action: ChildFlow, params: %{value: input(:value)})
      output(result("child"))
    end
  end

  defmodule BranchFlow do
    use Jido.Flow, name: "state_backend_branch"

    flow do
      step("left", action: AddOne, params: %{value: input(:value)})
      step("right", action: AddOne, params: %{value: input(:value)})
      output(%{left: result("left", :value), right: result("right", :value)})
    end
  end

  test "parallel branches with continuations keep canonical results" do
    expected = {:ok, %{left: 3, right: 3}}

    assert Exec.run(BranchFlow, %{value: 2}, %{}, max_concurrency: 2) == expected

    for backend <- [:owner_map, :graph_ets] do
      assert Exec.run(BranchFlow, %{value: 2}, %{},
               state_backend: backend,
               max_concurrency: 2
             ) == expected
    end
  end

  defmodule ProbeContext do
    use Jido.Action, name: "state_backend_probe_context"

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
    use Jido.Flow, name: "state_backend_probe_flow"

    flow do
      step("probe", action: ProbeContext, params: %{})
      output(result("probe"))
    end
  end

  defmodule NestedProbeAction do
    use Jido.Action, name: "state_backend_nested_probe"

    @impl true
    def run(_params, context) do
      case Jido.Exec.run(ProbeFlow, %{}, context) do
        {:ok, result} -> {:ok, %{nested: result}}
        {:error, error} -> {:error, error}
      end
    end
  end

  defmodule NestedProbeFlow do
    use Jido.Flow, name: "state_backend_nested_probe_flow"

    flow do
      step("nested", action: NestedProbeAction, params: %{})
      output(result("nested"))
    end
  end

  test "backend and frame mode propagate through actions and nested tool-style Exec runs" do
    assert {:ok, %{nested: nested}} =
             Exec.run(NestedProbeFlow, %{}, %{}, state_backend: :graph_ets)

    assert nested.frame_mode == :continuation
    assert nested.state_backend == :graph_ets
    assert nested.has_frame_store
  end

  test "real nested Flow produces identical results on Original and normalized backends" do
    expected = {:ok, %{value: 3}}

    assert Exec.run(ParentFlow, %{value: 2}) == expected
    assert Exec.run(ParentFlow, %{value: 2}, %{}, state_backend: :owner_map) == expected
    assert Exec.run(ParentFlow, %{value: 2}, %{}, state_backend: :graph_ets) == expected
  end

  test "continuation frame dispatch value size stays constant as ancestor depth grows" do
    sizes =
      for depth <- [1, 20, 100] do
        store = FrameStore.new()

        root =
          Frame.root(Frame.new(%{request: :large, pad: String.duplicate("x", 20_000)}), store)

        parent =
          Enum.reduce(1..depth, root, fn i, parent ->
            Frame.nest(parent, %{round: i}, :continuation)
          end)

        step = Runic.step(fn _ -> :ok end, name: :state_backend_probe)
        fact = Fact.new(value: parent)
        runnable = Runnable.new(step, fact, CausalContext.basic(step.hash, fact, 0))

        workflow = Workflow.new(state_backend: :graph_ets)
        {state, command} = State.prepare_dispatch(workflow.state, runnable)
        command_size = :erts_debug.flat_size(command)

        worker_size =
          State.run_command(command, fn fetched ->
            :erts_debug.flat_size(fetched.input_fact.value)
          end)

        assert %{input: %{round: _}} = Frame.resolver_state(parent, %{})
        assert worker_size < 200, "depth #{depth} worker value grew to #{worker_size} words"
        State.release(state)
        FrameStore.release(store)
        command_size
      end

    assert Enum.uniq(sizes) |> length() == 1,
           "dispatch command grew with depth: #{inspect(sizes)}"
  end

  test "materialization walks the id chain only at the explicit boundary" do
    store = FrameStore.new()
    root = Frame.root(Frame.new(%{request: :root}), store)
    child = Frame.nest(root, %{round: 1}, :continuation)
    materialized = Frame.materialize(child, store)
    assert materialized == Frame.nest(Frame.new(%{request: :root}), %{round: 1})
    FrameStore.release(store)
  end
end
