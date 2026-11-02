defmodule JidoActionTest.Exec.NormalizedLifecycleTest do
  use ExUnit.Case, async: false

  alias Jido.Action.Error
  alias Jido.Exec

  @backends [:owner_map, :graph_ets]

  defmodule RetryProbe do
    use Jido.Action, name: "state_backend_retry_probe"

    @impl true
    def run(%{succeed_on: succeed_on}, %{counter: counter}) do
      attempt = Agent.get_and_update(counter, fn count -> {count + 1, count + 1} end)

      if attempt < succeed_on do
        {:error, Error.execution_error("retry", %{retry: true, attempt: attempt})}
      else
        {:ok, %{attempt: attempt}}
      end
    end
  end

  defmodule RetryFlow do
    use Jido.Flow, name: "state_backend_retry_flow"

    flow do
      step("retry", action: RetryProbe, params: %{succeed_on: 3})
      output(result("retry"))
    end
  end

  defmodule TimeoutProbe do
    use Jido.Action, name: "state_backend_timeout_probe"

    @impl true
    def run(_params, _context) do
      receive do
        :release -> {:ok, %{released: true}}
      end
    end
  end

  defmodule TimeoutFlow do
    use Jido.Flow, name: "state_backend_timeout_flow"

    flow do
      step("timeout", action: TimeoutProbe, params: %{})
      output(result("timeout"))
    end
  end

  defmodule FailureProbe do
    use Jido.Action, name: "state_backend_failure_probe"

    @impl true
    def run(_params, _context) do
      {:error, Error.execution_error("normalized failure", %{reason: :expected})}
    end
  end

  defmodule FailureFlow do
    use Jido.Flow, name: "state_backend_failure_flow"

    flow do
      step("failure", action: FailureProbe, params: %{})
      output(result("failure"))
    end
  end

  test "retry attempts remain distinct and succeed on the normalized route" do
    for backend <- @backends do
      {:ok, counter} = Agent.start_link(fn -> 0 end)

      assert Exec.run(RetryFlow, %{}, %{counter: counter},
               state_backend: backend,
               max_attempts: 3,
               backoff: :none,
               base_delay_ms: 0,
               max_delay_ms: 0
             ) == {:ok, %{attempt: 3}}

      assert Agent.get(counter, & &1) == 3
    end
  end

  test "timeout is reported on the normalized route" do
    for backend <- @backends do
      assert {:error, %Error.TimeoutError{timeout: 20}} =
               Exec.run(TimeoutFlow, %{}, %{}, state_backend: backend, timeout: 20)
    end
  end

  test "failure is reported on the normalized route" do
    for backend <- @backends do
      assert {:error, %Error.ExecutionFailureError{message: "normalized failure"}} =
               Exec.run(FailureFlow, %{}, %{}, state_backend: backend)
    end
  end

  test "duplicate fact acceptance does not create a second occurrence or content version" do
    alias Runic.Workflow.{Fact, State}

    for backend <- @backends do
      state = State.new(backend)
      fact = Fact.new(value: %{payload: "same"})
      {state, ref} = State.put_fact(state, fact)
      before = State.stats(state)
      {state, ref_again} = State.put_fact(state, fact)
      after_stats = State.stats(state)

      assert ref_again.hash == ref.hash
      assert after_stats.facts == before.facts
      assert after_stats.fact_dup == before.fact_dup + 1
      assert State.fetch_fact(state, ref) == {:ok, fact}
      State.release(state)
    end
  end
end
