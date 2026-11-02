defmodule JidoActionTest.Exec.BranchConcurrencyTest do
  use ExUnit.Case, async: false

  alias Jido.Exec

  defmodule SleepBranch do
    use Jido.Action, name: "state_backend_sleep_branch"

    @impl true
    def run(%{id: id}, _context) do
      Process.sleep(100)
      {:ok, %{id: id}}
    end
  end

  defmodule EightBranchFlow do
    use Jido.Flow, name: "state_backend_eight_branch"

    flow do
      step("branch_1", action: SleepBranch, params: %{id: 1})
      step("branch_2", action: SleepBranch, params: %{id: 2})
      step("branch_3", action: SleepBranch, params: %{id: 3})
      step("branch_4", action: SleepBranch, params: %{id: 4})
      step("branch_5", action: SleepBranch, params: %{id: 5})
      step("branch_6", action: SleepBranch, params: %{id: 6})
      step("branch_7", action: SleepBranch, params: %{id: 7})
      step("branch_8", action: SleepBranch, params: %{id: 8})

      output(%{
        branch_1: result("branch_1", :id),
        branch_2: result("branch_2", :id),
        branch_3: result("branch_3", :id),
        branch_4: result("branch_4", :id),
        branch_5: result("branch_5", :id),
        branch_6: result("branch_6", :id),
        branch_7: result("branch_7", :id),
        branch_8: result("branch_8", :id)
      })
    end
  end

  defp measure(fun) do
    t0 = System.monotonic_time(:microsecond)
    result = fun.()
    {result, System.monotonic_time(:microsecond) - t0}
  end

  test "eight independent 100ms branches retain concurrency on Original and normalized routes" do
    expected =
      {:ok,
       %{
         branch_1: 1,
         branch_2: 2,
         branch_3: 3,
         branch_4: 4,
         branch_5: 5,
         branch_6: 6,
         branch_7: 7,
         branch_8: 8
       }}

    {serial, serial_us} =
      measure(fn -> Exec.run(EightBranchFlow, %{}, %{}, max_concurrency: 1) end)

    assert serial == expected

    runs =
      for backend <- [:original, :owner_map, :graph_ets] do
        {result, us} =
          measure(fn ->
            Exec.run(EightBranchFlow, %{}, %{}, max_concurrency: 8, state_backend: backend)
          end)

        assert result == expected, "backend #{backend} changed canonical branch results"
        {backend, us}
      end

    File.mkdir_p!("../../results/integration")

    File.write!(
      "../../results/integration/branch-concurrency-#{System.system_time(:millisecond)}-#{System.unique_integer([:positive])}.json",
      :json.encode(%{
        branches: 8,
        delay_ms: 100,
        serial_us: serial_us,
        parallel: Map.new(runs),
        speedups: Map.new(runs, fn {backend, us} -> {backend, serial_us / us} end)
      })
    )

    for {backend, us} <- runs do
      assert us < serial_us * 0.75,
             "#{backend} did not retain concurrency: serial=#{serial_us}us parallel=#{us}us"
    end
  end
end
