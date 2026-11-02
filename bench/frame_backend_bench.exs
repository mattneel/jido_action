# Fresh-VM normalized frame benchmark.
# mix run --no-start bench/frame_backend_bench.exs -- --mode full|continuation \
#   --depth N --rep K --out path.json
# Measures only the execution window: sampler starts before Workflow.execute and
# stops immediately after it returns. Materialization/validation happens after.
argv =
  case System.argv() do
    ["--" | rest] -> rest
    args -> args
  end

{opts, _, _} =
  OptionParser.parse(argv,
    strict: [
    mode: :string,
    state: :string,
    depth: :integer,
    rep: :integer,
    out: :string,
    entries: :integer
  ]
  )

mode = Keyword.fetch!(opts, :mode) |> String.to_atom()
state_backend = Keyword.get(opts, :state, "graph_ets") |> String.to_atom()
depth = Keyword.fetch!(opts, :depth)
rep = Keyword.get(opts, :rep, 0)
entries = Keyword.get(opts, :entries, 2000)
out = Keyword.fetch!(opts, :out)
if File.exists?(out), do: raise("refusing to overwrite existing benchmark artifact: #{out}")
require Runic

defmodule FrameBench do
  alias Jido.Exec.Frame
  alias Jido.Exec.Frame.Store.Ets, as: Store
  alias Runic.Workflow

  @limit 512 * 1024 * 1024
  @rss_limit 768 * 1024 * 1024
  @abs_rss_limit 1024 * 1024 * 1024
  @deadline_ms 60_000
  @page 4096

  def run(mode, state_backend, depth, entries, rep, out) do
    payload = for i <- 1..entries, do: %{id: i, text: "entry-#{i}", tags: [:a, :b]}
    store = if mode == :continuation, do: Store.new()
    root = if mode == :continuation, do: Frame.root(Frame.new(%{payload: payload}), store), else: Frame.new(%{payload: payload})
    workflow = build_workflow(mode, state_backend, depth, store)
    :erlang.garbage_collect()
    base = snapshot()
    sampler = start_sampler(base, out, %{mode: mode, depth: depth, rep: rep})
    t0 = System.monotonic_time(:microsecond)
    {workflow, _execution} = Workflow.execute(workflow, root, async: true, max_concurrency: 1, runnable_order: :stable)
    elapsed = System.monotonic_time(:microsecond) - t0
    {peak, samples, series} = stop_sampler(sampler)

    :erlang.garbage_collect()
    stats = Workflow.state_stats(workflow)
    final = workflow |> Workflow.productions(:"frame_step_#{depth}") |> List.last()
    final_input = Frame.resolver_state(final.value, %{}).input
    final_value = final.value
    materialized = if mode == :continuation, do: Frame.materialize(final_value, store), else: final_value
    materialized_words = :erts_debug.flat_size(materialized)

    result = %{
      mode: mode,
      state_backend: state_backend,
      depth: depth,
      rep: rep,
      entries: entries,
      elapsed_us: elapsed,
      final_round: final_input.round,
      final_value_words: :erts_debug.flat_size(final),
      materialized_words: materialized_words,
      materialized_equal_depth: nested_depth(materialized) == depth,
      store: stats,
      frame_store:
        if store do
          %{rows: Store.size(store), bytes: :ets.info(store.tid, :memory) * :erlang.system_info(:wordsize)}
        end,
      base: base,
      peak: peak,
      peak_increase: diff(peak, base),
      samples: samples,
      series: series,
      status: "ok"
    }

    File.write!(out, :json.encode(result))
    IO.puts(:json.encode(result))
    if store, do: Store.release(store)
    Workflow.release_state(workflow)
  end

  defp build_workflow(mode, state_backend, depth, store) do
    {workflow, _prev} =
      Enum.reduce(1..depth, {Workflow.new(state_backend: state_backend), nil}, fn r, {workflow, prev} ->
        name = :"frame_step_#{r}"

        step =
          Runic.step(
            fn frame ->
              if mode == :continuation do
                Frame.nest(frame, %{round: r}, :continuation, store)
              else
                Frame.nest(frame, %{round: r})
              end
            end,
            name: name
          )

        workflow =
          if prev, do: Workflow.add(workflow, step, to: prev), else: Workflow.add(workflow, step)

        {workflow, name}
      end)

    workflow
  end

  defp nested_depth({:jido_nested_flow_frame, 1, parent, _child}), do: 1 + nested_depth(parent)
  defp nested_depth({:jido_flow_frame, 1, _, _, _}), do: 0

  defp start_sampler(base, out, meta) do
    parent = self()

    spawn(fn ->
      loop(%{
        parent: parent,
        base: base,
        peak: base,
        started: System.monotonic_time(:millisecond),
        out: out,
        meta: meta,
        samples: 0,
        series: []
      })
    end)
  end

  defp loop(state) do
    receive do
      {:stop, from, ref} ->
        state = take(state)
        send(from, {ref, state.peak, state.samples, Enum.reverse(state.series)})
    after
      5 ->
        state = take(state)
        elapsed = System.monotonic_time(:millisecond) - state.started

        if state.peak.total - state.base.total > @limit or
             is_integer(state.peak.rss) and state.peak.rss - state.base.rss > @rss_limit or
             is_integer(state.peak.rss) and state.peak.rss > @abs_rss_limit or
             elapsed > @deadline_ms do
          reason =
            cond do
              state.peak.total - state.base.total > @limit -> "vm_total_growth_limit"
              state.peak.rss > @abs_rss_limit -> "absolute_rss_limit"
              state.peak.rss - state.base.rss > @rss_limit -> "rss_growth_limit"
              true -> "time_limit"
            end

          File.write!(
            state.out,
            :json.encode(
              Map.merge(state.meta, %{
                status: "censored",
                censor_reason: reason,
                completed_rounds: 0,
                peak_increase: diff(state.peak, state.base)
              })
            )
          )

          System.halt(3)
        else
          loop(state)
        end
    end
  end

  defp take(state) do
    snap = snapshot()
    peak = Map.new(snap, fn {k, v} -> {k, max(v || 0, state.peak[k] || 0)} end)
    elapsed = System.monotonic_time(:millisecond) - state.started
    sample = %{ms: elapsed, total: snap.total, processes: snap.processes, ets: snap.ets, rss: snap.rss}
    %{state | peak: peak, samples: state.samples + 1, series: [sample | state.series]}
  end

  defp stop_sampler(pid) do
    ref = make_ref()
    send(pid, {:stop, self(), ref})

    receive do
      {^ref, peak, samples, series} -> {peak, samples, series}
    end
  end

  defp snapshot do
    mem = :erlang.memory()
    %{
      total: mem[:total],
      processes: mem[:processes],
      system: mem[:system],
      binary: mem[:binary],
      ets: mem[:ets],
      rss: rss()
    }
  end

  defp rss do
    case :file.read_file("/proc/self/statm") do
      {:ok, data} ->
        [_size, resident | _] = String.split(data)
        String.to_integer(resident) * @page

      _ ->
        nil
    end
  end

  defp diff(a, b), do: Map.new(a, fn {k, v} -> {k, if(is_integer(v) and is_integer(b[k]), do: v - b[k])} end)
end

FrameBench.run(mode, state_backend, depth, entries, rep, out)
