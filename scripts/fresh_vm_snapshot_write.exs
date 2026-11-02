defmodule FreshVm.WriteCheck do
  def runtime_handle_path(term, path) do
    cond do
      is_pid(term) or is_port(term) or is_reference(term) ->
        [path]

      is_list(term) ->
        Enum.flat_map(Enum.with_index(term), fn {v, i} -> runtime_handle_path(v, path ++ [i]) end)

      is_tuple(term) ->
        term
        |> Tuple.to_list()
        |> Enum.with_index()
        |> Enum.flat_map(fn {v, i} -> runtime_handle_path(v, path ++ [i]) end)

      is_struct(term) ->
        runtime_handle_path(Map.from_struct(term), path)

      is_map(term) ->
        Enum.flat_map(term, fn {k, v} ->
          runtime_handle_path(k, path ++ [:key]) ++ runtime_handle_path(v, path ++ [k])
        end)

      true ->
        []
    end
  end
end

require Runic

alias Jido.Exec.Frame
alias Jido.Exec.Frame.Store.OwnerMap
alias Runic.Runner
alias Runic.Workflow

[path, ids_path] = System.argv()

store = OwnerMap.new()
root = Frame.root(Frame.new(%{request: :fresh_vm}), store)
child = Frame.nest(root, %{round: 1}, :continuation)
child = Frame.put_result(child, "tool", %{value: 42}, [])

workflow =
  Workflow.new(state_backend: :graph_ets)
  |> Workflow.add(Runic.step(fn _ -> child end, name: :emit))
  |> Workflow.react_until_satisfied(:go)

snapshot = Runner.encode_snapshot(workflow)
File.write!(path, snapshot)
File.write!(ids_path, :erlang.term_to_binary({store.id, root.id, child.id, store.pid, System.pid()}))

case FreshVm.WriteCheck.runtime_handle_path(:erlang.binary_to_term(snapshot), []) do
  [] -> IO.puts("WRITE_OK backend=#{workflow.state_backend} snapshot_bytes=#{byte_size(snapshot)}")
  paths -> raise "snapshot retained runtime handles: #{inspect(paths)}"
end

Workflow.release_state(workflow)
OwnerMap.release(store)
