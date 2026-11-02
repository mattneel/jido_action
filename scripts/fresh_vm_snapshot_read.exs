defmodule FreshVm.ReadCheck do
  def store_module(%{__struct__: module}), do: module
end

alias Jido.Exec.Frame
alias Jido.Exec.Frame.Store.OwnerMap
alias Runic.Runner
alias Runic.Workflow

[path, ids_path] = System.argv()

snapshot = File.read!(path)
{:ok, workflow} = Runner.decode_snapshot(snapshot)
{old_store_id, old_root_id, old_child_id, old_owner, old_os_pid} = :erlang.binary_to_term(File.read!(ids_path))

continuations =
  workflow
  |> Workflow.facts()
  |> Enum.map(& &1.value)
  |> Enum.filter(&is_struct(&1, Frame.Continuation))

[restored | _] = continuations
store = restored.store
new_store = OwnerMap.new()
_ = Frame.root(Frame.new(%{request: :fresh_vm}), new_store)
new_frame_id = FreshVm.ReadCheck.store_module(new_store).new_id()

true = is_pid(old_owner)
true = old_os_pid != System.pid()
true = store.id == old_store_id
true = restored.id == old_child_id
true = restored.parent_id == old_root_id
true = store.id != new_store.id
true = new_frame_id != old_root_id
true = FreshVm.ReadCheck.store_module(store).size(store) >= 3

{:ok, _} = FreshVm.ReadCheck.store_module(store).get(store, old_root_id)
{:jido_nested_flow_frame, 1, _parent, {:jido_flow_frame, 1, _input, results, _effects}} =
  Frame.materialize(restored, store)

%{"tool" => %{value: 42}} = results
IO.puts("READ_OK old_store=#{inspect(old_store_id)} new_store=#{inspect(new_store.id)} new_frame=#{inspect(new_frame_id)}")

Workflow.release_state(workflow)
OwnerMap.release(store)
OwnerMap.release(new_store)
