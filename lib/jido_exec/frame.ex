defmodule Jido.Exec.Frame do
  @moduledoc false

  alias Jido.Flow.Error
  alias Jido.Exec.Frame.Continuation

  @tag :jido_flow_frame
  @nested_tag :jido_nested_flow_frame
  @version 1

  @type t :: {:jido_flow_frame, 1, term(), map(), %{optional(String.t()) => [term()]}}
  @type nested_t :: {:jido_nested_flow_frame, 1, t() | nested_t(), t()} | Continuation.t()

  @doc false
  @spec new(term()) :: t()
  def new(input), do: {@tag, @version, input, %{}, %{}}

  @doc """
  Creates a normalized root continuation and stores its local frame record.
  """
  @spec root(t(), term()) :: Continuation.t()
  def root({@tag, @version, input, results, effects}, store) do
    id = store_module(store).new_id()

    local = %{
      parent_id: nil,
      input: input,
      results: results,
      effects: effects
    }

    :ok = store_module(store).put(store, id, local)

    %Continuation{
      id: id,
      parent_id: nil,
      input: input,
      results: results,
      effects: effects,
      store: store
    }
  end

  @doc false
  @spec nest(t() | nested_t(), term()) :: nested_t()
  def nest(parent, input), do: nest(parent, input, :full)

  @doc false
  @spec nest(t() | nested_t(), term(), :full | :continuation) :: nested_t()
  def nest(parent, input, :continuation), do: nest(parent, input, :continuation, store(parent))
  def nest(parent, input, _mode), do: {@nested_tag, @version, parent, new(input)}

  @doc """
  Nests in continuation mode. A tuple parent is first registered as a root;
  a continuation parent supplies its own store and id.
  """
  @spec nest(t() | nested_t(), term(), :continuation, term()) :: Continuation.t()
  def nest(%Continuation{} = parent, input, :continuation, _store) do
    child(parent.store, parent.id, input)
  end

  def nest({@tag, @version, _, _, _} = parent, input, :continuation, store) do
    root = root(parent, store)
    child(store, root.id, input)
  end

  def nest(%Continuation{} = parent, input, _mode, _store),
    do: child(parent.store, parent.id, input)

  def nest({@tag, @version, _, _, _} = parent, input, _mode, _store),
    do: {@nested_tag, @version, parent, new(input)}

  def nest(parent, input, mode, _store) when mode != :continuation,
    do: {@nested_tag, @version, parent, new(input)}

  defp child(store, parent_id, input) do
    id = store_module(store).new_id()

    local = %{
      parent_id: parent_id,
      input: input,
      results: %{},
      effects: %{}
    }

    :ok = store_module(store).put(store, id, local)

    %Continuation{
      id: id,
      parent_id: parent_id,
      input: input,
      results: %{},
      effects: %{},
      store: store
    }
  end

  @doc false
  @spec merge(t() | nested_t() | [t() | nested_t()]) ::
          {:ok, t() | nested_t()} | {:error, Exception.t()}
  def merge({@tag, @version, _input, _results, _effects} = frame), do: {:ok, frame}

  def merge(%Continuation{} = frame), do: {:ok, frame}

  def merge({@nested_tag, @version, parent, child} = frame) do
    with {:ok, _parent} <- merge(parent), {:ok, _child} <- merge(child), do: {:ok, frame}
  end

  def merge([first | rest]) do
    with {:ok, first} <- merge(first) do
      Enum.reduce_while(rest, {:ok, first}, fn next, {:ok, current} ->
        case merge_pair(current, next) do
          {:ok, merged} -> {:cont, {:ok, merged}}
          {:error, error} -> {:halt, {:error, error}}
        end
      end)
    end
  end

  def merge(value) do
    {:error,
     Error.execution_error("Runic supplied an invalid Flow fact", %{
       reason: :invalid_flow_frame,
       value: value
     })}
  end

  @doc false
  @spec put_result(t() | nested_t(), String.t(), term(), [term()]) :: t() | nested_t()
  def put_result({@tag, @version, input, results, effects}, name, value, requests) do
    {
      @tag,
      @version,
      input,
      Map.put(results, name, value),
      Map.put(effects, name, requests)
    }
  end

  def put_result({@nested_tag, @version, parent, child}, name, value, requests) do
    {@nested_tag, @version, parent, put_result(child, name, value, requests)}
  end

  def put_result(%Continuation{} = frame, name, value, requests) do
    local = %{
      Continuation.local(frame)
      | results: Map.put(frame.results, name, value),
        effects: Map.put(frame.effects, name, requests)
    }

    id = store_module(frame.store).new_id()
    :ok = store_module(frame.store).put(frame.store, id, local)
    Continuation.from_local(id, frame.store, local)
  end

  @doc false
  @spec item_id(term(), non_neg_integer()) :: String.t()
  def item_id(owner_id, index) do
    {owner_id, index}
    |> :erlang.term_to_binary([:deterministic])
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
  end

  @doc false
  @spec resolver_state(t() | nested_t(), map()) :: map()
  def resolver_state({@tag, @version, input, results, _effects}, context) do
    %{input: input, context: context, results: results}
  end

  def resolver_state({@nested_tag, @version, _parent, child}, context) do
    resolver_state(child, context)
  end

  def resolver_state(%Continuation{input: input, results: results}, context) do
    %{input: input, context: context, results: results}
  end

  @doc false
  @spec effects(t() | nested_t(), [String.t()]) :: [term()]
  def effects({@tag, @version, _input, _results, effects}, order) do
    Enum.flat_map(order, &Map.get(effects, &1, []))
  end

  def effects({@nested_tag, @version, _parent, child}, order), do: effects(child, order)

  def effects(%Continuation{effects: effects}, order) do
    Enum.flat_map(order, &Map.get(effects, &1, []))
  end

  @doc false
  @spec fetch_result(t() | nested_t(), String.t()) :: {:ok, term()} | :error
  def fetch_result({@tag, @version, _input, results, _effects}, name),
    do: Map.fetch(results, name)

  def fetch_result({@nested_tag, @version, _parent, child}, name),
    do: fetch_result(child, name)

  def fetch_result(%Continuation{results: results}, name), do: Map.fetch(results, name)

  @doc false
  @spec effects_for(t() | nested_t(), String.t()) :: [term()]
  def effects_for({@tag, @version, _input, _results, effects}, name),
    do: Map.get(effects, name, [])

  def effects_for({@nested_tag, @version, _parent, child}, name),
    do: effects_for(child, name)

  def effects_for(%Continuation{effects: effects}, name), do: Map.get(effects, name, [])

  @doc false
  @spec complete_nested(nested_t(), String.t(), term(), [term()]) :: t() | nested_t()
  def complete_nested(frame, name, value, requests),
    do: complete_nested(frame, name, value, requests, store(frame))

  @doc """
  Applies a nested child's output to its parent level. Only the parent's local
  record is read and updated; the child frame is discarded.
  """
  @spec complete_nested(nested_t(), String.t(), term(), [term()], term()) :: t() | nested_t()
  def complete_nested({@nested_tag, @version, parent, _child}, name, value, requests, _store) do
    put_result(parent, name, value, requests)
  end

  def complete_nested(
        %Continuation{parent_id: parent_id, store: store} = _frame,
        name,
        value,
        requests,
        store
      )
      when not is_nil(parent_id) do
    local =
      case store_module(store).get(store, parent_id) do
        {:ok, local} -> local
        :error -> raise ArgumentError, "missing parent continuation record: #{inspect(parent_id)}"
      end

    local = %{
      local
      | results: Map.put(Map.fetch!(local, :results), name, value),
        effects: Map.put(Map.fetch!(local, :effects), name, requests)
    }

    id = store_module(store).new_id()
    :ok = store_module(store).put(store, id, local)
    Continuation.from_local(id, store, local)
  end

  def complete_nested(%Continuation{}, _name, _value, _requests, _store) do
    raise ArgumentError, "nested continuation has no parent frame"
  end

  @doc """
  Rebuilds the legacy nested tuple by walking parent ids. This is the only
  operation that walks the ancestor chain; normal orchestration does not call it.
  """
  @spec materialize(t() | nested_t(), term()) :: t() | nested_t()
  def materialize(frame, store \\ nil)

  def materialize(%Continuation{} = frame, store) do
    store = store || frame.store
    materialize_local(Continuation.local(frame), store)
  end

  def materialize({@nested_tag, @version, parent, child}, store) do
    {@nested_tag, @version, materialize(parent, store), materialize(child, store)}
  end

  def materialize({@tag, @version, _input, _results, _effects} = frame, _store), do: frame

  defp materialize_local(local, store) do
    child = {@tag, @version, local.input, local.results, local.effects}

    case local.parent_id do
      nil ->
        child

      parent_id ->
        case store_module(store).get(store, parent_id) do
          {:ok, parent_local} ->
            {@nested_tag, @version, materialize_local(parent_local, store), child}

          :error ->
            raise ArgumentError, "missing parent continuation record: #{inspect(parent_id)}"
        end
    end
  end

  defp store(%Continuation{store: store}), do: store
  defp store(_frame), do: nil

  defp store_module(%{__struct__: module}), do: module

  defp merge_pair(
         {@tag, @version, input, left_results, left_effects},
         {@tag, @version, input, right_results, right_effects}
       ) do
    with {:ok, results} <- merge_equal(left_results, right_results, :result),
         {:ok, effects} <- merge_equal(left_effects, right_effects, :effects) do
      {:ok, {@tag, @version, input, results, effects}}
    end
  end

  defp merge_pair(
         %Continuation{
           parent_id: parent_id,
           input: input,
           store: store,
           results: left_results,
           effects: left_effects
         },
         %Continuation{
           parent_id: parent_id,
           input: input,
           results: right_results,
           effects: right_effects
         }
       ) do
    with {:ok, results} <- merge_equal(left_results, right_results, :result),
         {:ok, effects} <- merge_equal(left_effects, right_effects, :effects) do
      local = %{parent_id: parent_id, input: input, results: results, effects: effects}
      new_id = store_module(store).new_id()
      :ok = store_module(store).put(store, new_id, local)
      {:ok, Continuation.from_local(new_id, store, local)}
    end
  end

  defp merge_pair(
         {@nested_tag, @version, parent, left_child},
         {@nested_tag, @version, parent, right_child}
       ) do
    with {:ok, child} <- merge_pair(left_child, right_child) do
      {:ok, {@nested_tag, @version, parent, child}}
    end
  end

  defp merge_pair(_left, _right) do
    {:error,
     Error.execution_error("Flow branches do not share one input", %{
       reason: :incompatible_flow_frames
     })}
  end

  defp merge_equal(left, right, kind) do
    Enum.reduce_while(right, {:ok, left}, fn {key, value}, {:ok, merged} ->
      case Map.fetch(merged, key) do
        :error ->
          {:cont, {:ok, Map.put(merged, key, value)}}

        {:ok, ^value} ->
          {:cont, {:ok, merged}}

        {:ok, other} ->
          {:halt,
           {:error,
            Error.execution_error("Flow branches contain conflicting data", %{
              reason: :conflicting_flow_frame,
              kind: kind,
              component: key,
              left: other,
              right: value
            })}}
      end
    end)
  end
end
