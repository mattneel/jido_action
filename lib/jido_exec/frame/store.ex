defmodule Jido.Exec.Frame.Store do
  @moduledoc """
  Behaviour for normalized Flow continuity records.

  A store maps an opaque frame id to the local level of a continuation:
  `%{parent_id: id | nil, input: term, results: map, effects: map}`. It never
  stores a parent term under a child; parent links are ids.

  Implementations are runtime-scoped. `Ets` is the reference implementation
  used by the integration path; it is not a durable store.
  """

  @type id :: term()
  @type local :: %{
          parent_id: id() | nil,
          input: term(),
          results: map(),
          effects: map()
        }
  @type handle :: term()

  @callback new() :: handle()
  @callback put(handle(), id(), local()) :: :ok
  @callback get(handle(), id()) :: {:ok, local()} | :error
  @callback delete(handle(), id()) :: :ok
  @callback release(handle()) :: :ok
  @callback size(handle()) :: non_neg_integer()
  @callback reap(handle(), [id()]) :: {:ok, non_neg_integer()}
  @callback new_id() :: id()
end
