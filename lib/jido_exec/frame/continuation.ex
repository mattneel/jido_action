defmodule Jido.Exec.Frame.Continuation do
  @moduledoc """
  Opt-in normalized nested Flow frame.

  The continuation stores only its own frame id, a parent **id**, and its local
  input/results/effects. It never embeds a parent frame term. Parent levels
  live in a `Jido.Exec.Frame.Store`; `Jido.Exec.Frame.materialize/2` walks ids
  only at a boundary that explicitly requires the legacy nested tuple.

  Default Flow execution remains the tuple representation. The continuation is
  selected with `frame_mode: :continuation`.
  """

  @enforce_keys [:id, :input, :store]
  defstruct [:id, :parent_id, :input, :store, results: %{}, effects: %{}]

  @type t :: %__MODULE__{
          id: term(),
          parent_id: term() | nil,
          input: term(),
          store: term(),
          results: map(),
          effects: map()
        }

  @doc false
  def local(%__MODULE__{} = frame) do
    %{
      parent_id: frame.parent_id,
      input: frame.input,
      results: frame.results,
      effects: frame.effects
    }
  end

  @doc false
  def from_local(id, store, local) do
    %__MODULE__{
      id: id,
      parent_id: Map.fetch!(local, :parent_id),
      input: Map.fetch!(local, :input),
      results: Map.fetch!(local, :results),
      effects: Map.fetch!(local, :effects),
      store: store
    }
  end

  @doc false
  def put(%__MODULE__{} = frame) do
    :ok = apply(frame.store.__struct__, :put, [frame.store, frame.id, local(frame)])
    frame
  end
end

defimpl Runic.Identity.Projectable, for: Jido.Exec.Frame.Continuation do
  def identity_document(frame) do
    %{
      codec: :jido_frame_continuation,
      version: 1,
      id: inspect(frame.id),
      parent_id: inspect(frame.parent_id)
    }
  end
end
