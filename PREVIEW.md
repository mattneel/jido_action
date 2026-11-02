# Experimental preview: normalized Jido state and managed continuation

This branch is a preview of an isolated research integration. It is not
upstream-ready and has no upstream PR.

Base: `177e7a4ce4342a9a6b7bc668f540dafbd7169d54` (`agentjido/jido_action` main)

## What is here

- Opt-in normalized Runic state for immediate and managed Jido execution:
  `state_backend: :owner_map | :graph_ets` and
  `frame_mode: :full | :continuation`.
- Parent-ID/local-delta continuations with append-only frame versions and
  branch isolation; no embedded parent frame term.
- `Jido.Exec.Frame.Store.OwnerMap`: process-owned frame records for
  OwnerMap. `Jido.Exec.Frame.Store.Ets` remains the ETS route. Both expose
  stable 128-bit resource IDs and portable catalogs for snapshots.
- Managed `Jido.Exec.start/6` and `resume/4` accept normalized options;
  checkpoint/restart restores state and continuation resources.
- Durable outputs reject process-local user data while allowing internal
  continuation frames whose stores are externalized by the snapshot layer.

## Companion dependency

This branch needs the Runic preview branch:

`mattneel/runic` branch `exp/runic-normalized-state-20261102`

For local review, point the `:runic` dependency at that checkout, for example:

```elixir
{:runic, path: "../runic"}
```

`mix.exs` in this preview intentionally retains the upstream dependency
declaration. No hex release or upstream PR is implied.

## State

This is a review checkpoint, not a completed or production-ready branch.
Targeted tests are green; full validation and real offline ReAct evidence are
still in progress in the experiment checkout.
