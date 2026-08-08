# map — repark-core/src/runtime

## Purpose

File-backed tests for the executor-handle type (`../runtime.rs`): `EngineRuntime`, the name the
engine gives the **embedding's** Tokio runtime. Landed additively by phase-3 PR-3 (EC-5;
`docs/design/python-facade.md` §3, §4 Q7), honoring the phase-1 omissions ledger's recorded
resolution that the type "becomes engine API the day the binding ports". New-seam tests —
additive, not part of the ported v1 census.

## Contents

- `tests.rs` — `engine_runtime_clone_shares_one_executor_and_drives_futures`: cloning the handle
  is an `Arc` clone (one executor, not one per clone) and `block_on` drives a future on it
  (`#[cfg(test)] mod tests;` in `../runtime.rs`).

## Pointers

- Up: [../map.md](../map.md)
- The INSTANCE (a process-wide `OnceLock<EngineRuntime>`) lives in the embedding:
  [../../../repark-python/src/map.md](../../../repark-python/src/map.md), whose
  `sequential_sessions_share_one_tokio_runtime` pin observes the same sharing property one layer
  up.
- Design: [../../../../docs/design/python-facade.md](../../../../docs/design/python-facade.md)
  §4 Q7 (type in core, instance in the binding).

## Debug

| Symptom | First check |
|---|---|
| "core builds a runtime" suspicion | It does not — `runtime.rs` has no `Runtime::new` and no `Default`; the only constructor takes an `Arc<Runtime>` the embedder already owns. |
| Two sessions do not share one executor | The embedder minted two handles from two runtimes; the sharing is a property of the `Arc`, not of this type — check the embedding's `OnceLock`. |
| A blocking call appears inside core | `block_on` here is called BY the embedding through the handle; no `repark-core` entry point calls it. |

First checks: `cargo test -p repark-core runtime`. Escalate to: [../map.md#debug](../map.md).
