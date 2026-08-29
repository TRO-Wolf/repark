# map — repark-core/src/pre_execute

## Purpose

File-backed tests for the shared pre-execute belt (`../pre_execute.rs`): the ONE choke point
every door's planned statement passes through before it executes (Z-2 / Z-3).

## Contents

- `tests.rs` — the belt's sequencing contract: `plan` does not execute (with the
  `SessionContext::sql` contrast that shows the eager path publishing a `SELECT … INTO` sink),
  `run` = plan → guard → execute, and `guard` is a no-op outside its scope
  (`#[cfg(test)] mod tests;` in `../pre_execute.rs`).

## Pointers

- Up: [../map.md](../map.md)
- The refusal the guard runs: `../sorted_view.rs`
  (`refuse_iceberg_create_of_tightened_ddl`, resolved-catalog gated).

## Debug

| Symptom | First check |
|---|---|
| A door still persists a tightened `CREATE VIEW` / `SELECT … INTO` | That door does not call `PreExecute::guard` on its planned statement — wire it to the belt, do not add a fourth private copy of the refuse. |
| A write happens before a refusal fires | The call site used `SessionContext::sql` (plans AND executes). Use `PreExecute::plan` → `guard` → `execute`. |
| A statement outside the tighten scope is refused | The guard gates on the RESOLVED catalog (`datafusion.catalog.default_catalog` / `default_schema`); check what the target name resolves to. |

First checks: `cargo test -p repark-core pre_execute`. Escalate to: [../map.md#debug](../map.md).
