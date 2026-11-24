# map — repark-core/src/pre_execute

## Purpose

File-backed tests for the shared pre-execute belt (`../pre_execute.rs`): the ONE choke point
every door's planned statement passes through before it executes (Z-2 / Z-3); and `setop_all.rs`,
the multiset set-operation rewrite and the `MINUS ALL` spelling.

## Contents

- `tests.rs` — the belt's sequencing contract: `plan` does not execute (with the
  `SessionContext::sql` contrast that shows the eager path publishing a `SELECT … INTO` sink),
  `run` = plan → guard → execute, and `guard` is a no-op outside its scope
  (`#[cfg(test)] mod tests;` in `../pre_execute.rs`).
  **Exception round (2026-10-15, ruling 90(a); walk extended 2026-10-15, rulings 99 (c) and 110):**
  `guard_refuses_an_update_whose_set_value_is_a_scalar_subquery` plans `UPDATE`s over two
  `MemTable`s (a direct scalar subquery under `WHERE true`, an `IN` list, no `WHERE`, a
  subquery with its own `WHERE`, nested parentheses beside a second assignment, a cast inside
  the subquery, `arrow_cast` of a subquery, `1 + (SELECT …)`, `(SELECT …) + 1`, `-(SELECT …)`,
  `abs((SELECT …))`, `CASE … THEN (SELECT …) END`, `COALESCE((SELECT …), 0)`, and a
  `named_struct` built around a subquery) and holds `guard` to the refusal text;
  `guard_passes_an_update_whose_set_value_has_no_scalar_subquery` holds it silent for a
  literal, an expression of the column, a subquery in the `WHERE` only, and `arrow_cast` of
  the column. The walk is `Expr::contains_scalar_subquery` over the whole assigned expression.
  **SCALAR-SUBQUERY-EMPTY-NULL-1 (2026-11-23):** `execute_wraps_empty_scalar_subquery_in_prepare_and_create_view`
  pins `PreExecute::execute`'s off-list wrap of `Statement::Prepare` and `DdlStatement::CreateView`
  inputs before DataFusion's bare optimizer and `apply_type_coercion`.
- `setop_all.rs` — **SQL-SETOP-ALL-MULTISET-1 (2026-11-23, owner ruling 2026-11-22; it retires
  SQL-SETOP-ALL-REFUSE-1):** `MultisetSetOperations` (`setop_all_multiset`), the analyzer rule
  `insert_multiset_set_operations` seats right after `struct_setop_names` (every core session and the
  `F.expr` context). DataFusion plans `INTERSECT ALL` / `EXCEPT ALL` as a left-semi / left-anti join
  with null-safe keys on every column and no left `DISTINCT`; the rule finds that shape
  (`multiset_join`, the signature `struct_setop.rs` and `decimal_float_widen.rs` read) and replaces it
  with Spark's own rewrite: each side projected to its coerced keys and a count tag, a `UNION ALL`, an
  aggregate of the per-side counts, and `range(0, copies)` (the smaller count, or left minus right
  floored at zero) under an `Unnest`, with no filter, under a projection that keeps the join's
  schema; `range` builds one list of `copies` per key, so allocation is O(max copies per key).
  Delta 2: a top-level `DataType::Null` output column emits `lit(ScalarValue::Null)` (matching the
  distinct twin's `Null` schema instead of failing to cast back); any non-lossless cast back from
  coerced key type to output type (`is_lossless_round_trip`) refuses loudly during analysis with
  `DataFusionError::Plan` naming `SETOP-MIXED-TYPE-COERCION-1` (before any write). The distinct forms
  and joins written as joins do not match. `minus_all_as_except_all` (re-exported at the crate root)
  uses `sqlparser` (`DatabricksDialect`) to respell only `SetExpr::SetOperation { Minus, All }`
  positions as `EXCEPT` (preserving comments and spacing, leaving identifiers, aliases, literals and
  unparsable SQL untouched), on the Spark door only (the Databricks parser); the native door refuses
  `MINUS ALL` as it refuses `MINUS` (owner ruling: Trino, then DuckDB, then refuse). Delta 3:
  `resolve_ddl_dml_fallback` also accepts `VALUES`, `TABLE`, and any depth-0 `(` trailing query
  suffix (such as `BY NAME (SELECT …)`), and `SparkDialect` runs `minus_all_as_except_all` after
  Spark normalizers (`canonicalize_verbatim`, `time_travel`, `rewrite_sql_for_execute`,
  `strip_schema_evolution`) across statements, view bodies, `filter` strings, and `F.expr`; non-owning
  dialects still run it at `ReparkSession::sql_with_write_options_inner`. The distinct `MINUS` is
  untouched (SQL-MINUS-REFUSE-1).
- [setop_all/](setop_all/map.md) — its tests.

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
