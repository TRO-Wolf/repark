# map — repark-core/src/pre_execute

## Purpose

File-backed tests for the shared pre-execute belt (`../pre_execute.rs`): the ONE choke point
every door's planned statement passes through before it executes (Z-2 / Z-3); and `setop_all.rs`,
the statement-text refusal that runs before any door plans.

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
- `setop_all.rs` — **SQL-SETOP-ALL-REFUSE-1 (2026-11-12, owner ruling, signed-zero part B Q2):**
  the statement-level refusal of SQL `INTERSECT ALL`, `EXCEPT ALL` and `MINUS ALL`, which DataFusion
  answers with the wrong multiset (left-semi and left-anti joins without the left `DISTINCT`).
  `multiset_set_operation` tokenizes the text in a sqlparser dialect and finds an unquoted
  `INTERSECT`, `EXCEPT` or `MINUS` word followed by an unquoted `ALL`, whitespace and comments
  between; string literals, comments and quoted identifiers never match, and a text that does not
  tokenize is left to the door's parser. A byte pre-check skips the tokenizer unless the text holds
  `all` and an operator word. `refuse_in_session_sql` is the seat both SQL doors pass: the first
  line of `ReparkSession::sql_with_write_options_inner` (`../session/write_options.rs`), before the
  session's `SqlDialect`, in the session's `sql_parser.dialect` (Databricks on the Spark door,
  Generic on `repark.sql()`), so nothing is planned or written; it answers
  `Error::NotImplemented` (`UnsupportedOperationException`). `refuse_multiset_set_operation`
  (re-exported at the crate root) is the same check as an `UnsupportedMarker` DataFusion error,
  which `engine_err` maps to the same text; the Spark door calls it for SQL fragments
  (`refuse_sql_fragment`) and stored view bodies (`view_ddl/read.rs`). The text names the
  operator, the distinct form (`EXCEPT` for `MINUS ALL`), the `row_number()` workaround and the card
  SQL-SETOP-ALL-MULTISET-1, in the DataFrame `intersectAll` refusal's words.
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
