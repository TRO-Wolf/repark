# map — repark-spark/src/view_ddl

## Purpose

Spark-door view DDL: the pre-parse seam that intercepts view statements before
DataFusion mis-routes them, the execution path over the `repark-iceberg` view
service, and the wrapper-based read path that expands stored SQL per query.

## Contents

- `mod.rs` — module wiring: `parse` / `execute` / `read` / `describe`.
- `parse.rs` — grammar only: `CREATE [OR REPLACE] [IF NOT EXISTS] VIEW` with
  alias/COMMENT/TBLPROPERTIES forms and verbatim body capture,
  `is_create_view_statement` (the durable head sniff the router skip uses),
  the `ALTER VIEW … AS` refusal shape, `SHOW VIEWS [IN ns] [LIKE]`.
  TEMPORARY forms never match (PR3). Unit tests per form.
  Also `sql_has_bare_name_mark`: the `/* repark:bare-name */` leading-trivia
  mark the facade emits for one-part targets. The scan stays in leading
  trivia, so the mark in a body or inside another comment never matches.
- `execute.rs` — `execute_create_view` (name completion, body prepare + plan
  for the output schema, service call), `execute_drop_view`,
  `execute_show_views` (`namespace`/`viewName`/`isTemporary` rows, LIKE
  filter), `refuse_view_write_target` (INSERT/DELETE/UPDATE/BY NAME guard).
  A marked statement skips the tighten calibration (the session-scoped
  allowance for bare names); an unmarked one enforces it before the catalog
  write, so the refusal precedes any viewless-catalog refusal.
- `read.rs` — `ViewSchemaProvider` (`table` tries inner, then `load_view`,
  and returns a read-only provider planning the stored SQL under the stored
  defaults with aliases applied; `table_names` stays tables-only);
  `ensure_view_wrappers`; time-travel prepare plus stored-namespace
  qualification with CTE shadowing; the 100-deep `VIEW_NESTED_DEPTH_LIMIT`
  guard; non-query bodies refused loud.
- `describe.rs` — stub module this PR; PR2 owns DESCRIBE / SHOW CREATE /
  SHOW TBLPROPERTIES / ALTER VIEW.
  pins: ice-views-1/C-007, C-008, C-016

## Pointers

- Up: [../map.md](../map.md)
- View service: [../../../repark-iceberg/src/view/map.md](../../../repark-iceberg/src/view/map.md)

## Debug

First checks: `cargo test -p repark-spark --lib view_ddl::`. Escalate to:
[../map.md#debug](../map.md).
