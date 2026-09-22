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
  TEMPORARY forms never match (PR3). Unit tests per form. The facade still
  prefixes one-part targets with `/* repark:bare-name */`; the engine never
  reads the mark — it only records that the user spelled the name bare
  (V-001, 2026-09-22).
- `execute.rs` — `execute_create_view` (name completion, body prepare + plan
  for the output schema, service call), `execute_drop_view`,
  `execute_show_views` (`namespace`/`viewName`/`isTemporary` rows, LIKE
  filter), `refuse_view_write_target` (INSERT/DELETE/UPDATE/BY NAME guard,
  fail-closed since R2: metadata-table writes refuse up front, `is_view` is a
  `Result`, and names that cannot be views (branch selectors) fall through to
  the table path).
  The tighten refusal runs unconditionally once `catalog_handle` resolves a
  registered catalog and before `create_or_replace_view` — a bare-name-marked
  target is still a catalog write, and CREATE OR REPLACE shares this site —
  so the refusal precedes any viewless-catalog refusal.
- `read.rs` — `ViewSchemaProvider` (`table` tries inner, then `load_view`,
  and returns a read-only provider planning the stored SQL under the stored
  defaults with aliases applied; `table_names` stays tables-only);
  `ensure_view_wrappers`; time-travel prepare plus stored-namespace
  qualification with CTE shadowing; the 100-deep `VIEW_NESTED_DEPTH_LIMIT`
  guard; non-query bodies refused loud.
- `describe.rs` — **PR2 (2026-09-22, V-DESCRIBE):** `describe_view_frame`
  is the view probe on the `TableNotFound` arm of `execute_describe_table`
  (`../describe_show.rs`): a loaded view answers, `ViewNotFound` re-arms the
  unchanged `TABLE_OR_VIEW_NOT_FOUND` refusal (fail-closed), every other load
  error propagates through `iceberg_err`. `describe_view_rows` /
  `describe_view_batch` render the stored schema's columns ONLY —
  `spark_ddl_type_name` spellings, a doc-less column renders `""` (the table
  path renders null), no blank/`# Partitioning`/`# Metadata Columns` trailer,
  and EXTENDED is the same columns-only answer. SHOW CREATE /
  SHOW TBLPROPERTIES / ALTER VIEW stay later PRs.
  pins: ice-views-1/C-017

## Pointers

- Up: [../map.md](../map.md)
- View service: [../../../repark-iceberg/src/view/map.md](../../../repark-iceberg/src/view/map.md)

## Debug

First checks: `cargo test -p repark-spark --lib view_ddl::`. Escalate to:
[../map.md#debug](../map.md).
