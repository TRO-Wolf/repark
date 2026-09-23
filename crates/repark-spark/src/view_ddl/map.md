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
  the `ALTER VIEW … AS` refusal shape, `SHOW VIEWS [IN ns] [LIKE]`,
  and **PR3 (2026-09-22)** `try_parse_alter_view` for `ALTER VIEW` …
  `SET TBLPROPERTIES`, `UNSET TBLPROPERTIES [IF EXISTS]` and `RENAME TO`
  (anything else after the name, `AS` included, stays `None`; a recognised
  verb with a malformed tail is `Some(Err)`). TEMPORARY forms never match.
  Unit tests per form. The facade still prefixes one-part targets with
  `/* repark:bare-name */`; the engine never reads the mark — it only
  records that the user spelled the name bare (V-001, 2026-09-22).
- `execute.rs` — `execute_create_view` (name completion, body prepare + plan
  for the output schema, service call), `execute_drop_view`,
  `execute_show_views` (`namespace`/`viewName`/`isTemporary` rows, LIKE
  filter), `execute_alter_view` (**PR3:** `load_view` first; `ViewNotFound` and
  `FeatureUnsupported` both count as no view; SET/UNSET answer
  `UNSUPPORTED_FEATURE.CATALOG_OPERATION` for a missing view AND a table
  alike, UNSET refuses the first absent key in statement order unless
  `IF EXISTS`; RENAME refuses a cross-catalog target, answers
  `TABLE_OR_VIEW_NOT_FOUND`/`42P01` for a missing name, redirects a table
  name to ALTER TABLE, and `VIEW_ALREADY_EXISTS` on collision),
  `refuse_view_write_target` (INSERT/DELETE/UPDATE/BY NAME guard,
  fail-closed since R2: metadata-table writes refuse up front, `is_view` is a
  `Result`, and names that cannot be views (branch selectors) fall through to
  the table path). pins: ice-views-1/C-017
  Name completion for CREATE, DROP, ALTER, RENAME targets, and the write guard
  reads `use_ddl::session_defaults(catalogs)`; one-part SHOW VIEWS IN reads its
  catalog from the same defaults. A bare name with no current namespace uses
  `TABLE_OR_VIEW_NOT_FOUND`, as `use_ddl::complete_name` does.
  Unit tests (r2b): `parse.rs` pins every malformed ALTER VIEW tail by its full
  Plan text (several say `could not parse CREATE NAMESPACE`, because the tail reuses the
  namespace property helpers); `execute.rs` pins the commit refusal for a key both set and
  removed.
  The tighten refusal runs unconditionally once `catalog_handle` resolves a
  registered catalog and before `create_or_replace_view` — a bare-name-marked
  target is still a catalog write, and CREATE OR REPLACE shares this site —
  so the refusal precedes any viewless-catalog refusal.
- `parse.rs` + `execute.rs` + the router arm — **PR4 (2026-09-22,
  V-SHOW-TBLPROPERTIES):** `try_parse_show_tblproperties` (quoted, unquoted
  and dotted keys; malformed tails fail loud; other SHOW forms stay `None`),
  the `try_preparse_intercepts` arm after `try_parse_alter_view` that falls
  through on `None`, and `execute_show_tblproperties` /
  `show_tblproperties_batch` (`key`/`value` rows, reserved
  `location`/`provider`/`format-version` then sorted stored properties, keyed
  misses answer Spark's sentence, missing names fail closed with
  `TABLE_OR_VIEW_NOT_FOUND`, tables fall through; viewless catalogs treat
  `FeatureUnsupported` as no view and also reach the table path).
  pins: ice-views-1/C-017
- `read.rs` — `ViewSchemaProvider` (`table` tries inner, then `load_view`,
  and returns a read-only provider planning the stored SQL under the stored
  defaults with aliases applied; `table_names` stays tables-only);
  `ensure_view_wrappers`; time-travel prepare plus stored-namespace
  qualification with CTE shadowing; the 100-deep `VIEW_NESTED_DEPTH_LIMIT`
  guard; non-query bodies refused loud.
- `describe.rs` — **PR2 (2026-09-22, V-DESCRIBE):** `describe_view_frame`
  is the view probe on the `TableNotFound` arm of `execute_describe_table`
  (`../describe_show.rs`): a loaded view answers, `ViewNotFound` and
  `FeatureUnsupported` count as no view and re-arm the
  unchanged `TABLE_OR_VIEW_NOT_FOUND` refusal (fail-closed), every other load
  error propagates through `iceberg_err`. `describe_view_rows` /
  `describe_view_batch` render the stored schema's columns ONLY —
  `spark_ddl_type_name` spellings, a doc-less column renders `""` (the table
  path renders null), no blank/`# Partitioning`/`# Metadata Columns` trailer,
  and EXTENDED is the same columns-only answer. SHOW CREATE stays a later PR.
  pins: ice-views-1/C-017

## Pointers

- Up: [../map.md](../map.md)
- View service: [../../../repark-iceberg/src/view/map.md](../../../repark-iceberg/src/view/map.md)

## Debug

First checks: `cargo test -p repark-spark --lib view_ddl::`. Escalate to:
[../map.md#debug](../map.md).
