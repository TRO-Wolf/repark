# map — repark-spark/src/view_ddl

## Purpose

Spark-door view DDL: the pre-parse seam that intercepts view statements before
DataFusion mis-routes them, the execution path over the `repark-iceberg` view
service, the wrapper-based read path that expands stored SQL per query, and
(IPI-40 PR6) session temporary views: the SQL `CREATE [OR REPLACE] TEMPORARY
VIEW` door and the temp-first DROP / DESCRIBE / SHOW VIEWS answers.

## Contents

- `mod.rs` — module wiring: `parse` / `execute` / `read` / `describe` / `show_create`.
- `mod.rs` — module wiring: `parse` / `execute` / `read` / `describe` /
  `temp_parse` / `temp_ddl` / `temp_view`.
- `parse.rs` — grammar only: `CREATE [OR REPLACE] [IF NOT EXISTS] VIEW` with
  alias/COMMENT/TBLPROPERTIES forms and verbatim body capture,
  `is_create_view_statement` (the durable head sniff the router skip uses),
  the `ALTER VIEW … AS` refusal shape, `SHOW VIEWS [IN ns] [LIKE]`,
  and **PR3 (2026-09-22)** `try_parse_alter_view` for `ALTER VIEW` …
  `SET TBLPROPERTIES`, `UNSET TBLPROPERTIES [IF EXISTS]` and `RENAME TO`
  (anything else after the name, `AS` included, stays `None`; a recognised
  verb with a malformed tail is `Some(Err)`). TEMPORARY forms never match
  here — they are `temp_parse.rs`'s. The helpers both parsers share
  (`unquoted_head_words`, `consume_head_word`, `split_view_statement`,
  `parse_view_clauses`) are `pub(super)`. **PR6a2:** a bare `SHOW VIEWS
  [LIKE]` parses with an empty namespace (the old "requires an explicit
  namespace" refusal is gone; the executor answers the current namespace).
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
  **IPI-40 PR6:** `route_create_temp_view` / `execute_create_temp_view` plan
  the temp body under the session defaults, refuse
  TEMP_TABLE_OR_VIEW_ALREADY_EXISTS for a plain CREATE on an existing name,
  check RECURSIVE_VIEW and register through `TempViewSession` (a session-less
  door refuses with `NO_TEMP_VIEW_HOME`); `execute_show_views_with` lists
  catalog rows then temp rows `["", name, true]`, LIKE on both, and a bare
  SHOW VIEWS reads the current namespace (`show_view_rows_batch`).
  pins: ice-views-1/C-018
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
  and dotted keys; a quoted key ends at the string; a tail that tokenizes but
  does not parse refuses with the parser's `Plan` text; a tail the tokenizer
  rejects, such as an unclosed backtick, returns `None` and falls through to
  the ordinary path, which answers the tokenizer error; on the Spark SQL
  front door an unterminated quote is rejected earlier, by the literal
  canonicalizer; other SHOW forms stay `None`),
  the `try_preparse_intercepts` arm after `try_parse_alter_view` that falls
  through on `None`, and `execute_show_tblproperties` /
  `show_tblproperties_batch` (`key`/`value` rows, reserved
  `location`/`provider`/`format-version` then sorted stored properties; the
  stored `comment` is hidden from the listing and from a keyed lookup, as in
  Spark (PR5 r2, p5 `vc.props`), keyed
  misses answer Spark's sentence, missing names fail closed with
  `TABLE_OR_VIEW_NOT_FOUND`, tables fall through; viewless catalogs treat
  `FeatureUnsupported` as no view and take the same table/missing split).
  **PR4 r2 (2026-09-23):** bare and two-part names complete through
  `complete_view_name(catalogs, …)` from `use_ddl::session_defaults`, so
  they follow `USE` like ALTER VIEW.
  pins: ice-views-1/C-017
- `temp_parse.rs` — **IPI-40 PR6** grammar for `CREATE [OR REPLACE] [GLOBAL]
  TEMP|TEMPORARY VIEW`: `try_parse_create_temp_view` (verbatim body, column
  aliases with COMMENT, view COMMENT accepted) and the Spark-measured parse
  refusals as ParseException-class `DataFusionError::SQL` (`spark_parse_error`):
  PARSE_SYNTAX_ERROR for a body that does not open a query, the legacy IF NOT
  EXISTS / OR REPLACE + IF NOT EXISTS / TBLPROPERTIES texts,
  TEMP_VIEW_NAME_TOO_MANY_NAME_PARTS (two parts) and
  IDENTIFIER_TOO_MANY_NAME_PARTS (three or more); GLOBAL refuses as
  `NotImplemented` (`GLOBAL_TEMP_VIEW_REFUSAL`). Split out of `parse.rs`
  after the PR4 rebase (PR6b1). pins: ice-views-1/C-018
- `temp_ddl.rs` — **IPI-40 PR6** the session dispatcher
  `route_temp_view_statement`, called from `router.rs::execute_in_session`
  after canonicalization: CREATE TEMPORARY VIEW always; with a session
  attached, DROP TABLE / DROP VIEW (any IF EXISTS / PURGE form) of a
  registered temp name — a bare name or the temp HOME-qualified name the
  facade emits — drops it through `TempViewSession::drop_temp_view`, so
  one-part names are temp-first; DESCRIBE [TABLE] [EXTENDED] of a temp view
  answers Spark rows with an Arrow NULL comment (alias COMMENT clauses kept); SHOW
  VIEWS appends the session's temp rows. Every other target falls through to
  the catalog arms unchanged. pins: ice-views-1/C-018
- `temp_view.rs` — **IPI-40 PR6** `ReplanningTempView`, the provider behind
  a SQL temp view (registered through `create_or_replace_temp_view_from`): it
  re-plans the stored body at every scan under the creation-time catalog and
  namespace and the temp names captured at CREATE, conforms the answer to the
  creation columns by name (Spark's up-cast widening, else
  INCOMPATIBLE_VIEW_SCHEMA_CHANGE / CANNOT_UP_CAST_DATATYPE), answers
  TABLE_OR_VIEW_NOT_FOUND for a dropped temp dependency, shares the session's
  nested-view counter and refuses the 101st nested temp view with Spark's
  `VIEW_EXCEED_MAX_NESTED_DEPTH` (54K00) naming that view, and refuses CREATE OR
  REPLACE cycles with RECURSIVE_VIEW (the cycle walk keeps a 4096-view budget,
  refused with `VIEW_NESTED_DEPTH_LIMIT`); the direct-reference walk is one pass,
  because `LogicalPlanBuilder::scan` (DataFusion 54.1) inlines a view provider
  that carries a plan whenever no filters are attached, which is how the SQL
  planner builds scans; `temp_view_column_comments` feeds DESCRIBE. PR6b3b module unit tests pin
  the cycle-walk budget and the inlined-reference walk.
  pins: ice-views-1/C-018
- `read.rs` — `ViewSchemaProvider` (`table` tries inner, then `load_view`,
  and returns a read-only provider planning the stored SQL under the stored
  defaults with aliases applied; `table_names` stays tables-only);
  `ensure_view_wrappers`; time-travel prepare plus stored-namespace
  qualification with CTE shadowing; the 100-deep `VIEW_NESTED_DEPTH_LIMIT`
  guard; non-query bodies refused loud. **PR6:** `prepare_view_body_sql_with`
  takes a `TempHomes` source (none for durable views, the live session at
  temp-view CREATE, the captured map at re-plan) so bare temp names qualify
  to their home first; `refuse_write_query_body` answers PARSE_SYNTAX_ERROR
  for a `WITH … INSERT/UPDATE/DELETE/MERGE` temp body; `nested_depth_refusal`
  is shared with `temp_view.rs`.
- `describe.rs` — **PR2 (2026-09-22, V-DESCRIBE):** `describe_view_frame`
  is the view probe on the `TableNotFound` arm of `execute_describe_table`
  (`../describe_show.rs`): a loaded view answers, `ViewNotFound` and
  `FeatureUnsupported` count as no view and re-arm the
  unchanged `TABLE_OR_VIEW_NOT_FOUND` refusal (fail-closed), every other load
  error propagates through `iceberg_err`. `describe_view_rows` /
  `describe_view_batch` render the stored schema's columns ONLY —
  `spark_ddl_type_name` spellings, a doc-less column renders `""` (the table
  path renders null), no blank/`# Partitioning`/`# Metadata Columns` trailer,
  and EXTENDED is the same columns-only answer; any supplied column refuses
  with Spark's `UNRESOLVED_COLUMN.WITHOUT_SUGGESTION`.
  pins: ice-views-1/C-017
- `show_create.rs` — **PR5 (2026-09-24, V-SHOW-CREATE):**
  `execute_show_create_view` is the `Ok(true)` arm of `try_show_create_intercept`
  (`../show_create.rs`; `AS SERDE` on a view keeps main's fall-through). It
  loads the view with `load_view`; `ViewNotFound` and `FeatureUnsupported`
  answer `TABLE_OR_VIEW_NOT_FOUND`, and other load errors propagate through
  `iceberg_err`. `render_create_view` builds Spark's `CREATE VIEW
  <catalog>.<ns>.<view> (` column list, adding `COMMENT` for a documented column,
  then a `COMMENT` line when the view has a `comment` property. Its
  `TBLPROPERTIES` are `execute.rs`'s `show_tblproperties_rows` (which already
  hides `comment`), sorted by key, rendered through `render_tblproperties_clause`.
  The body is the stored SQL from `view_read_spec`, verbatim. A version with no
  SQL representation refuses with that function's `Plan` error, and an unregistered
  catalog refuses with `catalog_handle`'s. **r2 (2026-09-24):** the renderer has
  no property filter of its own. Residues: D-VIEW-SHOWCREATE-1.
  pins: ice-views-1/C-017
  and EXTENDED is the same columns-only answer. SHOW CREATE stays a later PR.
  **PR6a2:** `describe_rows_batch` is the shared batch builder the temp-view
  DESCRIBE in `temp_ddl.rs` reuses. pins: ice-views-1/C-017

## Pointers

- Up: [../map.md](../map.md)
- View service: [../../../repark-iceberg/src/view/map.md](../../../repark-iceberg/src/view/map.md)

## Debug

First checks: `cargo test -p repark-spark --lib view_ddl::`. Escalate to:
[../map.md#debug](../map.md).
