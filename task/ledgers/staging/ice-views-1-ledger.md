# Unit ledger — ICE-VIEWS-1 · Iceberg catalog views, Spark door

**Date:** 2026-09-20 · **Branch:** `fix/ipi-40-views-1` · **Base:** `ed15699b`
**Model:** muse-spark-1.3-contributor · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the unit's last PR lands.

**Why now.** IPI-40: RePark mis-routes SQL views — `CREATE VIEW` falls through the
router into DataFusion's `CreateView` (`register_table` error), `SHOW VIEWS` is
refused, nothing reaches the owned fork's complete view API. Packet
`/tmp/oc-worker/plan-packets/ipi-40-views.md` (13 cells + A-12..A-15). WO-1 (this
PR): the catalog door and the read path on the memory catalog. PR2 owns
DESCRIBE / SHOW CREATE / SHOW TBLPROPERTIES / ALTER VIEW. PR3 owns SQL
temporary views and the dbt adapter.

**Not in this unit:** fork changes, pin bumps, `Cargo.toml`, `Cargo.lock`,
`SHOW TABLES IN` (ST-1), `STATUS.md`, version bumps, tags, AWS commands.

## PROPOSITION LEDGER — ICE-VIEWS-1 — 2026-09-20

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | A created view stores the user SQL verbatim as `Sql(dialect "spark")`, the resolved output schema, session default catalog/namespace, and `location = {warehouse}/{ns}/{view}` with no trailing slash. | `view/tests.rs` store test; metadata-JSON readback in the battery. | PROVEN | `create_view_stores_sql_schema_defaults_and_location`; `test_view_version_log_after_replace` reads the stored body; probe stored `'SELECT id, data FROM t WHERE id > 0'` byte-identical. |
| C-002 | `CREATE OR REPLACE` on an existing view routes through `replace_version()`: the version log grows, current moves, history is kept. | Rust replace test plus the JSON pin. | PROVEN | `create_or_replace_adds_a_version_and_moves_current` (order-robust: current differs from the pre-replace id); `test_view_version_log_after_replace`. |
| C-003 | View statements in a missing namespace report schema-not-found, never a bare catalog error. | Rust missing-namespace tests. | PROVEN | `list_views_reports_schema_not_found_for_a_missing_namespace`, `create_view_in_a_missing_namespace_reports_schema_not_found`. |
| C-004 | `CREATE VIEW IF NOT EXISTS` on an existing view is a noop that keeps the body. | Rust noop test plus end-to-end rows. | PROVEN | `create_view_if_not_exists_is_a_noop_that_keeps_the_body`; `test_create_view_if_not_exists_is_noop` still reads `[[1],[2]]`. |
| C-005 | On a viewless catalog, CREATE/CREATE OR REPLACE refuse with Spark's `Creating/Replacing a view is not supported by catalog: <name>` (UnsupportedOperationException, null condition, no SQLSTATE, no wrapper), SHOW VIEWS returns empty, DROP VIEW reports VIEW_NOT_FOUND — the refusal is driven off fork `FeatureUnsupported` from the mutating `create_view`/`update_view` call per statement, never emitted from a `view_exists` probe. | `ViewlessCatalog` Rust tests plus the router SQL-door battery (Python cannot construct a viewless catalog offline: registration eagerly lists namespaces). | PROVEN | Exact-marker downcast pins in `create_view_on_a_viewless_catalog_reports_creating_unsupported` / `replace_view_on_a_viewless_catalog_reports_replacing_unsupported`; `test_views_refuse_on_glue_and_s3tables` drives CREATE, CREATE OR REPLACE (incl. over an existing table) and SHOW VIEWS=[] through the SQL door on `glue`/`s3tables` stubs; `list_views_on_a_viewless_catalog_returns_empty`; `drop_view_on_a_viewless_catalog_reports_view_not_found`. |
| C-006 | An unqualified body name resolves against the view's stored namespace at read time, not the reader's session namespace. | A-14 cross-namespace pin. | PROVEN | `test_view_body_resolves_stored_namespace`: view in `ns1` over bare `t`, read from `ns2`, still reads `ns1.t`. |
| C-007 | Every reachable D-9 row carries Spark's message, SQLSTATE and condition: duplicate CREATE (VIEW_ALREADY_EXISTS/42P07), DROP VIEW missing (VIEW_NOT_FOUND/42P01), DROP TABLE over a view (TABLE_OR_VIEW_NOT_FOUND/42P01, `drop_table` never called), DROP VIEW over a table (VIEW_NOT_FOUND/42P01), INSERT INTO a view (TABLE_OR_VIEW_NOT_FOUND/42P01, never writes). DELETE/UPDATE/BY NAME over a view refuse with the same INSERT-shaped full sentence (declared: D-9 has no row for them; live Spark 4.1.2 answers DELETE with an INTERNAL_ERROR/XX000 plan crash and UPDATE with UNSUPPORTED_FEATURE.TABLE_OPERATION/0A000 naming table `unknown`, neither of which is replicated). | Battery contract test plus catalogue unit tests. | PROVEN | `test_view_error_contract` asserts condition plus the full backticked sentence plus SQLSTATE per row (INSERT/DROP TABLE/DELETE/UPDATE/BY NAME share the sentence), then reads the view back with SELECT to prove DROP TABLE never dropped and INSERT never wrote; `spark_error.rs` condition tests; `execute_drop_table` checks `is_view` before `drop_table`; INSERT/DELETE/UPDATE/BY NAME arms call the fail-closed `refuse_view_write_target`; TRUNCATE keeps its EXPECT_TABLE_NOT_VIEW loud refusal. MERGE INTO a view is measure-only (D-9 has no MERGE row): RePark raises a loud TableNotFound-shaped AnalysisException and writes nothing. |
| C-008 | `ALTER VIEW <name> AS <query>` is refused with Spark's exact sentence and a null condition; it is never executed. | Battery refusal pin. | PROVEN | `test_alter_view_as_refuses`: exact `ALTER VIEW <viewName> AS is not supported. Use CREATE OR REPLACE VIEW instead`, no `[...]` condition token. |
| C-009 | After two distinct CREATE OR REPLACE bodies, the view metadata JSON under `{warehouse}/{ns}/{view}/` holds 2 versions and current points at the second body. | Battery JSON pin, order-robust: the fork serializes `versions` from a HashMap, so positional "second" is read as max id plus version-log tail, never an absolute warehouse path. | PROVEN | `test_view_version_log_after_replace`: len 2, current is max id, current SQL is the second body, version-log tail equals current. |
| C-010 | The `repark-common` catalogue renders the Spark condition/message/SQLSTATE shapes the view errors cite. | Catalogue unit tests. | PROVEN | `spark_error.rs` view-condition tests; duplicate/create-over-table/drop pins assert the rendered text. |
| C-011 | A body carrying `VERSION AS OF` reads through the view: a branch body follows the branch, a snapshot-id body stays pinned across a later INSERT. | Battery branch and snapshot pins. | PROVEN | `test_view_body_with_version_as_of_branch` reads `[[1],[2]]`; `test_view_body_with_version_as_of_snapshot_stays_pinned` still reads `[[1],[2]]` after INSERT adds row 3. The calibrated rewrite chain is skipped for CREATE VIEW so the stored body keeps the clause verbatim instead of a released temp name. |
| C-012 | A view over a view reads through; 100 nested views read and the 101st raises a typed depth error on both the read door and creation validation. | Battery nesting pins. | PROVEN (depth-100 behavior and view-over-view; the `[VIEW_NESTED_DEPTH_LIMIT]` token itself is declared, Spark bytes unmeasured — no p1/p2 probe row records Spark's nested-depth condition or SQLSTATE) | `test_view_over_view`; `test_nested_view_depth_guard` (w50/w99 read, SELECT w100 and CREATE w101 raise). |
| C-013 | DROP NAMESPACE on a view-only namespace refuses with the non-empty error class; no fork change. | Rust guard test plus end-to-end pin. | PROVEN | `namespace_drop.rs` view-only pin; `test_drop_namespace_on_view_only_namespace_refuses` matches "not empty". |
| C-014 | Table listings omit views: `table_names()` stays `list_tables`-only and the working listing surface shows no view. | Battery listing pin. | PROVEN | `test_show_tables_excludes_views`: `listTables("sc.ns")` returns `["t"]` after a view is created. `SHOW TABLES IN` (ST-1) untouched. |
| C-015 | `DROP VIEW IF EXISTS` on a missing view stays a quiet success (V-DROP-IF-EXISTS EQUAL). | Battery pin. | PROVEN | `test_drop_view_if_exists_missing_stays_equal`. |
| C-016 | The door reads: create+select `[[1,"d1"],[2,"d2"]]`, OR REPLACE second wins `[["d0"],["d1"],["d2"]]`, SHOW VIEWS lists `[["ns","vs_…",false]]` with LIKE filtering and empties after DROP. | Battery door pins. | PROVEN | `test_create_view_and_select`, `test_create_or_replace_view_second_wins`, `test_show_views_and_drop`. |
| C-017 | PR2: DESCRIBE / DESCRIBE EXTENDED / SHOW CREATE TABLE / SHOW TBLPROPERTIES on views and ALTER VIEW SET/UNSET/RENAME answer packets M-3..M-6. | Packet §7 V-* pins. | PROVEN | PR2 (V-DESCRIBE, V-DESCRIBE-EXTENDED): `test_ice_views_2_describe.py::test_describe_view_answers_stored_columns`, `::test_describe_view_renders_aliases_and_column_comments`, `::test_describe_extended_view_is_columns_only`; `crates/repark-spark/src/tests/describe_view_routing.rs`. PR3 (V-ALTER-UNSET, D-VIEW-ALTER-PROPS): `test_ice_views_3_alter.py::test_alter_view_unset_tblproperties_cell`, `::test_alter_view_set_tblproperties_and_rename_cell`; `crates/repark-spark/src/tests/alter_view_routing.rs`; residues D-VIEW-ALTER-1. PR4 (V-SHOW-TBLPROPERTIES): `test_ice_views_4_showprops.py::test_show_tblproperties_key_cell`, `::test_no_key_lists_reserved_then_stored_sorted`; `crates/repark-spark/src/tests/show_tblproperties_routing.rs`; residues D-VIEW-SHOWPROPS-1. PR5 (V-SHOW-CREATE, EQUAL to `p2.json` `C.show_create` / `C.v2.show_create`): `test_ice_views_5_showcreate.py::test_show_create_table_on_view`, `::test_show_create_table_on_plain_view`; `crates/repark-spark/src/tests/show_create_view_routing.rs`, `crates/repark-spark/src/view_ddl/show_create.rs` unit tests; unmeasured shapes D-VIEW-SHOWCREATE-1. |
| C-018 | PR3: SQL temporary views (`CREATE [OR REPLACE] TEMPORARY VIEW`, shadowing, `None` DESCRIBE comments) and the dbt `materialized='view'` follow-up. | Packet D-TEMP-VIEW cell plus dbt gold-model updates. | OPEN | Out of WO-1; TEMP forms keep today's refusal. Which macro rows flip? |

## Notes

- A-14's version-log pin is read order-robust (C-009): fork `ViewMetadata`
  serializes `versions` from a `HashMap`, so "current-version-id is the second"
  is pinned as max id plus version-log tail rather than array position.
- The facade expander leaves durable `CREATE VIEW` bodies verbatim (C-001,
  C-006); only TEMPORARY bodies expand against session defaults (F1 pins).
- `execute_calibrated` routes `CREATE VIEW` straight to `execute_inner`: the
  time-travel/lineage rewrites mint ephemeral temp names that must never reach
  the stored body (C-011). The view read path re-runs the query-safe stages.
- The CREATE VIEW on a metadata table refusal (`view_ddl/parse.rs`,
  `view_ddl/execute.rs`) keeps its house `read-only` wording: it is the MT-2
  declared diagnostic (`docs/spark-sql-iceberg-parity.md`), never a Spark-parity
  claim — no ledger clause pins its bytes, and p1/p2 carry no probe row for it.
