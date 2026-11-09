# DOOR-PROTECTION-INVENTORY-1, Part 1: the code inventory

Read on `origin/main` (`a4284f4d`). The router is `crates/repark-sql/src/router.rs`; the Python native door is
`PyReparkSession.native()` behind `repark.sql()`. The finding that frames every row: `crates/repark-sql` has **no
production caller**. The Cargo manifests of `repark-python`, `repark-spark`, `repark-connect` and `repark-distributed`
name no `repark-sql` edge (`crates/repark-python/Cargo.toml:56` records the deliberate non-edge), so `AnsiDialect`
(`crates/repark-sql/src/dialect.rs:16`) is constructed only by the crate's own tests. The router's protections run on
no shipped door. The Python native door runs `DataFusionDialect` (`crates/repark-core/src/dialect.rs:146`) wrapped in
`LocalFsGuardedDialect` (`crates/repark-core/src/local_fs_policy.rs:49`), plus the Python-side prelude
`prepare_session_sql` (`crates/repark-python/src/session_runtime.rs:398`).

Columns: **R** = router file:line; **Pin** = the Rust test that pins the router behaviour (path and name, each grepped
to exist on `a4284f4d`); **Spark** = the Spark door's equivalent; **Native** = the Python native door's equivalent
(file:line, or **none**); **Cell** = the measured cell in [measured.md](measured.md).

## Measured protections

| id | protection (kind) | R | Pin | Spark | Native | Cell, statement |
|---|---|---|---|---|---|---|
| P01 | multi-statement refusal (refuse) | `guards.rs:32` via `guards.rs:22`, `router.rs:25` | `guards/tests.rs::two_statements_refuse_with_parse_syntax_error_class` | `normalize/statement_guard.rs:9` via `router.rs:305` | none in repo code; DataFusion `sql()` rejects a second statement upstream | A1 `SELECT 1; SELECT 2`, A2 two INSERT statements |
| P02 | write to a branch/tag name (refuse) | `guards.rs:144`, `router.rs:25` | `guards/tests.rs::write_to_branch_refuses` | `router.rs:180` (`write_to_branch::apply_write_to_branch`) | none; a four-part name fails DataFusion's identifier parse, the two-part form is not measured | B1 `INSERT INTO t.branch_dev ...` |
| P03 | collation spellings (refuse) | `guards.rs:326`, `:454`, `:464`; `router.rs:100`, `:107`, `:126` | `guards/tests.rs::collation_valve_fires_on_expression_collate`, `..._on_create_table_column_collate`, `..._on_cast_as_string_collate` | `router.rs:332`; `spark_ast.rs:134`, `:203`, `:245` | none | C1 `SELECT 'a' COLLATE UNICODE`, C2 `CREATE TABLE (s STRING COLLATE UNICODE)`, C3 CAST |
| P04 | declared-absent / deferred functions (refuse) | `declared_refuse.rs:8`, `router.rs:127` | `router/tests.rs::execute_refuses_every_armed_declared_name` | `router.rs:333`; `spark_ast.rs:135` | `session_runtime.rs:398` (`prepare_session_sql` calls `repark_spark::refuse_declared_function_in_sql`) | D1 `java_method(...)`, D2 `hll_sketch_agg(id)` |
| P05 | local-filesystem DDL/COPY (refuse) | `guards.rs:303`, `router.rs:343` | `guards/tests.rs::create_external_table_outside_warehouse_refuses`, `::local_filesystem_plan_refuses` | `spark_ast.rs:267` | `local_fs_policy.rs:49-52`, `:87`, enabled at `repark-python/src/session.rs:161` | E1 `CREATE EXTERNAL TABLE`, E2 `COPY TO` |
| P06 | DML subquery predicate: identity route for simple forms, G3-E8 refusal for the rest (route + refuse) | `router.rs:200`, `:246-251`, `:268` | `guards/ansi_door.rs::dml_subquery_in_delete_executes_and_deletes_exactly_the_match`, `::dml_subquery_valve_refuses_end_to_end_and_writes_nothing`, `::dml_subquery_exists_delete_executes_uncorrelated_and_correlated`, `::dml_subquery_correlated_in_and_update_in_execute` | `spark_ast.rs:154`, `:380-396`; `router/delete_update.rs:26-27`, `:79-80` | `dialect.rs:154` (`refuse_dml_subquery_where`, no identity path: it refuses every form) | F1 `DELETE ... IN (SELECT)`, F2 `UPDATE ... IN`, F3 `NOT IN`, F4 `= (scalar)`, F5 `= ANY`, F6 correlated `EXISTS` |
| P07 | UPDATE SET column = (subquery) (refuse) | `pre_execute.rs:45`, `:70` via `router.rs:344` | `pre_execute/tests.rs::guard_refuses_an_update_whose_set_value_is_a_scalar_subquery` | `spark_ast.rs:269` (same belt) | `dialect.rs:155` (same belt) | G1 |
| P08 | UPDATE incompatible-cast refusal (refuse) | `update_cast.rs:5`, `router.rs:202` | `tests/ansi_update_cast.rs::ansi_update_string_literal_into_bigint_stamps_cannot_safely_cast` | `update_cast.rs:120` (in `repark-spark`) | none; the planner fails incidentally, other text | G2 `UPDATE t SET v = 'abc'` |
| P09 | INSERT VALUES with too few columns (refuse) | `insert_arity.rs:33`, `router.rs:213` | `tests/ansi_write_defaults.rs::ansi_short_values_insert_stamps_not_enough_data_columns` | `insert_arity.rs:35` via `router.rs:463` | none; DataFusion's values-list width check | H1 |
| P10 | INSERT column list, required column, write defaults (route + rewrite + refuse) | `router.rs:114-123`, `:213`; `insert_defaults.rs:197`, `:324` in `repark-iceberg` | `tests/ansi_write_defaults.rs::ansi_insert_missing_required_column_refuses`, `::ansi_insert_column_list_fills_write_default` | `insert_by_name.rs`, `router/insert_positional.rs` | none; Arrow write-time non-null check refuses the required case | H2 `INSERT INTO t (v)` on `id NOT NULL`, H3 `INSERT INTO t (id)` |
| P11 | whole-table INSERT OVERWRITE stays refused (refuse); PARTITION forms route | `router.rs:160-162`, `insert_overwrite.rs:23`, `refusals.rs:8` | `partition_overwrite.rs::whole_table_insert_overwrite_stays_q9` | `router.rs:461` (`insert_overwrite.rs:40`, executes the overwrite) | none; DataFusion plans it as a table replace | I1 |
| P12 | CALL and ALTER TABLE EXECUTE (refuse) | `router.rs:173` (`refusals.rs:21`), `router.rs:28` (`refusals.rs:44`) | `refusals/tests.rs::call_refusal_steers_to_callable_ops_and_names_the_trigger`, `::alter_execute_recognizer_fires_on_the_statement_shape` | `router.rs:404` (`call.rs:72`, executes the procedure) | none; unsupported statement | J1, J2 |
| P13 | TRUNCATE TABLE (route) | `router.rs:174`, `truncate.rs:21` | `truncate_tests.rs::truncate_table_wipes_rows_stamps_delete_and_preserves_history` | `router.rs:405`, `truncate.rs:23` | none; DataFusion refuses TRUNCATE on a base table | K1 |
| P14 | empty DML and metadata-delete door (route) | `router.rs:243`, `:273-290` | `tests/ansi_meta_delete.rs::the_ansi_door_commits_an_empty_delete_snapshot_on_a_no_match` (no pin for the `false`, `NULL`, `1 = 0` literals) | `router/delete_update.rs:36`, `:42` | none | L1 `WHERE false`, L2 `WHERE NULL`, L3 `1 = 0`, L4 no-match |
| P15 | plain identity DML and three-valued logic (route; controls) | `router.rs:252-266` | none cited; Spark semantics | `spark_ast.rs:396` | none | M1 `<>` with a NULL row, M2 `v = v + 1`, M3 `IN (1, 2)` |
| P16 | MERGE INTO and its clause guards (route + refuse) | `router.rs:158`, `merge.rs:39`, `:275`, `:285`, `:322` | `merge/tests.rs::lowers_the_classic_upsert` | `router.rs:389`, `merge.rs:46`, `:81` | none; the planner rejects MERGE | N1 |
| P17 | ALTER TABLE column and property ops (route) | `router.rs:157`, `alter.rs:39`, `:289`, `:433` | `tests/alter_map_key.rs` (ADD COLUMN cases) | `router.rs:385` | none | O1 `ADD COLUMN` |
| P18 | FOR VERSION / TIMESTAMP AS OF (rewrite) | `router.rs:89`, `time_travel.rs:81` | `time_travel/exec_tests.rs::version_as_of_pins_the_first_snapshot` | `router.rs:228` | none | P1 |
| P19 | CREATE TABLE clause refusals, nanosecond columns, path-escape names (refuse) | `router.rs:130`, `create_table.rs:67`, `:446-452`, `:751`, `:880` | `create_table/tests.rs::nanosecond_timestamp_columns_refuse_with_column_and_precision`, `::silently_droppable_clauses_all_refuse`; `schema_ddl/tests.rs::escaping_identifiers_are_rejected` | `router.rs:362` | none | Q1 `TIMESTAMP(9)`, Q2 TEMPORARY, Q3 COMMENT, Q4 `"a/b"`, Q5 columns plus AS SELECT |
| P20 | CREATE SCHEMA / DROP SCHEMA / DROP TABLE (route + refuse) | `router.rs:131-156`, `schema_ddl.rs:16`, `:56`, `:94`, `:188` | `schema_ddl/tests.rs::drop_schema_nonempty_refuses_and_keeps_everything`, `::unknown_schema_property_refuses_listing_support` | `router.rs:370`, `:383`, `:856` | none in these crates (DataFusion DDL against the Iceberg catalog provider; not read) | R1 `DROP SCHEMA` with a table, R2 `CREATE SCHEMA ... WITH (foo = 'bar')` |
| P21 | checked INT arithmetic at session build (valve) | `repark-sql/src/dialect.rs:31-58` | `tests/cross_door_int_overflow.rs::cross_door_int32_add_overflow_raises_on_both_doors_by_default` | the Spark extension | `repark-python/src/session.rs:26` (`install_shared_analyzer_rules`, `bool_decimal.rs:28`, `integer_spark.rs:39`) | S1 |
| P22 | merge-on-read multi-spec valve (refuse) | `guards.rs:186`; `router.rs:201`, `:269`, `:287`, `:298` | `src/tests.rs::mor_unpartitioned_multi_spec_dml_refuses` | `spark_ast.rs:430`; `router/delete_update.rs:34`, `:87` | none | T1 (facade and Spark only: the native door cannot build the table) |
| P23 | encrypted-table write refusal (refuse) | `guards.rs:238`, `:257`; `router.rs:218-220`, `:242`, `:337`; `truncate.rs:50`; `merge.rs:49` | `tests/enc_1.rs::encrypted_table_insert_values_refuses` | `catalog_ops.rs:251`; `spark_ast.rs:279` | none | T2 (facade only) |
| P24 | partition-aligned metadata delete (route) | `router.rs:273` | `tests/ansi_meta_delete.rs::the_ansi_door_removes_whole_files_in_one_delete_snapshot_on_both_modes` | `router/delete_update.rs:42` | none | T3 (facade and Spark only) |
| P25 | branch/tag DDL (route) | `router.rs:31`, `ref_ddl.rs:80` | `src/tests.rs::branch_and_tag_ddl_round_trips` | `router.rs:920` | none | T4 |

## Inventoried, not measured

| id | protection | R | Pin | Spark | Native | Why not measured |
|---|---|---|---|---|---|---|
| X01 | read-only (Postgres) catalog DML refusal | `guards.rs:60` | `guards/tests.rs::read_only_catalog_dml_refuses_generically` | `router.rs:357`, `:478` | none | needs a live Postgres server |
| X02 | Postgres UPDATE refusal, INSERT route | `pg_insert.rs:39`, `:220`; `router.rs:178`, `:214` | `pg_insert.rs::pg_append_routes_to_the_driver_not_the_default_hook` | `router/pg_insert.rs:48`, `:118` | none | needs a live Postgres server |
| X03 | session-conf INSERT route | `session_insert.rs:8`, `router.rs:222` | `session_write_conf.rs::native_partition_overwrite_stamps_the_session_snapshot_property` | none found | none | needs session write conf the native door cannot set |
| X04 | v3 lineage-column rewrite | `router.rs:61-80`, `:93`; `repark-core/src/lineage_columns.rs:70` | `v3/partitioned_equality_deletes.rs::ansi_filtered_lineage_select_returns_matching_rows` | `router.rs:193-209` | none | v3 fixtures are unreachable |
| X05 | map-cast rewrite | `router.rs:48` | `tests/ansi_door_map_column.rs` | the Spark extension | `session_runtime.rs:400` | present on both |
| X06 | SET PROPERTIES, nested CREATE type, column-move rewrites | `router.rs:34-47` | `alter/tests.rs::rewrite_removes_only_the_properties_keyword` | `router.rs:845` | none | the statements parse-fail on the native door (see O1) |
| X07 | error-text upgrade (sniff) | `router.rs:101`, `:327`, `sniff.rs:39` | `sniff/tests.rs::insert_overwrite_is_recognized` | none | none | changes text only |
| X08 | EXPLAIN ANALYZE / PREPARE of a write | `router.rs:104`, `statement_writes.rs:47` | `repark-iceberg` statement_writes tests | `spark_ast.rs:205` | `dialect.rs:166` | pinned by the Python test `test_repark_sql_update_subquery_where_1.py` |
| X09 | tightened-DDL and source-DDL belt | `pre_execute.rs:42-47`, `router.rs:344` | `pre_execute/tests.rs` | `spark_ast.rs:269` | `dialect.rs:155` | the belt is shared |
| X10 | AnsiDialect UDF registrations other than integer overflow (store casts, NTZ, zone seam, ns casts) | `repark-sql/src/dialect.rs:36-58` | `tests/ansi_store_kernel_registry.rs::the_ansi_door_resolves_every_store_kernel_at_session_build` | the Spark extension | `session_runtime.rs:429` registers `log1p`, map casts, the timestamp cast only | a typed-write matrix is its own unit |

## Entry points

| entry | path to SQL | door taken |
|---|---|---|
| `repark.sql(query)` | `python/repark/src/repark/__init__.py:73`, `:97-98` -> `PyReparkSession.native()` (`session.rs:150`) -> `PyReparkSession::sql` (`session.rs:171`) -> `prepare_session_sql` (`session_runtime.rs:398`) -> `ReparkSession::sql` (`repark-core/src/session.rs:418`) -> `LocalFsGuardedDialect` (`local_fs_policy.rs:49`) -> `DataFusionDialect::execute` (`dialect.rs:146`) | direct DataFusion plus the belt; **not** the router |
| `PyReparkSession.native()` | `session.rs:150-166` (the same session `repark.sql()` holds) | direct DataFusion |
| `ReparkSession.sql` (facade) | `session_core.py:149` -> `sql_run.py:17`, `:46` -> `PyReparkSession::sql`/`sql_built` on a session built by `PyReparkSession::new` (`session.rs:114-131`, `SparkDialect` + `SparkExtension`) | the Spark door (`repark-spark` router) |
| `ReparkSession._sql_built` | `session_core.py:176` -> `session.rs:201` | the Spark door, facade-built SQL |
| dbt-repark | `python/dbt-repark/src/dbt/adapters/repark/session.py:70-72` builds a `ReparkSession`; `:173` `self._session.sql(statement)` | the Spark door |
| `F.expr(str)` | `functions.py:312` -> `PyColumn.sql` (`column/mod.rs:234`, `:41`) -> `plan_expr_column` (`column/expr_build.rs`) running `SELECT (<expr>)` on the session context | direct DataFusion; only `refuse_sql_fragment`; no router, no belt |
| `DataFrame.filter(str)` | `dataframe/core.py:1117` -> `filter_sql` (`dataframe/mod.rs:490`) -> `filter_frame_with_sql` (`dataframe_names.rs:106`) -> `parse_canonical_predicate` (`expr_build.rs:46`) | direct DataFusion expression parse |
| CLI or server binary | none: the only `[[bin]]`-class targets are benches in `repark-functions`, `repark-spark`, `repark-ta`; `repark-connect` and `repark-distributed` have no statement entry (`repark-distributed/src/iceberg_provider.rs:209` is an internal file-list query) | not applicable |
| `AnsiDialect` (the router) | `repark-sql/src/dialect.rs:16`; constructed only in `crates/repark-sql/src/**/tests.rs` and `crates/repark-sql/tests/*.rs` | **no production caller** |

Up: [map.md](map.md).
