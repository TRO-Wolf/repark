# map — python/repark/tests/session

## Purpose

The session and its runtime: builder, `getOrCreate`, config knobs, the `spark.catalog` API and namespaces, the error taxonomy, threading, tracing, secrets and local-filesystem policy, version and package-structure pins.

Part of the FACADE-TESTS-LAYOUT-1 (2026-10-19) layout; the directory rules live in
[../map.md](../map.md) "Layout". A new test goes in the directory of the subsystem it
pins.

## Contents

- [test_a3_secrets_redaction.py](test_a3_secrets_redaction.py) — SEC-04 — RuntimeConfig.getAll redacts secret-shaped values; get() does not.
- [test_auto_memory_catalog.py](test_auto_memory_catalog.py) — R-AUTO-MEMCAT — the bare-session default memory catalog (duckdb ``:memory:`` analogue).
- [test_builder_config_map.py](test_builder_config_map.py) — R-TAIL — ``ReparkSession.Builder.config(..., map=...)`` PySpark 3.4+ parity.
- [test_c6_census_r8.py](test_c6_census_r8.py) — C6 mechanism pins: Catalog.registerFunction / functionExists + UDF.deterministic.
- [test_catalog_1.py](test_catalog_1.py) — CATALOG-1 facade pins: the session catalog is ``spark_catalog``, like Spark.
- [test_catalog_hygiene.py](test_catalog_hygiene.py) — Catalog hygiene pins (#100): bare-session listTables regression.
- [test_catalog_staleness.py](test_catalog_staleness.py) — T6 / CQ-008 / BUG-007 — catalog listing list-on-access (out-of-band create/drop).
- [test_catalog_surface.py](test_catalog_surface.py) — R-CURCAT-FACADE / G-INT catalog surface — current catalog + list* over existing primitives.
- [test_catalog_surface_1.py](test_catalog_surface_1.py) — CATALOG-SURFACE-1: the 13 Catalog names against the run-15b facade oracle.
- [test_config_mirror.py](test_config_mirror.py) — CFG-1: unknown display and session keys refuse; digit strings are accepted, negative knobs refuse.
- [test_describe_namespace.py](test_describe_namespace.py) — Facade tests for ``DESCRIBE NAMESPACE [EXTENDED]`` and its ``DATABASE``/``SCHEMA`` synonyms.
- [test_dropin_disclosure.py](test_dropin_disclosure.py) — Drop-in no-op / accepted-ignored disclosure surface (WG-4 Clause 2, OTH-010).
- [test_e1_errorclass.py](test_e1_errorclass.py) — Error-class harvest pins — true-EC mechanisms (check_error class + parameter keys).
- [test_errors.py](test_errors.py) — Facade tests for the error taxonomy.
- [test_f1_errorclass.py](test_f1_errorclass.py) — True-EC residual pins — class + parameter-key equality only.
- [test_getorcreate_catalogs.py](test_getorcreate_catalogs.py) — ``getOrCreate`` on an existing session registers NEWLY-configured catalogs (R-GETORCREATE).
- [test_namespace_location_guard.py](test_namespace_location_guard.py) — Facade pin: namespace-create location guard (G-6 Q1 / R-6).
- [test_pa_local_fs_policy_1.py](test_pa_local_fs_policy_1.py) — PA-02 local-filesystem policy: `EXPLAIN ANALYZE COPY` outside the warehouse refuses and writes nothing.
- [test_panic_fatal_suite_1.py](test_panic_fatal_suite_1.py) — PANIC-FATAL-SUITE-1 (2026-10-21): the suite runs with `REPARK_PANIC_FATAL=1` armed by `conftest.py`; the module reports `_native.__panic_fatal__` true, and a child interpreter that inherits the suite environment dies on the test-only `_native.__panic_probe__('method')` panic with `PanicException` and no `repark internal error`, while the same child with the flag cleared keeps the message. **MAP-COLUMN-PANIC-NATIVE-DOOR-1 (2026-10-21):** the probe replaces the native `repark.sql()` door's `map('a', <column>)` panic, which that unit fixed; it is method-only, and the Arrow C stream fence is pinned by the Rust child-process tests in `fence.rs`.
- [test_production_file_size.py](test_production_file_size.py) — Pin the session split compatibility contract. **SPARK-DOOR-TEMP-VIEW-DDL-1 fix round 3 (2026-11-02):** the body hash of `resolve_table_name` is re-baselined for the raw temp-view token lookup (commit `c45fc2b2`). **IDENT-DIALECT-1 (2026-11-02):** the three symbol hashes of the identifier grammar are re-baselined.
- [test_profiles1_probe_rerun.py](test_profiles1_probe_rerun.py) — PROFILES-1 probe re-runnability pins — REVIEW-FIX-8 (Q-21, Q-55, Q-56).
- [test_pyc_1_nested_defs.py](test_pyc_1_nested_defs.py) — PYC-1: ``core.py``, ``plan_collapse.py``, and ``udf_bridge.py`` have no nested ``def``.
- [test_pyc_2_nested_defs.py](test_pyc_2_nested_defs.py) — PYC-2: remaining shipped nested ``def``s are lifted or pragma-sanctioned.
- [test_pyc_3_dataclasses.py](test_pyc_3_dataclasses.py) — PYC-3: shipped dataclass containers converted to Pydantic v2 BaseModel.
- [test_registry_16b_1.py](test_registry_16b_1.py) — registry-16b-1 pins: conf unset, WAP keys and the JDBC format door answer main today.
- [test_repark_log_subscriber.py](test_repark_log_subscriber.py) — R-TRACE-SUBSCRIBER — env-gated tracing reaches the wheel for live MERGE phase profiles.
- [test_sec_boundaries.py](test_sec_boundaries.py) — SB1 — trust boundaries: array cardinality ceilings (SEC-01) + local DDL gate (SEC-02).
- [test_session.py](test_session.py) — Facade tests for ``repark`` — import smoke, the builder chain, and an end-to-end SQL round-trip through the native engine to Arrow / Polars.
- [test_session_config_knobs.py](test_session_config_knobs.py) — Engine-knob config validation at the REAL user entry point (audit SAF-006 / SAF-007).
- [test_session_range.py](test_session_range.py) — X1 — SparkSession.range parity pins (Apache DataFrameTests.test_range + forms).
- [test_session_surface_1.py](test_session_surface_1.py) — SESSION-SURFACE-1 — the 19 SparkSession names against the PySpark 4.1.2 oracle.
- [test_set_ansi_runtime_1.py](test_set_ansi_runtime_1.py) — Runtime `SET` / `spark.conf.set` of the ANSI and time-zone knobs apply — SET-ANSI-RUNTIME-1.
- [test_show_namespaces.py](test_show_namespaces.py) — Facade tests for ``SHOW NAMESPACES`` and its ``SCHEMAS``/``DATABASES`` synonyms.
- [test_temp_view_session_settings_1.py](test_temp_view_session_settings_1.py) — TEMP-VIEW-SESSION-SETTINGS-1 part 1 — a SQL temp view answers under the settings captured at CREATE (ANSI, the zone, the decimal legacy switch), in both ANSI modes and unset: the SQL door, view over view, DataFrame over SQL, `spark.table`, the `ALTER TABLE` rename, the critic's 22 SQL-door filter cells, nested `current_timezone()`, `CREATE OR REPLACE`, `RESET`, `spark.conf.set`, the self-join, CTAS / INSERT / `writeTo` from the view and the SQL-view caches; the ruled loud class and the direct gaps pinned as they answer, by name; `EXPLAIN` identity with main's plans on the matching path. Cells [_temp_view_session_settings_1_cells.py](_temp_view_session_settings_1_cells.py), recorder [_record_temp_view_session_settings_1.py](_record_temp_view_session_settings_1.py), oracle [temp_view_session_settings_1_spark_oracle.json](temp_view_session_settings_1_spark_oracle.json), main's plans [temp_view_session_settings_1_plans.json](temp_view_session_settings_1_plans.json). Delta 1 (2026-11-18): the sweep [_temp_view_session_settings_1_sweep.py](_temp_view_session_settings_1_sweep.py) (a DataFrame frame made under one setting, inlined into a SQL view made under the other, read under each: `timestamp`/`date` against strings, string arithmetic and casts, mixed `CASE`, implicit function casts, decimal widening, two levels), its non-Spark answers pinned by class in [temp_view_session_settings_1_sweep_standing.json](temp_view_session_settings_1_sweep_standing.json).
- [test_temp_view_session_settings_2.py](test_temp_view_session_settings_2.py) — TEMP-VIEW-SESSION-SETTINGS-1 part 2 — a DataFrame temp view answers under its frame's own settings, in both ANSI modes and unset: every ANSI and zone body on the DataFrame door in four positions, SQL over a DataFrame view, CTAS from it, nests across doors two and three deep, the held derivations, `registerTempTable`, the held-frame writes (`writeTo` takes the writer's zone), the DataFrame-door filters, `CREATE OR REPLACE`, the self-join, `RESET`, `spark.conf.set`, the current-time cells, the caches and two walker cells; the ruled loud class, the direct gaps and crossJoin's native join pinned as they answer, by class; `EXPLAIN` identity with main's plans; `-NULL` written into `DATE` and `BOOLEAN` through `writeTo`, `insertInto` and SQL `INSERT` from a frame view refuses as Spark under matching and differing settings (delta 2), and still refuses while two threads loop `spark.conf.set` on other keys (delta 3). Cells [_temp_view_session_settings_2_cells.py](_temp_view_session_settings_2_cells.py) (part 1's `Cells` extended), recorder [_record_temp_view_session_settings_2.py](_record_temp_view_session_settings_2.py), oracle [temp_view_session_settings_2_spark_oracle.json](temp_view_session_settings_2_spark_oracle.json), main's plans [temp_view_session_settings_2_plans.json](temp_view_session_settings_2_plans.json), standing answers [temp_view_session_settings_2_standing.json](temp_view_session_settings_2_standing.json).
- [test_temp_view_reader_lineage.py](test_temp_view_reader_lineage.py) — TEMP-VIEW-SESSION-SETTINGS-1 part 2 delta 1 — a view whose lineage reaches the reader's JSON or CSV scan parses under the reader's zone, as part 1 and Spark do, in both ANSI modes: a JSON frame view read in another zone, read made under the frame's zone, a SQL view over it in a third zone and in the reader's, its `filter` and `selectExpr` views, a CSV `inferSchema` frame view and its `filter` view.
- [test_temp_view_cache_lazy_parse_1.py](test_temp_view_cache_lazy_parse_1.py) — TEMP-VIEW-CACHE-LAZY-PARSE-1 A1 — a lazy `cache()` fills at its first use under the live settings, as Spark's: 56 Spark-recorded cells over `json_s` and `csv_i` (created under Los Angeles): Fable's five must-hold shapes (a child's `collect` or `count`, then `unpersist` or `clearCache`; a child after the fill; `unpersist` and a second `cache()`), `explain` and `EXPLAIN` before the first use, `take(1)` (fills the whole table) and `limit(0)` (fills nothing), a view read by `spark.table` and SQL, `INSERT … SELECT` over a view into Iceberg, the four `writeTo` verbs with the frame's settings differing from and equal to the live ones, a first own-plan action before `cache()`, a child built before the fill, siblings, `clearCache` then a `count` and a later zone, `unpersist(blocking=True)` and a re-read, and a sorted cached frame read in order at its first use. Red on main (24) and green on the head. Also RePark-only pins: a child over `repark.cache.max_bytes` raises and leaves the table armed; `repr` of an unfilled cache runs nothing; a 2,000-filter lineage cached, read through a view, released and read again in a subprocess on an 8 MB thread. Cells [_temp_view_cache_lazy_parse_1_cells.py](_temp_view_cache_lazy_parse_1_cells.py), recorder [_record_temp_view_cache_lazy_parse_1.py](_record_temp_view_cache_lazy_parse_1.py), oracle [temp_view_cache_lazy_parse_1_spark_oracle.json](temp_view_cache_lazy_parse_1_spark_oracle.json).
- [test_temp_view_frame_reader_parse_1.py](test_temp_view_frame_reader_parse_1.py) — TEMP-VIEW-FRAME-READER-PARSE-1 — a JSON or CSV reader's parse runs under the session that reads, the expressions above it under the frame's settings, as Spark's file scan: zone and ANSI frame views over `json_s` and `csv_i` (`csv_s` for ANSI) both ways, a SQL view over a frame view, the exact-key join and `crossJoin` (E5), a three-zone nesting, the held frame acted on directly with no view (`direct`, `cjdirect`, `cache_after`, `pathwrite`), `IN` and scalar subqueries under matching and differing settings, a headerless CSV's `_cN` names and `EXPLAIN` (G3) and `sameSemantics` / `semanticHash`; the F1 delta's cells (`F1/…`): a second own-plan action on the same frame after a `SET` keeps the first's parse (A2, A4, B2, the `spark.sql` frame E1, both `newSession` frames, `toLocalIterator` pulled after the `SET`), each own-plan first action that fixes it (`toLocalIterator`, `toArrow`, `tail`, `foreach`, `foreachPartition`, `localCheckpoint`, `checkpoint`) and the two checkpoints as second actions, while a derived frame (A3), `count` first (C1) and a write (D1) decide afresh. Cells [_temp_view_frame_reader_parse_1_cells.py](_temp_view_frame_reader_parse_1_cells.py), recorder [_record_temp_view_frame_reader_parse_1.py](_record_temp_view_frame_reader_parse_1.py), oracle [temp_view_frame_reader_parse_1_spark_oracle.json](temp_view_frame_reader_parse_1_spark_oracle.json).
- [test_threaded_collect_segv_1.py](test_threaded_collect_segv_1.py) — THREADED-COLLECT-SEGV-1 — the collect family answers on fresh non-main threads.
- [test_version_ssot.py](test_version_ssot.py) — Version SSOT pins (design/python-facade.md §4 Q6, armed with release.yml).
- [facade_catalog_oracle.json](facade_catalog_oracle.json) — Recorded Spark oracle (51 cells) read by `test_catalog_surface_1.py`.
- [facade_session_oracle.json](facade_session_oracle.json) — Recorded Spark oracle (47 cells) read by `test_session_surface_1.py`.

## Path expressions

The move put every file here one level below the old flat directory. These files had a
path expression repointed in the same unit and no other line changed:

| file | change |
|---|---|
| `test_production_file_size.py` | repository-relative anchors gain one level (`parents[N]` to `parents[N+1]`). |
| `test_profiles1_probe_rerun.py` | repository-relative anchors gain one level (`parents[N]` to `parents[N+1]`). |
| `test_pyc_1_nested_defs.py` | repository-relative anchors gain one level (`parents[N]` to `parents[N+1]`). |
| `test_pyc_2_nested_defs.py` | repository-relative anchors gain one level (`parents[N]` to `parents[N+1]`). |
| `test_pyc_3_dataclasses.py` | repository-relative anchors gain one level (`parents[N]` to `parents[N+1]`). |

## Pointers

- Up: [../map.md](../map.md)
