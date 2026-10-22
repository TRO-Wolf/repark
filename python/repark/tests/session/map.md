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
- [test_panic_fatal_suite_1.py](test_panic_fatal_suite_1.py) — PANIC-FATAL-SUITE-1 (2026-10-21): the suite runs with `REPARK_PANIC_FATAL=1` armed by `conftest.py`; the module reports `_native.__panic_fatal__` true, and a child interpreter that inherits the suite environment dies on the test-only `_native.__panic_probe__` panic with no `repark internal error` (the `method` shape through a method fence ends in `PanicException`, the `stream` shape read through `pyarrow` aborts in the Arrow C stream fence) while the same child with the flag cleared keeps the message. **MAP-COLUMN-PANIC-NATIVE-DOOR-1 (2026-10-21):** the probe replaces the native `repark.sql()` door's `map('a', <column>)` panic, which that unit fixed; the pin reads the unarmed message through `pyarrow`'s `External error:` prefix for the `stream` shape. The child turns core dumps off with `prctl`, since the stream shape aborts.
- [test_production_file_size.py](test_production_file_size.py) — Pin the session split compatibility contract.
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
