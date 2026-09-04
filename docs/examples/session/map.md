# map — docs/examples/session/

## Purpose

Worked examples for `repark.sql`, the `ReparkSession` construction door, and
the session-level surface: the builder chain, the active-session trio, conf,
the catalog property, frame builders, the file readers, the memory Iceberg
catalog, temp-view listing, name resolution, the display style, and the two
loud refusals. Examples construct the session as
`repark = ReparkSession.builder…` (owner ruling, 2026-09-01). Local
filesystem, memory catalog, and temp views only — no cloud, no JVM, no
network.

## Contents

- [sql.py](sql.py) — native `repark.sql` plus `ReparkSession.builder.getOrCreate`.
- [builder.py](builder.py) — `Builder.app_name` / `Builder.master` /
  `Builder.config` / `Builder.get_or_create`: the snake_case builder chain,
  with the app name, master, and shuffle-partition values read back through
  `conf` and `sparkContext`.
- [session_state.py](session_state.py) — `SparkSession.active` /
  `SparkSession.getActiveSession` / `SparkSession.newSession`: the builder
  session is active, the spare is a distinct object that answers, and it does
  not steal the active slot before any action (the promotion arm is §7
  `EX-SES-2`).
- [session_conf.py](session_conf.py) — `SparkSession.conf`: string and bool
  round-trips, plus the two-arg default for an unset key (the unset-key error
  contract is §7 `EX-SES-4`).
- [session_catalog.py](session_catalog.py) — `SparkSession.catalog`: the
  `Catalog` type and the untouched default names.
- [frame_builders.py](frame_builders.py) — `SparkSession.create_dataframe` and
  `SparkSession.range`: row-list frames, the explicit-schema empty frame (the
  name-list empty arm is §7 `EX-SES-3`), and the exclusive
  `range(start, end[, step])` including the negative step.
- [read_files.py](read_files.py) — `SparkSession.read_csv` /
  `SparkSession.read_json` / `SparkSession.read_parquet` over files the example
  writes (CSV with an explicit `header` option, NDJSON, a Parquet directory);
  a missing path with the format's extension raises `AnalysisException`
  (§7 `EX-SES-5`).
- [register_catalog.py](register_catalog.py) —
  `SparkSession.register_memory_catalog` / `SparkSession.create_namespace`:
  the registered catalog lists and becomes current, the namespace exists after
  creation.
- [iceberg_tables.py](iceberg_tables.py) — `SparkSession.read_iceberg_table` /
  `SparkSession.list_iceberg_table_names` /
  `SparkSession.list_df_schema_table_names` /
  `SparkSession.refresh_catalog_provider` over one CTAS table in the memory
  catalog.
- [temp_views.py](temp_views.py) — `SparkSession.list_temp_view_names`: a bare
  session lists none; two created views both list. The assertion holds the sorted
  names — the listing order is not creation-ordered.
- [resolve_names.py](resolve_names.py) — `SparkSession.resolve_table_name`:
  bare and two-part qualification, the temp-view home under
  `prefer_temp_view=True`, and the plain form.
- [display_style.py](display_style.py) — `SparkSession.display_style`: the
  `spark` default, the `polars` switch, and the `conf` mirror.
- [legacy_refusals.py](legacy_refusals.py) — `SparkSession.registerTempTable` /
  `SparkSession.pandas_api`: both refuse loud with
  `UnsupportedOperationException` naming the supported route; neither name
  exists on live PySpark 4.1.2.

## Pointers

- Up: [../map.md](../map.md)
- Pins: [../../../python/repark/tests/test_examples_window_catalog.py](../../../python/repark/tests/test_examples_window_catalog.py)
- Ledger: [../../../task/ledgers/staging/ex-21-catalog-session-ledger.md](../../../task/ledgers/staging/ex-21-catalog-session-ledger.md)
