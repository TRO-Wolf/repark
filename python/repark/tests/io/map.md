# map — python/repark/tests/io

## Purpose

Data in and out of the engine: file readers and writers (CSV, JSON, text, ORC, parquet), Postgres / JDBC, S3 paths, source mounts, `createDataFrame` ingestion, pandas / polars / Arrow interchange, and the Arrow bridge to Python code (`mapInArrow`, `applyInPandas`, `pandas_udf`, `udf`, `udtf`).

Part of the FACADE-TESTS-LAYOUT-1 (2026-10-19) layout; the directory rules live in
[../map.md](../map.md) "Layout". A new test goes in the directory of the subsystem it
pins.

## Contents

- [test_applyinpandas.py](test_applyinpandas.py) — GroupedData.applyInPandas over the mapInArrow bridge.
- [test_applyinpandas_oracle.py](test_applyinpandas_oracle.py) — Live PySpark 4.1.2 oracle for GroupedData.applyInPandas (U6 named deliverable).
- [test_boundary_shapes_parity.py](test_boundary_shapes_parity.py) — Facade-boundary container-shape corpus (H-2 gap G10) — pandas / Arrow interchange.
- [test_create_dataframe_materialize.py](test_create_dataframe_materialize.py) — R-PERF-VALUES — createDataFrame materializes once (MemTable), not re-plan VALUES.
- [test_csv_infer_perf_1.py](test_csv_infer_perf_1.py) — CSV inferSchema plan-time cost and Spark-equal promotion shapes.
- [test_datasets_facade.py](test_datasets_facade.py) — DS-4 — facade pins for the five torture-dataset families.
- [test_e2_readwriter.py](test_e2_readwriter.py) — Readwriter + session residuals: bare-name resolution, parquet path forms, ndarray lit.
- [test_facade_1_arrow_c_stream.py](test_facade_1_arrow_c_stream.py) — FACADE-1 Arrow C Stream boundary pins: capsule protocol, pyarrow optional, IPC fallback.
- [test_facade_3_cdf_dispatch.py](test_facade_3_cdf_dispatch.py) — FACADE-3 step 2 dispatch pin: the named shapes take the Rust Arrow builder.
- [test_facade_3_cdf_step3.py](test_facade_3_cdf_step3.py) — FACADE-3 step 3 pins: the named-rows funnel and the timetuple-free temporal path.
- [test_facade_3_create_dataframe_goldens.py](test_facade_3_create_dataframe_goldens.py) — FACADE-3 createDataFrame inference goldens against the committed JSON file.
- [test_foreach_wrap_1.py](test_foreach_wrap_1.py) — The foreach doors raise a masked copy of the user's exception and keep the error credential-free.
- [test_grouped_surface_1.py](test_grouped_surface_1.py) — grouped-surface-1 pins: apply / applyInArrow / cogroup and the state-API refusals.
- [test_interchange_parity.py](test_interchange_parity.py) — G-INT interchange parity — ``toPandas`` / ``createDataFrame`` / ``to_polars``.
- [test_io_declared_1.py](test_io_declared_1.py) — io-declared-1 pins: the orc / xml / jdbc declared IO refusals and ``na.replace``.
- [test_io_orc_1.py](test_io_orc_1.py) — IO-ORC-1 — the read-only ORC scan against the oracle.
- [test_io_text_1.py](test_io_text_1.py) — IO-TEXT-1 — DataFrameReader.text and DataFrameWriter.text against the oracle.
- [test_io_text_2.py](test_io_text_2.py) — IO-TEXT-1 round 5 — probe5 oracle pins for the X-1/X-2/X-3 follow-up — plus round 6 probe6 pins for the Y-2 overlay types, plus round 7 probe7 pins for the Z-1 session-zone wall clocks.
- [test_mapinarrow.py](test_mapinarrow.py) — U-SPIKE-MAPINARROW: DataFrame.mapInArrow facade streaming bridge pins.
- [test_mapinarrow_oracle.py](test_mapinarrow_oracle.py) — Live PySpark 4.1.2 oracle for DataFrame.mapInArrow (U-SPIKE-MAPINARROW).
- [test_pandas_udf.py](test_pandas_udf.py) — Scalar pandas_udf: facade projection-rewrite over the mapInArrow bridge.
- [test_pandas_udf_oracle.py](test_pandas_udf_oracle.py) — Live PySpark 4.1.2 oracle for pandas_udf (U7 SCALAR + M5 SCALAR_ITER / GROUPED_AGG).
- [test_pg_acceptance.py](test_pg_acceptance.py) — PG4 acceptance battery -- env-gated; skip-loud without REPARK_PG_DSN (P7/P9).
- [test_pg_catalog.py](test_pg_catalog.py) — PG3 offline pins: catalog config kind + registered-catalog resolve surface.
- [test_pg_jdbc_options.py](test_pg_jdbc_options.py) — Offline pure-logic pins for spark.read.jdbc / format('postgres') option surface (PG2).
- [test_pg_jdbc_oracle.py](test_pg_jdbc_oracle.py) — Named oracle deliverable for PG2 (env-gated live + DuckDB skip-loud).
- [test_r1_read_formats.py](test_r1_read_formats.py) — R1 — CSV/JSON read + write facade pins (Arrow path: value AND type).
- [test_r2_read_formats2.py](test_r2_read_formats2.py) — R2 — writer option matrix / path modes / partitionBy honesty pins (Arrow path).
- [test_s3_path_write_1.py](test_s3_path_write_1.py) — Moto pins for the 38 W-PATH-S3-* oracle cells in ``u12-spark.json``.
- [test_session_sources.py](test_session_sources.py) — Declared source mounts: `sources()` lists them with redacted properties and `ping` resolves through the mount.
- [test_source_url_redact_1_fn.py](test_source_url_redact_1_fn.py) — SOURCE-URL-REDACT-1: URL passwords are masked on the builder, `repark.toml` and `conf.set` doors.
- [test_source_url_redact_2.py](test_source_url_redact_2.py) — SOURCE-URL-REDACT-2: config values and refusals never echo a credential (three shapes on every door).
- [test_source_url_redact_2_doors.py](test_source_url_redact_2_doors.py) — SOURCE-URL-REDACT-2: tracebacks from user callbacks on the Arrow bridge doors are masked.
- [test_source_url_redaction_1.py](test_source_url_redaction_1.py) — SOURCE-URL-REDACT-1: source rows, config dumps and SET listings never carry the password.
- [test_stream_ipc_ingest.py](test_stream_ipc_ingest.py) — R-STREAM-IPC-INGEST: Arrow C Stream register + mapInArrow bridge switch pins.
- [test_t1_cdf_ingest.py](test_t1_cdf_ingest.py) — createDataFrame ingestion parity (dict key-union + nested List/Struct).
- [test_t4_csv_smart.py](test_t4_csv_smart.py) — smartCsv + inference PROTOCOL pins (Arrow path: value AND type).
- [test_udf.py](test_udf.py) — Classic scalar Python udf: per-row facade projection-rewrite over mapInArrow.
- [test_udf_oracle.py](test_udf_oracle.py) — Live PySpark 4.1.2 oracle for classic scalar Python udf (U8).
- [test_udtf.py](test_udtf.py) — UDTF scalar-arg core (mapInArrow + FROM name(lit_args)).
- [test_writer.py](test_writer.py) — Facade tests for ``DataFrame.write`` (Group E: E6).
- [_record_boundary_shapes_goldens.py](_record_boundary_shapes_goldens.py) — Record mode for the G10 boundary-shape corpus — re-derive every Spark half.
- [applyinpandas_oracle_funcs.py](applyinpandas_oracle_funcs.py) — Picklable helpers for the live PySpark applyInPandas oracle (must be importable).
- [mapinarrow_oracle_funcs.py](mapinarrow_oracle_funcs.py) — Picklable helpers for the live PySpark mapInArrow oracle (must be a real importable module).
- [pandas_udf_oracle_funcs.py](pandas_udf_oracle_funcs.py) — Picklable helpers for the live PySpark pandas_udf oracle (importable module).
- [udf_oracle_funcs.py](udf_oracle_funcs.py) — Picklable helpers for the live PySpark classic scalar udf oracle (U8).
- [facade_3_create_dataframe_goldens.json](facade_3_create_dataframe_goldens.json) — Golden file read by `test_facade_3_create_dataframe_goldens.py`.
- [facade_grouped_data_oracle.json](facade_grouped_data_oracle.json) — Recorded Spark oracle (25 cells) read by `test_grouped_surface_1.py`.
- [facade_orc_oracle.json](facade_orc_oracle.json) — Recorded Spark oracle read by `test_io_orc_1.py`.

## Pointers

- Up: [../map.md](../map.md)
