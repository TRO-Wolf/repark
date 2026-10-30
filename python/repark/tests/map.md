# map — python/repark/tests

## Layout

**FACADE-TESTS-LAYOUT-1 (2026-10-19).** The suite is split into subsystem directories, each with
its own `map.md`; `git mv` only, so every test name, body and assertion is unchanged and
`pytest --collect-only` lists the same node ids with the directory added. pytest's default
prepend import mode puts each test file's own directory and this directory (the root
`conftest.py`) on `sys.path`, so a module imports a helper from its own directory or from the
root; basenames stay unique across the whole tree.

| directory | test files | charter |
|---|---|---:|
| [iceberg/](iceberg/map.md) | 76 | Iceberg table format: catalogs and namespaces, DDL and schema evolution, metadata tables and columns, time travel, branches and tags, maintenance procedures, views, scans and pruning, and format v3 (deletion vectors, row lineage, upgrade). |
| [writes/](writes/map.md) | 62 | The Iceberg write path: INSERT, INSERT OVERWRITE, CTAS/RTAS, MERGE, UPDATE and DELETE on both SQL doors, the DataFrame writers (`write`, `writeTo`), write options and session write confs, distribution and ordering, store assignment, and concurrent commits. |
| [timestamps/](timestamps/map.md) | 29 | Timestamp semantics across every door: TIMESTAMP_LTZ / TIMESTAMP_NTZ, `timestamp_ns` / `timestamptz_ns`, session time zones and zone spellings, timestamp casts and store assignment, epoch constructors and wall-clock rendering. |
| [types/](types/map.md) | 27 | Data types and casts: the `types` module and DDL type strings, cast vocabulary and cast failures, decimal / integer / floating-point arithmetic semantics, literal typing, nullability, nested containers and logical widths. |
| [functions/](functions/map.md) | 90 | The `repark.spark.functions` surface and its SQL-door twins: scalar, string, collection, datetime, aggregate, window, higher-order, bitmap and generator functions, the `repark.ta` indicators, and the registered function divergences. |
| [dataframe/](dataframe/map.md) | 73 | The DataFrame door: transformations, actions, joins, grouping and pivot, explode and dynamicFlatten, Column and Row, display (`show`, repr, printSchema, explain), eager mode, and the polars-style API. |
| [resolution/](resolution/map.md) | 30 | Column and attribute resolution: attribute ids and self-joins, duplicate names, case sensitivity, alias qualifiers, temp-view semantics and the attribute-stamp handle. |
| [sql/](sql/map.md) | 18 | The SQL front ends: Spark-dialect grammar and literals, the native `repark.sql()` door, `SET` / `RESET`, statement routing and eagerness, identifier quoting, table-valued functions, subquery and deep-expression safety, and the fuzz battery. |
| [session/](session/map.md) | 35 | The session and its runtime: builder, `getOrCreate`, config knobs, the `spark.catalog` API and namespaces, the error taxonomy, threading, tracing, secrets and local-filesystem policy, version and package-structure pins. |
| [io/](io/map.md) | 41 | Data in and out of the engine: file readers and writers (CSV, JSON, text, ORC, parquet), Postgres / JDBC, S3 paths, source mounts, `createDataFrame` ingestion, pandas / polars / Arrow interchange, and the Arrow bridge to Python code (`mapInArrow`, `applyInPandas`, `pandas_udf`, `udf`, `udtf`). |
| [perf/](perf/map.md) | 26 | Performance and resource budgets: perf gates, spill and memory pools, eager and cache retention, small-stack gates, and the TPC-H / TPC-DS / write / window / scale bench harnesses. |
| [parity/](parity/map.md) | 26 | Cross-subsystem parity: the acceptance harness and its live AWS legs, the cutover battery, the live PySpark oracle tier, dogfood and revalidation records, and the EX-* PySpark-example sweep. |
| [ml/](ml/map.md) | 5 | `repark.spark.ml`: params, pipelines, persistence, feature transformers, estimators, cross-validation and the boosting extension. |

Rules for new files:

- A new test goes in the directory of the subsystem it pins. A new subsystem is a new directory
  with its own `map.md` (charter plus one row per file) and a row in the table above.
- A helper (`_*.py`) or data file used from one directory lives in that directory; one used from
  two or more directories, by this `conftest.py`, by the parity harness or by a script stays here.
  The root keeps [conftest.py](conftest.py), [_live_parity.py](_live_parity.py),
  [_oracle_pins.py](_oracle_pins.py), [_acceptance.py](_acceptance.py),
  [_spark_python.py](_spark_python.py), the five `_sql_harden_cutover_*.py` helpers,
  [facade_reader_writer_oracle.json](facade_reader_writer_oracle.json) and
  [fixtures/](fixtures/map.md).
- Test modules that import each other share a directory.
- A recorder (`_record_*.py`) runs from the repository root by its new path. One that imports a
  root helper puts this directory on `sys.path` itself (`Path(__file__).resolve().parents[1]`),
  or says `PYTHONPATH=python/repark/tests` in its usage block.

## Purpose

Facade tests for the `repark` wheel: they need the compiled native module and exercise the real
boundary end to end. Each subsystem directory owns its tests and carries its own map with one row
per file; this map routes to those maps and indexes only the files that stay at the root. The
testing contract is [docs/testing.md](../../../docs/testing.md); the pre-pilot record is
[docs/history/facade-tests/map.md](../../../docs/history/facade-tests/map.md).

## Contents

- [conftest.py](conftest.py) — shared fixtures; arms `REPARK_PANIC_FATAL=1` so a caught Rust panic fails the run.
- [_live_parity.py](_live_parity.py) — the live PySpark oracle tier scenario registry.
- [_oracle_pins.py](_oracle_pins.py) — pinned oracle-environment coordinates for record drivers.
- [_acceptance.py](_acceptance.py) — helpers for the real-AWS acceptance harness.
- [_spark_python.py](_spark_python.py) — resolves the interpreter for the live-PySpark legs.
- [_sql_harden_cutover_golden.py](_sql_harden_cutover_golden.py) — the cutover verdict per program, joining both engine halves.
- [_sql_harden_cutover_programs.py](_sql_harden_cutover_programs.py) — the cutover shape inventory.
- [_sql_harden_cutover_repark.py](_sql_harden_cutover_repark.py) — the recorded RePark half of the cutover matrix.
- [_sql_harden_cutover_run.py](_sql_harden_cutover_run.py) — runners executing one cutover program on a duck-typed session.
- [_sql_harden_cutover_spark.py](_sql_harden_cutover_spark.py) — the recorded Spark half of the cutover matrix.
- [facade_reader_writer_oracle.json](facade_reader_writer_oracle.json) — recorded reader/writer oracle read by the interchange pins.
- [fixtures/](fixtures/map.md) — Spark-written binary fixtures behind the IO pins, with their provenance.

## I want to...

| ...do this | go to |
|---|---|
| Pin an Iceberg format behavior | [iceberg/](iceberg/map.md) |
| Pin a write-path behavior | [writes/](writes/map.md) |
| Pin a timestamp semantic | [timestamps/](timestamps/map.md) |
| Pin a type or cast behavior | [types/](types/map.md) |
| Pin a function behavior | [functions/](functions/map.md) |
| Pin a DataFrame behavior | [dataframe/](dataframe/map.md) |
| Pin a resolution behavior | [resolution/](resolution/map.md) |
| Pin a SQL-door behavior | [sql/](sql/map.md) |
| Pin a session or runtime behavior | [session/](session/map.md) |
| Pin a reader, writer or interchange behavior | [io/](io/map.md) |
| Pin a perf or resource budget | [perf/](perf/map.md) |
| Pin a cross-subsystem parity case | [parity/](parity/map.md) |
| Pin an ML behavior | [ml/](ml/map.md) |
| Find a binary fixture's provenance | [fixtures/](fixtures/map.md) |
| Run the suite | `make develop` then `make py-test-facade` (`make py-test-facade-fast` skips the `slow` tier) |
| Run the live oracle tier | `make parity-live` (needs a JVM; routine CI stays JVM-free) |
| Record a divergence from Spark | [docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md) |
| Read the pre-pilot record | [docs/history/facade-tests/map.md](../../../docs/history/facade-tests/map.md) |

## Pointers

- Up: [../map.md](../map.md)
- Testing contract: [docs/testing.md](../../../docs/testing.md)
- Divergence registry: [docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md)
- Pre-pilot record: [docs/history/facade-tests/map.md](../../../docs/history/facade-tests/map.md)

## Debug

| Symptom | First check |
|---|---|
| `ModuleNotFoundError: repark._native` | Build first: `make develop`, then `make py-test-facade` |
| `test_parity_live.py` live tests SKIPPED | Expected unless `REPARK_PARITY_LIVE=1` with a JVM; run `make parity-live` |
| Live tier red | Triage per [docs/testing.md](../../../docs/testing.md): a golden-only red is pin drift, a live-Spark divergence from an unchanged pin is oracle drift |

First checks: `make develop` then `make py-test-facade`. Escalate to: [../map.md#debug](../map.md).
