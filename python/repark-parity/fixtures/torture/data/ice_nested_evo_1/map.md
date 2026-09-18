# map — fixtures/torture/data/ice_nested_evo_1 (Spark oracle + four nested-evolved Iceberg tables)

## Purpose

The recorded Spark 4.1.2 answers for ICE-NESTED-EVO-1 — reads after a nested child was added
to a struct, a list element struct or a map value struct, and nested DDL (`CREATE TABLE` with a
struct column, `ADD COLUMN s.c`, `RENAME COLUMN s.a TO a2`, `DROP COLUMN s.b`, the required-child
refusal) — and four Spark-written tables the JVM-free tier adopts with `register_table`.
Recorded 2026-09-17 by `python/repark/tests/_record_ice_nested_evo_1.py` on PySpark 4.1.2 +
iceberg-spark-runtime-4.1_2.13:1.11.0 (Hadoop catalog, one local JVM). v2 and v3 answer
identically. Never hand-edited: re-run the driver.

## Contents

- `oracle.json` — `spark_version`, `iceberg_runtime`, `warehouse` (the baked table root) and one
  `cells` entry per `(label, statements, query)` from `build_cells()`: `error` (null, or the
  Python class, JVM class, Spark condition, JVM cause class and first message line), `query`,
  Spark's `schema` JSON and `rows`. The required-child cell records
  `org.apache.spark.SparkException` / `_LEGACY_ERROR_TEMP_2045` caused by
  `java.lang.IllegalArgumentException`: `Unsupported table change: Incompatible change: cannot
  add required column: r`.
- `st_add_v2/`, `st_add_v3/` — `(id INT, s STRUCT<a INT>)`, row 1 written, then
  `ADD COLUMN s.b STRING`, then row 2 `{a:2,b:'y'}`. Row 1's data file has no `s.b` column.
  `metadata/v4.metadata.json` is the adoption point.
- `list_add_v3/` — `(id INT, arrs ARRAY<STRUCT<x INT>>)`, row 1, `ADD COLUMN arrs.element.y INT`,
  row 2. `metadata/v4.metadata.json`.
- `map_add_v3/` — `(id INT, m MAP<STRING, STRUCT<p INT>>)`, row 1, `ADD COLUMN m.value.q STRING`,
  row 2. `metadata/v4.metadata.json`.
- `map.md` — this file.

Every table's metadata carries absolute paths under `/tmp/repark-ice-nested-evo-1/ns/`, so the
pins copy a table there under a directory lock and remove the copy on exit. Hadoop `.crc`
side files are not copied.

## Pointers

- Up: [../map.md](../map.md)
- Cells: [../../../../../repark/tests/test_ice_nested_evo_1.py](../../../../../repark/tests/test_ice_nested_evo_1.py)
- Driver: [../../../../../repark/tests/_record_ice_nested_evo_1.py](../../../../../repark/tests/_record_ice_nested_evo_1.py)
- Ledger: `task/ledgers/staging/ice-nested-evo-1-ledger.md`
