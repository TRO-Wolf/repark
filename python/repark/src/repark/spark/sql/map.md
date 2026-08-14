# map — python/repark/src/repark/spark/sql

## Purpose

Alias subpackage so `sed 's/pyspark/repark.spark/'` works on multi-import consumer
scripts (`from pyspark.sql import …`, `pyspark.sql.types`, `pyspark.sql.functions`,
`pyspark.sql.window`). **Aliases only** — one canonical implementation under
`repark.spark` / `repark.spark.functions` / `repark.spark.types` /
`repark.spark.window`. Absent pyspark.sql names raise loud (ImportError /
AttributeError naming the gap), never stubs.

## Contents

- `__init__.py` — re-exports `SparkSession`, `DataFrame`, `Row`, `Column`, `Window`/
  `WindowSpec`, `Catalog`, `GroupedData`, `DataFrameReader`, plus submodule handles
  `functions` / `types` / `window`. `__getattr__` names unimplemented pyspark.sql
  surfaces (`SQLContext`, `UDFRegistration`, writers, …).
- `functions.py` — re-export of `repark.spark.functions.__all__` (`is` identity).
  Includes `explode` / `explode_outer` / `posexplode` / `posexplode_outer` once those
  names are on the canonical `__all__` (octo C4-Q-001; sed-swap `repark.spark.sql.functions`).
- `types.py` — re-export of `repark.spark.types.__all__` (`is` identity).
- `window.py` — re-export of `Window` / `WindowSpec` (`is` identity).
- `map.md` — this file.

## I want to…

| I want to… | Go to |
|---|---|
| Change a real function/type/window | canonical `../functions.py` / `../types.py` / `../window.py` — never only here |
| Add a new sql alias for a new repark surface | `__init__.py` + matching re-export module if submodule |
| Pin import identity / sed-swap smoke / failing `import repark.sql` | `../../../../tests/test_sql_alias.py` |

## Pointers

- Unit charter: overnight slate N2 R-SQLALIAS
- Pins: `python/repark/tests/test_sql_alias.py`

## Debug

| Symptom | Check |
|---|---|
| `from repark.spark.sql import X` ImportError for a name we ship | `__all__` + import in `__init__.py` |
| Alias is a copy not identity | test `is` pins against `repark.spark.*` |
| Silent stub for missing pyspark name | must raise via `__getattr__` / missing import |
