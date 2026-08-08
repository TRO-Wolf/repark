# map — python/repark/src/repark/sql

## Purpose

Alias subpackage so `sed 's/pyspark/repark/'` works on multi-import consumer scripts
(`from pyspark.sql import …`, `pyspark.sql.types`, `pyspark.sql.functions`,
`pyspark.sql.window`). **Aliases only** — one canonical implementation under
`repark` / `repark.functions` / `repark.types` / `repark.window`. Absent pyspark.sql
names raise loud (ImportError / AttributeError naming the gap), never stubs.

## Contents

- `__init__.py` — re-exports `SparkSession`, `DataFrame`, `Row`, `Column`, `Window`/
  `WindowSpec`, `Catalog`, `GroupedData`, `DataFrameReader`, plus submodule handles
  `functions` / `types` / `window`. `__getattr__` names unimplemented pyspark.sql
  surfaces (`SQLContext`, `UDFRegistration`, writers, …).
- `functions.py` — re-export of `repark.functions.__all__` (`is` identity).
  Includes `explode` / `explode_outer` / `posexplode` / `posexplode_outer` once those
  names are on the canonical `__all__` (octo C4-Q-001; sed-swap `repark.sql.functions`).
- `types.py` — re-export of `repark.types.__all__` (`is` identity).
- `window.py` — re-export of `Window` / `WindowSpec` (`is` identity).
- `map.md` — this file.

## I want to…

| I want to… | Go to |
|---|---|
| Change a real function/type/window | canonical `../functions.py` / `../types.py` / `../window.py` — never only here |
| Add a new sql alias for a new repark surface | `__init__.py` + matching re-export module if submodule |
| Pin import identity / sed-swap smoke | `../../../tests/test_sql_alias.py` |

## Pointers

- Unit charter: overnight slate N2 R-SQLALIAS
- Pins: `python/repark/tests/test_sql_alias.py`

## Debug

| Symptom | Check |
|---|---|
| `from repark.sql import X` ImportError for a name we ship | `__all__` + import in `__init__.py` |
| Alias is a copy not identity | test `is` pins against `repark.*` |
| Silent stub for missing pyspark name | must raise via `__getattr__` / missing import |
