# map — python/repark/src/repark

## Purpose

The importable `repark` package exposes the ANSI-door callable `repark.sql()` and
compatibility exports from `repark.spark`. `import repark.sql` is not a module.

Carve-outs that stay here: `repark._native` (maturin module-name), `repark.errors`
(taxonomy identity), `__version__`.

## Contents

- `__init__.py` — `sql()` ANSI callable; shim re-exports of facade names; `__version__`;
  display-style assignment guard.
- `errors.py` — PySpark-shaped exception taxonomy (does not move).
- `functions.py` — re-export binding of `repark.spark.functions`.
- `spark/` — the facade package. See [spark/map.md](spark/map.md).
- `py.typed` — PEP 561 marker.

## I want to…

| I want to… | Go to |
|---|---|
| Change facade behavior | [spark/map.md](spark/map.md) |
| Change the ANSI callable | `__init__.py` `sql()` |
| Change the exception taxonomy | `errors.py` |
| Pin failing `import repark.sql` + callable | `../../../tests/test_sql_alias.py` |

## Pointers

- Up: [../../map.md](../../map.md)
- Design: [../../../../docs/design/python-facade.md](../../../../docs/design/python-facade.md)

## Debug

| Symptom | First check |
|---|---|
| `import repark.sql` succeeds | a leftover `sql/` directory or `sql.py` — must be gone |
| `repark.sql("SELECT 1")` is Spark `/` (2.5 float) | native constructor wired SparkExtension — see `PyReparkSession.native` |
| `from repark import ReparkSession` fails | shim imports in `__init__.py` |
