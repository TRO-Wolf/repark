# map — docs/examples/dataframe/

## Purpose

Worked examples for DataFrame, GroupedData, and the na/stat helpers. Examples
construct the session as `repark = ReparkSession.builder…`; see
[../map.md](../map.md).

## Contents

- [select_filter.py](select_filter.py) — `select` then `filter` / `where`, then `collect`.
- [agg_stats.py](agg_stats.py) — `agg` (expression and dict forms), `corr`, `cov`,
  `approxQuantile`, and `crosstab` — every value measured Spark-equal.
- [cube.py](cube.py) — `cube` over two keys: per-combination rows, subtotals, grand total.
- [views.py](views.py) — `alias`, `createOrReplaceTempView` /
  `create_or_replace_temp_view` (replace arm), and `createTempView` /
  `create_temp_view` (fresh-name arm).
- [cross_join.py](cross_join.py) — `crossJoin` / `cross_join` product, count and full row set.
- [dedup_nulls.py](dedup_nulls.py) — `distinct`, `dropDuplicates` / `drop_duplicates`,
  `dropna` (how/subset/thresh), `fillna` (scalar/dict/subset), `drop`.
- [declare_sorted.py](declare_sorted.py) — repark extension `declareSorted` /
  `declare_sorted`: verified sorted input, refused unsorted input. No Spark analog.
- [inspect_cache.py](inspect_cache.py) — `columns`, `dtypes`, `count`, `cache`,
  `coalesce`, and `explain` (plan print asserted non-empty, never text-pinned).
- [describe_ingest.py](describe_ingest.py) — repark extension `describe_ingest`:
  smartCsv ingest decisions, and the empty report on a non-ingest frame. No Spark analog.

Divergent names stay on the backlog with §7 registry rows
([EX-DF-1](../../spark-sql-iceberg-parity.md), EX-DF-2, EX-DF-3, EX-DF-4, EX-DF-5, EX-DF-6) and
pins in `python/repark/tests/test_examples_dataframe_a.py`: `colRegex` / `col_regex`,
the three global-temp-view spellings, `exceptAll` / `except_all`, the
`describe` row order, the `corr` / `cov` NULL-pair arm, and the `createTempView` /
`create_temp_view` replace-on-existing arm (the examples keep the arms where the
engines agree).

## Pointers

- Up: [../map.md](../map.md)
- Pins: [../../../python/repark/tests/test_examples_dataframe_a.py](../../../python/repark/tests/test_examples_dataframe_a.py)
