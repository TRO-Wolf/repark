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
- [first_head.py](first_head.py) — `first` and `head`/`head(n)` row access, including the
  empty-frame arms.
- [group_by.py](group_by.py) — `groupBy` / `group_by` / `groupby`: count, expression agg,
  dict agg.
- [joins_hints.py](joins_hints.py) — `join` (name, list, and Column conditions; inner/left/
  anti/semi), `hint`, and `intersect`.
- [rows_nulls.py](rows_nulls.py) — `limit`, `offset`, `orderBy` / `order_by` (null ordering),
  `melt`, and the `na` fill/drop surface.
- [state_cache.py](state_cache.py) — `isEmpty` / `is_empty`, `isStreaming` / `is_streaming`,
  the `is_cached` arc, `persist`, and `localCheckpoint`.
- [bridges.py](bridges.py) — `mapInArrow` / `map_in_arrow`, `mapInPandas` / `map_in_pandas`,
  and the `pl` polars door (no Spark analog).
- [print_schema.py](print_schema.py) — `printSchema` / `print_schema` in Spark's exact tree
  lines.
- [random_split.py](random_split.py) — `randomSplit` / `random_split`: two weighted parts,
  every row placed exactly once.

Divergent names stay on the backlog with §7 registry rows
([EX-DF-1](../../spark-sql-iceberg-parity.md), EX-DF-2, EX-DF-3, EX-DF-4, EX-DF-5) and pins in
`python/repark/tests/test_examples_dataframe_a.py`: `colRegex` / `col_regex`,
the three global-temp-view spellings, `exceptAll` / `except_all`, the
`describe` row order, and the `corr` / `cov` NULL-pair arm (the examples keep
the arms where the engines agree). The EX-16 batch adds EX-DF-6 (`intersectAll` /
`intersect_all` refuse; Spark answers the multiset intersect), EX-DF-7 (`groupingSets` /
`grouping_sets` take one column each; Spark's documented shape takes a list of sets and the
measured answers differ), and EX-DF-8 (`mergeInto` / `merge_into` work through the
string-sugar arm; live Spark refuses every locally reachable shape), with pins in
`python/repark/tests/test_examples_dataframe_b.py`.

## Pointers

- Up: [../map.md](../map.md)
- Pins: [../../../python/repark/tests/test_examples_dataframe_a.py](../../../python/repark/tests/test_examples_dataframe_a.py)
