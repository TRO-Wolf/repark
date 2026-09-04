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
- [repartition.py](repartition.py) — `repartition`, `repartitionByRange`, and
  `repartitionById`: every partitioning call keeps the row multiset and count.
- [rollup_stat.py](rollup_stat.py) — `rollup` grouping sets with the grand total, and the
  `stat` accessor's `crosstab` cells.
- [replace_sample.py](replace_sample.py) — `replace` (scalar and dict, subset-scoped),
  `sample` (fraction 1.0), and `sampleBy` (the 1.0 and 0.0 strata arms).
- [same_semantics.py](same_semantics.py) — `sameSemantics` / `same_semantics`: one object
  True, distinct plans False.
- [schema_select.py](schema_select.py) — `schema` (`simpleString`, `jsonValue`), and
  `selectExpr` / `select_expr` SQL projections.
- [show_sort.py](show_sort.py) — `show` (cells and row counts, never the rendering), and
  `sort` plus `sortWithinPartitions` / `sort_within_partitions` (ascending, descending,
  single-partition).
- [storage_level.py](storage_level.py) — `storageLevel` / `storage_level`: NONE,
  MEMORY_AND_DISK_DESER under `cache`, NONE again after `unpersist`.
- [subtract_summary.py](subtract_summary.py) — `subtract` (int and string arms), and
  `summary("count")` (single-stat arm).
- [take_tail.py](take_tail.py) — `take` and `tail` from both ends of one ordered frame.
- [export_arrow.py](export_arrow.py) — `toArrow` / `to_arrow`, the repark extension
  `toArrowBatches` / `to_arrow_batches` (no PySpark analog), and `toDF` / `to_df`.
- [export_local.py](export_local.py) — `toLocalIterator` / `to_local_iterator`,
  `toPandas` / `to_pandas`, and the repark extensions `to_numpy` and `to_polars`
  (no PySpark analog).

Divergent names and arms stay on the backlog with §7 registry rows
([EX-DF-1](../../spark-sql-iceberg-parity.md) … [EX-DF-16](../../spark-sql-iceberg-parity.md)).
EX-18's rows and pins live in `python/repark/tests/test_examples_dataframe_c.py`:
the `sameSemantics` alias arm (EX-DF-10), `replace` outside the subset arms (EX-DF-11),
`sample` below fraction 1.0 (EX-DF-12), seeded `sampleBy` fractions (EX-DF-13),
`summary` multi-stat order, string-column raise, and bare refusal (EX-DF-14), the `show`
rendering and missing truncation trailer (EX-DF-15), and the `toJSON` refusal (EX-DF-16 —
the one whole name that stays on the backlog). The examples keep the arms where the
engines agree.

## Pointers

- Up: [../map.md](../map.md)
- Pins: [../../../python/repark/tests/test_examples_dataframe_a.py](../../../python/repark/tests/test_examples_dataframe_a.py)
- Pins: [../../../python/repark/tests/test_examples_dataframe_c.py](../../../python/repark/tests/test_examples_dataframe_c.py)
