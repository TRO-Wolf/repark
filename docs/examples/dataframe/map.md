# map — docs/examples/dataframe/

## Purpose

Worked examples for DataFrame, GroupedData, and the na/stat helpers. Examples
construct the session as `repark = ReparkSession.builder…`; see
[../map.md](../map.md). Each example teaches by its statements and fails
loud: a drifted measured value exits with a named `SystemExit` showing the
got and expected reprs (the corpus form).

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
  anti/semi), `hint`, `intersect`, and `mergeInto` / `merge_into` into a local Iceberg table
  (the bare-key sugar and the `target.`/`source.` Column condition, both answering Spark's
  merged rows).
- [rows_nulls.py](rows_nulls.py) — `limit`, `offset`, `orderBy` / `order_by` (null ordering),
  `melt` (the full 12-row multiset, duplicate proved), and the `na` fill/drop surface.
- [state_cache.py](state_cache.py) — `isEmpty` / `is_empty`, `isStreaming` / `is_streaming`,
  the `is_cached` arc, `persist`, and `localCheckpoint`.
- [bridges.py](bridges.py) — `mapInArrow` / `map_in_arrow`, `mapInPandas` / `map_in_pandas`
  (each with a NULL `v` riding the bridge back to NULL), and the `pl` polars door
  (no Spark analog).
- [print_schema.py](print_schema.py) — `printSchema` / `print_schema`: the captured tree lines;
  Spark's own stdout carries one more trailing blank line (§7 `EX-DF-10`, FIXED by DF-PRINTSCHEMA-1); both arms compare after `rstrip`.
- [random_split.py](random_split.py) — `randomSplit` / `random_split`: two weighted parts,
  every row placed exactly once.

Divergent names stay on the backlog with §7 registry rows
([EX-DF-1](../../spark-sql-iceberg-parity.md), EX-DF-2, EX-DF-3, EX-DF-4, EX-DF-5, EX-DF-6) and
pins in `python/repark/tests/test_examples_dataframe_a.py`: `colRegex` / `col_regex`,
the three global-temp-view spellings, `exceptAll` / `except_all`, the
`describe` row order, the `corr` / `cov` NULL-pair arm, and the `createTempView` /
`create_temp_view` replace-on-existing arm (the examples keep the arms where the
engines agree). The EX-16 batch adds EX-DF-7 (`intersectAll` /
`intersect_all` refuse; Spark answers the multiset intersect), EX-DF-8 (`groupingSets` /
`grouping_sets` take one column each; Spark's documented shape takes a list of sets and the
measured answers differ), EX-DF-9 (`mergeInto`'s bare-key sugar and `target.`/`source.`
qualifiers; Spark wants a table-name/alias SQL condition — the covered merge program answers
Spark's rows), and EX-DF-10 (`printSchema`'s stdout ended one newline short of Spark's — FIXED by DF-PRINTSCHEMA-1 capture),
with pins in `python/repark/tests/test_examples_dataframe_b.py`.

## Pointers

- Up: [../map.md](../map.md)
- Pins: [../../../python/repark/tests/test_examples_dataframe_a.py](../../../python/repark/tests/test_examples_dataframe_a.py)
