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
  the stdout tail has matched Spark's since DF-PRINTSCHEMA-1 (§7 `EX-DF-10` FIXED); both arms compare after `rstrip`.
- [random_split.py](random_split.py) — `randomSplit` / `random_split`: two weighted parts,
  every row placed exactly once.
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
- [subtract_summary.py](subtract_summary.py) — `subtract` (int, string, and NULL arms), and
  `summary("count")` plus the NULL-column `count`/`min`/`max` cells.
- [take_tail.py](take_tail.py) — `take` and `tail` from both ends of one ordered frame.
- [export_arrow.py](export_arrow.py) — `toArrow` / `to_arrow`, the repark extension
  `toArrowBatches` / `to_arrow_batches` (no PySpark analog), and `toDF` / `to_df`.
- [export_local.py](export_local.py) — `toLocalIterator` / `to_local_iterator`,
  `toPandas` / `to_pandas`, and the repark extensions `to_numpy` and `to_polars`
  (no PySpark analog).
- [set_ops.py](set_ops.py) — `union` / `unionAll` (the duplicate kept), `unionByName` /
  `union_by_name` (reordered columns, and the `allowMissingColumns` NULL-fill arm).
- [frame_shape.py](frame_shape.py) — `transform` (the callable with args), `withColumn` /
  `with_column` (add and replace), `withColumns` / `with_columns` (the atomic swap and the
  appended column).
- [rename_columns.py](rename_columns.py) — `withColumnRenamed` / `with_column_renamed`
  (including the absent-name no-op), `withColumnsRenamed` / `with_columns_renamed`
  (the plain map and the sequential chain).
- [unpivot_rows.py](unpivot_rows.py) — `unpivot`: string and list ids, string and list
  values, and a NULL-bearing wide frame.
- [cache_write.py](cache_write.py) — `unpersist` (returns the frame, count unchanged) and
  `writeTo` / `write_to`: the V2 `create` and the SQL read-back.
- [na_surface.py](na_surface.py) — `DataFrameNaFunctions.fill` (scalar, string, dict, subset)
  and `DataFrameNaFunctions.drop` (default, `thresh=1`, `thresh=2` with subset).
- [stat_helpers.py](stat_helpers.py) — the `stat` surface: `approxQuantile`, `corr`, `cov`,
  `crosstab`, and `sampleBy` (the 1.0/0.0 arms measured exact, the 0.5 arm as a containment
  property).
- [grouped_agg.py](grouped_agg.py) — `GroupedData.agg` (expression and dict), `count`,
  `sum` / `avg` / `mean` / `min` / `max` (named and no-argument numeric arms), and the NULL
  grouping key (one NULL group, Spark-equal).
- [grouped_pivot.py](grouped_pivot.py) — `GroupedData.pivot` (explicit values, discovery,
  multi-aggregate naming) and `applyInPandas` / `apply_in_pandas` (the per-group pandas
  bridge).
- [row_dicts.py](row_dicts.py) — `Row.asDict` / `Row.as_dict` (flat, and recursive over a
  struct field), and the repark extensions `Row.from_mapping` and `Row.from_ordered_fields`
  (duplicate field names kept). No Spark analog for the two builders (`hasattr` measured
  False on live PySpark 4.1.2).
- [dynamic_flatten.py](dynamic_flatten.py) — `dynamicFlatten` / `dynamic_flatten`:
  struct expansion plus list explosion, the no-explode arm, both spellings agreeing
  (EX-26). Repark extension, no Spark analog (`hasattr` measured False on live
  PySpark 4.1.2).
  pins: ex-26-io-session/C-013

Divergent names and arms stay on the backlog with §7 registry rows
([EX-DF-1](../../spark-sql-iceberg-parity.md) … [EX-DF-17](../../spark-sql-iceberg-parity.md)), pinned in
`python/repark/tests/test_examples_dataframe_{a,b,c}.py`. The snake spellings `same_semantics`, `select_expr`, `sort_within_partitions`, `storage_level`, `to_arrow`, `to_arrow_batches`, `to_df`, `to_local_iterator`, `to_numpy`, `to_pandas`, `to_polars` are repark-only — PySpark 4.1.2 raises `ATTRIBUTE_NOT_SUPPORTED` for each — and the examples keep the arms where the engines agree.
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
Spark's rows), and EX-DF-10 (`printSchema`'s stdout tail, FIXED by DF-PRINTSCHEMA-1),
with pins in `python/repark/tests/test_examples_dataframe_b.py`.
The EX-19 batch adds §7 EX-DF-18 (`withColumnsRenamed` refuses duplicate final names; Spark answers the duplicate-named frame), EX-DF-19 (`stat.freqItems` refuses; the name stays on the backlog), and EX-ROW-1 (a struct-valued `Row` field is a dict in repark; Spark keeps the nested `Row`), pinned in `test_examples_dataframe_d.py`; `Row.as_dict`, `Row.from_mapping`, and `Row.from_ordered_fields` are repark extensions (`hasattr` False on live PySpark 4.1.2).

## Pointers

- Up: [../map.md](../map.md)
- Pins: [../../../python/repark/tests/test_examples_dataframe_a.py](../../../python/repark/tests/test_examples_dataframe_a.py)
- Pins: [../../../python/repark/tests/test_examples_dataframe_c.py](../../../python/repark/tests/test_examples_dataframe_c.py)
- Pins: [../../../python/repark/tests/test_examples_dataframe_b.py](../../../python/repark/tests/test_examples_dataframe_b.py)
- Pins: [../../../python/repark/tests/test_examples_dataframe_d.py](../../../python/repark/tests/test_examples_dataframe_d.py)
