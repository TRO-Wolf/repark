# NAN-COMPARE-SPARK-1: the insertion inventory

Every place a FLOAT or DOUBLE NaN is compared, ordered, deduplicated or pruned, under the insertion-inventory rule:
for each seat, **"seen at the seat: yes / no, because"**, where "seen" means the seat gives Spark's answer for every
NaN shape (canonical `7ff8…`, sign-set `fff8…`, payload `7ff8dead…`, sign-set payload `fff8dead…`). Spark's rule,
measured in the [domain table](domain-table.md): every NaN is one value, equal to itself and larger than `+Infinity`.

Line numbers are for the vendored crates in the Cargo registry at the versions `Cargo.lock` pins on main
`2707924e`: arrow 58.4.1, parquet 58.4.1, DataFusion 54.1.0; the fork `TRO-Wolf/iceberg-rust` at the pinned rev
`92242cb4`; RePark main `2707924e` and part B's head `9d92adf6` (branch `fix/signed-zero-normalize-1-part-b`).
`§n` points at the domain table's sections.

## How it was searched

- **Arrow and DataFusion:** `grep -n "total_cmp\|is_eq\|partial_cmp\|is_nan\|to_bits\|make_comparator"` over
  `arrow-array`, `arrow-ord`, `arrow-row`, `arrow-arith` and `parquet` 58.4.1, and over the DataFusion crates the
  operations reach: `functions-aggregate(-common)` (`min_max.rs`), `functions` (`core/greatest.rs`, `least.rs`),
  `functions-nested` (`sort.rs`, `min_max.rs`), `physical-plan` (`sorts/`, `topk/`, `joins/`, `aggregates/`),
  `physical-expr` (`in_list/`), `physical-optimizer` (`aggregate_statistics.rs`), `pruning`,
  `datasource-parquet` and `common` (`scalar/mod.rs`, `hash_utils.rs`). Each hit was read at the call site.
- **The fork:** `grep -rn "nan\|NaN\|total_cmp\|partial_cmp"` over `crates/iceberg/src/expr/visitors/`,
  `spec/values/datum.rs`, `arrow/` (`reader.rs`, `record_batch_predicate.rs`, `row_filter_plan.rs`), `scan/`,
  `writer/file_writer/` and `integrations/datafusion/src/` (`physical_plan/`, `table/`).
- **RePark:** `git grep -n "total_cmp\|is_nan()\|canonical_f64"` and the dispatch names (`"greatest"`,
  `"array_sort"`, `"sort_array"`, `"array_max"`, `"approx_count_distinct"`) over `crates/` on main and on
  `9d92adf6`; part B's seat module `crates/repark-core/src/session/df_guards/signed_zero/` read in full.
- **Measured, not only read:** every "yes / no" below names the cells that show it. Plans were read with
  `EXPLAIN ANALYZE` on the RePark doors (the `IcebergTableScan … N=` file count and the Parquet pruning metrics).

## A. The root: Arrow's float order

| # | Seat | file:line | Seen | Because |
|---|---|---|---|---|
| A1 | `ArrowNativeTypeOp` for `f32`/`f64` | `arrow-array/src/arithmetic.rs:392-399` | **no** | `compare` is `total_cmp`; `is_eq` is `to_bits() ==`. A sign-set NaN sorts below `-Infinity`, a payload NaN above the canonical one, and two NaNs with different bits are unequal; `-0.0 < 0.0`. Every kernel below inherits it. |
| A2 | Comparison kernels `eq`, `neq`, `lt`, `lt_eq`, `gt`, `gt_eq`, `distinct`, `not_distinct` | `arrow-ord/src/cmp.rs:220` (`compare_op`), `:402-419` | **no** | Through A1 (the module docs say totalOrder, `:75`). §2 rows `f_*`, `p_*`, `case_*`. |
| A3 | `make_comparator` (the dynamic comparator `greatest`/`least`, `IN` and merges use) | `arrow-ord/src/ord.rs:113-124`, `:495` | **no** | `l_values[i].compare(r_values[j])`, A1. |
| A4 | Primitive sort | `arrow-ord/src/sort.rs:68`, `:331` | **no** | Sorts by A1. |
| A5 | Row format (multi-column sort, merge, TopK) | `arrow-row/src/fixed.rs:155-160` | **no** | Encodes `to_bits` with the sign trick, which is totalOrder. |
| A6 | `min`/`max` kernels | `arrow-arith/src/aggregate.rs:97-137`, `:717-725`, `:928`, `:943` | **no** | `is_lt`/`is_gt` from A1. The doc line "NaN is greater than any other value" (`:918`) holds for positive NaNs only. |

## B. `min` and `max`, and the statistics shortcut

| # | Seat | file:line | Seen | Because |
|---|---|---|---|---|
| B1 | Ungrouped `MaxAccumulator`/`MinAccumulator` (`max_batch`/`min_batch`) | `datafusion-functions-aggregate-common/src/min_max.rs:497-503`, `:686`, `:764` | **canonical and payload yes, sign-set no** | `compute::max/min` (A6). A positive NaN is the largest under totalOrder, so a canonical or payload NaN answers as Spark does; a sign-set NaN is the smallest, so `max` skips it and `min` returns it. §2 `agg_max`/`agg_min`: `===` on canonical and payload, `WWW` on sign-set. |
| B2 | Grouped `PrimitiveGroupsAccumulator` for float `max`/`min` | `datafusion-functions-aggregate/src/min_max.rs:116-128` (max), `:139-151` (min) | **no, for every NaN** | `match new.partial_cmp(cur) { Some(Greater) \| None => *cur = new }`: a NaN on either side gives `None`, so a NaN replaces the running value and the next value replaces the NaN. The answer depends on input order. Measured with a canonical NaN: `GROUP BY id % 2` gives `max = 0.0` where Spark gives NaN, and `min = NaN` where Spark gives `-0.0` (§3). |
| B3 | Sliding window `SlidingMaxAccumulator`/`SlidingMinAccumulator` (`MovingMax`/`MovingMin`) | `min_max.rs:382-407`, `:886-930` | **no** | `ScalarValue::partial_cmp` for floats is `total_cmp` (`datafusion-common/src/scalar/mod.rs:644-646`), so A1. §2 `win_*_slide`. |
| B4 | Whole-frame and running window `max`/`min` | the B1 accumulators under `WindowAggExec`/`BoundedWindowAggExec` | **canonical yes, sign-set no** | B1. §2 `win_max_all`, `win_max_run`, `win_min_run`. |
| B5 | `AggregateStatistics` (answer `max`/`min` from statistics) | `datafusion-physical-optimizer/src/aggregate_statistics.rs:115`, `:150-154`; `min_max.rs:163-200`, `:366`, `:478`, `:639` | **no, for every NaN** | Fires when the column's `max_value`/`min_value` is `Precision::Exact`. Parquet's statistics leave NaN out (C below), and `datasource-parquet/src/metadata.rs:550-575` marks them exact, so `max(d)` over a `spark.read.parquet` frame holding NaN is answered as the largest non-NaN value without a scan. §6 `agg_max` on `pq_*` (`stats` in the scan column). The fork's Iceberg scan reports only `num_rows` (`integrations/datafusion/src/physical_plan/scan.rs:300-318`), so the shortcut never fires on an Iceberg table. |
| B6 | `array_min`/`array_max` | `datafusion-functions-nested/src/min_max.rs:34`, `:115`, `:193`, `:212` | **canonical yes, sign-set no** | `min_batch`/`max_batch`, B1. §2 `arr_min`, `arr_max`. RePark binds DataFusion's (`crates/repark-python/src/column/function_dispatch.rs:444-451`). |

## C. Sort, TopK and the array sorts

| # | Seat | file:line | Seen | Because |
|---|---|---|---|---|
| C1 | `SortExec` in one batch | `datafusion-physical-plan/src/sorts/sort.rs:812-822` (`lexsort_to_indices`) | **no** on main and #1325; **yes** on part B | A4/A5. Part B wraps every float `Sort` key in its canonicalising call (keys.rs `:114-129`, `sort_key` `:480`), so the canonical NaN sorts last, as Spark's NaN does. §2 `ord_*`: `WW=`. |
| C2 | Merge of sorted runs | `sorts/stream.rs:149` (`RowConverter`) | as C1 | A5; part B's wrap is below it. |
| C3 | TopK | `topk/mod.rs:205-213` (`RowConverter`) | as C1 | A5. §2 `topk_asc`, `topk_desc`: `WW=`. |
| C4 | Window `ORDER BY` | the window's `SortExec` (C1) | as C1 | Part B wraps window order keys (`rewrite_window`, `:333`). §2 `win_rank`: `WW=`. |
| C5 | `array_sort`, `sort_array` | `datafusion-functions-nested/src/sort.rs:150-175`; RePark's shim `crates/repark-functions/src/collection/array_sort.rs:21-22`, `:99-110`, `:164-178` | **no** on every build | The shim forwards to DataFusion's `array_sort`, which sorts the child values with A4. Part B does not reach array elements. §2 `arr_sort`, `sort_arr_*`: `WWW`. |

## D. `greatest` and `least`

| # | Seat | file:line | Seen | Because |
|---|---|---|---|---|
| D1 | DataFusion `greatest`/`least` | `datafusion-functions/src/core/greatest.rs:86`, `:106`; `least.rs:99`, `:119`, `:122` | **canonical yes, sign-set no** | `cmp::gt_eq`/`lt_eq` (A2) or `make_comparator` (A3). RePark binds them on every door (`function_dispatch.rs:290-297`; the Spark door's `timestamp_unit_widen.rs:20-21` and `cardinality.rs:417` only widen types and check arity). Part B leaves projections' function arguments alone. §2 `greatest`, `least`: `WWW` on sign-set. |

## E. Comparisons and keys

| # | Seat | file:line | Seen | Because |
|---|---|---|---|---|
| E1 | `BinaryExpr` comparison against a literal (`=`, `<>`, `<`, `<=`, `>`, `>=`, `<=>`) | A2 | **canonical yes; sign-set and payload no** on every build | Part B wraps a comparison only when neither side is literal-like (`compare_node`, keys.rs `:575-596`; `is_literal_like` `:535`), so `d = CAST('NaN' AS DOUBLE)` and `d > CAST('Infinity' AS DOUBLE)` compare bits on all three builds. Part A's literal seat moves zero literals only; its map says "NaN results do not move". §2 `f_*_nan`, `f_gt_inf`, `f_gt_half`, `f_lt_zero`, `p_*`: `WWW`. A payload NaN is also `> NaN` and `<> NaN` (`f_gt_nan`, `f_ne_nan` on `payload`). |
| E2 | `IN` list | `datafusion-physical-expr/src/expressions/in_list/array_static_filter.rs:84`, `:96`, `:131` | **no** for non-canonical NaN | A3 equality inside the hash probe. Part B treats `InList` with literal items as literal (keys.rs `:621-635` wraps only non-literal lists). §2 `f_in_nan`, `f_notin_nan`. |
| E3 | `BETWEEN` | planned as `>=` and `<=` (A2) | **no** for non-canonical NaN | Part B skips a `BETWEEN` whose bounds are both literal-like (keys.rs `:597-600`). §2 `f_between`. |
| E4 | Simple `CASE x WHEN lit` and searched `CASE WHEN x > lit` | A2 | simple form **yes on part B**, searched form **no** | Part B's `simple_case_compares` (keys.rs `:672`) wraps a simple `CASE`'s operand; a searched `WHEN` with a literal is E1. §2 `case_eq` `WW=`, `case_gt` `WWW`. |
| E5 | Column-to-column comparison | A2 | **yes on part B** | `compare_node` wraps both sides. §2 `f_eq_col`, `p_eq_col`, `p_lt_col`: `WW=`. |
| E6 | Hash join keys | `datafusion-physical-plan/src/joins/utils.rs:1878-1894`; hashes `datafusion-common/src/hash_utils.rs:187-199` | **yes on part B** | Bits hash and bits equality; part B wraps `Join::on` (keys.rs `:89-112`). §2 `join_col`: `WW=`. |
| E7 | `GROUP BY`, `DISTINCT`, `count(DISTINCT)` | `aggregates/group_values/single_group_by/primitive.rs:38-64`; hashes as E6 | **yes on part B** | Part B wraps group keys and distinct arguments (`rewrite_aggregate` `:227`, `aggregate_call` `:300-331`). §2 `group_by`, `cnt_distinct`. |
| E8 | `approx_count_distinct` | DataFusion's `approx_distinct` | **not reached** | RePark refuses it for FLOAT and DOUBLE on every door ("Support for 'approx_distinct' for data type Float64 is not implemented"); Spark answers. §2 `approx_distinct`: `EEE`. Outside this unit (a missing function shape, not a comparison). |

## F. DataFusion's Parquet path (`spark.read.parquet` frames)

| # | Seat | file:line | Seen | Because |
|---|---|---|---|---|
| F1 | Parquet writer statistics | `parquet/src/column/writer/mod.rs:1359-1400` | — (input to F2–F4) | `update_stat` returns early on NaN (`:1393`): min/max leave every NaN out, and an all-NaN chunk has no min/max. The footers measured in `evidence/raw/stats.jsonl` agree for the pyarrow, RePark and Spark writers. |
| F2 | `PruningPredicate` over row-group statistics | `datafusion-pruning/src/pruning_predicate.rs:1625-1665` (`>`, `>=`, `<`), `:1708` (`=` → `min <= lit AND lit <= max`), `:519` (`prune`); applied at `datasource-parquet/src/row_group_filter.rs:258` | **no** | The statistics expression runs on A2 against bounds that exclude NaN. `d = CAST('NaN' …)`: `NaN <= max` is false under totalOrder, so every row group is pruned, canonical NaN included: this is **PARQUET-NAN-EQUALITY-1**'s cause (§6 `f_eq_nan` on `pq_*`, base `[]`). `d > 0.5` keeps a row group only if its non-NaN max is above 0.5, so a NaN row in a group whose other values are all lower is dropped (§6 `f_gt_half`). |
| F3 | Page index pruning | `datasource-parquet/src/page_filter.rs:176`, `:412` | **no** | The same predicate over page statistics. |
| F4 | `max`/`min` from file statistics | B5 | **no** | See B5. |
| F5 | Row-level filter in the Parquet reader | `datasource-parquet/src/row_filter.rs:1015` | **no** | Evaluates the physical predicate with A2. |

## G. The fork: Iceberg pushdown and pruning

| # | Seat | file:line | Seen | Because |
|---|---|---|---|---|
| G1 | DataFusion → Iceberg predicate | `integrations/datafusion/src/physical_plan/expr_to_predicate.rs:174`, `:507-526` (`nan_comparison`), `:528-546` (`IN`) | **yes** | A NaN literal in `=`/`<=>` becomes `is_nan(col)`, which every evaluator answers with NaN counts and `f64::is_nan` (sign-blind); any other comparison with a NaN literal is not pushed; `IN (NaN, …)` becomes `is_nan OR in(rest)`. Pushdown is `Inexact` (`table/mod.rs:236-241`), so DataFusion's filter (E1) still runs above the scan and drops the sign-set NaN rows the scan kept: §6 `f_eq_nan` on Iceberg sources. |
| G2 | `InclusiveMetricsEvaluator` (data-file pruning) | `expr/visitors/inclusive_metrics_evaluator.rs:86-89`, `:110-135`, `:225-249`, `:251-285`, `:395-430`; called at `scan/context.rs:375` | **no** for `>`, `>=`, `=` with a non-NaN literal | A port of Java's evaluator, with Java's IEEE reading: `NaN > x` is false. Bounds exclude NaN, a NaN-only file is `contains_nans_only` and cannot match `>`. So the file holding `(NaN, -0.0, NULL)` (upper bound `0.0` or `-0.0`) is dropped for `d > 0.5`, and the NaN-only file for every `>`: **ICEBERG-NAN-FILE-PRUNE-1**. Spark-on-Iceberg drops the same files on the same layout (§6, "Spark on the source" vs "Spark in memory"). |
| G3 | `ManifestEvaluator` (partition summaries) | `expr/visitors/manifest_evaluator.rs:231-269` (`>`, `>=`), `:271-300` (`=`), `:449-465`; called at `scan/cache.rs:142` | **no** for `>`/`>=` | The summary's upper bound leaves NaN out and `contains_nan` is not read for `>`: a manifest whose largest non-NaN partition value is `+Infinity` is skipped for `pk > Infinity`. This is the **Avro partition-column oddity** (§6 `pk`), and it is not Avro-specific: Spark answers `[]` for `pk > CAST('Infinity' …)` on its own Parquet, ORC and Avro tables alike. |
| G4 | `ExpressionEvaluator` (partition value per file) | `expr/visitors/expression_evaluator.rs:162-172`, `:174-185`; called at `scan/cache.rs:182` | **no** for sign-set NaN partition values | `&datum > literal` uses `Datum`'s order, `iceberg_float_cmp_f64` = `total_cmp` (`spec/values/datum.rs:225-227`, `:255-261`). Java's `Comparators.forType(double)` is `Double.compare`, where every NaN is one value above `+Infinity`: the fork differs from Java here, and only for a sign-set or payload partition value (RePark's writes before NAN-PARTITION-VALUE-1 and NAN-WRITE-CANONICAL-1). |
| G5 | `RowGroupMetricsEvaluator` and the page index evaluator | `expr/visitors/row_group_metrics_evaluator.rs:278-325`; called at `arrow/reader.rs:1408`; `page_index_evaluator.rs` | **no** for `>` | Same IEEE reading as G2 over Parquet row-group statistics. |
| G6 | Row filter inside the fork's reader | `arrow/record_batch_predicate.rs:62` (`arrow_ord::cmp`), `:175`, `:227-250`; `arrow/row_filter_plan.rs:142` | **no** for `>`/`<` on sign-set NaN, **yes** for `is_nan` | Comparisons use A2; `is_nan`/`not_nan` masks use `f64::is_nan` (sign-blind). |
| G7 | Writer metrics | `writer/file_writer/parquet_writer.rs:299-320` (bounds from F1), `orc_writer.rs:306` | — (input) | Parquet and ORC files carry `nan_value_counts` and NaN-free bounds; RePark's and Spark's Avro data files carry **no** column metrics (`evidence/raw/stats.jsonl`), so no data-column pruning happens on Avro, on either engine. |

## H. RePark's own seats

| # | Seat | file:line | Seen | Because |
|---|---|---|---|---|
| H1 | Comparison functions of its own | none on main | — | `git grep total_cmp` finds only quantiles and the partition planner; no RePark comparison, `min`/`max`, sort or `greatest` implementation exists. Every operation above is DataFusion's. |
| H2 | Part A, the zero-literal seat | `crates/repark-core/src/session/df_guards/signed_zero/literals.rs` (main) | **not a NaN seat** | Rewrites zero literals into `[-0.0, +0.0]` bounds; "NaN results do not move". |
| H3 | Part B, the key seat | `signed_zero/keys.rs` on `9d92adf6`: `rewrite_node` `:82-173`, `aggregate_call` `:300-331`, `wrap_key` `:516-533`, `compare_node` `:575-640`; kernel `crates/repark-common/src/float_canon.rs:1-17` | **yes for keys, sorts and column comparisons; no for literal comparisons, `min`/`max`, `greatest`/`least`, arrays** | `canonical_f64` maps every NaN to `7ff8…` and `-0.0` to `+0.0`. It wraps group, join, distinct, set-operation, window partition and order, and `Sort` keys, and comparisons with two non-literal sides; aggregate arguments are wrapped only when `DISTINCT`. Measured: head3 is `=` on every sort, key and column-comparison cell (§2), and `W` on the rest. |
| H4 | NAN-WRITE-CANONICAL-1 (#1325), the write seat | `crates/repark-iceberg/src/write/` on `948d7fb3` (with the fork at `d6592644`) | **not a compare seat** | Stores `7ff8…` in Parquet and ORC data and partition values; Avro data keeps its bits. It changes which bits later reads see on RePark-written tables, so it moves storage cells only (§5, `nat_parquet_l5`). |
| H5 | `array_sort`/`sort_array` shim; `greatest`, `least`, `array_max`, `array_min` dispatch | `collection/array_sort.rs:21-22`; `function_dispatch.rs:290-297`, `:444-451`, `:466-473` | **no** | They forward to C5, D1 and B6 unchanged. These are the natural homes for the array and `greatest`/`least` seats. |

## What the inventory says about seats

- **One root, three families of consumers.** A1 is the cause everywhere. The consumers group as: per-value
  comparison (A2, A3: E1–E4), order (A4, A5: C1–C5, B3), and reductions (A6: B1, B4, B6, D1), plus two that are
  wrong for every NaN shape, not just sign-set: the grouped accumulator (B2) and the statistics paths (B5, F2,
  G2, G3).
- **Part B already covers C1–C4 and E5–E7.** What remains for this unit: E1–E4 (literal comparisons), B1–B6,
  C5, D1, and the pruning seats F2, G2, G3, G4.
- **No seat inside Arrow can be changed** without forking Arrow; every proposal in the [sketch](sketch.md)
  sits in RePark's plan rewrite or in RePark-owned function bindings, with two optional fork changes (G4's
  `Datum` order and G3's `contains_nan` read) and one upstream DataFusion fix (B2).
