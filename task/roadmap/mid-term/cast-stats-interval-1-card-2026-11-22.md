# Card CAST-STATS-INTERVAL-1: a cast that reorders values carried its column's statistics, and a full-range BIGINT bound broke filter selectivity

**Date:** 2026-11-22. **Filed and built by:** the builder (`claude-opus-5-5`), branch
`fix/cast-stats-interval-1` off main `c5370be6`. **Source:** a workload measurement of plain Parquet reads against
Spark 4.1.2 (two NEW S2 families, both ANSI modes); a third and a fourth family, S1, found while answering which
other paths read the same statistics (the fourth by the PR's verifier). **Severity:** S2 (two loud internal
errors) and **S1** (silent wrong `max`/`min`; silent rows dropped by footer pruning), all in released 1.5.6 and on
main.

**Ids (owner, 2026-11-22: "Go with your recommendations"):** CAST-STATS-INTERVAL-1 holds the two loud S2s (defects
1 and 2); **CAST-STATS-SILENT-1** (S1) holds the two silent wrong answers (defects 3 and 4), with its own registry
entry and the section below. One barrier closes all four.

## The defects

1. **STATS-INTERVAL-ASSERT (S2).** DataFusion 54.1.0 carries a column's Parquet statistics through a `CAST` by
   casting its min and max (`datafusion-physical-expr` `projection.rs`, `project_column_statistics_through_expr`,
   and `expressions/cast.rs`, `evaluate_bounds`). A STRING column's min and max are string order: ids `'10'`, `'3'`,
   `'25'`, `'7'` have min `'10'` and max `'7'`, so `CAST(id AS INT)` reported the range `[10, 7]`. Any filter planned
   above that projection (a `row_number()` dedup's `rn = 1`, a `BETWEEN` on the cast, which common subexpression
   elimination turns into a projection) built its interval from it and failed:
   `Internal error: Assertion failed: interval.lower.is_null() || ... Interval's lower bound 10 is greater than the
   upper bound 7`. Ids of mixed widths (`'10'` and `'9'`) or with a leading zero (`'04'`) always do this.
2. **STATS-SELECTIVITY (S2).** DataFusion 54.1.0 counts an integer interval's values as `distance + 1` in `u64`
   (`datafusion-expr-common` `interval_arithmetic.rs`, `cardinality`). A BIGINT column whose statistics are exactly
   `[i64::MIN, i64::MAX]` counts zero, so `x > 0`, `x = 5` and `CAST(x AS BIGINT) > 0` failed with
   `Selectivity is out of limit: inf`. Two files `[MIN, -1]` and `[0, MAX]` merge to the same range.
3. **STATS-CAST-MINMAX-FOLD (S1, new).** The same carried statistics answer `max` and `min` of the cast from the
   footer without reading a row (DataFusion's statistics shortcut): `max`/`min` of `CAST(id AS INT)` answered
   `7`, `10` for Spark's `25`, `3`; of BIGINT `9, 10, 11` cast to STRING `'11'`, `'9'` for `'9'`, `'10'`; of BIGINT
   `-1, 0, 1` cast to BOOLEAN `true`, `true` for `true`, `false`. No error, wrong values.
4. **STATS-CAST-NAN-PRUNE (S1, new; found by the verifier, delta 1).** DataFusion's `PruningPredicate` accepts a
   FLOAT or DOUBLE to integer cast and prunes by the cast footer bounds. The footer leaves NaN out, and the cast
   maps NaN inside the range (Spark: 0 under ANSI off, `CAST_OVERFLOW` under ANSI on). One row group of DOUBLE
   `5.0, NaN, 10.0` (footer `[5.0, 10.0]`): `CAST(d AS INT) = 0` answered no row where Spark answers row 2 (ANSI
   off) or refuses (ANSI on). No error, rows dropped. The same through DOUBLE to FLOAT (delta 2, found by the PR's
   re-check): `CAST(d AS FLOAT) > 100`, `>= 11`, `= CAST('NaN' AS FLOAT)` and `CAST(CAST(d AS FLOAT) AS DOUBLE) >
   100` answered no row where Spark answers row 2 in both modes, because Spark does not push a narrowing cast down;
   also over a dictionary-encoded column and a NULL-plus-NaN row group (footer `[3.0, 3.0]`, Spark row 1).

## Measured (Spark 4.1.2; release packages of `c5370be6` and of the branch head)

Synthetic Parquet, both ANSI modes (every RePark cell answered the same in both). "error" is DataFusion's internal
assertion above.

| Case | Spark 4.1.2 | `c5370be6` | head |
|---|---|---|---|
| `row_number()` dedup over ids `1, 2, 3, 04` cast to INT | 4 rows | error (`[4, 3]`) | 4 rows, equal |
| the same over `10, 3, 25, 7`; over two files of two row groups | 4 rows; 8 rows | error | equal |
| `= 4` and `IN (8, 100)` after the dedup | 1 row; 2 rows | error | equal |
| `CAST(id AS INT) BETWEEN 5 AND 20` (DataFrame and SQL), `BETWEEN 8 AND 30` over the two files | 2 rows; 5 rows | error | equal |
| `max`/`min` of `CAST(id AS INT)`: `10, 3, 25, 7`; `05, 10, 6, 055`; the two files | `25, 3`; `55, 5`; `100, 2` | **`7, 10`; `6, 5`; `9, 11`** | equal |
| `max`/`min` of BIGINT `9, 10, 11` as STRING; of `-1, 0, 1` as BOOLEAN | `'9', '10'`; `true, false` | **`'11', '9'`; `true, true`** | equal |
| `max`/`min` of INT `9, 10, 11` as BIGINT (an order-keeping cast) | `11, 9` | `11, 9`, folded from the footer | `11, 9`, still folded |
| `x > 0`, `x = 5`, `CAST(x AS BIGINT) > 0`, `filter(x > 0)` over `0, MAX, MIN, 5, NULL` | rows 1, 3; 3; 1, 3 | error (`inf`) | equal |
| the same over two files `[MIN, -1]` and `[0, 5, MAX]` of row groups of two | rows 3, 4; 3; 3, 4 | error (`inf`) | equal |
| `= 10`, `> 8`, `IN (10, 55)`, `BETWEEN 9 AND 60` over `05, 10, 6, 055` (range `[5, 6]`, wrong but ordered) | Spark's rows | equal | equal |
| one row group DOUBLE `5.0, NaN, 10.0`: `CAST(d AS INT) = 0`, `CAST(d AS BIGINT) <= 0`, `CAST(d AS SMALLINT) < 3`, `CAST(d AS INT) IN (0, 100)`, `count(*)`, a FLOAT column, `filter(col("d").cast("int") == 0)` | ANSI off: row 2 (`count` 1); ANSI on: `ArithmeticException` `[CAST_OVERFLOW]` | **no row (`count` 0), both modes** | refuses, both modes: `PySparkException` "Arrow error: Cast error: Can't cast value NaN to type Int32" (Int64, Int16 alike) |
| the same file: `CAST(d AS FLOAT) > 100`, `>= 11`, `= CAST('NaN' AS FLOAT)`, `CAST(CAST(d AS FLOAT) AS DOUBLE) > 100`; dictionary-encoded; NULL-plus-NaN group | row 2 (row 1 for the NULL-plus-NaN group), both modes | **no row** | equal (delta 2) |
| the same file: `d > 100`; `CAST(f AS DOUBLE) > 100`; `CAST(f AS DOUBLE) > 7` | none; none; rows 2, 3 | equal | equal |

The first two of those last controls skip the NaN row on every engine: RePark prunes them by the footer, and Spark
pushes them down and its reader skips the NaN row too (with `spark.sql.parquet.filterPushdown=false` Spark answers
it). They are equal because both prune, not because the NaN is read; this unit leaves them as they are.

Under ANSI on the head refuses as Spark does, with a different class: RePark raises `PySparkException` with Arrow's
cast message and no `CAST_OVERFLOW` error class or SQLSTATE. Under ANSI off it refuses where Spark answers 0 for the
NaN: that is RePark's separate cast gap (NaN to an integer type under ANSI off), held by a strict xfail; this unit
only removes the silent drop.

The pins hold the measured Spark answers: `python/repark/tests/io/test_cast_stats_interval_1.py`.

## CAST-STATS-SILENT-1 (S1, silent)

- **(a) `max`/`min` from the footer** (defect 3). Witnesses: `max`/`min` of `CAST(id AS INT)` over `10, 3, 25, 7`
  answered `7, 10` (Spark `25, 3`); over `05, 10, 6, 055` `6, 5` (`55, 5`); over two files of two row groups
  `9, 11` (`100, 2`); BIGINT `9, 10, 11` to STRING `'11', '9'` (`'9', '10'`); BIGINT `-1, 0, 1` to BOOLEAN
  `true, true` (`true, false`).
- **(b) pruning through a float cast to an integer, DECIMAL or narrower float type** (defect 4). Witness: one row group of DOUBLE `5.0, NaN, 10.0`,
  `CAST(d AS INT) = 0`, also BIGINT, SMALLINT, `IN`, `count(*)`, a FLOAT column and the DataFrame route: no row on
  main; Spark answers row 2 (ANSI off) or `[CAST_OVERFLOW]` (ANSI on). DOUBLE to FLOAT: the four shapes above,
  dictionary-encoded and NULL-plus-NaN, no row on main where Spark reads the NaN row in both modes.
- **Fix in this PR:** the barrier below; (b) needs the float arm: a FLOAT or DOUBLE source keeps its statistics only
  to the same or a wider float type (delta 1 barred integer and DECIMAL targets, delta 2 DOUBLE to FLOAT). (a) now
  equals Spark; (b) through an integer type refuses loudly in both modes (the ANSI-off NaN-to-0 cast gap stays its
  own finding); (b) through DOUBLE to FLOAT equals Spark. Uncast float filters and FLOAT to DOUBLE keep pruning by
  the footer and skip a NaN row, as Spark's pushdown does; this unit claims nothing for them.
- **Workaround on 1.5.6** (measured on `c5370be6`): for (a), `try_cast` in place of `CAST` (`25, 3`), or
  `datafusion.execution.collect_statistics` set to false; for (b) none (that key and
  `datafusion.execution.parquet.pruning` set to false still drop the row).
- **Pins:** `test_max_and_min_of_a_string_id_cast_to_int_read_the_rows`,
  `test_max_and_min_of_casts_that_reorder_values_read_the_rows`, the `max`/`min` SQL and native-door cells,
  `test_a_float_cast_to_an_integer_reads_the_nan_row_its_footer_leaves_out` (14),
  `test_a_double_cast_to_float_reads_the_nan_row_its_footer_leaves_out` (12), the controls
  `test_uncast_and_widening_float_filters_follow_spark_pushdown` (6) and the strict xfail
  `test_an_ansi_off_float_cast_to_an_integer_answers_spark_rows` (7), in
  `python/repark/tests/io/test_cast_stats_interval_1.py`; the order table in `stats_barrier_tests.rs`.

## Fix

- **The barrier.** `StatsBarrierRule` (`repark_stats_barrier`, the last rule of the core optimizer list; the Spark extension's
  `DecimalInRanges` runs after it and builds no casts,
  `crates/repark-core/src/session/df_guards/cast_shield/stats_barrier.rs`) wraps the operand of every `CAST` whose
  value order the cast does not keep in `__repark_stats_barrier__(x)`, an identity UDF. DataFusion then knows no
  bounds for the cast: no inverted range, no interval, no shortcut. The order-keeping family is DataFusion's own
  (`is_order_preserving_cast_family`: numeric or BOOLEAN to numeric, the same type), narrowed so that a FLOAT or
  DOUBLE source keeps order only to the same or a wider float type (delta 1 barred a float to an integer or DECIMAL,
  delta 2 DOUBLE to FLOAT, so pruning no longer reads the footer through them), plus text to text and binary to
  binary, narrowed on time to DATE and zone-less TIMESTAMP conversions and TIMESTAMP to TIMESTAMP with the zone
  kept or kept absent. Those casts keep their statistics. A `TRY_CAST` is left alone (DataFusion carries nothing
  through it), as is a cast of a constant. Names are kept.
- **The extreme bound.** The plain Parquet format (`spark_nullable.rs`, both seats: per file and per scan, also when
  a filter is pushed into the scan) drops a BIGINT, UNSIGNED BIGINT, `Date64` or TIMESTAMP bound that sits at its
  type's extreme (`spark_nullable/extreme_bounds.rs`). An absent bound already means that extreme, so nothing is
  lost, and a full range cannot form when files merge. The cost: a `max` or `min` over a column whose bound sits at
  its type's extreme no longer folds from the footer and scans instead (same answer).
- **Caveat on the order-keeping list.** It holds only while RePark refuses overflowing narrowing integer casts
  under ANSI off. Spark wraps them (`max` of `CAST(1 AS TINYINT)` and `CAST(300 AS TINYINT)` is 44), so a folded
  `max` would be wrong; when that refusal is fixed, narrowing integer casts must leave the list.

No answer that equalled Spark changed: the barrier is an identity, and the dropped bound was the type's own limit.

## Which other paths read these statistics (point 3 of the brief)

- **`max`/`min` from statistics:** yes, the S1 above. Fixed by the barrier.
- **Row-group, page and file pruning:** for casts from and to text, no. They read the Parquet footer and page index
  through DataFusion's `PruningPredicate`, which refuses a cast between a string and a non-string type
  (`datafusion-pruning` `pruning_predicate.rs`, `verify_support_type_for_prune`), so a STRING id cast to INT never
  prunes. **For a FLOAT or DOUBLE cast to an integer type, yes: on main pruning dropped the NaN row of a file whose
  footer leaves NaN out (family 4 above), a second silent path; DOUBLE to FLOAT did the same.** Delta 1 and 2 bar
  those casts, so the footer is no longer read through them. Uncast float pruning (`d > 100`) and FLOAT to DOUBLE are
  left alone: they still skip a NaN row, and equal Spark only because Spark's pushdown skips it too. Measured: a wrong but ordered range (`[5, 6]` while 10 and 55 are present) drops no row under `=`, `>`,
  `IN` and `BETWEEN` on main and head. An INT to BOOLEAN cast is not refused there; over row groups
  `[-1, 0, 1]` and `[5, 6, 7]`, `CAST(x AS BOOLEAN) = false` and `NOT CAST(x AS BOOLEAN)` keep the 0 row on both.
- **A join's dynamic filter and a sort's TopK filter:** runtime predicates over the key expression, pruned by the
  same `PruningPredicate` and evaluated per row. A join on a cast key and `ORDER BY` the cast `LIMIT 2` equal Spark
  on main and head.
- **Sort elimination:** reads equivalence ordering, which DataFusion derives only for its order family, so a STRING
  to INT cast is never taken as sorted.

## Not covered (residual)

The selectivity fault is DataFusion's (`cardinality` should not wrap at `u64::MAX`). The guard stops a full range
at its source; a plan that builds one from literals at the type's limits (`x >= -9223372036854775808 AND x <=
9223372036854775807` under a second filter that cannot merge with it) can still reach it. To be reported upstream
with the cast fault (`project_column_statistics_through_expr` and `CastExpr::evaluate_bounds` should carry bounds
only through an order-keeping cast). Iceberg scans report no column bounds (row counts only), so neither fault
reaches them.

## Pins

`python/repark/tests/io/test_cast_stats_interval_1.py` (facade, both ANSI modes; the native door for the full range
and the cast `max`/`min`), `crates/repark-core/src/session/df_guards/cast_shield/stats_barrier_tests.rs`,
`crates/repark-core/src/spark_nullable/extreme_bounds_tests.rs`. With the barrier rule removed from the optimizer
list, every cast pin goes red (53, the 26 NaN pins among them) and the full-range pins and controls stay green
(36); with the whole float arm reverted, exactly the 26 NaN pins (14 integer, 12 DOUBLE to FLOAT) go red; with only
delta 2's narrowing reverted, exactly the 12 DOUBLE to FLOAT pins go red. On `c5370be6` 71 of 89 fail and the 7
strict xfails hold; on the head 89 pass.
