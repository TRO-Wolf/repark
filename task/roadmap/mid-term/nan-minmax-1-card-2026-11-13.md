# Card NAN-MINMAX-1: float `max`/`min` and the Parquet statistics RePark alone consumes answer as Spark does

**Date:** 2026-11-13. **Filed by:** Claude Opus 5.5 (`claude-opus-5-5`), the part 1 builder. **Source:** the
[NAN-COMPARE-SPARK-1 sketch](../../wo/nan-compare-spark-1/sketch.md) §4.2(a) and §5 (unit 1), its inventory rows B5
and I2 and its xhigh critic (M1); the owner's rulings of 2026-11-13: "Go with default" (about 20:00 UTC, Q1: this unit
runs **before** the scoped label ICE-V2-FLAT-AWS-LABEL-1) and "defaults" (about 20:50 UTC) on the sketch's twelve
questions, among them Q3 (the statistics seat first, as its own PR, S1), Q4 (an `Inexact` `FileFormat` wrapper, not
`collect_statistics` off), Q5 (pruning follows Spark's own reader) and Q10 (PARQUET-NAN-EQUALITY-1's constant-column
half moves here). Evidence: [task/wo/nan-minmax-1/](../../wo/nan-minmax-1/map.md).

**Severity:** S1, label-blocking. **Status:** part 1 **FIXED** on branch `fix/nan-minmax-stats-1` (not merged), after
one delta round: both verifiers failed #1333 on records (class (b), no class (a) S1/S2), and the delta fixed verifier
1's F1. Items (i) and (ii) below are **owner words owed**. Part 2 **open**.

## Part 1: the float statistics seat (FIXED, unmerged)

**What was wrong** (step 0 on main `631d283c`, [table](../../wo/nan-minmax-1/evidence/step0/table.md)). Parquet's
footer min and max leave NaN out, and DataFusion marked them exact. Two RePark-only consumers acted on that:

- **the constant-column replacement** (inventory I2): a plain Parquet file whose other float values are one value
  read every NaN as that value, in the projection and the predicate;
- **the `max`/`min` shortcut** (inventory B5): `max(d)` was answered from the footer without the NaN.

Measured on canonical, sign-set and payload NaN, DOUBLE and FLOAT, one file, row groups and many files, both doors:
every NaN read as `1.0`, `isnan` counted 0 and `max` answered `1.0`, where Spark reads NaN. The wrong read became a
**wrong write**: `spark.read.parquet(f).write.parquet(…)` and `spark.read.parquet(f).writeTo(<Iceberg>)` stored `1.0`
for the NaN rows (sketch §7 L3), as pyarrow and Spark read them back.

**Every read path** ([paths.md](../../wo/nan-minmax-1/evidence/step0/paths.md)): `spark.read.parquet`,
`read.format("parquet").load` and the native `read_parquet` reach the consumers through `spark_nullable.rs`; the
native `CREATE EXTERNAL TABLE … STORED AS PARQUET` reaches them through the session's `"parquet"` file-format
factory. `` parquet.`p` ``, `CREATE TABLE … USING parquet` and the Spark door's `CREATE EXTERNAL TABLE` are refused.

**The seat.** `crates/repark-core/src/spark_nullable.rs`: `FloatBoundsInexactFormat`, a `FileFormat` wrapper around
DataFusion's Parquet format, with two demotions to `Precision::Inexact`:

- **per file** (`infer_stats`, `infer_stats_and_ordering`): a FLOAT or DOUBLE column whose footer reports
  `min == max`. That is the constant-column replacement's only trigger, so it never fires on a float column.
  **The delta adds** a float column with a row group that holds a non-null value but no footer min or max (all NaN,
  or NULL and NaN; verifier 1's F1). DataFusion merges the other row groups' bounds into an exact file bound
  (`datafusion-datasource-parquet-54.1.0/src/metadata.rs:609-641`, `:748`), so the file pruner dropped the whole
  file: `WHERE d > 5.0` and `= NaN` answered 0 where Spark answers 2, on main and on #1333's head `c9c3b8d4` alike
  (the sketch's shape kept the file). The wrapper reads the footer from DataFusion's file-metadata cache
  (`unbounded_float_columns`). An all-NULL row group keeps the bounds exact, and a footer it cannot read demotes
  every float bound of that file. The fix commit is `1a8f2749`; its evidence is
  [delta/](../../wo/nan-minmax-1/evidence/delta/map.md).
- **per scan** (`create_physical_plan`): every float `min_value`/`max_value` in the table and file-group statistics,
  which are what the `max`/`min` shortcut reads.

Everything else is delegated. `read_parquet_nullable` builds its `ListingTable` with the wrapper, and
`FloatBoundsInexactFactory` replaces the session's `"parquet"` factory (`crates/repark-core/src/session/df_guards.rs`).
Row counts, null counts and every non-float statistic stay exact; Iceberg reads are untouched (the fork reports no
column statistics).

**Why not every float bound per file** (the sketch's first shape, measured first). DataFusion's file pruner takes only
`Exact` bounds (`datafusion-common-54.1.0/src/pruning.rs:330-367`), so demoting every float bound per file turned off
file-level pruning. Row-group pruning still kept the scan to one row group of 200, but the 100-file selective filter
ran 1.166x main. The sketch's "file pruning: Inexact is enough" does not hold in DataFusion 54.1.0.

**What a spread file's exact bounds still feed** (every row group bounded, so the delta leaves them exact):

- **file pruning** of the user's predicate, which cuts no row that row-group pruning by the same footers would keep;
  Spark's reader prunes by those footers too (Q5);
- **DataFusion's aggregate dynamic filter** for `max`/`min`, which Spark does not have (verifier 2's B2). Over nine
  high files and one low spread file whose last row is NaN, `max(d)` missed the NaN in 29 of 30 runs; Spark answers
  NaN. Part 2's I8 owes it, and a strict xfail pins it.

**After** (the head): 126 of main's 136 unequal cells equal Spark; the other ten are the four refused paths and the
six sign-set cells below. The delta adds verifier 1's three files with such a row group (`R_one`, `R_many`,
`R_nullnan`): `d > 5.0` and `= NaN` equal Spark on both doors. The pins:
`crates/repark-core/src/spark_nullable/float_bounds_tests.rs` (each demotion and the factory fail alone when removed)
and `python/repark/tests/io/test_nan_minmax_stats_1.py`.

**Cost** ([cost](../../wo/nan-minmax-1/evidence/cost/map.md)): six alternating pairs on the delta head `1a8f2749`.
`count(*)` over 100 Parquet files 1.024x and a selective filter 1.034x main (#1333's head: 1.040x and 0.958x), the
filter pruning exactly as main (108 of 109 file ranges, then 1 of 2 row groups). `max(d)` over a ten-million-row file
with statistics now reads the column: 2 ms to 21 ms (reported, not held to 1.05x). On verifier 1's five 100-file
sets, file ranges and row groups equal main's; they move only on the three files with an unbounded row group.

## Owner words owed

**(i) The seat shape.** The middle shape above: per file, equal float bounds and float bounds with an unbounded row
group turn inexact; per scan, every float bound. A spread file keeps file pruning (the cost rows), and its exact bounds
still feed the aggregate dynamic filter (part 2, I8).

**(ii) The cells that move from equal-to-Spark to unequal: a family, not one cell.** Counts are from the two verdicts
(verifier 1's grid of 2,577 head cells, verifier 2's of 4,784; a cell is one query's whole answer):

| family | verifier 1 | verifier 2 | why main was equal | owed by |
|---|---|---|---|---|
| sign-set `max`/`min` on constant files (`min`: Spark 1.0, head NaN) | the `min` half of rows already unequal on main | the same | the shortcut, or the constant replacement with a filter in the query | part 2 (B1–B3) |
| sign-set `min` on a spread file (`pa_spread_one_s`, no filter) | the `min` half of 1 row | the same | the shortcut | part 2 (B1–B3) |
| sign-set `>` and `>=` on constant files (`d >= 1.0`: Spark ids 1–4, head 2, 4) | 32 cells | 20 cells | the constant replacement read the NaN as `1.0` | NAN-COMPARE-SPARK-1's literal unit, NAN-COMPARE-LITERAL-1 (sketch §4.1, E1–E4) |
| a join to a NaN literal on constant files (Spark 0, head 2 to 4) | 56 cells | 56 cells | the NaN read as the number | NAN-COMPARE-SPARK-1 (sketch §6): Spark's 0 is its reader pruning the join-inferred `d = NaN` by the NaN-free footer; RePark infers no such filter and already answers the NaN rows on every spread file |
| grouped `min` over a canonical NaN (`pa_many_one_nan_file_c`: Spark 1.0, head NaN) | 1 cell, row-order dependent (2 answers in 6 runs) | — | the shortcut | part 2's grouped accumulator (B1–B3) |

The verifiers' totals of moved cells, 88 and 76, are the third and fourth families (32 + 56, 20 + 56); the first two
move only the `min` inside rows main's `max` already made unequal. Main's answers here were right only because it read
NaN as a number or skipped it: where neither the shortcut nor the replacement fires (a spread file, and for `min` a
filter in the query), main already answers each family as the head does. **Corrected (B3):** the first records said
main answers the sign-set `min` as NaN "whenever the shortcut does not fire (a filter in the query, data in memory)".
That holds on a spread file only; on the constant files, main answers `min 1.0` with `WHERE id > 0` too, through the
constant replacement. The builder's ad-hoc probe used a spread file. The facade test holds the sign-set `max`/`min`
and `d >= 1.0` over `one_s` as strict xfails. RePark's own arithmetic makes sign-set NaN, and its plain-Parquet path
write keeps the bits. Against these, the head turns 757 (verifier 1) or 852 (verifier 2) wrong cells right.

## Part 2: RePark's float `max`/`min` (open)

The rest of sketch §4.2(a), a later brief:

- a float `max`/`min` UDAF whose grouped accumulator starts from no value (inventory I1: `max` of a `-Infinity`-only
  group answers `-1.7976931348623157e+308`) and orders every NaN as one value above `+Infinity` (B1–B3), ungrouped and
  in sliding windows, and `array_min`/`array_max`;
- its internal name not `max`/`min`, so DataFusion's aggregate dynamic filter does not attach (I8);
- quantiles: `median` and `percentile_approx` over a sign-set NaN (I4);
- the pins on the TopK dynamic filter (I3) and on a multi-file ungrouped `max` with dynamic filters on: verifier 2's
  B2 (nine high files and one low spread file holding a NaN; `max(d)` NaN on Spark) is the strict xfail
  `test_max_over_many_files_with_a_nan_in_a_spread_file_equals_spark`.

## Close

Part 2 merged with its pins, and the sign-set `max`/`min` and the multi-file `max` xfails in
`test_nan_minmax_stats_1.py` turned green. The `d >= 1.0` xfail closes with NAN-COMPARE-LITERAL-1.
