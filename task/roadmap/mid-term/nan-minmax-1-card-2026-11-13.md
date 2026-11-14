# Card NAN-MINMAX-1: float `max`/`min` and the Parquet statistics RePark alone consumes answer as Spark does

**Date:** 2026-11-13. **Filed by:** Claude Opus 5.5 (`claude-opus-5-5`), the part 1 builder. **Source:** the
[NAN-COMPARE-SPARK-1 sketch](../../wo/nan-compare-spark-1/sketch.md) §4.2(a) and §5 (unit 1), its inventory rows B5
and I2 and its xhigh critic (M1); the owner's rulings of 2026-11-13: "Go with default" (about 20:00 UTC, Q1: this unit
runs **before** the scoped label ICE-V2-FLAT-AWS-LABEL-1) and "defaults" (about 20:50 UTC) on the sketch's twelve
questions, among them Q3 (the statistics seat first, as its own PR, S1), Q4 (an `Inexact` `FileFormat` wrapper, not
`collect_statistics` off), Q5 (pruning follows Spark's own reader) and Q10 (PARQUET-NAN-EQUALITY-1's constant-column
half moves here). Evidence: [task/wo/nan-minmax-1/](../../wo/nan-minmax-1/map.md).

**Severity:** S1, label-blocking. **Status:** part 1 **FIXED** on branch `fix/nan-minmax-stats-1` (not merged);
part 2 **open**.

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
- **per scan** (`create_physical_plan`): every float `min_value`/`max_value` in the table and file-group statistics,
  which are what the `max`/`min` shortcut reads.

Everything else is delegated. `read_parquet_nullable` builds its `ListingTable` with the wrapper, and
`FloatBoundsInexactFactory` replaces the session's `"parquet"` factory (`crates/repark-core/src/session/df_guards.rs`).
Row counts, null counts and every non-float statistic stay exact; Iceberg reads are untouched (the fork reports no
column statistics).

**Why not every float bound per file** (the sketch's first shape, measured first). DataFusion's file pruner takes only
`Exact` bounds (`datafusion-common-54.1.0/src/pruning.rs:330-367`), so demoting every float bound per file turned off
file-level pruning. Row-group pruning still kept the scan to one row group of 200, but the 100-file selective filter
ran about 1.18x main. A spread file's exact bounds feed only pruning, which cuts NaN as row-group pruning by the same
footer does, and as Spark's reader does (Q5). The sketch's "file pruning: Inexact is enough" does not hold in
DataFusion 54.1.0.

**After** (the head): 126 of main's 136 unequal cells equal Spark; the other ten are the four refused paths and the
six sign-set cells below. The pins: `crates/repark-core/src/spark_nullable/float_bounds_tests.rs` (each demotion and
the factory fail alone when removed) and `python/repark/tests/io/test_nan_minmax_stats_1.py`.

**Cost** ([cost](../../wo/nan-minmax-1/evidence/cost/map.md)): six alternating pairs. `count(*)` over 100 Parquet files 1.040x and a selective
filter 0.958x main, the filter pruning exactly as main (108 of 109 file ranges, then 1 of 2 row groups). `max(d)` over
a ten-million-row file with statistics now reads the column: 2 ms to 21 ms (reported, not held to 1.05x).

**A cell part 1 moves from right to wrong.** Over a file holding a sign-set NaN (`fff8…`), main's `min` answered from
the footer, which was right by accident; the head's `min` answers the sign-set NaN, as main already did whenever the
shortcut did not fire (a filter in the query, data in memory). `max` there is wrong on both. Part 2 owes it; the
facade test holds it as a strict xfail. RePark's own arithmetic makes sign-set NaN, and its plain-Parquet path write
keeps the bits.

## Part 2: RePark's float `max`/`min` (open)

The rest of sketch §4.2(a), a later brief:

- a float `max`/`min` UDAF whose grouped accumulator starts from no value (inventory I1: `max` of a `-Infinity`-only
  group answers `-1.7976931348623157e+308`) and orders every NaN as one value above `+Infinity` (B1–B3), ungrouped and
  in sliding windows, and `array_min`/`array_max`;
- its internal name not `max`/`min`, so DataFusion's aggregate dynamic filter does not attach (I8);
- quantiles: `median` and `percentile_approx` over a sign-set NaN (I4);
- the pins on the TopK dynamic filter (I3) and on a multi-file ungrouped `max` with dynamic filters on.

## Close

Part 2 merged with its pins, and the sign-set `max`/`min` xfail in `test_nan_minmax_stats_1.py` turned green.
