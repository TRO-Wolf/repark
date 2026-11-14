# Card NAN-MINMAX-1: float `max`/`min` and the Parquet statistics RePark alone consumes answer as Spark does

**Date:** 2026-11-13. **Filed by:** Claude Opus 5.5 (`claude-opus-5-5`), the part 1 builder. **Source:** the
[NAN-COMPARE-SPARK-1 sketch](../../wo/nan-compare-spark-1/sketch.md) §4.2(a) and §5 (unit 1), its inventory rows B5
and I2 and its xhigh critic (M1); the owner's rulings of 2026-11-13: "Go with default" (about 20:00 UTC, Q1: this unit
runs **before** the scoped label ICE-V2-FLAT-AWS-LABEL-1) and "defaults" (about 20:50 UTC) on the sketch's twelve
questions, among them Q3 (the statistics seat first, as its own PR, S1), Q4 (an `Inexact` `FileFormat` wrapper, not
`collect_statistics` off), Q5 (pruning follows Spark's own reader) and Q10 (PARQUET-NAN-EQUALITY-1's constant-column
half moves here). Evidence: [task/wo/nan-minmax-1/](../../wo/nan-minmax-1/map.md).

**Severity:** S1, label-blocking. **Status:** part 1 **FIXED** on branch `fix/nan-minmax-stats-1` (not merged), after
two delta rounds: both verifiers failed #1333 on records (class (b), no class (a) S1/S2), and the first delta fixed
verifier 1's F1. Its re-check failed the head on **N1**, a class (a) S1 that part 1 introduced, and the second delta,
a cap exception on the #1261 precedent, fixed it (below). Items (i) and (ii) are **ruled** (owner, 2026-11-14, about
09:50 UTC, "Go with defaults"). Part 2 **open**.

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
- **DataFusion's aggregate dynamic filter** for `max`/`min`: that consumer is **off by default** since the second
  delta, because it prunes where Spark's reader does not (Q5 (E)). Spark has no such filter. It was wrong twice:
  - **B2** (verifier 2): over nine high files and one low spread file whose last row is NaN, `max(d)` missed the NaN
    in 29 of 30 runs; Spark answers NaN. With the filter off it answers NaN in 10 of 10 runs on both doors.
  - **N1** (the re-check), below.

**N1: found by the re-check, fixed in the second delta.** An ungrouped `max`/`min` over plain Parquet **with no NaN**,
where a file range or row group is all NULL, answered a wrong `min`, sometimes run to run. The cause is a **typed
NULL, not NaN**: in DataFusion 54.1.0 (`datafusion-physical-plan-54.1.0/src/aggregates/no_grouping.rs:254-262`),
`scalar_cmp_null_short_circuit` matches only the untyped `ScalarValue::Null`. A partition that has seen only NULLs
evaluates to `Float64(None)` (or `Int64(None)`: any type), which `scalar_min` ranks below every value and keeps, so
the shared `min` bound becomes NULL. The filter then narrows from `d < min OR d > max` to `d > max`, and file and
row-group pruning drop the rows that hold the true `min`.

| cell (10 runs, each door) | Spark 4.1.2 | main `631d283c` | head `1a8f2749` | delta 2 `d7ae795a` |
|---|---|---|---|---|
| three files `[3,4]`, `[NULL,NULL \| 1,2]`, `[5,6]`: `max(d), min(d)`, `max(f), min(f)`, `min(d), max(id)` | `min` 1.0 | right (the footer) | `min` 3.0 | right |
| one 1 M-row file, first row group of 100,000 all NULL: the same three queries | `min` 100010 | right (the footer) | `min` 200010 to 500010 in some runs | right |
| P1: the same file, integer `max(k), min(k) WHERE id >= 0` | 1000009, 100010 | `min` 200010 | `min` 600010 | right |

- Main never hit it unfiltered: it answered `max`/`min` from the footer. Part 1 removed that shortcut for float
  columns, so the filter ran. Main is wrong only where its shortcut cannot fire: **P1**, a filtered `max`/`min` of
  any type, which the same switch fixes.
- **The fix** (`crates/repark-core/src/session/df_guards.rs`, `apply_df_54_1_config_guards`, the seat the two
  DataFusion 54.1 regression guards use): `datafusion.optimizer.enable_aggregate_dynamic_filter_pushdown = false` as a
  RePark session default. A user still opts in with that key (`spark.conf.set`, SQL `SET` or the builder), and a pin
  shows the filter returns to the plan.
- **The umbrella key.** DataFusion's `ConfigOptions::set` writes all four dynamic-filter keys from
  `datafusion.optimizer.enable_dynamic_filter_pushdown` (`datafusion-common-54.1.0/src/config.rs:1556-1561`).
  Measured on `1a8f2749` ([umbrella-1a8f2749.txt](../../wo/nan-minmax-1/evidence/delta2/umbrella-1a8f2749.txt)):
  after the specific key was set `false`, the umbrella set `true` brought `min` 3.0 back on the three files while
  `spark.conf.get` of the specific key still answered `false`. So RePark refuses the umbrella
  at `true` on every door (SQL `SET`, `spark.conf.set`, the builder), naming the specific keys; `false` stays
  allowed, since it turns every dynamic filter off.
- **Residual, measured, not fixed:** `RESET` of the specific key, and `spark.conf.unset` of it, restore DataFusion's
  own default `true` and turn the filter back on: `min` 3.0 in 10 of 10 runs on both doors, and after the SQL `RESET`
  `spark.conf.get` still answers `false`
  ([umbrella-d7ae795a.txt](../../wo/nan-minmax-1/evidence/delta2/umbrella-d7ae795a.txt)).
  Neither reaches the guard's seat; a third product file would. An owner word is owed on whether a later unit
  closes it.
- **Upstream:** the DataFusion defect (`no_grouping.rs:254-262`; the same function is at
  `aggregate_stream.rs:254-262` in 55.2.0) is to be reported upstream: the short circuit should catch a typed NULL
  (`is_null()`). No issue is filed yet.
- Spark's answers were measured live on the pins' own files (Spark 4.1.2, 2026-11-14,
  [spark-4.1.2.txt](../../wo/nan-minmax-1/evidence/delta2/spark-4.1.2.txt)); the main and head columns are the
  re-check's and the red runs of the pins.

**After** (the head): 126 of main's 136 unequal cells equal Spark; the other ten are the four refused paths and the
six sign-set cells below. The delta adds verifier 1's three files with such a row group (`R_one`, `R_many`,
`R_nullnan`): `d > 5.0` and `= NaN` equal Spark on both doors. The second delta adds N1's and P1's cells, B2 as a
positive pin, and the switch pins, each over 10 runs on both doors. The pins:
`crates/repark-core/src/spark_nullable/float_bounds_tests.rs` (each demotion and the factory fail alone when removed;
the three-file `min` fails with the default removed; the umbrella refusal and the specific key's opt-in) and
`python/repark/tests/io/test_nan_minmax_stats_1.py`.

**Cost** ([cost](../../wo/nan-minmax-1/evidence/cost/map.md)): six alternating rounds of main `631d283c`, the first
delta `1a8f2749` and the second delta `d7ae795a` (release wheels; each sample under the exclusive measure lock, then
both cargo locks, on cores 48-57; raw `cost-raw-d7ae795a.jsonl`; the C-012 table
[delta2/c012-l4l.md](../../wo/nan-minmax-1/evidence/delta2/c012-l4l.md)). Second delta against main, median of pair
ratios:

| row | main (ms) | `1a8f2749` (ms) | `d7ae795a` (ms) | `d7ae795a` / main |
|---|---|---|---|---|
| `count(*)`, 100 files (held) | 4.50 | 4.37 | 4.26 | 0.963 |
| selective filter, 100 files (held) | 10.52 | 10.22 | 10.27 | 0.977 |
| TopK `ORDER BY d DESC LIMIT 10`, 100 files (held; its own dynamic filter, untouched) | 75.72 | 75.58 | 75.82 | 0.999 |
| C-012 like-for-like set, 43,889 cells, five runs (held; summed work, s) | 73.5 | — | 73.5 | 1.001 |
| `max(d)`, one ten-million-row file (reported) | 1.89 | 21.06 | 31.15 | 16.2 |
| `max(d)`, 100 files written in descending order (reported) | 4.20 | 10.16 | 24.76 | 5.9 |

- The held rows have no `max`/`min`, so the switch cannot reach them; the selective filter prunes as main (109 → 1
  file ranges, 2 → 1 row groups). The first delta measured 1.024x and 1.034x on the first two.
- The two reported rows are where the switch costs. Main answers both from the footer. With the filter on,
  `1a8f2749` pruned the single file's ranges 10 → 7 and the descending set's 109 → 1; with it off, `d7ae795a` reads
  every range: 1.50x and 2.43x the first delta.
- On verifier 1's five 100-file sets, file ranges and row groups equal main's; they move only on the three files with
  an unbounded row group.

## Owner rulings (2026-11-14, about 09:50 UTC, "Go with defaults")

**(i) The seat shape: RULED, accepted.** The middle shape above: per file, equal float bounds and float bounds with
an unbounded row group turn inexact; per scan, every float bound. A spread file keeps file pruning (the cost rows). Its
exact bounds also fed DataFusion's aggregate dynamic filter; that consumer is off by default because it prunes where
Spark's reader does not (Q5 (E)).

**(ii) The cells that move from equal-to-Spark to unequal: RULED, accepted.** A family, not one cell. N1 is not one of
them: it was a regression of this unit's own, fixed in the second delta, not a cell left unequal. Counts are from the
two verdicts
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
- its internal name not `max`/`min`, so DataFusion's aggregate dynamic filter does not attach (I8). Belt and braces
  since the second delta: the filter is off by default, and the name keeps it off for a user who opts back in;
- quantiles: `median` and `percentile_approx` over a sign-set NaN (I4);
- the pins on the TopK dynamic filter (I3).

## Close

Part 2 merged with its pins, and the sign-set `max`/`min` xfails in `test_nan_minmax_stats_1.py` turned green (the
multi-file `max` is a positive pin since the second delta). The `d >= 1.0` xfail closes with NAN-COMPARE-LITERAL-1.
