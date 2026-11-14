# map — repark-core/src/spark_nullable

## Purpose

The test cohort of `../spark_nullable.rs`'s float statistics seat (NAN-MINMAX-1 part 1, 2026-11-13). Parquet's
footer min and max leave NaN out, and DataFusion takes them as exact, so two RePark-only consumers lost NaN rows: the
opener's constant-column replacement (a file whose other float values are one value read every NaN as that value)
and the `max`/`min` statistics shortcut. `FloatBoundsInexactFormat` wraps DataFusion's Parquet format and demotes to
`Precision::Inexact`:

- **per file** (`infer_stats`, `infer_stats_and_ordering`): a FLOAT or DOUBLE column whose bounds are equal, the
  constant replacement's only trigger, or with a row group that holds a non-null value but no footer min or max (all
  NaN, or NULL and NaN). DataFusion merges the other row groups' bounds into an exact file bound
  (`datafusion-datasource-parquet-54.1.0/src/metadata.rs:609-641`, `:748`), and the file pruner then dropped the
  whole file. `unbounded_float_columns` reads the footer from DataFusion's file-metadata cache; a footer it cannot
  read demotes every float bound of the file;
- **per scan** (`create_physical_plan`): every float bound in the table and file-group statistics, which the
  shortcut reads.

A spread file whose row groups all carry bounds keeps them exact, so DataFusion's file pruner (exact bounds only)
still prunes it, as row-group pruning by the same footers would. Those bounds also feed DataFusion's aggregate dynamic
filter, which can skip a NaN file under a multi-file `max` (NAN-MINMAX-1 part 2, I8). Row counts, null counts and
every non-float statistic are untouched. `read_parquet_nullable` builds its listing with the wrapper;
`FloatBoundsInexactFactory` replaces the session's `parquet` file-format factory (registered in
`../session/df_guards.rs`), which the native `CREATE EXTERNAL TABLE … STORED AS PARQUET` uses. Evidence:
`task/wo/nan-minmax-1/`.

## Files

- `float_bounds_tests.rs` — the seat's pins: a constant file's float bounds turn inexact while every other statistic
  (row count, byte size, null counts, the integer and string columns) is unchanged; a spread file's bounds stay exact;
  a row group of NaN, or of NULL and NaN, turns the file's float bounds inexact while an all-NULL row group does not,
  and `read_parquet` keeps those NaN rows under `> 5.0` and `= NaN`; `read_parquet` and the external table read
  every NaN back, `max` answers NaN and `min` the smallest number, on a
  constant and a spread file, one and two row groups. Removing the per-file demotion, the per-scan demotion or the
  factory registration, or the row-group check, each turns a pin red.

Up: [../map.md](../map.md).
