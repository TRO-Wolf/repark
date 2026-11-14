# map — repark-core/src/spark_nullable

## Purpose

The test cohort of `../spark_nullable.rs`'s float statistics seat (NAN-MINMAX-1 part 1, 2026-11-13). Parquet's
footer min and max leave NaN out, and DataFusion takes them as exact, so two RePark-only consumers lost NaN rows: the
opener's constant-column replacement (a file whose other float values are one value read every NaN as that value)
and the `max`/`min` statistics shortcut. `FloatBoundsInexactFormat` wraps DataFusion's Parquet format and demotes to
`Precision::Inexact`:

- **per file** (`infer_stats`, `infer_stats_and_ordering`): a FLOAT or DOUBLE column whose bounds are equal, the
  constant replacement's only trigger;
- **per scan** (`create_physical_plan`): every float bound in the table and file-group statistics, which the
  shortcut reads.

A spread file keeps its exact bounds, so DataFusion's file pruner (exact bounds only) still prunes it, as row-group
pruning by the same footer would. Row counts, null counts and every non-float statistic are untouched.
`read_parquet_nullable` builds its listing with the wrapper; `FloatBoundsInexactFactory` replaces the session's
`parquet` file-format factory (registered in `../session/df_guards.rs`), which the native `CREATE EXTERNAL TABLE …
STORED AS PARQUET` uses. Evidence: `task/wo/nan-minmax-1/`.

## Files

- `float_bounds_tests.rs` — the seat's pins: a constant file's float bounds turn inexact while every other statistic
  (row count, byte size, null counts, the integer and string columns) is unchanged; a spread file's bounds stay exact;
  `read_parquet` and the external table read every NaN back, `max` answers NaN and `min` the smallest number, on a
  constant and a spread file, one and two row groups. Removing the per-file demotion, the per-scan demotion or the
  factory registration each turns a pin red.

Up: [../map.md](../map.md).
