# map — repark-python/src/column

CC-4 (2026-08-30): remaining banner files condensed to the one-line rule
(pins: cc-3-comment-condensation/C-009).

CC-2 closing-critic remediation: review-round label narration swept from prose; safety and
accuracy contracts restored in condensed form (see the unit ledger's findings dispositions).

## Purpose

`PyColumn` is an immutable DataFusion expression wrapper. Its constructors and operators provide
the Python facade's Column surface while DataFrame methods bind expressions to input schemas.

## Modules

- [`mod.rs`](mod.rs) owns `PyColumn`, constructors, operators, aggregates, and window attachment.
- [`function_dispatch.rs`](function_dispatch.rs) owns scalar and aggregate function dispatch.
  LOG1P-1: `log1p` / `expm1` arms embed `repark_functions::expr_fn` kernels.
  pins: log1p-1-precise-kernels/C-002
  **DATE-FN-1:** `unix_timestamp` / `to_unix_timestamp` (0 or 1 arg). PySpark has no `F.date`.
  pins: date-fn-1-spark-date-spelling/C-002
  **FN-FIX-1:** `isnan` / `sha2` / array kernels. pins: fn-fix-1-registry-rows/C-002
- [`expr_build.rs`](expr_build.rs) owns type parsing, alias handling, and expression inspection.
  **FN-FIX-1:** `window_from_aggregate` copies `IGNORE NULLS`. pins: fn-fix-1-registry-rows/C-002
- [`window.rs`](window.rs) owns Spark frame conversion and unordered-window policy.
- [`door_parity_tests.rs`](door_parity_tests.rs) pins standalone facade UDF behavior against SQL.

## Contracts

- `literal` distinguishes Python `bool` from `int` and accepts only supported scalar types.
  A Python `int` that fits in Int32 is an Int32 literal (Spark `IntegerType`), so
  `col(int32) + 1` stays Int32. pins: f-y10-1-int-overflow/C-002
- `sql` analyzes standalone expressions before handoff; parse errors map to `ParseException` and
  unresolved names map to `AnalysisException`. This path bypasses the Spark SQL router, so
  FNP-15/16 declared-function names refuse through `refuse_sql_fragment` (collation +
  declared-absent). pins: fnp-15-16/C-001
- Higher-order lambda variables are resolved against the consuming DataFrame schema.
- Nested higher-order functions refuse loudly rather than producing an invalid plan.
- `concat` propagates NULL and returns Spark-compatible UTF-8 output.
- Window frames use Spark-relative offsets. Count-like unsigned results are cast to signed types.
- Unknown scalar, aggregate, cast, or window names fail with typed Python exceptions.

## Change locations

FNP-7 try_* scalar and aggregate names dispatch here (`try_divide` … `try_to_time`,
`try_sum`, `try_avg`). pins: fnp-7-try-inversions/C-013
SEM-1 `log` embeds `SparkLog` (1- or 2-arg); `ln` stays DataFusion `ln`.
pins: sem-1-spark-answer-parity/C-005, C-006

Add a Column method in `mod.rs`, a scalar or aggregate dispatch arm in `function_dispatch.rs`, a
builder rule in `expr_build.rs`, or a frame rule in `window.rs`. Add the matching parity test.

## Verification

Run `cargo fmt --check`, `cargo test -p repark-python`, the exact-equivalence scanner, and map
sync after changes.

## Pointers

- Up: [src map](../map.md)
- Crate: [repark-python map](../../map.md)
