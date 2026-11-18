# map — repark-functions/src/aggregate

## Purpose

**SUM-DEC-I128WRAP-1 (2026-11-17):** Spark's decimal `sum` and `try_sum` on the Spark door. The
parent `aggregate.rs` registers both by name in `functions()` (after DataFusion's defaults and
datafusion-spark, so they replace `sum` and `try_sum`), exports `sum_udaf()` / `try_sum_udaf()`
for the DataFrame arm (`repark-python` `unary_aggregate_udaf`, which also carries `pivot` and
`F.call_function("sum")`), and holds the ANSI rule (`SparkAvgAnsiMode`, `spark_avg_ansi_mode`)
that now also places each `sum`. Only the arithmetic is shared with `avg`
(`avg_decimal::WideDecimal`, `add_batch`, `sub_batch`, `safe_rows`); the accumulators are
`sum`'s own. Step 0 and evidence: [task/wo/sum-dec-i128wrap-1/](../../../../task/wo/sum-dec-i128wrap-1/map.md).

## Contents

- `spark_sum.rs` — `SparkSum`, one UDAF per name (`sum`: `Ansi`, `Legacy` after the rule with
  ANSI off; `try_sum`: `Try`) wrapping DataFusion's `Sum` or datafusion-spark's `SparkTrySum`.
  A `Decimal128` input takes the decimal accumulators; every other type delegates unchanged
  (signature, coercion, return type, state, accumulators, retract, `reverse_expr`,
  `simplify_expr_op_literal`, which declines only a decimal literal). `placed` / `legacy` are the
  rule's two arms: with ANSI on a `sum` is placed `Grouped`, `Ungrouped` or `Window`; with ANSI
  off it becomes `Legacy`. A non-decimal `try_sum` keeps datafusion-spark's accumulator, so
  `SlidingFrameReScan` still re-scans its frames (Spark's `TRY` integral add checks each row in
  frame order).
- `sum_decimal.rs` — the decimal accumulators, exact in every phase (decision I, owner 2026-11-18,
  delta 2). Every row and every merged partial is added exactly; no phase before `evaluate`
  checks or marks. The state follows the input precision (`Carrier`): at precision 19 and under
  one raw `i128` column, which cannot wrap (rows × 10^19 stays under 2^127; a partial that does
  wrap means input past its declared precision and raises an error, never a marker); from
  precision 20 the low word and an `Int64` wrap count. Each non-window total is checked once at
  `evaluate` against the result precision (`|exact| < 10^p`): `ARITHMETIC_OVERFLOW` with the
  `try_sum` hint with ANSI on, NULL with ANSI off and in `try_sum`. `DecimalSum` (ungrouped, and
  every window frame, retracting; a window checks each frame, `NUMERIC_VALUE_OUT_OF_RANGE`);
  `DecimalSumGroups` (grouped; `convert_to_state` keeps DataFusion's partial-skip path);
  `DecimalSumDistinct` (the mixed `DISTINCT`, unchanged). Batches bound their magnitude with
  Arrow `min` / `max` so ordinary `p ≥ 35` data keeps Arrow's `sum`.
- `sum_tests.rs` — the unit's Rust pins: the exact total and its one check in both modes (SQL),
  the window's per-frame check, the distinct sum, every partial order and a final step's spill,
  128-bit wraps and their return from precision 20, the 38-digit limit, the one- or two-column
  state at precision 19 and 20, the narrow state's raw total (no marker), the wide carrier
  through an Arrow IPC round trip, a spilling aggregate, `RepartitionExec` and skipped partials
  at precision 19, 20 and 38, every arm's retracting sliding accumulator, and the delegated
  non-decimal answers and linear-rewrite hook.

## Pointers

- Up: [../map.md](../map.md)
- Session-level K2-R pin: [../../../repark-spark/src/extension/sum_window_tests.rs](../../../repark-spark/src/extension/sum_window_tests.rs)
- Facade pins: [../../../../python/repark/tests/types/test_sum_dec_i128wrap_1.py](../../../../python/repark/tests/types/test_sum_dec_i128wrap_1.py)
