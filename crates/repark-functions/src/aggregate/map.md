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
- `sum_decimal.rs` — the decimal accumulators and Spark's check (`|exact| < 10^p`, `p` the result
  precision). `DecimalSum` (ungrouped, every window frame, retracting): exact low part plus wrap
  count; a partial is checked at `state()` (ANSI raises `ARITHMETIC_OVERFLOW`, otherwise it ships
  the marker `OVERFLOWED`, `i128::MIN`, in the one state column), the total at `evaluate`:
  `NUMERIC_VALUE_OUT_OF_RANGE` after a merge and in a window, `ARITHMETIC_OVERFLOW` single-phase.
  `DecimalSumGroups` (grouped): the partial step checks after every row, sticky per group (ANSI
  raises at once, so its state stays one column with no marker); the final step adds the merged
  partials exactly (a wrap count per group) and checks each total once at `evaluate`
  (`ARITHMETIC_OVERFLOW`, Spark's grouped condition; NULL with ANSI off and in `try_sum`), so the
  answer no longer depends on the partials' arrival order (delta 1, owner 2026-11-18). Where
  DataFusion skips the partial step (`convert_to_state`), the rows reach the final step and are
  totalled once too; turning the skip off cost K1 1.25x. `DecimalSumDistinct`: a value
  set summed exactly at `evaluate` (the mixed `DISTINCT`). Batches bound their magnitude with Arrow
  `min` / `max` so ordinary `p ≥ 35` data keeps Arrow's `sum`.
- `sum_tests.rs` — the unit's Rust pins: Spark's check places in both modes (SQL), Q6's two
  conditions, the sticky grouped state and its marker, the grouped final merge checked once in
  every partial order (and through a final step's spill), the distinct sum, the marker through an
  Arrow IPC round trip, a spilling aggregate, `RepartitionExec` and skipped partials (their rows
  totalled once), every arm's
  retracting sliding accumulator, and the delegated non-decimal answers and linear-rewrite hook.

## Pointers

- Up: [../map.md](../map.md)
- Session-level K2-R pin: [../../../repark-spark/src/extension/sum_window_tests.rs](../../../repark-spark/src/extension/sum_window_tests.rs)
- Facade pins: [../../../../python/repark/tests/types/test_sum_dec_i128wrap_1.py](../../../../python/repark/tests/types/test_sum_dec_i128wrap_1.py)
