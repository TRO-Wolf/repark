# map — docs/examples/window/

## Purpose

Worked examples for the `Window` / `WindowSpec` surface: the static spec
builders, the chained spec methods, the frame setters, and the frame-bound
constants. Examples construct the session as `repark =
ReparkSession.builder…` and import the class root as `from repark.spark
import Window`; see [../map.md](../map.md). Fixtures use a globally unique
order key: on tied keys the ordered default frame diverges (registry §7
EX-WIN-1), and the examples keep the arms where the engines agree.

## Contents

- [spec_builders.py](spec_builders.py) — `Window.partitionBy` /
  `partition_by` and `Window.orderBy` / `order_by` statics plus the chained
  `WindowSpec.partitionBy` / `partition_by` / `orderBy` / `order_by` forms:
  per-partition `row_number`, global cumulative `sum`, and `rank` over a
  descending key.
- [frames.py](frames.py) — `Window.rowsBetween` / `rows_between` and
  `Window.rangeBetween` / `range_between` statics (whole-frame, cumulative)
  plus the chained `WindowSpec` frame setters: running sums, a `±5` range,
  a sliding three-row `avg`, peer-only `rangeBetween(0, 0)` bounds, and a
  NULL partition-key / NULL-value control (NULL keys form their own
  partition, `sum` skips NULL values).
- [bounds.py](bounds.py) — the frame-bound constants `currentRow` /
  `current_row`, `unboundedPreceding` / `unbounded_preceding`,
  `unboundedFollowing` / `unbounded_following` (values and use as running /
  trailing frame bounds).

The snake_case spellings are repark extensions (`hasattr` measured False on
live PySpark 4.1.2 for every one) and are covered beside their camelCase
twins, measured Spark-equal. The tied-key ordered default frame is §7
[EX-WIN-1](../../spark-sql-iceberg-parity.md), pinned in
`python/repark/tests/test_examples_window_catalog.py`.

## Pointers

- Up: [../map.md](../map.md)
- Pins: [../../../python/repark/tests/test_examples_window_catalog.py](../../../python/repark/tests/test_examples_window_catalog.py)
- Ledger: [../../../task/ledgers/staging/ex-20-window-catalog-ledger.md](../../../task/ledgers/staging/ex-20-window-catalog-ledger.md)
