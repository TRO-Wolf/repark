# map — repark-core/src/stack

PERF-UNPIVOT-1 step 1 (2026-09-12): Spark `stack(n, expr…)` as a native Unpivot
plan node, linear in columns. SQL rewrite lives on the Spark door after integer
literal narrowing. `apply_stack` is the DataFrame entry used by `F.stack`.

Step 2 remediation (2026-09-12, S2-21 review): `apply_labeled_stack` / `StackLabels`
is the internal describe door — row-label literals (stat names) plus a row-major
input-column index per stacked cell, so `describe`/`summary` feed `UnpivotExec`
the raw chunked aggregate with no expression projection at all. In labeled mode
the exec emits the label column itself and coerces every cell to `Utf8` inside
the batch via `cast_with_options` with `CastOptions { safe: false,
format_options: DEFAULT_FORMAT_OPTIONS }` — the exact kernel and options
DataFusion's own `CAST(x AS STRING)` physical expression evaluates with, so the
strings are byte-identical to the engine cast (`exec::labeled` pins them against
a live `SessionContext` oracle on floats incl. `0.0`/`-0.0`/`1e16`/`1e-20`/NaN/±inf,
Int64, Decimal, Date32, Timestamp, Boolean, Utf8). `STACK_COLUMN_DIFF_TYPES`
cannot fire in labeled mode; the SQL/`F.stack` door is unchanged and still
refuses mixed types. `UnpivotExec` now reports `elapsed_compute`/`output_rows`
via `ExecutionPlanMetricsSet` + `BaselineMetrics` (the `record_poll` pattern
from `ProjectionExec`).

`repark-core` declares `futures` (workspace pin) so `UnpivotStream` can implement
`RecordBatchStream`'s `Stream` supertrait. Step 1 used `collect` to avoid that crate.

This file closes when PERF-UNPIVOT-1 merges.

## Contents

- `../stack.rs` — `UnpivotNode`, `StackLabels`, `apply_stack`,
  `apply_labeled_stack`, `register_stack`.
- `exec.rs` — `UnpivotExec` / `UnpivotStream`: one `interleave` per stacked
  column (shared `(piece, row)` index per batch), no `concat`+`take`; the exec
  polls the input stream batch-by-batch (no partition `collect`). Labeled mode
  gathers each cell by its `cells` index map and coerces to Utf8 with the
  engine's own cast options; `exec::labeled` is the string-identity pin.
  pins: perf-unpivot-1/C-006, C-007
  pins: perf-unpivot-1/C-014, C-017
- `planner.rs` — `StackQueryPlanner` + extension planner.
- `rewrite.rs` — `StackRewrite` analyzer: `stack(...)` projection → Unpivot.
- `udf.rs` — marker `stack` ScalarUDF (never executed).
- `tests.rs` — reshape, pad, passthrough, type-mismatch pins.

pins: perf-unpivot-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007

## Pointers

- Up: [../map.md](../map.md)
