# map — repark-core/src/stack

PERF-UNPIVOT-1 step 1 (2026-09-12): Spark `stack(n, expr…)` as a native Unpivot
plan node, linear in columns. SQL rewrite lives on the Spark door after integer
literal narrowing. `apply_stack` is the DataFrame entry used by `F.stack`.

`repark-core` declares `futures` (workspace pin) so `UnpivotStream` can implement
`RecordBatchStream`'s `Stream` supertrait. Step 1 used `collect` to avoid that crate.

This file closes when PERF-UNPIVOT-1 merges.

## Contents

- `../stack.rs` — `UnpivotNode`, `apply_stack`, `register_stack`.
- `exec.rs` — `UnpivotExec` / `UnpivotStream`: one `interleave` per stacked
  column (shared `(piece, row)` index per batch), no `concat`+`take`; the exec
  polls the input stream batch-by-batch (no partition `collect`).
  pins: perf-unpivot-1/C-006, C-007
- `planner.rs` — `StackQueryPlanner` + extension planner.
- `rewrite.rs` — `StackRewrite` analyzer: `stack(...)` projection → Unpivot.
- `udf.rs` — marker `stack` ScalarUDF (never executed).
- `tests.rs` — reshape, pad, passthrough, type-mismatch pins.

pins: perf-unpivot-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007

## Pointers

- Up: [../map.md](../map.md)
