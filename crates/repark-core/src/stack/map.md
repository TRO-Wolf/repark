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
  `apply_labeled_stack`, `register_stack`. IPI-51 PR2: the `DATATYPE_MISMATCH`
  texts render through `repark_common::spark_error`, byte-identical.
- `exec.rs` — `UnpivotExec` / `UnpivotStream`: one `interleave` per stacked
  column (shared `(piece, row)` index per batch), no `concat`+`take`; the exec
  polls the input stream batch-by-batch (no partition `collect`). Labeled mode
  gathers each cell by its `cells` index map and coerces to Utf8 with the
  engine's own cast options; `exec::labeled` is the string-identity pin.
- `planner.rs` — `StackQueryPlanner` + extension planner. EMPTY-DML-SHARED-GUARD-1 (2026-11-08): `create_physical_plan` runs `empty_dml::plan_empty_dml_as_no_op` on the optimized plan before `DefaultPhysicalPlanner`; every core session carries this planner (`session/df_guards.rs`), so the seat sees the plan that executes.
- `rewrite.rs` — `StackRewrite` analyzer: `stack(...)` projection → Unpivot.
- `udf.rs` — marker `stack` ScalarUDF (never executed). IPI-51 PR2: the
  `WRONG_NUM_ARGS` / `DATATYPE_MISMATCH` texts render through
  `repark_common::spark_error`, byte-identical.
- `tests.rs` — reshape, pad, passthrough, type-mismatch pins.
- [empty_dml.rs](empty_dml.rs) — **EMPTY-DML-SHARED-GUARD-1 (2026-11-08):** `plan_empty_dml_as_no_op`, a logical rewrite. A `Dml(Delete | Update)` whose input, past any `Projection`, is `EmptyRelation { produce_one_row: false }` becomes a one-row `Values` node in the DML's own output schema with a zero count, so DataFusion's rebuilt statement (no filter, no assignments) never reaches the provider. Insert, Truncate, a one-row relation and a non-empty input are untouched; a plan with no such node is returned borrowed.
- `empty_dml/tests.rs` — the recording-provider pins: the Step 0.3 empty predicates never call `delete_from` or `update` and answer `count = 0`; the controls (`id = 1`, `true`, no WHERE, identity `SET`) still reach the provider; TRUNCATE and an empty INSERT source are unchanged; `EXPLAIN ANALYZE` and `PREPARE` of an empty DML stay refused. Pinned by `an_empty_delete_or_update_never_reaches_the_provider_and_answers_a_zero_count`, `an_empty_dml_nested_in_a_wider_plan_is_guarded_too` and the rewrite-shape tests.

## Pointers

- Up: [../map.md](../map.md)
