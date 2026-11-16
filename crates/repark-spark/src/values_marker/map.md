# map — repark-spark/src/values_marker

## Purpose

Seat V of DECIMAL-DOUBLE-UNION-COERCION-1 part 2 (2026-11-16). The SQL planner casts every cell of a multi-row
`VALUES` to one common type before any analyzer rule runs, and its cast is byte-identical to a user's. For a
DECIMAL beside a FLOAT or DOUBLE that type is `decimal(30,15)`, where Spark's is `double`. The parent module,
`../values_marker.rs`, marks each cell at the Spark-door parse; this directory consumes the marks at the head of
the Spark-door analyzer list.

## Contents

- `../values_marker.rs` — `__repark_values_cell(x)`, an immutable identity UDF whose return field is its argument's
  (`values_cell_udf`, `values_cell_argument`), and the AST pass `mark_values_cells` / `mark_df_statement`, which
  wraps each cell of every `VALUES` with two or more rows, `SetExpr` operands included. Placeholders, a bare
  `DEFAULT` and an already-marked cell are left alone, so the pass is idempotent. `mark_df_statement` descends into
  `EXPLAIN`. `plan_marked_sql` parses, marks and plans one statement (the `INSERT OVERWRITE` empty-target
  validation and the `F.expr` fragment use it). The callers: `spark_ast.rs` (the passthrough, after the INSERT
  rewrites and before planning, and `restate_range_frames_and_replan`), `merge.rs` (the `USING` factor, before `lower`),
  `insert_overwrite.rs`, and `repark-python`'s `column/expr_build.rs` (`F.expr`, `selectExpr`, and `filter(str)` on
  a Spark-door frame). Every seat marks only when its session registers the marker (`session_marks_values`), so a
  context built without `SparkExtension` plans as before. The UDF is registered on the Spark-door session, in `build_expr_context`, and on the native
  session (Q4: a native frame may carry a fragment's marker; it resolves and folds).
- `consume.rs` — `SparkValuesTyping` (`spark_values_typing`), seated by `insert_values_typing_first` immediately
  before `spark_timestamp_unit_widen` (index 1 of the Spark-door list). Per marked `VALUES` column, when the marker
  arguments are numeric and hold a DECIMAL and a FLOAT or DOUBLE:
  - a standalone column (its type is the planner's own common type) becomes `CAST(x AS DOUBLE)` and the field
    `Float64`; every later node recomputes its schema, and a stale union is rebuilt;
  - a target-typed column (an `INSERT … VALUES` source directly under the insert projection, or a `VALUES` the
    planner typed to the target inside an insert source) becomes `CAST(CAST(x AS DOUBLE) AS <target>)` and keeps
    its type: the inline table first, then the store cast, in Spark's order;
  - otherwise the markers are dropped and the planner's type stays (a marked `NULL` under a planner cast becomes
    the typed null literal the planner writes for an unmarked one). A user's cast from DOUBLE to DECIMAL beside a
    decimal cell gets a same-type outer cast, so the second analyzer pass cannot read it as a planner cast.

  A `VALUES` under `RecursiveQuery` keeps the planner's type (the recursive term is already cast to the anchor).
  **The tripwire:** an unmarked multi-row `VALUES` whose DECIMAL column holds a FLOAT/DOUBLE cell under a cast and a
  decimal cell refuses with a plan error naming the unit, and a marker outside a `VALUES` cell refuses. The native
  door has neither rule.
- `tests.rs` — the marked mixed `VALUES` is `double`; an unmarked one refuses naming the unit; the shapes main
  answers (user casts on both cells, all-double, all-decimal, one row, INT beside DOUBLE, 1,000 rows with one
  double) stay quiet; a second analyzer pass finds nothing to refuse; a `VALUES` in an `IN` subquery and in a
  union; an `INSERT … VALUES` casts through `double` and keeps the target type; the AST pass marks only multi-row
  cells, twice is once.

## Pointers

- Up: [../map.md](../map.md)
- The unit: [task/wo/decimal-double-union-coercion-1/](../../../../task/wo/decimal-double-union-coercion-1/map.md)

## Debug

| Symptom | First check |
|---|---|
| A mixed `VALUES` answers `decimal(30,15)` on the Spark door | the seat that planned the text must call `mark_values_cells` before planning |
| `DECIMAL-DOUBLE-UNION-COERCION-1: a multi-row VALUES … without its cell markers` | a Spark-door seat plans user text without marking it; mark it there |
| `Invalid function '__repark_values_cell'` | the session that planned marked text lacks `values_cell_udf` |

First checks: `cargo test -p repark-spark values_marker::`.
