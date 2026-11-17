# map — repark-spark/src/values_marker

## Purpose

Seat V of DECIMAL-DOUBLE-UNION-COERCION-1 part 2 (2026-11-16). The SQL planner casts every cell of a multi-row
`VALUES` to one common type before any analyzer rule runs, and its cast is byte-identical to a user's. For a
DECIMAL beside a FLOAT or DOUBLE that type is `decimal(30,15)`, where Spark's is `double`. The parent module,
`../values_marker.rs`, marks each cell at the Spark-door parse; this directory consumes the marks at the head of
the Spark-door analyzer list.

## Contents

- `../values_marker.rs` — the marker, an immutable identity UDF whose return field is its argument's
  (`values_cell_udf`, `values_cell_argument`). **Its name is per process** (`values_cell_name()`:
  `__repark_values_cell_<16 hex>`, from `RandomState` as `repark-core`'s attribute ids are), so no user text can
  name it: a user's `__repark_values_cell(…)`, in any case or position, on either door, is an unresolved routine
  exactly as on main (part 2 delta, F1). The AST pass `mark_values_cells` / `mark_df_statement` classifies each column
  of every `VALUES` with two or more rows (`SetExpr` operands included) from its cells: a decimal literal or a
  `CAST(… AS DECIMAL)`, a `CAST(… AS DOUBLE|FLOAT|REAL)`, an integer literal, `NULL` / `DEFAULT`, another type (a
  string, a boolean, a date), or a cell the AST cannot type (an expression, an exponent literal, a placeholder):
  - a column that cannot hold both a DECIMAL and a FLOAT or DOUBLE is left alone;
  - a column whose cells the AST types fully and that mixes (**decided**) is cast in the AST: every decimal, float
    and integer cell becomes `CAST(… AS DOUBLE)`, which is what the consumer writes, so the planner sees Spark's
    `double` and no marker is planned (the cost fix of the delta);
  - a column with a cell the AST cannot type (**undecided**) is marked cell by cell, as before.
  When nothing was marked, one non-`NULL` cell carries a **witness** marker, so the consumer knows a seat saw the
  `VALUES` (the tripwire stays meaningful) without marking every cell. Placeholders, `DEFAULT` and an already-marked
  cell are left alone, and a marked cell is classified by its argument, so the pass is idempotent.
  `mark_fragment_cells` / `plan_marked_fragment` are the `F.expr` / `selectExpr` fragment's form: a decided column
  is marked, not cast (the door-free context must leave a native frame DataFusion's answer, Q4), and
  `plan_marked_fragment` returns `None` when a mixing column was marked, so `expr_build.rs` plans the fragment
  unresolved and the frame's analyzer types it (F3). `plan_marked_sql` parses, marks and plans one statement (the
  `INSERT OVERWRITE` empty-target validation). The callers: `spark_ast.rs` (the passthrough, after the INSERT
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
  - otherwise the markers are dropped and the planner's type stays (a planner cast over a marked bare `NULL` becomes
    the typed null literal the planner writes for an unmarked one). A user's cast from DOUBLE to DECIMAL beside a
    decimal cell gets a same-type outer cast, so the second analyzer pass cannot read it as a planner cast.

  A `VALUES` under `RecursiveQuery` keeps the planner's type (the recursive term is already cast to the anchor).
  Only a cell that holds a marker is stripped. **Outer references (part 2 delta, F2):** after a `VALUES` is retyped,
  `refresh_outer_references` walks the plan once more and gives every `OuterReferenceColumn` (a `LATERAL` right
  side, a correlated scalar subquery, `EXISTS` or `IN`, nested) the type its column now has in the enclosing scopes,
  innermost first, and recomputes each schema above it, so no node keeps the planner's `decimal(30,15)`.
  **The tripwire:** an unmarked multi-row `VALUES` whose DECIMAL column holds a FLOAT/DOUBLE cell under a cast and a
  decimal cell refuses with a plan error naming the unit, and a marker outside a `VALUES` cell refuses. The native
  door has neither rule.
  **DECIMAL-COMMON-TYPE-1 (2026-11-16):** a column of decimals, or of integers and decimals, takes Spark's type from
  `spark_decimal_type` (an `INT`-sized integer literal in Spark's `INT` form) when it differs from the planner's:
  standalone, the field is retyped and each cell cast; under an insert, each cell is cast to Spark's type and then to
  the target. The legacy flag skips it. `../values_marker.rs` marks a plain column for it at the statement seat when
  it holds a decimal beside an integer literal or a decimal of 20 or more digits (`spark_decimal_column`).
- `tests.rs` — the marked mixed `VALUES` is `double`; an unmarked one refuses naming the unit; the shapes main
  answers (user casts on both cells, all-double, all-decimal, one row, INT beside DOUBLE, 1,000 rows with one
  double) stay quiet; a second analyzer pass finds nothing to refuse; a `VALUES` in an `IN` subquery and in a
  union; an `INSERT … VALUES` casts through `double` and keeps the target type; the AST pass marks only multi-row
  cells, twice is once; a decided column is cast in the AST and the fragment form marks it; a user-written
  `__repark_values_cell` does not resolve; an outer reference to a retyped column follows its type.
  `seat_v_phase_timing` (ignored, run by hand) times parse, mark, plan, analyze and optimize for 10,000 rows.

## Pointers

- Up: [../map.md](../map.md)
- The unit: [task/wo/decimal-double-union-coercion-1/](../../../../task/wo/decimal-double-union-coercion-1/map.md)

## Debug

| Symptom | First check |
|---|---|
| A mixed `VALUES` answers `decimal(30,15)` on the Spark door | the seat that planned the text must call `mark_values_cells` before planning |
| `DECIMAL-DOUBLE-UNION-COERCION-1: a multi-row VALUES … without its cell markers` | a Spark-door seat plans user text without marking it; mark it there |
| `Invalid function '__repark_values_cell_<hex>'` | the session that planned marked text lacks `values_cell_udf` |
| `UNRESOLVED_ROUTINE … __repark_values_cell` | user text names the marker's prefix; that is main's answer, by design |

First checks: `cargo test -p repark-spark values_marker::`.
