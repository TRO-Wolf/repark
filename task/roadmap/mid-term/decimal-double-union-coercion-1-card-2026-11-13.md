# Card DECIMAL-DOUBLE-UNION-COERCION-1: a DECIMAL literal unified with a DOUBLE types the column `decimal(30,15)`, so a `UNION` write stores 0.0 for 1e-20

**Date:** 2026-11-13. **Filed by:** the records clerk (`claude-opus-5-5`). **Sources:** orchestrator measurement,
2026-11-11, against Spark 4.1.2; Fable guidance, 2026-11-13, which flagged it as a label blocker missing from the
xhigh inquiry.

**Status:** part 2 **merged** as #1341, `cb7d3943`. Filed 2026-11-13, not dispatched; ruled 2026-11-13 (see the append). First put to the owner on the 2026-11-12 morning board
([page](../../../docs/artifacts/morning-board-2026-11-12.html), item 2, "New S1") without a ruling. The board's
default was to open it as the next lane unit, after signed-zero part B or ahead of it. Severity **S1**, a wrong
write. **Label-blocking:** it is on scalar columns, so it is inside the scoped label ICE-V2-FLAT-AWS-LABEL-1 and the
full format-v2 label.

**2026-11-16:** the Status line above is corrected: part 2 merged as #1341, `cb7d3943`; it read "not dispatched" before this date (records clerk, `claude-haiku-5-5`).

## Measured (2026-11-11; not re-measured since)

- **The typing.** RePark unifies a DECIMAL literal with a DOUBLE to `decimal(30,15)` in `VALUES` and in `UNION`;
  Spark types the column `double`.
- **Reads.** A `SELECT` over `VALUES (1, 0.0), (2, CAST(1e-20 AS DOUBLE))`, and over the `UNION ALL` form, reads 0
  where Spark reads 1e-20. `1e30` overflows on RePark where Spark answers. The sign of `-0.0` is lost.
- **Wrong write (S1).** `INSERT INTO t(d DOUBLE) SELECT 1, 0.1 UNION ALL SELECT 2, CAST(1e-20 AS DOUBLE)` stores
  0.0; Spark stores 1e-20. `INSERT … VALUES` alone writes correctly.

## Lead (likely seat, not a cause)

DataFusion's `decimal_coercion` (Float64 with Decimal128 to `Decimal128(30,15)`), which RePark's Spark-door
`widen_decimal_against_float` does not cover for set operations, `VALUES` or `coalesce`. Unmeasured as a cause.

## Relation

[COALESCE-NAN-DOUBLE-1](coalesce-nan-double-1-card-2026-10-03.md) (2026-10-03): the same root seen as an overflow
refusal, a DOUBLE NaN cast to `Decimal128(30, 15)`.

## Fix direction

None ruled. Fable's guidance (not a ruling) asks for a design sketch before any build.

## Close

Every `VALUES`, `UNION` and `coalesce` that mixes a DECIMAL literal with a DOUBLE types the column `double` as Spark
does, so the reads and the `UNION` write above store and answer Spark's values, pinned against the Spark oracle on
the read and write doors.

## Append (2026-11-13 afternoon): rulings, order and one more shape

**Sources:** the design sketch, merged as #1324 ([sketch](../../wo/decimal-double-union-coercion-1/sketch.md)); owner,
2026-11-13, about 14:52 UTC, verbatim "Defaults, and merge #1323 when green"; NAN-WRITE-CANONICAL-1 verifier 1,
2026-11-13 (#1325, finding C2).

- **Ruled.** Q1′ to Q8 are ruled to the sketch's defaults (its "Rulings" section): seat V plus seat E, the native door
  recorded, the `array` / `map` constructors folded in, a pre-fix stored view keeps its schema, both prerequisites
  first, and `MERGE … ON` follows Spark. The cards the rulings name:
  [DOUBLE-DECIMAL-STORE-ROUND-1](double-decimal-store-round-1-card-2026-11-13.md),
  [DECIMAL-FLOAT-ARITH-1](decimal-float-arith-1-card-2026-11-13.md),
  [DECIMAL-DOUBLE-NESTED-1](decimal-double-nested-1-card-2026-11-13.md) and
  [DECIMAL-PRUNING-CONJUNCT-1](decimal-pruning-conjunct-1-card-2026-11-13.md).
- **Order.** The build starts after SIGNED-ZERO-NORMALIZE-1 part B (#1326) and DOUBLE-DECIMAL-STORE-ROUND-1 merge.
- **One more shape (#1325 verifier 1, C2, pre-existing).** `UPDATE t SET d = 2.5 WHERE …` on a DOUBLE column that
  holds a NaN fails with "Cannot cast to Decimal128(30, 15). Overflowing on NaN"; Spark coerces the literal to
  DOUBLE. The same root as the `UNION` write, seen on an `UPDATE` assignment.

## Append (2026-11-15): part 1 built on the Spark door; Q8 and Q2 restated; what part 2 owes

**Sources:**
- the owner's split plan, 2026-11-14, about 13:38 UTC, verbatim "Merge and go with the split plan";
- step 0′ on main `dc6b4c94` ([evidence/step0b/table.md](../../wo/decimal-double-union-coercion-1/evidence/step0b/table.md));
- the orchestrator's rulings at the step 0′ check, 2026-11-15;
- the build on branch `fix/decimal-double-coercion-part1` ([evidence/fix/](../../wo/decimal-double-union-coercion-1/evidence/fix/map.md)).

**Status: part 1 FIXED on the Spark door (branch, not merged); part 2 open.** Part 1 has three pieces:
- **Seat E** (`SparkDecimalFloatWiden`, before `type_coercion` on the Spark-door list). A DECIMAL beside a FLOAT
  or DOUBLE unifies to `double` in:
  - comparisons, `IN`, `BETWEEN`, CASE, `coalesce` / `nvl` / `nvl2` / `if`, `greatest` / `least`, and the
    `array` and map constructors;
  - `IN (subquery)`, join keys, loose unions and the `INTERSECT` / `EXCEPT DISTINCT` join;
  - `nullif`, which widens its comparison and keeps its first argument's type.
- **K2:** an exact DECIMAL→DOUBLE, JDK 17's `BigDecimal.doubleValue()`.
- **K1 on written casts.**

On the head against Spark 4.1.2, both ANSI modes:
- **Joined cells:** 0 of the 6,855 that match on main move. 1,826 are fixed (913 per mode).
- **Coercion fuzz:** 0 held. 72,452 fixed per mode.
- **Structured `d = v` set:** 0 wrong.
- **Spark-door DML:** 46 of 48 fixed per mode. The two left are SIGNED-ZERO-UPDATE-IN-LIST-1.

**Q8 is re-ruled PRUNED-WRONG** (2026-11-14, Fable §5).
- Spark's `MERGE … ON` miss of `-0.0` against `0.00` from a constant source is Iceberg file pruning.
- Main's match (`[99]`) is kept on the head and pinned (`test_merge_on_zero_keeps_mains_match_as_q8_rules`).

**Q2, the native door:**
- It is not widened, and a native `CAST(v AS DECIMAL(10,2))` or `CAST(d AS DOUBLE)` keeps DataFusion's and
  Arrow's answers.
- Stores follow Spark on both doors (#1335), so what a table holds does not depend on the door that wrote it.
- The head answers all 56 native DML cells as main does. N1's cells match Spark on main since #1335 and stay so.

**What part 2 still owes** (seat V and the tripwire):
- **The `VALUES` cells:** 83 per mode, wrong on main and the head. `ins_values_dec_round` is now fixed, because
  K1 rounds the planner's cast of a `VALUES` cell to its target.
- **The MERGE source seat `merge.rs`:** `merge_union_src` and `merge_union_big`, whose union source is typed
  before analysis.
- **The `F.expr` fragment subqueries** (critic M1).
- **The DML `IN (VALUES …)` link pin** (critic item 5).
- **`restate_range_frames_and_replan`.**
- **The tripwire:** a Spark-door unmarked multi-row `VALUES` refuses.
- **Retiring `widen_decimal_against_float`**, which seat E subsumes. It stays for part 2 under the six-file cap.
  Its four literal shapes are pinned to Spark's answers with both rules in place.
- **The `IN` list of four or more DOUBLE constants against a DECIMAL column** (owner, 2026-11-15 evening; see the
  last append). Seat E rewrites it at plan time with the delta's bound search:
  - a decimal `IN` list where every constant's range holds at most one decimal;
  - otherwise the ranges joined by `OR`, under a size limit;
  - the per-row form beyond that limit.

  Part 1 measured the row at 3.86x (100M rows).

**Still unlike Spark, outside both parts:**
- the select-list `IN (subquery)` (IN-SUBQUERY-PROJECTION-1);
- DataFrame `intersectAll` / `exceptAll`, which refuse by design;
- nested leaves (DECIMAL-DOUBLE-NESTED-1);
- `decimal + FLOAT` (DECIMAL-FLOAT-ARITH-1);
- the recursive CTEs;
- [STRING-DECIMAL-EXPONENT-1](string-decimal-exponent-1-card-2026-11-15.md) and
  [CAST-FLOAT-DECIMAL-OVERFLOW-NULL-1](cast-float-decimal-overflow-null-1-card-2026-11-15.md), both new and
  pre-existing.

**Cost:** see the evidence; the written-cast and DECIMAL(38,18) K2 rows go to the owner at merge.


## Append (2026-11-15 afternoon): the owner's ruling on the cost halt, and the delta

**Sources:** owner, 2026-11-15, about 13:59 UTC; Fable's guidance on the cost halt (advice only); the delta's cost
section in [evidence/fix/cost.md](../../wo/decimal-double-union-coercion-1/evidence/fix/cost.md).

**The owner's ruling, verbatim:** "Merge 1338, proceed with A and yes to 3".

- **(A), the delta:** a DECIMAL column against a DOUBLE constant is rewritten into a pure decimal comparison at plan
  time. The K2 UDF answers DataFusion's `preimage` hook with the decimal range whose K2 doubles equal the constant,
  found by binary search over K2 itself, so the rewritten form cannot disagree with the per-row form. NaN,
  ±Infinity and a constant at or above the column's largest double keep the per-row form. The two halted rows
  move from 3.95x and 10.47x to 1.015x and 1.015x; every held row is at or under 1.05x and every answer is unchanged.
- **"Yes to 3": the three reported rows are this unit's cost exception.** Measured before the delta, at 100M rows,
  and not touched by it:

  | row | head/main | per value |
  |---|---|---|
  | `d38 < v`, column against column (K2's exact refinement at `DECIMAL(38,18)`) | 1.662 | +2.82 ns |
  | written `CAST(v AS DECIMAL(10,2))` (K1) | 1.341 | +0.66 ns |
  | written `CAST(v AS DECIMAL(38,18))` (K1) | 3.361 | +4.27 ns |

  Their speed card is [DOUBLE-DECIMAL-CAST-PERF-1](double-decimal-cast-perf-1-card-2026-11-14.md).
- **Reported, not held:** an `IN` of four or more DOUBLE constants does not reach the hook (3.86x at 100M); an `IN`
  of three and a `BETWEEN` do (1.06x and 0.98x).

## Append (2026-11-15 evening): the owner's ruling on the `IN` row

**The owner's ruling** (2026-11-15, about 17:27 UTC), verbatim: "Okay awesome, go with your recommendation". The
recommendation it adopts:
- part 1 merges as it is once both verifiers pass;
- an `IN` of four or more DOUBLE constants against a DECIMAL column is part 1's temporary cost exception. It
  answers correctly and measures 3.862x at 100M rows (`d10`, four constants);
- part 2 owes the rewrite, listed under "What part 2 still owes" above.

## Append (2026-11-16): part 2, built

**Sources:** the owner's ruling, 2026-11-16, about 01:54 UTC, verbatim: "Defaults. Go ahead and proceed with any
defaults for the next 8 hours". It adopts the default of each step 0 question: the #1339 regression is fixed inside
part 2 by seat V (not reverted); Q1 (a), `L = 32`; Q2 yes, no DML marker position; Q3 yes, the stale outer union is
part 2's; Q4 accepted on the critic's condition. The orchestrator capped the unit at 10 product files. Evidence:
[evidence/part2/](../../wo/decimal-double-union-coercion-1/evidence/part2/map.md); inventory section J's build note.

**Status: part 2 BUILT on the Spark door (branch `fix/decimal-double-coercion-part2`, not merged).**

**What each seat does:**
- **Seat V** (`crates/repark-spark/src/values_marker.rs`, `values_marker/consume.rs`). Every Spark-door seat that plans
  user text wraps each cell of a multi-row `VALUES` in an identity marker before planning: the passthrough (and its
  `EXPLAIN` arm), `restate_range_frames_and_replan`, the MERGE `USING` source, the `INSERT OVERWRITE` empty-target
  validation, the `F.expr` / `selectExpr` fragment, and a Spark-door `filter(str)`. `spark_values_typing`, at index 1
  of the Spark-door analyzer list, types a column whose marked cells hold a DECIMAL and a FLOAT or DOUBLE as `double`
  (a target-typed column casts through `double` to its target) and drops every other marker. A `VALUES` under
  `RecursiveQuery` keeps the planner's type. **The tripwire:** an unmarked mixed multi-row `VALUES` refuses with a
  plan error naming this unit, and so does a marker a seat left outside a `VALUES` cell. *(Corrected by the delta,
  below: as built, a user-written marker answered; since the delta the marker's name is per process, so a user's
  `__repark_values_cell` is an unresolved routine on both doors, as on main. Since the delta a seat also casts, or
  marks, only a column that can mix; see the delta.)*
- **The stale outer union** (seat E, `decimal_float_widen.rs`): a union whose inputs no longer mix but whose schema
  differs from its first input's is rebuilt, so three or more branches with the DECIMAL first are `double`.
- **The `IN` rewrite** (`DecimalInRanges`, `decimal_float_widen.rs`): an optimizer rule appended by
  `ctx.add_optimizer_rule`, so it runs at the end of each optimizer pass, after `push_down_filter`, and rewrites every
  node's expressions, `TableScan.filters` included. Form 1 (a decimal `IN` list) when every constant's range holds one
  decimal, which always holds for precision 15 or less; form 2 (the members plus up to 32 ranges joined by `OR`); else
  the per-row form.
- **The retirement:** `widen_decimal_against_float` is removed; seat E answers its four literal shapes. The gate pinned
  them through `F.expr` on a Spark-door and a native-door frame before and after; no native answer moved.

**Files (9 product files of the cap of 10, plus the `lib.rs` `mod` line and the native registration line):**
`values_marker.rs` (new), `values_marker/consume.rs` (new), `extension.rs`, `spark_ast.rs`, `merge.rs`,
`insert_overwrite.rs`, `extension/decimal_float_widen.rs`, `spark_literal_typing.rs` (repark-spark), and
`column/expr_build.rs` (repark-python); `lib.rs` and `session_runtime.rs` take one line each.
`decimal_double.rs` is not touched: the `IN` decision sits beside the rule and calls the public bound search.

**Correction to #1339's claim.** Part 1's records said loose unions were fixed. A union of three or more branches whose
first branch is the DECIMAL stayed wrong until part 2 (finding 1): the SQL planner builds `Union(Union(A, B), C)`
with loose types and the outer schema kept `decimal(30,15)`. Part 1's `merge_union_src` / `merge_union_big`
attribution to `merge.rs` was this shape.

**The cells part 1 moved and part 2 restores.** Part 1 moved 11 cells from EQUAL to WRONG, all one class: a DOUBLE whose
`decimal(30,15)` image collides with a truncated `VALUES` cell (`z = 1e-20` against `VALUES (0.1), (1e-20)`): plain
`IN (subquery)`, `IN` through a CTE, correlated `EXISTS`, an equi-join on a `VALUES` derived table, `INTERSECT`
against a `VALUES` leg, `IN` and a join against a temp view over `VALUES`, the three fragment cells, and a DataFrame
join over a `VALUES` frame. Seat V restores all 11, and the `0.0` / seven `-0.0` neighbours wrong on both mains. Part 1
fixed 25 cells in the same family (`negtiny`, `round16`, `big` across the same shapes); they stay fixed.

**The `IN` exception** closes for precision 15 or less. At precision 16 or more with 5 or more ranges it narrows to the
measured figures in [evidence/part2/cost.md](../../wo/decimal-double-union-coercion-1/evidence/part2/cost.md) (Q1 (a)).

**Finding 3, qualified:** every `WHERE` subquery the identity path declines refuses, and a `VALUES` source is always
declined, so `DELETE` / `UPDATE … WHERE v IN (SELECT … VALUES …)` refuses, unchanged (copy-on-write and merge-on-read).
Identity-admissible table-backed forms (`DELETE … WHERE id IN (SELECT id FROM src)`, a qualified correlated `EXISTS`)
execute correctly. Cards: `repark-sql-update-subquery-where-1` (WHERE) and `UPDATE-SUBQUERY-STORE-1` (SET).

**What part 2 leaves undone:**
- `INTERSECT ALL` / `EXCEPT ALL` over a mixed `VALUES` leg refuse by design (SQL-SETOP-ALL-REFUSE-1).
- The DML `WHERE`-subquery refusal above (its two cards).
- A native-door frame over an `F.expr` `VALUES` subquery keeps DataFusion's answer (Q2, Q4).
- The recursive CTE over `VALUES` keeps main's refusal (RECURSIVE-CTE-JOIN-ERROR-1).
- `<lit> IS DISTINCT FROM <col>` ([DISTINCT-FROM-LITERAL-LEFT-1](distinct-from-literal-left-1-card-2026-11-15.md)).
- A `VALUES` nested in an insert source whose target type equals the planner's own common type is typed standalone
  (no measured cell reaches it); no card, named here.

## Append (2026-11-16): part 2, delta

**Sources:** the owner's "Proceed", 2026-11-16, about 11:00 UTC, after two verifiers returned FAIL on PR #1341 (head
`d78d669f`) with three behaviour findings, and the build's cost runs halted on held rows. One delta under the
one-round cap. Evidence: [evidence/part2/](../../wo/decimal-double-union-coercion-1/evidence/part2/map.md)
(`bar.md`, the delta section; `cost.md`).

**Status: part 2 BUILT, delta applied (commit `12101e9b`, not merged).** The delta's package is `pkg-p2-12101e9b`.

**F3, a wrong answer (S1).** `bare.filter(F.expr("CAST('1e-20' AS DOUBLE) IN (SELECT c FROM VALUES (dec), (dbl)
…)"))` and its `EXISTS` twin answered `[]`; Spark and a06c answer `[1]`.
- **Cause:** a literal-only `F.expr` fragment is analyzed in the door-free `build_expr_context`
  (`expr_build.rs`, `plan_expr_column`), which has no consumer, so the probe was coerced to the planner's
  `decimal(30,15)` and folded to `0`. The frame's Spark-door analyzer then retyped the `VALUES` to `double` and
  compared `0.0` with `1e-20`. A column probe never hit this: it misses a column in that context and is planned
  unresolved.
- **Fix:** `plan_marked_fragment` reports a fragment whose `VALUES` has a mixing column, and `plan_expr_column` plans
  it unresolved, as a column-referencing fragment is, so the frame's own analyzer types the probe. The native door
  is unchanged (160 native cells identical to a06c).
- **Family:** 4 fragment seats and `filter(str)` × `IN`, `NOT IN`, `EXISTS`, `NOT EXISTS`, a scalar comparison × the
  probes `1e-20`, `0.0`, `-0.0`, `0.1`, and 7 pins: 107 cells per mode, 33 EQUAL → EQUAL, 50 fixed, 24 still wrong
  as on a06c (the select-list `IN` subquery the physical planner refuses), 0 EQUAL → WRONG.

**F2, an internal error.** `LATERAL` over a mixed `VALUES` raised `Optimizer rule 'eliminate_cross_join' failed`.
- **Cause:** seat V retyped the `VALUES` column, but every `OuterReferenceColumn` to it (planned by the SQL planner
  with the old type) kept `decimal(30,15)`, and so did the schemas built over it.
- **Fix:** after a retype, `refresh_outer_references` (`consume.rs`) gives each outer reference its column's current
  type, innermost scope first, a join's right side seeing its left, and recomputes every schema above. A `VALUES`
  whose column the AST types fully no longer needs a retype at all (the cost fix below), so the planner's outer
  references are right from the start.
- **Family:** `LATERAL` (comma, `JOIN`, `CROSS JOIN`, `LEFT JOIN … ON true`, with a `WHERE`, two columns, over a CTE,
  under and inside a `UNION`), correlated scalar subqueries in the select list and the `WHERE`, `EXISTS`, `NOT EXISTS`,
  correlated `IN`, `HAVING EXISTS`, two levels: 25 cells per mode, 21 now equal Spark (20 fixed), 4 still refuse with
  a06c's own text, 0 internal errors.

**F1, a user-written marker answered.** `SELECT __repark_values_cell(1)` answered `[1]`; a06c and Spark refuse
`[UNRESOLVED_ROUTINE]`.
- **Cause:** the marker was a registered UDF named `__repark_values_cell`, on the Spark door, the native door and the
  fragment context. `consume.rs:24` skipped plans without a `VALUES`, and `marked_argument` (`consume.rs:228-244`)
  took a user's marker inside a cell as a seat's.
- **Fix:** the marker's name is `__repark_values_cell_<16 hex>`, drawn once per process from `RandomState` (as
  `repark-core`'s attribute ids are). No user text names it, so a user's `__repark_values_cell`, in any case or
  position, on either door, plans as on main and refuses with main's error class and text. No plan pays for it.
- **Pins:** 21 shapes on each door and 6 fragment shapes on both, both modes; every stray cell of both verdicts is
  now EQUAL → EQUAL with a06c's exact text.

**The cost.** Seat V marked every cell of every multi-row `VALUES`. All of its time sat in `spark.sql` (parse, mark,
plan, eager analysis); `collect` of a planned frame cost the same as a06c's. A phase timing of 10,000 rows
(`values_marker/tests.rs`, `seat_v_phase_timing`, debug profile) puts about 87% of the extra in the SQL planner's
handling of the 20,000 marker calls (each `get_type` of a function call builds its field and its display name), about
13% in the analyzer, and the AST pass at about 18 ms. The delta:
- the AST pass leaves alone a column that cannot hold both a DECIMAL and a FLOAT or DOUBLE (a cell it cannot type
  counts as both);
- a mixing column whose cells it types fully (decimal and integer literals, `CAST`s to a numeric type, `NULL`) is cast
  to `DOUBLE` in the AST, cell by cell, which is what the consumer wrote; no marker is planned;
- only a mixing column with a cell it cannot type is marked, as before;
- otherwise one non-`NULL` cell carries a witness marker, so the tripwire still knows a seat saw the `VALUES`;
- the `F.expr` fragment form marks a mixing column instead of casting it, so a native frame keeps DataFusion's answer;
- `decimal_in_form` returns the per-row form at the 33rd distinct wide range instead of computing every constant's
  preimage on each optimizer pass.

**The cost, after** (5 alternating runs for the held and `VALUES` rows, 3 for `IN` and C-012, on `pkg-p2-12101e9b`;
`evidence/part2/cost.md`): `plan_values10k` 1.026 (was 1.132), `plan_values10k_none` 1.008 (1.115),
`plan_values10k_dbl` 1.033 (1.145), `ins_values10k_dec` 1.010 (1.153), `ins_values10k_dbl` 1.005 (1.089),
`merge_values1000` 1.004 (1.306), `in_dbl:d38:1000` 1.028 (1.083), `notin_dbl:d38:1000` 1.046 (1.134): every held
row at most 1.05×. Reported: `ins_values10k_dec_mixed` 1.379 (was 1.504; Spark's cast through `double`, then the
store cast), `ins_values10k_dbl_mixed` 1.001, `merge_values1000_mixed` 1.074. `prune_d_dbl` reads 1.090 inside its
control band (0.871 to 1.288; a 55 ms sample). C-012: 1.010 and 1.004. No halt.

**Files:** the delta changes 4 product files, all among the build's 9 (`values_marker.rs`,
`values_marker/consume.rs`, `extension/decimal_float_widen.rs` in `repark-spark`, `column/expr_build.rs` in
`repark-python`); the cap stays at 9 of 10. Pins: `python/repark/tests/sql/test_decimal_double_coercion_2_delta.py`
(336 cells: red on a06c for the family and on the build's head for F1, F2 and F3, green on the delta) and four new
Rust tests in `values_marker/tests.rs`.

**The bar** (`evidence/part2/bar.md`, the delta section): the build's sets give the build's counts cell for cell;
part 1's 1,209 preimage cells and step 0's 6,855 joined cells move 0; both verifiers' sets, re-run on the delta
against their own Spark and a06c raw sets, give 0 EQUAL → WRONG, 0 EQUAL → RAISE and 0 native moves.
