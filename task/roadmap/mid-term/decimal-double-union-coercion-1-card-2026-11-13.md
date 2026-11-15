# Card DECIMAL-DOUBLE-UNION-COERCION-1: a DECIMAL literal unified with a DOUBLE types the column `decimal(30,15)`, so a `UNION` write stores 0.0 for 1e-20

**Date:** 2026-11-13. **Filed by:** the records clerk (`claude-opus-5-5`). **Sources:** orchestrator measurement,
2026-11-11, against Spark 4.1.2; Fable guidance, 2026-11-13, which flagged it as a label blocker missing from the
xhigh inquiry.

**Status:** filed 2026-11-13, not dispatched; ruled 2026-11-13 (see the append). First put to the owner on the 2026-11-12 morning board
([page](../../../docs/artifacts/morning-board-2026-11-12.html), item 2, "New S1") without a ruling. The board's
default was to open it as the next lane unit, after signed-zero part B or ahead of it. Severity **S1**, a wrong
write. **Label-blocking:** it is on scalar columns, so it is inside the scoped label ICE-V2-FLAT-AWS-LABEL-1 and the
full format-v2 label.

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
