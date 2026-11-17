# Card DECIMAL-DECIMAL-UNION-WIDEN-1: a `UNION ALL` of mixed DECIMAL types refuses where Spark narrows to fit

**Date:** 2026-11-16. **Filed by:** the records clerk, from the 2026-11-15 records backlog (item 2; #1339 verifier
note N5, also fails on main before #1339). Measured on main `a06c1088`'s package against Spark 4.1.2, both ANSI
modes, the Spark door and the native door.

**Status:** merged as #1350 (`a792ce47`, 2026-11-17), after step 0 (#1347), the build and its verifier (PASS).

**2026-11-17:** the Status line above is corrected in place. It read "PROPOSED, not dispatched. Pre-existing: main refuses it too." after #1350 merged (the orchestrator, `claude-opus-5-5`).

**Severity: S1.** Beside the refusals, neighbouring two-branch cells answer with the wrong type and wrong values
(`i2_p2`, `i2_q3`, `i2_q4` below): a `UNION ALL` of decimals is a common shape, and a mistyped union silently rounds
away fractional digits (1.25 becomes 1).

## Finding

A `UNION ALL` whose branches are mixed DECIMAL types refuses at planning with
`datafusion engine error: Optimizer rule 'simplify_expressions' failed` wherever the common type would need more
than 38 digits of precision. The smallest failing shape is two branches: `DECIMAL(38,0)` with `DECIMAL(38,38)`.
Spark answers every shape below by narrowing the result to fit `decimal(38, *)`, rounding half up.

## Measured

All RePark rows are identical with ANSI on and off, on the Spark door and the native door, except where noted.
`A` is `SELECT CAST(1 AS DECIMAL(38,0))`, `B` is `SELECT CAST(0.5 AS DECIMAL(38,38))`,
`C` is `SELECT CAST(1.25 AS DECIMAL(10,2))`.

| cell | branches | RePark | Spark 4.1.2 (both modes) |
|---|---|---|---|
| 3-way | A + B + C | refuses (`simplify_expressions`) | `decimal(38,0)`, rows 1, 1, 1 |
| `i2_p1` | A + B | refuses | `decimal(38,0)`, rows 1, 1 |
| `i2_p3` | B + C | refuses | `decimal(38,30)`, rows 0.500…0 (30dp), 1.250…0 (30dp) |
| `i2_p2` | A + C | `decimal(38,2)`, rows 1.00, 1.25 | `decimal(38,0)`, rows 1, 1 |
| `i2_q3` | `DECIMAL(38,0)` + `DECIMAL(38,1)` | `decimal(38,1)`, rows 1.0, 1.5 | `decimal(38,0)`, rows 1, 2 |
| `i2_q4` | `DECIMAL(20,0)` + `DECIMAL(20,20)` | `decimal(38,20)` | `decimal(38,18)`, rows 1.0…0, 0.5…0 (18dp) |
| `i2_p4` | `DECIMAL(10,0)` + `DECIMAL(10,2)` | `decimal(12,2)` | `decimal(12,2)` — agrees |
| `i2_q2` | `DECIMAL(10,10)` + `DECIMAL(10,2)` | `decimal(18,10)` | `decimal(18,10)` — agrees |
| same-type | `(38,38)` + `(38,38)`, `(10,10)` + `(10,10)`, `(6,6)` + `(6,6)`, `(38,0)` + `(38,0)` | answers | answers — agrees |

So two defects share one seat: wide mixed pairs refuse outright, and narrower mixed pairs answer a wider type than
Spark's (Spark trims scale to hold precision 38; RePark keeps the full scale). Same-type pairs agree on the Spark
door. An overflow literal (`CAST(1.5 AS DECIMAL(38,38))`, ANSI off) refuses inside the union on RePark where Spark
answers the row as NULL; with ANSI on Spark raises `NUMERIC_VALUE_OUT_OF_RANGE`.

Side note, native door only: the same-type `(38,38)` union answers `0.4999…` (binary noise,
`dec:0.49999999999999998874404911728017014784`) for 0.5, where the Spark door answers `dec:0.5…0` exactly. A wrong
value on one door of an agreeing cell.

## Not covered elsewhere

[DECIMAL-DOUBLE-UNION-COERCION-1](decimal-double-union-coercion-1-card-2026-11-13.md) is the DECIMAL-beside-DOUBLE
half (unify to `double`); this card is DECIMAL-beside-DECIMAL. [NATIVE-UNION-LITERAL-INT-1](native-union-literal-int-1-card-2026-11-13.md)
is an INT arm against an untyped literal. Neither names decimal-decimal widening.

## Not yet known

The seat. From reading, the candidate is the set-operation decimal coercion beside the `widen_decimal_against_float`
path the DECIMAL-DOUBLE-UNION-COERCION-1 card names: step 0 must find where a union of two decimals computes a common
type past precision 38 instead of narrowing as Spark does, and why the narrower pairs keep full scale.

## Append (2026-11-16): ruled

The owner's word, 2026-11-16 about 14:40 UTC, verbatim: "defaults". The rulings are recorded in full at
[../../wo/cast-literal-unwrap-1/rulings-2026-11-16.md](../../wo/cast-literal-unwrap-1/rulings-2026-11-16.md).
What they change for this card:

- **Renamed DECIMAL-COMMON-TYPE-1.** This file keeps its name. The unit is one unit for Spark's whole decimal
  least-common-type family: unions, comparisons, `IN`, `CASE`, `coalesce` and the other positions where Spark picks
  one decimal type for several operands, not `UNION ALL` alone.
- **Label-blocking.** It blocks the scoped label ICE-V2-FLAT-AWS-LABEL-1. Severity stays S1.
- **Lossy comparison matches Spark.** RePark compares as Spark does, rounding to the common type: `DECIMAL(38,0)` 1
  equals `DECIMAL(10,2)` 1.25, because both become `decimal(38,0)`.
- **The order.** #1341 (DECIMAL-DOUBLE-UNION-COERCION-1 part 2), then CAST-LITERAL-UNWRAP-1, then this unit, then
  NAN-MINMAX-1 part 2, DOUBLE-DECIMAL-DATAFRAME-1 and SIGNED-ZERO-UPDATE-IN-LIST-1.
- **Depends on CAST-LITERAL-UNWRAP-1 for every cell with a constant.** Where a column is the operand that loses
  digits in the common type and the other side is a constant, DataFusion's simplifier moves the rounding cast onto
  the constant and compares exactly ([CAST-LITERAL-UNWRAP-1](cast-literal-unwrap-1-card-2026-11-16.md)). Those
  cells cannot pass until that unit lands; column-against-column cells do not depend on it.

## Append 2026-11-16: the c2 cell (records clerk, `claude-haiku-5-5`)

`VALUES (1), (decimal(10,2))` types `decimal(22,2)` on both repark builds of the #1341 re-check, where Spark types `decimal(12,2)`. The values are equal in both. The verdict classes it as a pre-existing int and decimal precision gap (class c, reproduced in both ANSI modes). It is a step 0 cell for this unit, the unit this card's rename note names DECIMAL-COMMON-TYPE-1. Source: the #1341 re-check verdict, held outside the repository. The text above is unchanged.

## Append (2026-11-16): step 0 measured

Measured on main `e50ed17f` against Spark 4.1.2, both ANSI modes, by Claude Opus 5.5 (`claude-opus-5-5`); the
sketch, domain table and inventory are in [../../wo/decimal-common-type-1/](../../wo/decimal-common-type-1/map.md).
The text above is unchanged.

- **Spark door, per ANSI mode:** 874 common-type cells, **825 wrong** with ANSI off (356 wrong answers, 7 wrong
  types, **38 wrong writes**, 424 refusals Spark answers), 819 wrong plus 6 both-raise with ANSI on; 161 of 161
  control cells agree. Native door: 612 of 655 wrong.
- **Wrong writes:** Iceberg `INSERT … UNION ALL`, `INSERT … VALUES`, CTAS, RTAS, `DELETE`, `UPDATE`, `MERGE ON`
  and `MERGE … INSERT`, copy-on-write and merge-on-read, and `DELETE` on decimal-partitioned tables; each is named
  in the domain table.
- **Spark's rule, measured:** integers take their decimal forms, an integer literal in a comparison its own
  digits, unions round once per binary node (so `c ∪ d ∪ a` rounds twice), functions and `VALUES` once.
- **The legacy flag:** in range it is DataFusion's rule (64 of 94 cells agree); 28 overflow cells answer NULL on
  Spark with ANSI off where main raises.
- **Recommended seat:** one shared Spark common-type function inside `SparkDecimalFloatWiden`'s walk, called by
  seat V too; one unit on the Spark door, with the other causes found beside it carded.
- **Revised after the inventory critic** (no cell contradicted): map keys, aliased `USING` joins, integer literals in
  `IN` lists and `INTERSECT`/`EXCEPT` membership are in the family too; with them, 890 of 946 Spark-door cells per
  mode are wrong with ANSI off (884 plus 8 both-raise with ANSI on).

## Append (2026-11-16): built (seat A)

Built by Claude Opus 5.5 (`claude-opus-5-5`) on branch `fix/decimal-common-type-1`, product commit `7bf32859`, on
main `33bfde4e`; not pushed. The bar, cost, probes and raw data are in
[../../wo/decimal-common-type-1/evidence/built/](../../wo/decimal-common-type-1/evidence/built/map.md). The text above is
unchanged.

**The rulings, verbatim.** The owner, 2026-11-16, about 14:40 UTC: "defaults" (S1, label-blocking; one unit for
Spark's decimal least-common-type family; match Spark's lossy comparison and union; after CAST-LITERAL-UNWRAP-1).
About 23:40 UTC, to step 0's seven questions: "Merge those and defaults." (one Spark-door unit with map keys, aliased
`USING`, `IN` literals and set-operation membership; Spark's double rounding in nested unions; the legacy flag only
skips the rule; the `VALUES (1), (decimal(10,2))` cell through seat V; struct fields in unions, struct comparisons a
both-raise pin; overflow literals answer as Spark's cast does, or are carded when the refusal is the literal's own
cast; the side cards filed by a records clerk). The label is scoped to ANSI on.

**Seat A, as designed** (Fable's guidance: one walk, one shared function):

- **The shared function**, `spark_decimal_common_type` (`crates/repark-spark/src/extension/decimal_common_type.rs`):
  Spark's `DecimalType.boundedPreferIntegralDigits`, integers as `TINYINT` (3,0), `SMALLINT` (5,0), `INT` (10,0),
  `BIGINT` (20,0); `Mode::Compare` keeps an integer literal's own digits, `Mode::Unify` (the `IN` mode, the critic's
  M3, and every unifying position) takes its type's form; `None` unless `range + scale > 38`. Structs, lists and maps
  unify field by field.
- **The walk**, `SparkDecimalFloatWiden` (`decimal_float_widen.rs`), one detect and one rewrite pass as before:
  comparisons, join keys, `BETWEEN` (split into its two comparisons, as Spark rewrites it), `IN` lists and subqueries,
  `CASE` (a simple `CASE` whose pairs differ turns searched), `if`, `coalesce`, `nvl`, `nvl2`, `nullif`, `greatest`,
  `least`, `array`, `map` keys and values, `map_concat`, `array_contains`, unions bottom up per binary node with struct
  fields, and the set-operation joins. An `INSERT` whose conform projection no longer matches the table type gets
  its cast back (`conform_insert`), so stored rows go through the store cast as Spark's do.

**The five explicit changes:** (1) seat V's gate (`values_marker/consume.rs`, `column_placement`) calls
`spark_decimal_type`, an `INT`-sized literal in Spark's `INT` form, and the statement marker
(`values_marker.rs`, `spark_decimal_column`) marks a column holding a decimal beside an integer literal or a decimal of
20 or more digits; (2) `map_concat` and `array_contains` join the walk's lists; (3) struct fields recurse
(`struct_shape`); (4) map keys are a position of their own, and a collision after rounding raises
`DUPLICATED_MAP_KEY`; (5) the `IN` mode. The legacy flag
`spark.sql.legacy.decimal.retainFractionDigitsOnTruncate=true` (builder, `SET`, `conf.set`; `RESET` and `unset`
restore) skips the decimal rule and seat V's decimal typing; part 1's double rule is unchanged.

**Product files (7 of the cap of 8):** `crates/repark-spark/src/extension/decimal_common_type.rs` (new),
`extension/decimal_float_widen.rs`, `values_marker/consume.rs`, `values_marker.rs`,
`crates/repark-functions/src/decimal_legacy.rs` (new, the flag's carrier beside the other conf carriers),
`crates/repark-python/src/session_runtime.rs`, `python/repark/src/repark/spark/session/builder_conf.py`; plus
one-line registrations in `crates/repark-spark/src/extension.rs` (the `mod` line and the builder-conf install line)
and `crates/repark-functions/src/lib.rs`.

**The bar** ([bar.md](../../wo/decimal-common-type-1/evidence/built/bar.md)): **0 EQUAL→WRONG, 0 EQUAL→RAISE** on both
doors and both modes. Of the 946 common-type cells per mode, 928 agree (54 on main) and all 38 wrong writes are
EQUAL; the 18 left per mode are named: 8 overflow-literal cells (ruling 6, carded: the literal's own cast refuses at
planning; carded as **DECIMAL-LITERAL-OVERFLOW-1**, new, for the records clerk), 2 `DUPLICATED_MAP_KEY` and 1 struct-comparison both-raise cells
(as ruled), `having_avg_max` (AVG-DECIMAL-ROUND-1), and three new findings for the records clerk, each reproduced on
main with user-written same-type casts: **MAP-FILTER-ORDER** (4 cells: a `map` evaluated above a filter the physical
plan moved after it meets a filtered-out NULL key), **FEXPR-DECIMAL-LITERAL** (1 cell: `F.expr("a = 1.25")` types the
literal as `double`), **DECIMAL-CAST-SELECTIVITY** (1 cell: DataFusion's selectivity assertion on a `BIGINT`-edge
cast). The native door answers as main's in all 912 cells.

**Cost** ([cost.md](../../wo/decimal-common-type-1/evidence/built/cost.md), 5 alternating runs of main, head, main):
every held row at most 1.05x — C-012 0.998 and 0.999, `plan_values10k` 0.998, `plan_union200` 1.003, `plan_free200`
0.978, `plan_wide_dd` 0.978, `plan_pred200` 1.005, K1 0.994 to 1.024 (`k1_pred200` the largest), K4 0.973, K5 1.018
(`DELETE`) and 1.012 (`UPDATE`); K3's cast form answers and scans as the hand-written range. Reported: K2's
200-comparison plan 5.5x (main's user-written casts cost the same), the five-branch union 0.976, the 10,000-row mixed
`VALUES` 0.861.

Up: [map.md](map.md).

## Append (2026-11-16): step 0 ruled

The owner, 2026-11-16, about 23:40 UTC, verbatim "Merge those and defaults.", ruled the seven questions of [step 0 (sketch §10)](../../wo/decimal-common-type-1/sketch.md) with the orchestrator's defaults. The record is [rulings-2026-11-16.md](../../wo/ice-v2-flat-aws-label-1/design/rulings-2026-11-16.md), section B:

- **B1, one unit on the Spark door,** with decimal map keys, aliased `USING` joins, integer literals in `IN` lists, and `INTERSECT` / `EXCEPT` / `UNION DISTINCT` membership.
- **B2, Spark's double rounding in nested unions:** one rounding per binary union node.
- **B3, the legacy flag's NULL-on-overflow is carded** as LEGACY-DECIMAL-TRUNCATE-OVERFLOW-1 (S3), not fixed. The flag only skips the rule.
- **B4, the `VALUES (1), (decimal(10,2))` typing cell is in this unit,** through seat V.
- **B5, struct fields in unions are in this unit.** Struct comparisons raise on both engines and get a pin only.
- **B6, overflow literals answer as Spark's cast does:** NULL with ANSI off, `NUMERIC_VALUE_OUT_OF_RANGE` with ANSI on. If the refusal is the literal's own cast, that is carded instead.
- **B7, the side cards are filed:** nine cards in [decimal-common-type-1-side/](decimal-common-type-1-side/map.md). AVG-DECIMAL-ROUND-1 is S1 and label-blocking; the other eight are not label-blocking. The sketch had proposed S2 for AVG-DECIMAL-ROUND-1.

This card's severity (S1) and label-blocking status are unchanged. The build is not dispatched by this record.
