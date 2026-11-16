# Card CAST-LITERAL-UNWRAP-1: a narrowing cast compared with a constant answers unlike Spark

**Date:** 2026-11-16. **Filed by:** the step-0 agent (`claude-opus-5-5`), from Fable's finding of the same day (§1 of
its reply on DECIMAL-DECIMAL-UNION-WIDEN-1) and the orchestrator's reproduction. Measured on main `83df6be1`'s
package against Spark 4.1.2, both ANSI modes, every door.

**Status:** step 0 measured; label-blocking, ruled 2026-11-16
([../../wo/cast-literal-unwrap-1/rulings-2026-11-16.md](../../wo/cast-literal-unwrap-1/rulings-2026-11-16.md)).
In the order after #1341 and before DECIMAL-COMMON-TYPE-1. Pre-existing: DataFusion 54.1.0 behaviour, unchanged in
55.2.0.

**Severity: S1.** Silent wrong answers and wrong writes on ordinary filters (`CAST(amount AS INT) = n`) over
scalar columns through the Spark door, and, with ANSI on, rows written where Spark raises.

## Finding

DataFusion's expression simplifier rewrites `CAST(c AS T) <op> k` as `c <op> k'` whenever the constant `k` converts
exactly to `c`'s type, without checking that the cast of `c` loses nothing
(`datafusion-optimizer-54.1.0/src/simplify_expressions/unwrap_cast.rs:64-141`). A rounding or truncating cast of the
column is thereby replaced by an exact comparison: `CAST(c AS INT) = 1` on a `DECIMAL(10,2)` column runs as
`c = 1.00`. With ANSI on, the overflowing values are never cast, so Spark's overflow error never happens. The cast
kernel itself matches Spark in range.

## Measured headline cells

| column, values | cell | Spark 4.1.2 | main |
|---|---|---|---|
| `DECIMAL(10,2)`: 0.50, 1.00, 1.25, 1.50, 1.99, … | `CAST(c AS INT) = 1` | 1.00, 1.25, 1.50, 1.99 | 1.00 |
| the same | `CAST(c AS DECIMAL(10,0)) = 1` | 0.50, 1.00, 1.25 | 1.00 |
| the same | `CAST(c AS DECIMAL(10,1)) = 1.3` | 1.25 | no rows |
| the same, Iceberg v2, CoW and MoR | `DELETE … WHERE CAST(c AS INT) > 1` | deletes 2.50, 99999999.99 | also deletes 1.25, 1.50, 1.99 |
| `BIGINT` holding 4294967297 | `CAST(b AS INT) = 1` | ANSI off: also 4294967297 (wraps); ANSI on: raises | 1 only, no raise |

Counts per ANSI mode, 3,702 cells: 1,320 wrong answers and 38 wrong writes with ANSI off; 711 wrong answers,
28 wrong writes and 738 missed raises with ANSI on. All of them in the numeric families: DECIMAL to an integer,
DECIMAL to a smaller scale or precision, an integer to a narrower integer. Every operator (`<=>` included), both
sides, `IN`, `NOT IN`, `BETWEEN`; `WHERE`, the select list, `CASE WHEN`, `HAVING`, `JOIN … ON`; the Spark door, the
DataFrame API, `F.expr`, `filter(str)` and the native door; temp views, Parquet and Iceberg tables alike; `DELETE`,
`UPDATE`, `MERGE ON` and `MERGE WHEN MATCHED AND`, copy-on-write and merge-on-read. FLOAT, DOUBLE, STRING and
timestamp-to-date casts are not unwrapped and agree, bar kernel refusals carded apart.

## Step 0

[../../wo/cast-literal-unwrap-1/](../../wo/cast-literal-unwrap-1/map.md): the sketch (seat W, an identity shield
around the narrowing cast whose `preimage` gives the exact range, recommended; seat P, patched DataFusion), the domain
table, the insertion inventory, the evidence, and six owner questions with defaults.

## Not covered elsewhere

[DECIMAL-DECIMAL-UNION-WIDEN-1](decimal-decimal-union-widen-1-card-2026-11-16.md), now DECIMAL-COMMON-TYPE-1,
depends on this unit for its cells with a constant. [CAST-FLOAT-DECIMAL-OVERFLOW-NULL-1](cast-float-decimal-overflow-null-1-card-2026-11-15.md)
and [CAST-INFINITY-TO-DECIMAL-NULL-1](cast-infinity-to-decimal-null-1-card-2026-11-02.md) are kernel refusals of
FLOAT and DOUBLE casts; no card names the unwrap.

## Append (2026-11-16): built (seat W, both placements; branch `fix/cast-literal-unwrap-1`, not merged)

**Rulings.** Owner, about 14:40 UTC, verbatim "defaults" (S1, label-blocking; #1341, then this unit, then
DECIMAL-COMMON-TYPE-1). Owner, about 16:25 UTC, verbatim "adopt Fable's, run the string-cast sizing", on step 0's six
questions: (1) seat W, with the shield rule also in the optimizer, before every `SimplifyExpressions` (the signed-zero
re-run included) and last; (2) the ANSI-off overflow refusal accepted with a disclosure, carded
[ANSI-OFF-NARROWING-OVERFLOW-1](ansi-off-narrowing-overflow-1-card-2026-11-16.md) (S2); (3) STRING casts held for
their own sizing; (4) the top-of-type comparison out of the unit, pinned, carded
[CAST-TOP-OF-TYPE-FOLD-1](cast-top-of-type-fold-1-card-2026-11-16.md) (S3); (5) cost bars 1.05x with both placements
in; (6) the four side cards to the records clerk.

**Seat.** `repark_cast_shield` wraps each narrowing `Cast` / `TryCast` compared with a column-free side (`=`, `<>`, `<`,
`<=`, `>`, `>=`, `<=>`, `IS [NOT] DISTINCT FROM`, `[NOT] IN`, `[NOT] BETWEEN`; directly, under widening casts, or under
`__repark_decimal_to_double__`) in `__repark_cast_shield__`, an identity UDF whose `preimage` gives the exact range on
the column: truncation toward zero to an integer type, half away from zero to a smaller scale, the lower bound clipped
to the column's range, declining for every cast that can overflow (the cast then runs per row). A shielded `[NOT] IN`
of up to 32 literals becomes its ranges; past 32 it stays per row. **Placements:** last among the analyzer rules
(`finish_session_build`), and in the optimizer before each `simplify_expressions`, before `signed_zero_literals`'
internal re-run, and last. A plan with no cast under a comparison pays one `exists` walk per placement.

**Files (4 of the 8-file cap):** `crates/repark-core/src/session/df_guards/cast_shield.rs` (the rule) and
`cast_shield/udf.rs` (the UDF, `preimage`, the narrowing / lossless / overflow predicates), both new;
`session/df_guards.rs` (module, registration, both placements); `session/df_guards/signed_zero/literals.rs` (the
shield before the re-run). One-line exports and registrations: `session.rs`, `lib.rs`, the `F.expr` context in
`repark-python` `column/expr_build.rs`. The UDF lives in `repark-core`, not `repark-functions`, because the core rule
builds it and `repark-core` (tier 2) may not depend on `repark-functions` (tier 3).

**Bar** ([evidence/built/bar.md](../../wo/cast-literal-unwrap-1/evidence/built/bar.md)), head `88999454` against main
`cb7d3943` and Spark 4.1.2, step 0's domain table, its shapes and the critic's four sets, both modes, 9,569 cells:
- main 4,573 EQUAL / 2,728 WRONG / 1,119 MISSED-RAISE / 1,149 REFUSE; head 7,593 / 24 / 0 / 1,952;
- **0 EQUAL→WRONG, 0 MISSED-RAISE**; EQUAL→REFUSE only in the named class **EQUAL→REFUSE (Q2), 122 cells** (ANSI off,
  overflow-capable casts over overflowing values, kernel refusals where Spark wraps or answers NULL);
- **WRONG→REFUSE (Q2), 648 cells**; **WRONG→REFUSE (Q4), 33 cells** (top-of-type);
- the 24 head WRONG cells are step 0's DataFrame `datetime`-literal exclusions and three `EXPLAIN` texts;
- 1,149 refusals unchanged (kernel refusals carded apart, `int()` / `bigint()`, the `createDataFrame` view chain);
- the native door moves as the Spark door (334 WRONG and 344 MISSED-RAISE to EQUAL).

**Cost** ([evidence/built/cost.md](../../wo/cast-literal-unwrap-1/evidence/built/cost.md)), 5 alternating runs:

| row | ratio | bar |
|---|---|---|
| C-012 `c012p6` / `c012rest` | 1.008 / 1.008 | 1.05x |
| `plan_pred200` / `plan_values10k` / `plan_union200` / `plan_free200` / `plan_wide_dd` | 1.009 / 1.011 / 1.004 / 1.027 / 1.045 | 1.05x |
| C2 `= 1` / `< 1`, cast vs hand-written range | 0.959 / 1.027 | 1.05x |
| C3 CoW and MoR, scan tasks cast vs hand-written | 16 = 16 | equal |
| `DELETE … CAST(c AS INT) = 1`, CoW / MoR, vs hand-written | 0.980 / 1.012 | 1.05x |
| C4 per row, `BIGINT`→`INT` Parquet / Iceberg; `DECIMAL(10,2)`→`TINYINT` Parquet / Iceberg | 0.960 / 1.036; 1.711 / 1.279 | reported |
| `IN` of 40, per row | 1.193 | reported |

**Pins:** `python/repark/tests/types/test_cast_literal_unwrap_1.py` (both modes; 177 of its 193 tests red on main, the
other 16 the controls and the one cell main already answered), and `repark-core` `session/df_guards/cast_shield/tests.rs`,
whose `the_ruling_one_shapes_need_the_optimizer_placement` shows s17, s16, s21 and s22 answering `[2]` with the
optimizer placement stripped and `[2, 3, 4, 5]` with both. **Found:** the Iceberg scan prunes no DECIMAL predicate,
shielded or hand-written (the fork converts no decimal literal).

Up: [map.md](map.md).
