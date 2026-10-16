# ICE-TSNS-TYPE-PLANNER-1 — amendment 1 (step 0 halts H1 to H4)

**Date:** 2026-10-13 late. **By:** the Frontier agent (Claude, claude-fable-5-1), the Fable High pass the orchestrator asked for. **Applies to:** `task/wo/ice-tsns-type-planner-1/sketch.md` at main `be011012`; step 0 head `3a00ba80`. The clerk lifts this file in as §2a and amends §4 and §5 as written below. No product code changes with this amendment.

## Step 0 result, accepted

Three of four halts met. H1 by one name, H2 by one spelling of eight, H3 by twenty cells, H4 not met. The round changed no product file and its evidence is committed. The executor's leans on H1 and H2 are adopted; H3 is resolved by the rule below, which the sketch foresaw and had left unnamed.

## H1 — resolved

Scalar `range` and `generate_series` over a timestamp input are refused by name with the other DataFusion-only names (§3 P5). The refusal says that Spark's `range` is a relation of integers and names `sequence` as the Spark spelling for a timestamp series. Integer `range` as a relation is untouched. One census cell per door per function.

## H2 — resolved

Proceed. `CREATE TABLE t (c TIMESTAMP)` resolves through the catalog mapping and already creates microseconds; `F.col("x").cast("timestamp")` bypasses the planner and already answers microseconds. Both become pinned census cells at microseconds so a later change to either path is caught. The planner is still registered once, in the session builder, and reaches the six expression spellings on both doors, as measured.

## H3 — resolved: §2a, the unit lattice rule

**Why it is needed.** DataFusion's `timeunit_coercion` (datafusion-expr-common 54.1, `type_coercion/binary.rs:2048`) joins two timestamp units to the **coarser** one: `(Microsecond, Nanosecond) → Microsecond`. It is a free function, not a hook, so it cannot be replaced. Once the planner types `TIMESTAMP` as microseconds, `coalesce(ns_col, TIMESTAMP '…')`, `array(ns_col, us_col)`, `CASE … THEN ns … ELSE us`, `UNION ALL` of a nanosecond and a microsecond branch, and `ns_col = us_col` would all drop digits by DataFusion's own coercion. Twenty measured cells, identical on plain DataFusion and on RePark's session.

**The rule.** `SparkTimestampUnitWiden`, an analyzer rule in `repark-functions`, inserted at the `type_coercion` position exactly as `SparkFractionalDivision` is (`lambda_rebind.rs:33–42`), ahead of every other pre-coercion rule so it sees the plan as the planner left it. It fires only where two or more timestamp-typed expressions must share one type **and their units differ**. In that case the shared type is the **finest** unit present, with the zone that the existing LTZ/NTZ rule already assigns to that position (this rule never touches zones), and every coarser input receives an explicit `Cast` to it. DataFusion's coercion then sees equal types and does nothing.

**Positions it covers,** by expression and plan shape, not by name of the producer:

| Shape | Where |
|---|---|
| unit-joining scalar functions | `coalesce`, `nvl`, `ifnull`, `nvl2`, `greatest`, `least`, `array` / `make_array`, `array_append`, `array_prepend`, `array_concat`, `array_contains`, `array_position`, `array_remove`, `element_at` with a timestamp value |
| conditionals | `CASE` (searched and simple), `if` |
| set operations | `UNION`, `UNION ALL`, `INTERSECT`, `EXCEPT` branches, per column |
| comparisons | `=`, `<>`, `<`, `<=`, `>`, `>=`, `BETWEEN`, `IN (list)`, `<=>`, join `ON` keys (equi and non-equi) |
| a `DATE` or a string beside a zoned timestamp (added 2026-10-16, owner ruling 119 (a), under ruling 100) | the shared positions above and `UNION` columns: the `DATE` or string is cast to the timestamp's unit and zone, Spark's typing of a `DATE` or `STRING` beside `TIMESTAMP` (PR #1082 commit `932bc5df`; restores `test_store_ts_doors_2`) |
| a `VALUES` column DataFusion coerced to `Timestamp(ns, None)` from a `DATE` (added 2026-10-16, owner ruling 119 (a), under ruling 100) | retyped before `type_coercion`: beside a zoned timestamp it takes that type; beside a naive expression that is not a written wall it is read as the instant, as main reads it. The choice is made by an expression-shape test (`values_evidence`), not by type (PR #1082 commit `ad52bcdc`; restores the mixed `VALUES` group, `ntz_values_mix`) |
| aggregation and windows | nothing; a single column keeps its unit |

**Semantics.** Widening a microsecond instant to nanoseconds is exact inside the nanosecond range (1677-09-21 to 2262-04-11). A microsecond value outside that range raises the overflow class in **both** ANSI modes, as the coercion ruling of 2026-10-12 requires; it never stores a floor and never yields NULL. The rule never narrows. It does not fire when units are equal, when only one side is a timestamp (string, integer and NULL coercions stay DataFusion's, and the planner's types make them microseconds or nanoseconds as written), or on a bare NULL beside a nanosecond value, which DataFusion already types as nanoseconds (pinned, since that was R-017's shape).

**Why this is not a plan walk.** The rule decides by the types of the two sides at the position, never by where an expression came from. There is no user-cast versus planner-cast question left, because the planner has already typed every written spelling, and a cast the rule inserts is distinguishable by its type, not by provenance.

**What it costs.** One analyzer pass over the plan, firing rarely. The 1.05 write and read cost medians stand as the gate.

**Amended 2026-10-16 (owner ruling 119 (a)).** The two rows added to the table above go beyond this section's text, which says the rule does not fire when only one side is a timestamp and decides by type, never by where an expression came from. The owner authorized both under ruling 100: each restores answers today's main gives, and no stored value loses digits (U-1 verifier, 2026-10-16, finding F-2). The text above stays as filed.

## H4 — not met, one producer added

No text reader infers nanoseconds. CSV with `inferSchema` and a header infers `timestamp[s]`, a seconds unit the inventory did not list. Added as **P15**: a reader-inferred timestamp of any unit other than microseconds is cast to microseconds at the read door (Spark infers `TimestampType`, microseconds), widening from seconds exactly; measured as its own cell on both doors. If the cell shows Spark's inferred value differs from ours in any arithmetic or display, it becomes its own registry row and does not block this unit.

## §4, amended halts for step 1

- H5: the rule must insert a cast at a position not in the table above → halt, name the shape, no silent extension of the table.
- H6: a join key in the **optimized** plan (EXPLAIN after optimization) still carries mixed units after the rule → halt; the rule's placement is wrong, not its table.
- H7: any cell where the widened result differs from the nanosecond-typed result of the same expression written with explicit `timestamp_ns` casts → halt.

## §5, amended pins

- The twenty H3 cells × both doors × three session zones, expected category derived by the harness from the rule (finest unit, exact value).
- Join keys read from the optimized plan, not the analyzed one.
- Postgres cells run with the rootless Docker socket inside the lane (`DOCKER_HOST=unix:///run/user/1000/docker.sock`); step 0 could not reach it and step 1 must.
- NULL beside a nanosecond value in every position of the table.
- Non-default session zones for every H3 cell.
- H1 refusal cells; H2 DDL and `Column.cast` cells at microseconds; P15 cell.
- Mutants added: M6 rule removed (the twenty cells narrow); M7 rule joins to the coarser unit; M8 rule fires on equal units (cost and plan-shape cells catch the needless casts).

## Order against R-009

R-009 and U-1 both touch `timestamp_ns_cast.rs`. U-1 lands first. R-009's measurement step 0 may run on Muse now; its build waits for the planner head and re-measures its 72 control cells there, since the planner removes the by-name path some of those doors go through today.
