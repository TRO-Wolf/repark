# Card UNIQUE-PROJECTION-NAMES-1: a select list with two casts of one column refuses "Projections require unique expression names", where Spark answers

**Date:** 2026-11-16. **Filed by:** the records clerk (`claude-haiku-5-5`), from Fable's simplifier audit of 2026-11-16 (difference 11). Measured before #1341 and #1345, on main's package `pkg-ddu1-7b21bde3` against Spark 4.1.2. The sweep re-run on main `e50ed17f` does not cover this audit cell; it was not re-measured on that main.

**Status:** ruled 2026-11-16, not dispatched; not label-blocking.

**Severity:** S2, ruled 2026-11-16, not label-blocking. A refusal where Spark answers, on an ordinary select list.

Ruled 2026-11-16 ("Merge those and defaults."): S2, not label-blocking; the label text lists this loud card as an exclusion.

## Finding

On the Spark door, a select list that casts one column twice, to two types, fails at planning with `Projections require unique expression names`. The two DataFusion expression names collide, so the planner refuses the list. Spark names the two casts apart and answers.

## Measured

| cell | Spark 4.1.2 | main |
|---|---|---|
| `SELECT id, CAST(ts AS BIGINT), CAST(ts AS STRING) FROM tt`, ANSI on | answers (row 0: `1704074400`, `2023-12-31 18:00:00`) | refuses: `Projections require unique expression names but the expression "CAST(datafusion.public.tt.ts AS Int64)" at position 1 and "CAST…` |
| `SELECT CAST(x AS BIGINT), CAST(x AS DOUBLE) FROM t` | answers | refuses: the same message |

The second cell is the audit's report figure; its v2 probe has no saved output in the check folder. The first is the recorded ANSI-on run.

Reproduction, with `<scratchpad>` the Fable audit scratch folder (not committed):
`python3 <scratchpad>/audit/check/verify.py repark true`, and the same with `spark`; the case `zone_seed`.

## Related, and not the same class

ROUND-DOUBLE-EXACT-1 records a side finding of the same message on the Spark door (`SELECT round(1.005D, 2), round(1.005E0, 2)`), not fixed. STRUCT-COMPARE-FAMILY-RESIDUE-1 clause C6 records duplicate `selectExpr` items failing the same way. Neither card carries a unit for the name collision itself.

## Not yet known

The seat: which name the two casts share, and where the Spark door builds the names.

Up: [map.md](map.md).
