# Card RAND-SHARED-VALUE-1: two rand() calls in one row share one value on main, where Spark draws each independently

**Date:** 2026-11-16. **Filed by:** the records clerk (`claude-haiku-5-5`), from Fable's simplifier audit of 2026-11-16 (difference 8). Measured before #1341 and #1345, on main's package `pkg-ddu1-7b21bde3` against Spark 4.1.2.

**Status:** PROPOSED, not dispatched; severity and label-blocking: owner to rule.

**Severity:** Proposed: S1 (owner to rule). A silent wrong answer on a plain query shape.

## Finding

On main, `rand() AS x, rand() AS y` gives the same value in both columns of every row. Spark draws two independent values. A conjunction of two `rand()` predicates keeps the wrong fraction of rows as well: on main about half, where two independent draws keep about a quarter.

## Measured

ANSI on, from the audit's recorded outputs. The count is over 100 rows.

| cell | Spark 4.1.2 | main |
|---|---|---|
| rows with `x = y`, `rand() AS x, rand() AS y` over 100 rows | 0 | 100 |

The conjunction figure is the audit report's: `rand() < 0.5 AND rand() < 0.5` over 1,000 rows kept 503 rows on main, where independent draws keep about 250. The audit's v2 case that holds this figure has no saved output in its check folder.

Reproduction, with `<scratchpad>` the Fable audit scratch folder (not committed):
`python3 <scratchpad>/audit/check/verify.py repark true`, and the same with `spark`; the case `rand_pair`.

## Not yet known

The seat. Not diagnosed. The non-determinism refusals in joins (`v1-5-4-release-notes-2026-10-10`) are a refusal class, not this sharing.

Up: [map.md](map.md).
