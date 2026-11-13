# Card DECIMAL-PRUNING-CONJUNCT-1: a pushdown-only decimal conjunct for widened decimal-column predicates, built later

**Date:** 2026-11-13. **Filed by:** the records clerk (`claude-opus-5-5`). **Sources:** the DECIMAL-DOUBLE-UNION-COERCION-1
design sketch (#1324, Q7 and "Cost and pruning"; [sketch](../../wo/decimal-double-union-coercion-1/sketch.md));
owner, 2026-11-13, about 14:52 UTC, verbatim "Defaults, and merge #1323 when green" (Q7: pruning comes later; today's
"no decimal pushdown" is pinned).

**Status:** filed 2026-11-13, not dispatched; deferred by ruling. Severity **S4**: a cost item, no wrong answer.

## Measured (main `04d84ab9`, a three-file table `(id INT, d DECIMAL(10,2), v DOUBLE)`, `EXPLAIN`)

| predicate | scan predicate | files |
|---|---|---|
| `d = 0.5` | none | 3 |
| `d = CAST(0.5 AS DOUBLE)` (today) | none | 3 |
| `CAST(d AS DOUBLE) = CAST(0.5 AS DOUBLE)` (widened) | none | 3 |
| `v = CAST(0.5 AS DOUBLE)` (control) | `v = 0.5` | 1 |

The fork converts no decimal literal to an Iceberg datum (`expr_to_predicate.rs:610-643`), so widening loses no
pruning today.

## The design (not built)

For a widened `CAST(d AS DOUBLE) op <double constant>` inside a filter, also push `d` against the constant rounded
outward to `d`'s scale, a superset, while the double predicate stays the exact filter.

## Trigger and close

The decimal unit pins today's "no decimal pushdown" rows; when the fork learns decimal datums, that pin fails and this
card is built, pinned by the files a widened decimal-column predicate scans.
