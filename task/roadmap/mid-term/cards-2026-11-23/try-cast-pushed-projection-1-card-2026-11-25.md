# Card TRY-CAST-PUSHED-PROJECTION-1: a shared `TRY_CAST` pushed into the Parquet scan's projection crashes

**Date:** 2026-11-25. **Filed by:** the orchestrator (`claude-opus-5-5`), from the delta-1 ruling on
TRY-CAST-NONNULL-FIELD-1 (PR #1429). **Severity:** S2 proposed; the owner rates it. It crashes loudly and never
answers wrongly. **Status:** filed, not dispatched.

## Measured

The cells are on the [TRY-CAST-NONNULL-FIELD-1 card](try-cast-nonnull-field-1-card-2026-11-24.md), under "A pushed
projection". Over a NOT NULL Parquet column `s` holding `'abc'`, `'7'` and `'-5'`, in both ANSI modes:

- `SELECT id, map('k', TRY_CAST(coalesce(s, 'x') AS INT))['k'] AS a, abs(TRY_CAST(coalesce(s, 'x') AS INT)) AS b
  FROM pq_nn` answers `[1, NULL, NULL], [2, 7, 7], [3, -5, 5]` on Spark 4.1.2.
- Main and both #1429 heads crash with `Found unmasked nulls for non-nullable StructArray field`. The same query with
  `map('m', …)['m']` as `b` crashes the same way.
- Without the `map` consumer, the query equals Spark.

## Cause

Under the default `pushdown_filters = false`, DataFusion 54.1.0 can push a bare `TRY_CAST` that common-subexpression
elimination shares between two consumers into the Parquet scan's own projection. #1429's physical seat cannot reach
that projection, because the field is private in DataFusion.

## First step

Measure which consumers and which settings put the shared cast in the scan projection, then choose between keeping
the shared expression out of the scan and repairing its field after the scan. Patching DataFusion is out of scope.

Up: [map.md](map.md).
