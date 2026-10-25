# Card CONTAINER-CAST-SHAPE-1: a whole-container cast whose fields do not line up with the target stores in UTC and errors in Spark, outside the R-011 gate

**Date:** 2026-10-25. **Filed by:** the GATE-RETIRE-1 lane (Claude Opus 5.5, builder) and the Muse MAX verifier, from the round-1 FAIL on `f7b78fed`; ruling GR-9 of the same day.

**Status:** open. Grade C (clerk): the measurement is recorded here; no build order is written.

**Why:** in a UTC session the R-011 gate does not run, because it is zone-scoped, and a whole-container cast whose fields do not line up with the target type does not error as Spark does. This is pre-existing on main; GATE-RETIRE-1 did not cause it. The R-011 arm refuses the same shapes in New York.

**Measured** on head `8c483ced`, UTC, one snapshot each:

- `INSERT INTO t SELECT id, CAST(named_struct('a', x, 'b', 1) AS STRUCT<a: TIMESTAMP_NTZ>), 0 FROM src` stores `{a: 1767323045123456}`, the UTC wall.
- `TRY_CAST(named_struct('a', x, 'b', 1) AS STRUCT<a: INT, b: TIMESTAMP_NTZ>)` stores `{a: NULL, b: 1}`.
- The cross-position `CAST` through `INSERT ... SELECT`, a view, a nested query, `UPDATE`, `MERGE` and the DataFrame door ends in an Arrow cast error.
- The `CASE`- and `UNION`-unified shapes store `{b: 1767323045123456}` in UTC; Spark's answer for them was not measured.
- Spark 4.1.2 raises `[DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION]` on these shapes (the orchestrator's live run).

**Retires:** when a container cast whose fields do not line up with the target raises Spark's `DATATYPE_MISMATCH` class in every session zone, and the R-011 pins stay green. The pins that must stay green are ledger C-002 and C-005 of GATE-RETIRE-1 (`ntz_nested_refuse`).

**Relation to R-011:** the arm (`casts_a_zoned_value_to_naive`) is retained, because these shapes depend on it in New York. Its retirement folds into WRITE-SPINE-1 step 4 and waits for this card.
