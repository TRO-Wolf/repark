# Card CREATEDATAFRAME-DECIMAL-SCHEMA-1: createDataFrame with a DECIMAL(20,0) or DECIMAL(38,0) schema refuses, as reported

**Date:** 2026-11-16. **Filed by:** the records clerk (`claude-haiku-5-5`), from Fable's type-pair sweep report of 2026-11-16 (family 15). Measured before #1341 and #1345, on main's package `pkg-ddu1-7b21bde3`, as the report states. It is not a sweep matrix cell, so the re-run on main `e50ed17f` does not cover it.

**Status:** PROPOSED, not dispatched; severity and label-blocking: owner to rule. **Unverified:** this clerk did not reproduce it.

**Severity:** Proposed: S2 (owner to rule), if reproduced as the report states.

## Finding

Fable's report lists, for `createDataFrame` with a schema that names `DECIMAL(20,0)` or `DECIMAL(38,0)`, a refusal on main in both ANSI modes, where Spark answers. One of the messages names `DECIMAL(38, 18)`. The DataFrame door is not the SQL door the sweep's matrix measures.

## Evidence status

The sweep's raw outputs in the named folder hold no `createDataFrame` refusal. The matrix loader catches a `createDataFrame` failure and falls back to a SQL CASE view, so the claim does not appear as a cell. The `DECIMAL(38, 18)` text in the sweep's spot records comes from an Arrow cast of a different shape. Step 0 measures the claim with the exact schema and message before anything is dispatched.

Reproduction to run at step 0, on both engines in both modes:
`createDataFrame([(1, 1)], "id BIGINT, v DECIMAL(38,0)")`, then a select of `v`.

## Not yet known

The seat. Related: DOUBLE-DECIMAL-DATAFRAME-1 (the DataFrame doors round a DOUBLE into DECIMAL, a different class) and DECIMAL-CACHE-1 (a `createDataFrame` with `DECIMAL(38,10)` answers, a valid shape).

Up: [map.md](map.md).
