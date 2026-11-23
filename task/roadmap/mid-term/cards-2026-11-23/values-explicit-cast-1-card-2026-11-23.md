# Card VALUES-EXPLICIT-CAST-1: explicit user CAST inside SQL VALUES and INSERT ... VALUES refuses with Arrow's error

**Date:** 2026-11-23. **Filed by:** the ANSI-OFF-NARROWING-OVERFLOW-1 delta-1 builder (`gemini-4-neon-xhigh`), under the
orchestrator's 2026-11-23 ruling on PR #1417 findings A-3 and B-2. **Severity:** S2 proposed (owner to rate): a refusal,
never a silent wrong answer. **Status:** filed, not dispatched.

## Finding

An explicit user-written `CAST` inside a SQL `VALUES` clause or `INSERT INTO ... VALUES (...)` refuses with Arrow's cast
error on the Spark door in both ANSI modes, for both the `STRING -> integer` slice (PR #1413, `STRING-CAST-LENIENT-1`)
and the narrowing numeric slice (PR #1417, `ANSI-OFF-NARROWING-OVERFLOW-1`).

## Measured

Spark 4.1.2 with `iceberg-spark-runtime-4.1_2.13-1.11.0` vs RePark main (`e80e6bc8`) and PR #1417 head (`3d3ecb0d2`):

| Statement | ANSI mode | Spark 4.1.2 | RePark (main and PR #1417) |
|---|---|---|---|
| `SELECT * FROM VALUES (CAST(' 7 ' AS INT)) AS t(r)` | `false` | `[[7]]` | Arrow cast error (refuses) |
| `SELECT * FROM VALUES (CAST('abc' AS INT)) AS t(r)` | `false` | `[[NULL]]` | Arrow cast error (refuses) |
| `SELECT * FROM VALUES (CAST('abc' AS INT)) AS t(r)` | `true` | `[INVALID_INLINE_TABLE.FAILED_SQL_EXPRESSION_EVALUATION]` (SQLSTATE `42000`) | Arrow cast error (refuses) |
| `SELECT * FROM VALUES (CAST(4294967297L AS INT)) AS t(r)` | `false` | `[[1]]` (`int`, non-null) | `Optimizer rule 'simplify_expressions' failed` (Arrow `Cast error`) |
| `SELECT * FROM VALUES (CAST(4294967297L AS INT)) AS t(r)` | `true` | `[INVALID_INLINE_TABLE.FAILED_SQL_EXPRESSION_EVALUATION]` (SQLSTATE `42000`) | `Optimizer rule 'simplify_expressions' failed` (Arrow `Cast error`) |
| `INSERT INTO t VALUES (1, CAST(4294967297L AS INT), CAST(CAST('10.00' AS DECIMAL(10,2)) AS DECIMAL(3,2)))` | `false` | inserts `[[1, 1, NULL]]` | `Optimizer rule 'simplify_expressions' failed` (Arrow `Cast error`) |
| `INSERT INTO t VALUES (1, CAST(4294967297L AS INT), CAST(CAST('10.00' AS DECIMAL(10,2)) AS DECIMAL(3,2)))` | `true` | `[INVALID_INLINE_TABLE.FAILED_SQL_EXPRESSION_EVALUATION]` (SQLSTATE `42000`), prior rows kept | `Optimizer rule 'simplify_expressions' failed` (Arrow `Cast error`), prior rows kept |

## Cause

`SparkStringIntegralCast` and `SparkCoercionCastMark` in `crates/repark-functions/src/cast_map/string_integral.rs` skip
every `LogicalPlan::Values(_)` node (`held`), because DataFusion's `values_to_plan` places both implicit row-type
coercions and `INSERT ... VALUES` store-assignment casts directly inside the `LogicalPlan::Values` expressions.
Rewriting all casts inside `LogicalPlan::Values` without separating user-written `CAST`s from `values_to_plan`'s
implicit casts would silently rewrite store-assignment casts and cross-row coercions.

## First step

Distinguish user-written explicit `CAST(...)` expressions inside SQL `VALUES` / `INSERT ... VALUES` from
`values_to_plan`'s implicit casts (analogous to `crates/repark-spark/src/update_cast.rs` for `UPDATE ... SET`), and align
ANSI-on evaluation failures inside inline `VALUES` tables with Spark's
`[INVALID_INLINE_TABLE.FAILED_SQL_EXPRESSION_EVALUATION]` (`SQLSTATE: 42000`) while keeping uncast store assignments
refused. Pin both the string-to-integer and narrowing numeric `VALUES` cells in both ANSI modes.

Up: [map.md](map.md).
