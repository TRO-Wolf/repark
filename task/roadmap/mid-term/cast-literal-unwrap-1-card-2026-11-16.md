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

Up: [map.md](map.md).
