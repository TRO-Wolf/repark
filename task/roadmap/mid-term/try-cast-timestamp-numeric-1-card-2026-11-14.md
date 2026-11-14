# Card TRY-CAST-TIMESTAMP-NUMERIC-1: `TRY_CAST(TIMESTAMP AS DECIMAL(20,0))` answers epoch microseconds; Spark answers seconds

**Date:** 2026-11-14. **Filed by:** the DOUBLE-DECIMAL-STORE-ROUND-1 stores builder (`claude-opus-5-5`).
**Sources:** both #1335 verifiers' pre-existing findings (verifier 1's P4, verifier 2's c1), identical on main and on
the #1335 head; this builder's re-measure on main `d843d37e`
([small-cells.md](../../wo/double-decimal-store-round-1/evidence/fix/small-cells.md)).

**Status:** filed 2026-11-14, not dispatched. Severity **S1**: a wrong value, in a cast that a write can store.

## Measured (Spark 4.1.2 on JDK 17, session zone UTC, both ANSI modes)

| cell | RePark main | Spark |
|---|---|---|
| `TRY_CAST(TIMESTAMP '2026-11-13 15:42:03.005' AS DECIMAL(20,0))` | `1794584523005000` | `1794584523` |
| `TRY_CAST(ts AS DECIMAL(38,18))`, `ts` = `2024-01-01 00:00:00.123455` | `1704067200123455.000000000000000000` | `1704067200.123455000000000000` |
| `TRY_CAST(ts AS DOUBLE)` | `1704067200123455.0` | `1704067200.123455` |
| `TRY_CAST(ts AS INT)` | NULL | `1704067200` |
| `TRY_CAST(ts AS BIGINT)` | `1704067200` | `1704067200` |

Verifier 1 measured the first row in another session zone (`1794618123005000` against `1794618123`). The plain
`CAST` of the same values answers seconds on main (`1794584523`, and `1704067200.123455` as a DOUBLE): the analyzer
lowers `CAST(ts AS DECIMAL)` through `__repark_epoch_seconds_real__` (`crates/repark-functions/src/analyzer.rs:193`),
and `TRY_CAST` is not lowered, so it reaches Arrow's cast of the microsecond count. The full rows are in the evidence
file above.

## Close

`TRY_CAST` of a TIMESTAMP to DECIMAL, DOUBLE, FLOAT and the integer types answers what the same `CAST` answers when it
succeeds, and NULL where Spark's `TRY_CAST` answers NULL, in both ANSI modes, pinned beside the `CAST` cells.

Up: [map.md](map.md).
