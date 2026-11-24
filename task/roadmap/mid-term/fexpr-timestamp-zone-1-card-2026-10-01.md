# FEXPR-TIMESTAMP-ZONE-1 — F.expr TIMESTAMP literals resolve in UTC, not the session zone

**Filed: 2026-10-01 from the overnight owner-items backlog** (the owner-items list of
2026-09-29, line 72; #888 re-verify 10). **Severity:** wrong results, not yet rated (owner). **Status:** C-001 fixed by
DATE-PART-SESSION-ZONE-1 (PR #1420, 2026-11-24). The NTZ remainder below is open, and S2 is proposed for it.
**Target:** mid-term.

## The divergence it records

`F.expr` TIMESTAMP literals resolve in UTC instead of the session zone (a
DataFrame `F.expr` door gap; SQL resolves them in the session zone).

## Repro (from the source description; re-measure on the day)

```python
spark.conf.set("spark.sql.session.timeZone", "America/New_York")
spark.sql("SELECT TIMESTAMP '2024-06-01 12:00:00'").collect()  # session zone
F.expr("TIMESTAMP '2024-06-01 12:00:00'")  # RePark: UTC; Spark 4.1.2: session zone
```

## Cause and suggested fix direction (a suggestion, not a decision)

The `F.expr` door parses TIMESTAMP literals without the session zone the SQL
door applies. Any fix direction is suggested by the verifier/orchestrator, not
a decision.

## Clauses (draft)

- **C-001** `F.expr` TIMESTAMP literals answer the SQL door's values under a non-UTC zone.
- **C-002** UTC and fixed-offset zones pinned as controls.
- **C-003** The neighbouring session-zone literal cells still answer Spark.

## Update 2026-11-24: the zone is fixed, the NTZ settings are not

**What #1420 fixed.** `F.expr` eagerly analyses a column-free fragment in one process-global context that has no
session settings. That context folded `TIMESTAMP '…'`, `CAST('…' AS TIMESTAMP)` and `TRY_CAST(…)` into UTC instants.
In New York, `hour`, `day`, `date_format`, `CAST … AS STRING`, `unix_timestamp` and comparisons over those literals
were therefore wrong. PR #1420 (part 3) marks the context as zone-deferred: zoneless literal text binds the session
zone at use, and text carrying its own zone still folds. Those cells now equal Spark, and C-001 holds.

**What is left**, measured on #1420's merged head and on Spark 4.1.2, in both ANSI modes, in America/New_York,
Asia/Kolkata and UTC. `T` is `TIMESTAMP '2024-01-01 02:00:00'`.

| Cell | Door | Spark 4.1.2 | RePark |
|---|---|---|---|
| `CAST(T AS TIMESTAMP_NTZ)` | `spark.sql` | `timestamp_ntz` | `timestamp_ntz` |
| `CAST(T AS TIMESTAMP_NTZ)` | `F.expr` | `timestamp_ntz` | **`timestamp`** (same displayed value) |
| `T`, with `spark.sql.timestampType=TIMESTAMP_NTZ` | `spark.sql` | `timestamp_ntz` | `timestamp_ntz` |
| `T`, with `spark.sql.timestampType=TIMESTAMP_NTZ` | `F.expr` | `timestamp_ntz` | **`timestamp`** (same displayed value) |
| unaliased `F.expr("TIMESTAMP '2024-01-01 02:00:00Z'")`, New York | column name | `TIMESTAMP '2023-12-31 21:00:00'` | `TIMESTAMP '2024-01-01 02:00:00Z'` |

**Likely cause (read, not traced).** `build_expr_context()` (`crates/repark-python/src/column/expr_build.rs`) carries
the default `timestampType`, LTZ, so the NTZ cast and the typed literal resolve there instead of in the session. The
risk is an LTZ-typed column holding an NTZ wall clock that is later read as an instant, after a write into an NTZ
column or a zone change. That is not yet measured.

**Not this card** (measured on both doors, the same on main):
- `TIMESTAMP 'bad'` and `TIMESTAMP '2024-01-01 02:00:00[UTC]'`: Spark raises `ParseException`. RePark answers NULL with
  ANSI off and raises `CAST_INVALID_INPUT` with ANSI on. This belongs with
  [SPARK-DOOR-OVER-ACCEPT-SYNTAX-1](cards-2026-11-23/spark-door-over-accept-syntax-1-card-2026-11-24.md).
- `T - TIMESTAMP '2024-01-01 00:00:00'`: `dtypes` reports `string` where Spark reports `interval day to second`, and
  the value agrees.

## Pointers

- Up: [map.md](map.md)
