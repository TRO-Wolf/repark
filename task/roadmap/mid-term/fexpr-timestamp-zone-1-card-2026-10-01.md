# FEXPR-TIMESTAMP-ZONE-1 — F.expr TIMESTAMP literals resolve in UTC, not the session zone

**Filed: 2026-10-01 from the overnight owner-items backlog** (the owner-items list of
2026-09-29, line 72; #888 re-verify 10). **Severity:** S1 (measured on the NTZ remainder, 2026-11-24 — silent 5-hour
wrong stored values in Iceberg `TIMESTAMP_NTZ` and Parquet writes confirmed by live Spark 4.1.2 cross-read, plus silent
wrong query values). **Status:** **FIXED** — C-001 (session zone) fixed by DATE-PART-SESSION-ZONE-1 (PR #1420,
2026-11-24); NTZ remainder fixed by FEXPR-TIMESTAMP-NTZ-1 (2026-11-24, evidence in
[task/wo/fexpr-timestamp-ntz-1/evidence/](../../wo/fexpr-timestamp-ntz-1/evidence/)). Display names split to
[FEXPR-TIMESTAMP-DISPLAY-NAME-1](cards-2026-11-23/fexpr-timestamp-display-name-1-card-2026-11-24.md).
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

## Resolution 2026-11-24 (FEXPR-TIMESTAMP-NTZ-1, SEVERITY: S1 measured)

**What was fixed** (branch `fix/fexpr-timestamp-ntz-1`, verified on `b9add8f73` across 1,771 domain cells — 510 fixed,
0 regressed — and 84/84 live Spark 4.1.2 Iceberg/Parquet cross-reads; pinned in `test_fexpr_timestamp_ntz_1.py`):

- **Deferred `spark.sql.timestampType` in `build_expr_context`:** added `DeferredSparkTimestampTypeConfig`
  (`crates/repark-functions/src/timestamp_type.rs`) and attached it in `build_expr_context()` so bare `TIMESTAMP`
  literals and casts survive `F.expr` eager analysis and resolve in the consumer session under its
  `spark.sql.timestampType` and `spark.sql.session.timeZone`.
- **Fragment timestamp cast & `TIMESTAMP_LTZ` literal lowering:** exposed `lower_fragment_timestamp_casts`,
  `canonicalize_fragment`, and `canonicalize_predicate` across all `F.expr` and Spark-door `filter(str)` seats so
  `CAST`/`::`/`TRY_CAST`/`SAFE_CAST` to `TIMESTAMP_NTZ`, explicit `TIMESTAMP_LTZ` casts and literals, and zoned
  `TIMESTAMP '...Z'` literals preserve their exact target semantics across deferral and under
  `spark.sql.timestampType=TIMESTAMP_NTZ`.
- **Analyzer, nullability, and Iceberg/DML write fixes:** added `Expr::TryCast` + LTZ-to-NTZ wall conversion in
  `rewrite_cast_as_ntz`, stale `LogicalPlan::Union` schema rebuild under NTZ, string-source nullability in
  `SparkTimestampNtzCast::return_field_from_args`, `TIMESTAMP_LTZ_CAST_NAME` emission in `conform_stage.rs::leaf_store`,
  and `retarget_dml_ltz_store_casts` so Iceberg/DML writes into `TIMESTAMP` (LTZ) columns succeed under
  `spark.sql.timestampType=TIMESTAMP_NTZ`.

## Still unlike Spark (measured on `b9add8f73`, out of scope for this unit)

1. **Unaliased column display names** (carded as
   [FEXPR-TIMESTAMP-DISPLAY-NAME-1](cards-2026-11-23/fexpr-timestamp-display-name-1-card-2026-11-24.md), S3 proposed):
   `F.expr` names `TIMESTAMP '2024-01-01 02:00:00Z'` as `TIMESTAMP '2024-01-01 02:00:00Z'` (Spark:
   `TIMESTAMP '2023-12-31 21:00:00'`) and `TIMESTAMP '2024-01-01 02:00:00'` under NTZ as
   `TIMESTAMP '2024-01-01 02:00:00'` (Spark: `TIMESTAMP_NTZ '2024-01-01 02:00:00'`); `spark.sql`/`selectExpr` render
   raw microsecond ticks or `__repark_cast_timestamp_ntz__(...)`.
2. **Runtime `spark.conf.set("spark.sql.timestampType", "TIMESTAMP_NTZ")` after session creation** (all doors): stored
   only in the Python conf dict without updating the live Rust `SparkTimestampTypeConfig` (3 cells in
   `runtime_conf_cells`).
3. **Pre-existing both-door divergences under `spark.sql.timestampType=TIMESTAMP_NTZ`:**
   - `CAST(0 AS TIMESTAMP)`: RePark answers `1970-01-01 00:00:00` (`timestamp_ntz`) across all 4 doors (pinned in
     `instant_ts/tests.rs`); Spark raises `DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION` (24 cells).
   - `CAST(TIMESTAMP '2024-01-01 02:00:00' AS BIGINT)`: RePark answers `1704074400` (`bigint`) across all 4 doors; Spark
     raises `DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION` (24 cells).
   - `to_timestamp('2024-01-01 02:00:00')`: RePark stays `timestamp` (LTZ) across all 4 doors (pinned in
     `test_timestamp_type.py`); Spark returns `timestamp_ntz` (24 cells).
4. **Pre-existing SQL-door (`spark.sql`/`selectExpr`) divergences under `spark.sql.timestampType=TIMESTAMP_NTZ`:**
   `TIMESTAMP '2024-01-01 02:00:00Z'`, `TIMESTAMP '2024-01-01 02:00:00+05:30'`, `CAST(... AS TIMESTAMP_LTZ)`, and
   `CAST(s AS TIMESTAMP_LTZ)` lower to bare `TIMESTAMP` and resolve as `timestamp_ntz` on `spark.sql`/`selectExpr` (60
   cells); `F.expr` (`select`/`withColumn`) matches Spark (`timestamp`).
5. **Original "Not this card" items above:** `TIMESTAMP 'bad'` / `TIMESTAMP '2024-01-01 02:00:00[UTC]'` over-acceptance
   and `T - T` interval `dtypes` reporting `string`.

## Pointers

- Up: [map.md](map.md)
