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

## Resolution 2026-11-24 / Delta 1 2026-11-25 (FEXPR-TIMESTAMP-NTZ-1, SEVERITY: S1 measured)

**What was fixed** (branch `fix/fexpr-timestamp-ntz-1`, verified on Delta 1 code head `22e77e4aa` across 1,771 domain
cells — **582 fixed, 0 regressed** — 84/84 live Spark 4.1.2 Iceberg/Parquet cross-reads, 16/16 verifier repro groups,
and 0 regressions across all verifier neighbour/pruning/write/DML suites; 123 cases pinned in
`test_fexpr_timestamp_ntz_1.py`):

- **Deferred `spark.sql.timestampType` in `build_expr_context`:** added `DeferredSparkTimestampTypeConfig`
  (`crates/repark-functions/src/timestamp_type.rs`) and attached it in `build_expr_context()` so bare `TIMESTAMP`
  literals and casts survive `F.expr` eager analysis and resolve in the consumer session under its
  `spark.sql.timestampType` and `spark.sql.session.timeZone`.
- **SQL & fragment timestamp cast and zoned-literal lowering:** lowered explicit `CAST`/`TRY_CAST(... AS TIMESTAMP_LTZ)`,
  zoned `TIMESTAMP '...'` literals (`Z`, offsets including `+HH:MM:SS`/`+HHMMSS`, named IANA/UTC/GMT zones via shared
  `spark_string_timestamp::{grammar, zone}`), and `TIMESTAMP_LTZ '...'` literals to LTZ markers across `spark.sql`,
  `selectExpr`, `MERGE`, `F.expr`, and `filter(str)`.
- **Pre-coercion NTZ + unified post-coercion LTZ rule (Delta 1, F-A1/F-A3):** seated `SparkNtzTimestampCast` before
  `type_coercion` so bare `TIMESTAMP` casts/literals become NTZ before coercion while preserving `Dml` and Iceberg
  `arrow_cast` LTZ store casts; unified post-coercion `SparkLtzTimestampCast` localizes every
  `Cast(Timestamp(µs, None) -> Timestamp(µs, UTC))` (including `TypeCoercion`-inserted casts in filters, `CASE`,
  `COALESCE`, `IN`, `UNION`, `MERGE`, `UPDATE`, and `INSERT`) in the session zone.
- **Direct NTZ wall-clock parsing & constant folding (Delta 1, F-A4/F-A5/F-A7):** NTZ string casts parse wall clocks
  directly in `spark_string_timestamp` without passing through the session zone (preserving NY DST gaps, stripping
  numeric offsets, raising `[CAST_INVALID_INPUT]` under ANSI, and retiring 146 post-2262 cells from
  `zone_horizon_render_1_residue.json`), with constant folding in `SparkTimestampNtzCast::simplify`.
- **`date_trunc`, nullability, and marker-free display (Delta 1, F-A8/F-A9/F-A10):** `DateTrunc` over NTZ localizes
  and returns `timestamp` (LTZ) across all 15 unit spellings; Spark nullability is restored for `CAST`/`TRY_CAST`
  string-to-timestamp, `CAST(DATE AS TIMESTAMP_LTZ)`, `CAST(TIMESTAMP_NTZ AS DATE)`, and legacy `!ansi`
  `unix_timestamp`; internal marker UDF names are unwrapped in `SparkProjectionDisplay` and `schema_name`/`display_name`.

## Still unlike Spark (measured on `22e77e4aa`, out of scope for this unit)

Every remaining cell below is **present on `main` (`8c07d619`)** (`0` new divergences introduced):

1. **Unaliased column display names** (78 cells in `unaliased_cells`, **present on main**; wrong display name only,
   values and dtypes match Spark; carded as
   [FEXPR-TIMESTAMP-DISPLAY-NAME-1](cards-2026-11-23/fexpr-timestamp-display-name-1-card-2026-11-24.md), S3 proposed):
   `F.expr` names `TIMESTAMP '2024-01-01 02:00:00Z'` as `TIMESTAMP '2024-01-01 02:00:00Z'` (Spark:
   `TIMESTAMP '2023-12-31 21:00:00'`) and `TIMESTAMP '2024-01-01 02:00:00'` under NTZ as
   `TIMESTAMP '2024-01-01 02:00:00'` (Spark: `TIMESTAMP_NTZ '2024-01-01 02:00:00'`); `spark.sql`/`selectExpr` render
   raw microsecond ticks or `__repark_cast_timestamp_ntz__(...)`. Separate display-name seat per brief.
2. **Runtime `spark.conf.set("spark.sql.timestampType", "TIMESTAMP_NTZ")` after session build** (3 cells in
   `runtime_conf_cells`, **present on main**; wrong dtype `timestamp` LTZ instead of `timestamp_ntz`): mutates only the
   Python conf dict without updating the live Rust `SparkTimestampTypeConfig`. Pre-existing session-conf plumbing
   outside `F.expr`.
3. **`CAST(0 AS TIMESTAMP)` under `spark.sql.timestampType=TIMESTAMP_NTZ`** (24 cells in `card_cells`, **present on
   main**; refusal difference — RePark returns `1970-01-01 00:00:00` `timestamp_ntz` across all 4 doors where Spark
   raises `DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION`): deliberately preserved because existing Rust pin
   `instant_ts::tests::ntz_opt_in_integer_cast_is_naive_microseconds_seconds` asserts this behaviour; changing it
   belongs to a dedicated cast-legality unit.
4. **`CAST(TIMESTAMP '2024-01-01 02:00:00' AS BIGINT)` under `spark.sql.timestampType=TIMESTAMP_NTZ` on `F.expr` only**
   (12 cells in `card_cells`, **present on main**; refusal difference — `F.expr` `select`/`withColumn` returns
   `1704074400` `bigint` where Spark raises `DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION`; `spark.sql` and `selectExpr`
   now refuse like Spark after Delta 1): `build_expr_context` rewrites `CAST(TIMESTAMP AS BIGINT)` in
   `SparkExprSemantics` before deferral sees `timestampType=TIMESTAMP_NTZ`; out of scope for this unit.
5. **`to_timestamp('2024-01-01 02:00:00')` under `spark.sql.timestampType=TIMESTAMP_NTZ`** (24 cells in `card_cells`,
   **present on main**; wrong dtype `timestamp` LTZ instead of `timestamp_ntz` across all 4 doors): deliberately
   preserved because existing Q10 pin `test_timestamp_type.py` asserts `to_timestamp` stays LTZ; changing it requires
   retiring that Q10 decision in its own unit.
6. **`TIMESTAMP_LTZ '2024-01-01 02:00:00'` typed literal on `spark.sql`/`selectExpr` under `TIMESTAMP_NTZ`**
   (**present on main**; wrong dtype `timestamp_ntz` on SQL doors, while `F.expr` matches Spark `timestamp`):
   pre-existing SQL pre-lexer `plan_timestamp_ltz_literal_regions` rewrites `TIMESTAMP_LTZ` to `TIMESTAMP` before
   parsing (pinned in `timestamp_ltz_literal.rs::a_typed_ltz_literal_becomes_a_timestamp_literal`); out of scope for
   `F.expr`.
7. **Original "Not this card" items above** (**present on main**): `TIMESTAMP 'bad'` /
   `TIMESTAMP '2024-01-01 02:00:00[UTC]'` over-acceptance (refusal difference) and `T - T` interval `dtypes` reporting
   `string`.

## Pointers

- Up: [map.md](map.md)
