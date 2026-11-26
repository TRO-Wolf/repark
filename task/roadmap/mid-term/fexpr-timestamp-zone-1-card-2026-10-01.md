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

## Delta 2 (2026-11-26, FEXPR-TIMESTAMP-NTZ-1, after the Neon re-check of `7791f6f84`)

The re-check failed `7791f6f84` on D1-A1 (time-only strings became today's date), D1-A2 (bracketed suffixes
accepted) and D1-A3 (explicit LTZ casts not nullable). Delta 2 closes them, with the owner's rulings 1 to 3:

- **D1-A1 (time-only NTZ strings):** `spark_string_timestamp/instant.rs::wall_only_micros` refuses a time-only parse,
  as Spark's `stringToTimestampWithoutTimeZone` does, and takes no clock. `CAST('02:00:00' AS TIMESTAMP_NTZ)` and
  `'T02:00:00'` now refuse with `CAST_INVALID_INPUT` under ANSI and answer NULL otherwise, on every door; CTAS and
  Parquet writes store NULL or refuse. Time-only input to `TIMESTAMP` (LTZ) keeps today's date in the session zone
  (the PROTECTED cells).
- **D1-A2 (bracketed suffixes):** `spark_string_timestamp.rs::string_to_ntz_wall_micros` no longer strips `[...]`,
  so `[UTC]`, `[INVALID_ZONE]`, `[]` and the other bracketed forms refuse as in Spark.
- **D1-A3 and Q4 (explicit LTZ casts):** `keyword_lower/ntz_cast_lower.rs::lower_explicit_timestamp_ltz_cast` wraps
  the string literal of a non-`TRY` `CAST` / `::` to `TIMESTAMP_LTZ` in the nullable marker on every door, so it is
  nullable as in Spark, zoned strings included (Q2).
  `spark_rewrites/timestamp_ltz_literal.rs::rewrite_fragment_timestamp_ltz_literals` now emits the LTZ marker call
  for a typed `TIMESTAMP_LTZ '…'` fragment literal, so the typed literal stays not nullable.
- **Q5 (explicit NTZ casts, ruling 2):** `ntz_cast_lower.rs::lower_timestamp_ntz_cast` wraps the string literal of a
  non-`TRY` `CAST` / `::` to `TIMESTAMP_NTZ` the same way. Round 1's R-5 stands in
  `SparkTimestampNtzCast::return_field_from_args`, so typed `TIMESTAMP` / `TIMESTAMP_NTZ` literals stay not nullable.
- **Ruling 3:** `instant_ts.rs::SparkToTimestamp::return_field_from_args` declares `to_timestamp` not nullable only
  over exactly one non-null Arrow `Timestamp` argument (Spark's rule, measured): `CAST(<ntz or ltz> AS TIMESTAMP_LTZ)`
  and the coalesce coercion of an NTZ value keep Spark's non-null answer; strings, dates, numbers and format arguments
  stay nullable.
- **Ruling 4:** `timestamp_ntz_cast.rs::cast_arg_name` peels the nullable marker, so a refusal names the cast's
  literal as Spark does (`CAST(CAST(2024-01-01 AS TIMESTAMP_NTZ) AS BIGINT)`).
- **Forced caller:** `timestamp_ntz_cast.rs::parse_timestamp_ntz_wall` calls the clock-free parser.

Measured on `c3bff6102` (release-fast wheel built from it) against live Spark 4.1.2 + Iceberg 1.11.0, main
`fd5c3fcb9` and `7791f6f84`; every write is read back:

| Suite | Cells | Head = Spark | `7791f6f84` = Spark | Main = Spark | Moved onto Spark | Moved off Spark | Main = Spark, head off |
|---|---:|---:|---:|---:|---:|---:|---:|
| step 0 (time-only, brackets, nullability, writes) | 4,828 | 4,343 | 2,123 | 2,076 | 2,220 | 0 | 0 |
| PROTECTED LTZ time-only | 600 | 420 | 240 | 178 | 180 | 0 | 0 |
| NOT NULL Iceberg guard and stored nullability | 360 | 324 | 270 | 246 | 54 | 0 | 0 |
| `attack_delta1.py` | 1,305 | 1,107 | 885 | 363 | 222 | 0 | 0 |
| `repro_delta1_findings.py` | 635 | 609 | 73 | 184 | 536 | 0 | 0 |
| `attack_writes.py` | 756 | 548 | 532 | 354 | 16 | 0 | 0 |
| `attack_writes_2.py` | 236 | 228 | 228 | 132 | 0 | 0 | 0 |
| `attack_regressions.py` | 2,272 | 1,534 | 1,361 | 926 | 173 | 0 | 0 |
| the 1,769-cell domain sweep | 1,769 | 1,628 | 1,628 | 1,048 | 0 | 0 | 0 |

- **Moved off Spark against `7791f6f84`:** 0 cells in all suites. **Equal to Spark on main and off on head:** 0.
- **NOT NULL guard:** INSERT SELECT, INSERT VALUES, INSERT OVERWRITE, MERGE and `writeTo().append()` of every newly
  nullable shape into a `NOT NULL` Iceberg column store the value, as Spark does; stored Iceberg `required` and Parquet
  nullability equal Spark's on every CTAS.
- **Mutant proofs:** the restored time-only branch, the restored bracket strip, the dropped LTZ wrap, the typed literal
  emitted as `CAST`, the dropped NTZ wrap and an always-nullable `to_timestamp` each fail their pins; R-5 dropped is
  killed by the facade pin `test_typed_literals_stay_not_nullable_under_ntz` (measured on the round's first head,
  which carried it). The `cast_arg_name` peel is held by `test_ntz_1.py::test_select_cells_replay_spark[UTC]`, which
  failed on the head without it.
- **Row-order flake:** `attack_regressions.py`'s `union_bad_and_valid_ts` (unordered `UNION ALL`, UTC, NTZ, ANSI off)
  once came back in the other row order with equal values; two re-runs on the same head gave Spark's order.
- **Facade:** the full `python/repark/tests` suite on the `c3bff6102` wheel: 47,034 cases, 998 skipped, and only main's 4 allow-listed failures (verdict PASS).

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

8. **Seen in delta 2, present on main:**
   - `col.cast("timestamp_ltz")` and `col.try_cast("timestamp_ltz")` raise `ParseException`; Spark accepts them (present
  on main).
   - `createDataFrame(df.collect(), df.schema)` declares every field nullable; Spark keeps the schema (present on main).
   - On `spark.sql` and `selectExpr`, `CAST('<valid>' AS TIMESTAMP)` (the bare keyword) is not nullable; Spark says
  nullable (present on main; the fragment doors already match).
   - Under `timestampType=TIMESTAMP_NTZ`, `to_timestamp_ltz(<non-null NTZ>)` is nullable; Spark says not nullable
  (present on main and on `7791f6f84`; a separate function from `to_timestamp`).

## Pointers

- Up: [map.md](map.md)
