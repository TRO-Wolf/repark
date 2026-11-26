# task/wo/fexpr-timestamp-ntz-1/evidence/summary.md — FEXPR-TIMESTAMP-NTZ-1 measurement evidence

## Provenance

- **RePark:** `main` at `8c07d6197` (measured on `57c92b411` release wheel; `8c07d6197` adds only Markdown cards) and Delta 1 code head `22e77e4aa` on `fix/fexpr-timestamp-ntz-1` (`249850e9a` + `22e77e4aa`, release wheel built from `22e77e4aa`; rebuilt from the final records commit so `BUILT_FROM == HEAD`). Initial round was measured on `b9add8f73` / `fb2c1e948` (`8e5c9deb1`).
- **Spark:** live PySpark 4.1.2 + Iceberg 1.11.0 (`local[2]`, Java 17), both ANSI modes (`spark.sql.ansi.enabled=true` and `false`), across `America/New_York`, `Asia/Kolkata`, and `UTC`.
- **Scripts:** [probe_domain.py](probe_domain.py) (1,771-cell domain sweep across all doors, zones, ANSI modes, zone changes, mixing, Iceberg/Parquet writes, and live cross-reads), [analyze_domain.py](analyze_domain.py) (summary and regression checker).
- **Full cell-by-cell table:** [domain-table.md](domain-table.md) (uniform JSON formatting via `json.dumps(..., sort_keys=True)`).

## Severity Verdict: S1 (measured)

The card proposed S2 provisionally and made Step 0 decisive: any cell writing or answering a wrong value makes the unit S1. Step 0 measured silent wrong stored values on disk (confirmed by live Spark 4.1.2 cross-read) and silent wrong query values on `main` in both ANSI modes:

1. **Iceberg write corruption (5 hours wrong on disk):** under `spark.sql.timestampType=TIMESTAMP_NTZ` in `America/New_York`, writing `F.expr("TIMESTAMP '2024-01-01 02:00:00'")` via `writeTo().append()` into an Iceberg `TIMESTAMP_NTZ` column stored `2024-01-01 07:00:00` (`hour=7`) instead of `2024-01-01 02:00:00` (`hour=2`). Live Spark 4.1.2 read back `2024-01-01 07:00:00` in both NY and UTC. On head `22e77e4aa`, Spark 4.1.2 reads back `2024-01-01 02:00:00` (`timestamp_ntz`, `hour=2`) in both NY and UTC.
2. **Parquet write corruption (5 hours wrong on disk):** writing `F.expr("CAST(T AS TIMESTAMP_NTZ)")`, `F.expr("CAST('2024-01-01 02:00:00' AS TIMESTAMP_NTZ)")`, or `F.expr("TIMESTAMP '2024-01-01 02:00:00'")` (under `timestampType=TIMESTAMP_NTZ`) to Parquet in `America/New_York` wrote `timestamp[us, tz=UTC]` (`1704092400000000`, `07:00 UTC`) instead of `timestamp[us]` (`1704074400000000`, `02:00`). Live Spark 4.1.2 read back `2024-01-01 07:00:00` (`timestamp`) in UTC. On head `22e77e4aa`, Spark 4.1.2 reads back `2024-01-01 02:00:00` (`timestamp_ntz`, `hour=2`) in both NY and UTC.
3. **Silent wrong query values across `F.expr`, `filter(str)`, zone changes, and mixing:**
   - `F.expr("CAST('2024-01-01 02:00:00+05:30' AS TIMESTAMP_NTZ)")` answered `2023-12-31 15:30:00` in NY / `2023-12-31 20:30:00` in UTC instead of `2024-01-01 02:00:00`.
   - `F.expr("TRY_CAST(T AS TIMESTAMP_NTZ)")` and `F.expr("TRY_CAST(ts AS TIMESTAMP_NTZ)")` answered `2024-01-01 07:00:00` in NY / `2023-12-31 20:30:00` in Kolkata instead of `2024-01-01 02:00:00`.
   - `F.expr("CAST(0 AS TIMESTAMP_NTZ)")` silently returned `1970-01-01 00:00:00` instead of refusing `DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION`.
   - `F.expr("(CAST(T AS TIMESTAMP_NTZ)) <=> ntz")`, `df.filter(F.expr("ntz = (CAST(T AS TIMESTAMP_NTZ))"))`, and `df.filter("ntz = (CAST(T AS TIMESTAMP_NTZ))")` returned `false` / `0` rows in NY and Kolkata instead of `true` / `1` row.
   - `unix_timestamp` over `F.expr` NTZ expressions answered `1704092400` in NY instead of `1704074400`.
   - Under `spark.sql.timestampType=TIMESTAMP_NTZ`, `F.expr("TIMESTAMP '2024-03-10 02:30:00'")` shifted across the NY DST spring-forward gap to `03:30:00`, `F.expr("CAST('2024-01-01 02:00:00Z' AS TIMESTAMP)")` returned `2023-12-31 21:00:00`, `F.expr("CAST(T AS TIMESTAMP_LTZ)")` returned `2024-01-01 07:00:00`, `TRY_CAST('2024-01-01 02:00:00' AS TIMESTAMP)` returned `timestamp` (`21:00` on SQL), and mixing with a real NTZ column shifted `coalesce` / `union` rows by 5 hours and dropped `join` / `filter` matches.

## Seat inventory (`path:line` on Delta 1 CI HEAD)

| # | Seat (`path:line`) | Role | Resolution |
|---|---|---|---|
| 1 | `crates/repark-spark/src/keyword_lower/ntz_cast_lower.rs:13-154`, `crates/repark-spark/src/keyword_lower.rs:111-244` | AST timestamp cast and zoned-literal lowering across SQL and fragment doors | Lower explicit `CAST`/`TRY_CAST(... AS TIMESTAMP_LTZ)`, zoned `TIMESTAMP '...'` literals, and `TIMESTAMP_LTZ '...'` literals to LTZ markers when `session_has_ltz_cast_marker` holds across `spark.sql`, `selectExpr`, `MERGE`, `F.expr`, and `filter(str)` (falling back to bare `TIMESTAMP` casts and untouched typed strings on unregistered bare sessions); wrap fragment bare-`TIMESTAMP` string-literal casts in `DECIMAL_CAST_NULLABLE_NAME` |
| 2 | `crates/repark-spark/src/spark_rewrites/timestamp_ltz_literal.rs:33-92` | Preserving explicit `TIMESTAMP_LTZ '...'` on fragment canonicalization | `rewrite_fragment_timestamp_ltz_literals` rewrites `TIMESTAMP_LTZ '...'` to `CAST('...' AS TIMESTAMP_LTZ)` before `canonicalize_verbatim` |
| 3 | `crates/repark-spark/src/values_marker.rs:150-166` | Column-free `F.expr` fragment planner (`plan_marked_fragment`) | Calls `lower_fragment_timestamp_casts` on Spark-door fragments when `session_has_ltz_cast_marker` holds |
| 4 | `crates/repark-python/src/column/mod.rs:238-250`, `crates/repark-python/src/column/expr_build.rs:50-130`, `crates/repark-python/src/column/expr_build/fragment_canon.rs:1-30` | `F.expr` column-referencing (`fragment_logical_expr`) and `filter(str)` (`marked_logical_expr`, `folded_qualifier_predicate`) | Uses `fragment_canon::{canonicalize_fragment, canonicalize_predicate}` and calls `lower_fragment_timestamp_casts` |
| 5 | `crates/repark-python/src/column/expr_build.rs:332-399`, `crates/repark-functions/src/timestamp_type.rs:186-210` | Process-global `EXPR_CONTEXT` timestamp-type deferral | `DeferredSparkTimestampTypeConfig` (`with_deferred_spark_timestamp_type`, `is_spark_timestamp_type_deferred`) installed in `build_expr_context` |
| 6 | `crates/repark-common/src/udf_names.rs:2-3`, `crates/repark-functions/src/instant_ts/ltz_cast.rs:1-130` | Explicit LTZ cast marker UDFs (`TIMESTAMP_LTZ_CAST_NAME`, `TRY_TIMESTAMP_LTZ_CAST_NAME`) | Marker UDFs delegating `schema_name`/`display_name` to the operand so unaliased projections never leak internal marker names |
| 7 | `crates/repark-functions/src/instant_ts/ntz_cast.rs:1-397`, `crates/repark-spark/src/extension.rs:160-180` | Pre-coercion `SparkNtzTimestampCast` analyzer rule (`spark_ntz_timestamp_cast`) | Installed before `type_coercion`; no-op under LTZ or deferral; under NTZ rewrites bare `TIMESTAMP` `Cast`/`TryCast`, `Values`, and stale `Union` to NTZ while preserving `Dml` LTZ target casts and Iceberg DML `arrow_cast` LTZ store casts |
| 8 | `crates/repark-functions/src/instant_ts.rs:366-378,502-767` | Post-coercion `SparkLtzTimestampCast` and `string_carries_timezone` | Unifies post-coercion LTZ localization for both LTZ and NTZ sessions so any `Cast(Timestamp(µs, None) -> Timestamp(µs, UTC))` (including `TypeCoercion`-inserted casts) localizes wall clock in session zone; delegates `string_carries_timezone` to `spark_string_timestamp::{grammar, zone}` |
| 9 | `crates/repark-functions/src/spark_string_timestamp/instant.rs:83-122`, `crates/repark-functions/src/spark_string_timestamp.rs:58-223`, `crates/repark-functions/src/timestamp_ltz_ntz.rs:282-320`, `crates/repark-functions/src/timestamp_ntz_cast.rs:205-207,315-465` | Direct NTZ wall-clock parsing, string nullability, and constant simplification | Parses NTZ wall clocks directly without session-zone round trip (preserving DST gaps and post-2262 years); strips offsets and raises `[CAST_INVALID_INPUT]` under ANSI; simplifies valid constants in `SparkTimestampNtzCast::simplify` |
| 10 | `crates/repark-functions/src/datetime.rs:775-865` | `date_trunc` over `TIMESTAMP_NTZ` | Localizes NTZ wall clock in session zone before truncating and returns `Timestamp(µs, UTC)` on all doors across all 15 unit spellings |
| 11 | `crates/repark-functions/src/spark_nullability.rs:18-155`, `crates/repark-functions/src/timestamp_cast.rs:341-351,528-534`, `crates/repark-functions/src/registration.rs:9-31`, `crates/repark-spark/src/spark_typed.rs:88-145,335-430` | Nullability parity and marker-free display/EXPLAIN | Restores Spark nullability for `CAST`/`TRY_CAST` string-to-timestamp, `CAST(DATE AS TIMESTAMP_LTZ)`, `CAST(TIMESTAMP_NTZ AS DATE)`, and legacy `!ansi` `unix_timestamp`; unwraps internal markers in `SparkProjectionDisplay` |
| 12 | `crates/repark-iceberg/src/write/conform_stage.rs:344-384` | Plan-time Iceberg conform stage (`leaf_store`) | Emits `TIMESTAMP_LTZ_CAST_NAME` when registered so NTZ/DATE writes into Iceberg `TIMESTAMP` (LTZ) columns survive `timestampType=TIMESTAMP_NTZ` |
| 13 | `python/repark/src/repark/spark/functions.py:312-339`, `python/repark/src/repark/spark/column.py:1136-1152`, `crates/repark-spark/src/spark_typed.rs:88-145,402-430` | Unaliased column display names (separate seat) | Out of scope per brief; filed as [FEXPR-TIMESTAMP-DISPLAY-NAME-1](../../../roadmap/mid-term/cards-2026-11-23/fexpr-timestamp-display-name-1-card-2026-11-24.md) |

## Domain probe summary

| Section | Total | Equal on main (`8c07d619`) | Equal on head (`22e77e4a`) | Fixed | Regressed | Moved (non-equal) |
|---|---:|---:|---:|---:|---:|---:|
| Scalar & Literal Cells (`card_cells`) | 744 | 376 | 684 | 308 | 0 | 8 |
| Column-Referencing Cells (`col_cells`) | 192 | 124 | 192 | 68 | 0 | 0 |
| Zone-Change Cells (`zone_change_cells`) | 36 | 18 | 36 | 18 | 0 | 0 |
| Mixing & Filter Cells (`mixing_cells`) | 360 | 218 | 360 | 142 | 0 | 0 |
| Neighbour Cells (`neighbour_cells`) | 228 | 216 | 228 | 12 | 0 | 0 |
| Write & Readback Cells (Iceberg & Parquet) (`write_cells`) | 100 | 72 | 100 | 28 | 0 | 0 |
| Unaliased Display-Name Cells (`unaliased_cells`) | 108 | 24 | 30 | 6 | 0 | 6 |
| Runtime `spark.conf.set` Cells (`runtime_conf_cells`) | 3 | 0 | 0 | 0 | 0 | 0 |
| **Total** | **1771** | **1048** | **1630** | **582** | **0** | **14** |

- **Live Spark 4.1.2 cross-read of head writes:** **84 / 84** Iceberg (`TIMESTAMP_NTZ` and `TIMESTAMP` LTZ) and Parquet artifacts written by `22e77e4aa` in `America/New_York` (both ANSI modes) read back on Spark 4.1.2 + Iceberg 1.11.0 with exact Spark values and types in both `America/New_York` and `UTC` (`0` mismatches).
- **Pre-hand-back seam corpus (`pre_handback_check.py --engine repark --profile full`):** `219 / 219` matches against `main`, `0` diffs.
- **14 non-equal moves (zero regressions):** 8 cells are `ttype_ntz_lit_to_bigint` on `F.expr` (`select`/`withColumn` in NY/Kolkata) moving from LTZ epoch seconds (`1704092400`/`1704054600`) to naive epoch (`1704074400`), while `sql`/`selectExpr` now refuse like Spark (`+12` fixed); 6 cells are `unaliased_ttype_ntz|select` whose dtype moved from wrong `timestamp` to Spark's `timestamp_ntz` while the unaliased display name stays carded.

## Delta 1 verifier re-verification (`22e77e4aa`)

- **All 10 class (a) verifier findings closed (`repro_findings.py`):** 16/16 finding & control groups match live Spark 4.1.2 (`repro_xread`: 0/7 diffs; `F-A7` matches `[CAST_INVALID_INPUT]` class and message).
- **Regression sweeps (0 regressions across every suite):**
  - Native door (`repark.sql()`): `12 / 12` identical to `main`, `0` diffs.
  - Neighbours & edge cases (`attack_regressions.py`): `1,560` cells, `393` fixed, **`0` regressions**.
  - Pruning & pushdown (`attack_regressions.py`): `576` cells, `52` fixed, **`0` regressions**.
  - Display / `EXPLAIN` / error marker leaks: **`0` new leaks**.
  - Write doors (`attack_writes.py`, live Spark cross-read): `189` cells, `157` match Spark, `71` fixed, **`0` regressions**; partitioned Iceberg tables (`days`/`hours` + `.files`): `28 / 28` match Spark, **`0` regressions**; Parquet edge writes (DST gap, overlap, pre-1970): all rows and dtypes match Spark in NY and UTC.
  - W2 DML doors (`attack_writes_2.py`, live Spark cross-read): `32 / 32` match Spark (`colcast`, `ins_scan`, `ins_ow`, `update`, `merge`, `delete` across LTZ/NTZ/ANSI); in-session Iceberg partition pruning: **`128 / 128` match Spark (`38` fixed, `0` regressions, `0` diffs)**.
- **Parquet / Iceberg files scanned & non-mixed `EXPLAIN` check (Ruling #1):**
  - Iceberg `PARTITIONED BY (days(ts))` and `PARTITIONED BY (hours(ts))` (3 files): pure NTZ predicate (`ts = TIMESTAMP_NTZ '2024-01-01 21:00:00'`) prunes to **`N=1` file** (`predicate:[ts = 2024-01-01 21:00:00]`); mixed NTZ-column vs LTZ-predicate (`TIMESTAMP_LTZ`, `CAST(... AS TIMESTAMP_LTZ)`, range `<`) scans **`N=3` files** (`FilterExec: to_timestamp(ts@1) ...`) and returns `[1]` matching Spark.
  - Parquet (`pushdown_filters=true` and `false`, 3 files): pure NTZ retains `pruning_predicate` min/max statistics (`ts_min <= ... <= ts_max`); mixed NTZ/LTZ uses `predicate=to_timestamp(ts@1) = ...` and returns `[1]` under both pushdown settings.
  - Non-mixed `EXPLAIN` vs `main`: `6 / 6` byte-identical under `TIMESTAMP_LTZ`; under `TIMESTAMP_NTZ`, `4 / 4` non-LTZ-literal queries are byte-identical, and the 2 `TIMESTAMP_LTZ`-literal queries keep identical plan shape (`IcebergTableScan ... N=1`) while fixing `main`'s 5-hour wrong literal constant (`02:00 UTC` -> `07:00 UTC`).

## Pins

- **Facade pin (`python/repark/tests/timestamps/test_fexpr_timestamp_ntz_1.py`):** **123** parametrized cases (100 original + 23 new Delta 1 cases covering F-A1..F-A10 and zoned-literal date casts across all doors, zones, ANSI modes, Parquet/Iceberg pushdown, MERGE/UPDATE/INSERT, DST gaps, offsets, `date_trunc`, nullability, and marker-free display/`EXPLAIN`) — on the first head (`fb2c1e948`) 21 of the 23 new cases fail and the 2 UTC control cases of `test_delta1_fa2_zoned_typed_literal_date_cast_on_sql_and_select_expr` pass (`21 failed, 102 passed`, re-measured in delta 2); **123 / 123 passed on `22e77e4aa`**.
- **Timestamps suite (`python/repark/tests/timestamps/`):** original `8e5c9deb1` run was **5,879 passed, 7 skipped (5,886 collected)** (corrected from the earlier 5,979 typo, F-B1). On Delta 1 (`22e77e4aa`), direct microsecond NTZ wall parsing also retired **146** post-2262 NTZ cells from `zone_horizon_render_1_residue.json` (`334` remain; `test_zone_horizon_render_1.py` `12 passed, 1 skipped`); total timestamps suite is **5,902 passed, 7 skipped (5,909 collected)**.
- **Rust unit tests:** deferral config (`timestamp_type.rs`), pre-coercion `SparkNtzTimestampCast` + explicit LTZ marker / mixed NTZ-LTZ comparison & `MERGE` `CASE` / gap & offset / Spark zone spellings / `date_trunc` / F-A9 nullability (`instant_ts/tests.rs`), direct wall parsing & `SparkTimestampNtzCast::simplify` (`timestamp_ntz_cast/tests.rs`), Iceberg conform stage (`conform_stage/tests.rs`), SQL & fragment timestamp lowering (`ntz_cast_lower.rs`, `ltz_values_cast.rs`, `timestamp_ltz_literal.rs`), analyzer seating (`extension/tests.rs`), and `F.expr` lowering (`expr_tests.rs`).
