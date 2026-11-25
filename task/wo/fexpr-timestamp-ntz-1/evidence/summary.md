# task/wo/fexpr-timestamp-ntz-1/evidence/summary.md — FEXPR-TIMESTAMP-NTZ-1 measurement evidence

## Provenance

- **RePark:** `main` at `14d2404a6` (measured on `57c92b411` release wheel; `14d2404a6` adds only one card row) and branch head `b9add8f73` (`fix/fexpr-timestamp-ntz-1`, release wheel built from `b9add8f73`).
- **Spark:** live PySpark 4.1.2 + Iceberg 1.11.0 (`local[2]`, Java 17), both ANSI modes (`spark.sql.ansi.enabled=true` and `false`), across `America/New_York`, `Asia/Kolkata`, and `UTC`.
- **Scripts:** [probe_domain.py](probe_domain.py) (1,771-cell domain sweep across all doors, zones, ANSI modes, zone changes, mixing, Iceberg/Parquet writes, and live cross-reads), [analyze_domain.py](analyze_domain.py) (summary and regression checker).
- **Full cell-by-cell table:** [domain-table.md](domain-table.md).

## Severity Verdict: S1 (measured)

The card proposed S2 provisionally and made Step 0 decisive: any cell writing or answering a wrong value makes the unit S1. Step 0 measured silent wrong stored values on disk (confirmed by live Spark 4.1.2 cross-read) and silent wrong query values on `main` in both ANSI modes:

1. **Iceberg write corruption (5 hours wrong on disk):** under `spark.sql.timestampType=TIMESTAMP_NTZ` in `America/New_York`, writing `F.expr("TIMESTAMP '2024-01-01 02:00:00'")` via `writeTo().append()` into an Iceberg `TIMESTAMP_NTZ` column stored `2024-01-01 07:00:00` (`hour=7`) instead of `2024-01-01 02:00:00` (`hour=2`). Live Spark 4.1.2 read back `2024-01-01 07:00:00` in both NY and UTC. On head `b9add8f73`, Spark 4.1.2 reads back `2024-01-01 02:00:00` (`timestamp_ntz`, `hour=2`) in both NY and UTC.
2. **Parquet write corruption (5 hours wrong on disk):** writing `F.expr("CAST(T AS TIMESTAMP_NTZ)")`, `F.expr("CAST('2024-01-01 02:00:00' AS TIMESTAMP_NTZ)")`, or `F.expr("TIMESTAMP '2024-01-01 02:00:00'")` (under `timestampType=TIMESTAMP_NTZ`) to Parquet in `America/New_York` wrote `timestamp[us, tz=UTC]` (`1704092400000000`, `07:00 UTC`) instead of `timestamp[us]` (`1704074400000000`, `02:00`). Live Spark 4.1.2 read back `2024-01-01 07:00:00` (`timestamp`) in UTC. On head `b9add8f73`, Spark 4.1.2 reads back `2024-01-01 02:00:00` (`timestamp_ntz`, `hour=2`) in both NY and UTC.
3. **Silent wrong query values across `F.expr`, `filter(str)`, zone changes, and mixing:**
   - `F.expr("CAST('2024-01-01 02:00:00+05:30' AS TIMESTAMP_NTZ)")` answered `2023-12-31 15:30:00` in NY / `2023-12-31 20:30:00` in UTC instead of `2024-01-01 02:00:00`.
   - `F.expr("TRY_CAST(T AS TIMESTAMP_NTZ)")` and `F.expr("TRY_CAST(ts AS TIMESTAMP_NTZ)")` answered `2024-01-01 07:00:00` in NY / `2023-12-31 20:30:00` in Kolkata instead of `2024-01-01 02:00:00`.
   - `F.expr("CAST(0 AS TIMESTAMP_NTZ)")` silently returned `1970-01-01 00:00:00` instead of refusing `DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION`.
   - `F.expr("(CAST(T AS TIMESTAMP_NTZ)) <=> ntz")`, `df.filter(F.expr("ntz = (CAST(T AS TIMESTAMP_NTZ))"))`, and `df.filter("ntz = (CAST(T AS TIMESTAMP_NTZ))")` returned `false` / `0` rows in NY and Kolkata instead of `true` / `1` row.
   - `unix_timestamp` over `F.expr` NTZ expressions answered `1704092400` in NY instead of `1704074400`.
   - Under `spark.sql.timestampType=TIMESTAMP_NTZ`, `F.expr("TIMESTAMP '2024-03-10 02:30:00'")` shifted across the NY DST spring-forward gap to `03:30:00`, `F.expr("CAST('2024-01-01 02:00:00Z' AS TIMESTAMP)")` returned `2023-12-31 21:00:00`, `F.expr("CAST(T AS TIMESTAMP_LTZ)")` returned `2024-01-01 07:00:00`, `TRY_CAST('2024-01-01 02:00:00' AS TIMESTAMP)` returned `timestamp` (`21:00` on SQL), and mixing with a real NTZ column shifted `coalesce` / `union` rows by 5 hours and dropped `join` / `filter` matches.

## Seat inventory (`path:line` on `b9add8f73`)

| # | Seat (`path:line`) | Role | Resolution |
|---|---|---|---|
| 1 | `crates/repark-spark/src/keyword_lower/ntz_cast_lower.rs:12-90`, `crates/repark-spark/src/keyword_lower.rs:173-180` | AST timestamp cast lowering for fragments | Exposed `lower_fragment_timestamp_casts`, `lower_fragment_timestamp_ltz_cast`, and `rewrite_fragment_timestamp_ltz_literals` |
| 2 | `crates/repark-spark/src/spark_rewrites/timestamp_ltz_literal.rs:33-94` | Preserving explicit `TIMESTAMP_LTZ '...'` on fragment canonicalization | Added `rewrite_fragment_timestamp_ltz_literals` so `TIMESTAMP_LTZ '...'` lowers to `CAST('...' AS TIMESTAMP_LTZ)` before `canonicalize_verbatim` |
| 3 | `crates/repark-spark/src/values_marker.rs:150-164` | Column-free `F.expr` fragment planner (`plan_marked_fragment`) | Calls `lower_fragment_timestamp_casts` on Spark-door fragments |
| 4 | `crates/repark-python/src/column/mod.rs:43`, `crates/repark-python/src/column/expr_build.rs:50-149`, `crates/repark-python/src/column/expr_build/fragment_canon.rs:1-28` | `F.expr` column-referencing (`fragment_logical_expr`) and `filter(str)` (`marked_logical_expr`, `folded_qualifier_predicate`) | Uses `fragment_canon::{canonicalize_fragment, canonicalize_predicate}` and calls `lower_fragment_timestamp_casts` |
| 5 | `crates/repark-python/src/column/expr_build.rs:332-399`, `crates/repark-functions/src/timestamp_type.rs:154-205` | Process-global `EXPR_CONTEXT` timestamp-type deferral | Added `DeferredSparkTimestampTypeConfig` (`with_deferred_spark_timestamp_type`, `is_spark_timestamp_type_deferred`) and installed it in `build_expr_context` |
| 6 | `crates/repark-common/src/udf_names.rs:64-65`, `crates/repark-functions/src/instant_ts/ltz_cast.rs:1-225` | Explicit LTZ cast marker UDFs and NTZ-session DML store-cast retarget | Added `__repark_cast_timestamp_ltz__`, `__repark_try_cast_timestamp_ltz__`, and `retarget_dml_ltz_store_casts` |
| 7 | `crates/repark-functions/src/instant_ts.rs:538-851` | `SparkLtzTimestampCast` analyzer rule | Skips eager bare-`TIMESTAMP` rewriting when deferred, folds zoned LTZ markers, unwraps explicit LTZ markers via LTZ path under NTZ sessions, adds `Expr::TryCast` + LTZ-to-NTZ wall conversion in `rewrite_cast_as_ntz`, and rebuilds stale `LogicalPlan::Union` schemas under NTZ |
| 8 | `crates/repark-functions/src/timestamp_ntz_cast.rs:306-323` | `SparkTimestampNtzCast::return_field_from_args` | Marks string-source NTZ casts nullable (matches Spark 4.1.2 in both ANSI modes, fixing `CAST('bad' AS TIMESTAMP_NTZ)` under `ansi=false`) |
| 9 | `crates/repark-iceberg/src/write/conform_stage.rs:344-384` | Plan-time Iceberg conform stage (`leaf_store`) | Emits `TIMESTAMP_LTZ_CAST_NAME` when registered so NTZ/DATE writes into Iceberg `TIMESTAMP` (LTZ) columns survive `timestampType=TIMESTAMP_NTZ` |
| 10 | `python/repark/src/repark/spark/functions.py:312-339`, `column.py:1136-1152`, `crates/repark-spark/src/spark_typed.rs:86-141,402-421` | Unaliased column display names (separate seat) | Out of scope per brief; filed as [FEXPR-TIMESTAMP-DISPLAY-NAME-1](../../../roadmap/mid-term/cards-2026-11-23/fexpr-timestamp-display-name-1-card-2026-11-24.md) |

## Domain probe summary

| Section | Total | Equal on main | Equal on head (`b9add8f73`) | Fixed | Regressed | Moved (non-equal) |
|---|---:|---:|---:|---:|---:|---:|
| Scalar & Literal Cells (`card_cells`) | 744 | 376 | 624 | 248 | 0 | 8 |
| Column-Referencing Cells (`col_cells`) | 192 | 124 | 180 | 56 | 0 | 0 |
| Zone-Change Cells (`zone_change_cells`) | 36 | 18 | 36 | 18 | 0 | 0 |
| Mixing & Filter Cells (`mixing_cells`) | 360 | 218 | 360 | 142 | 0 | 0 |
| Neighbour Cells (`neighbour_cells`) | 228 | 216 | 228 | 12 | 0 | 0 |
| Write & Readback Cells (`write_cells`) | 100 | 72 | 100 | 28 | 0 | 0 |
| Unaliased Display-Name Cells (`unaliased_cells`) | 108 | 24 | 30 | 6 | 0 | 6 |
| Runtime `spark.conf.set` Cells (`runtime_conf_cells`) | 3 | 0 | 0 | 0 | 0 | 0 |
| **Total** | **1771** | **1048** | **1558** | **510** | **0** | **14** |

- **Live Spark 4.1.2 cross-read of head writes:** **84 / 84** Iceberg (`TIMESTAMP_NTZ` and `TIMESTAMP` LTZ) and Parquet artifacts written by `b9add8f73` in `America/New_York` (both ANSI modes) read back on Spark 4.1.2 + Iceberg 1.11.0 with exact Spark values and types in both `America/New_York` and `UTC` (`0` mismatches).
- **Pre-hand-back seam corpus (`pre_handback_check.py --profile full`):** `219 / 219` matches against `main`, `0` diffs.
- **14 non-equal moves (zero regressions):** 8 cells are `ttype_ntz_lit_to_bigint` on `F.expr` (`select`/`withColumn` in NY/Kolkata) moving from LTZ epoch seconds (`1704092400`/`1704054600`) to match `spark.sql`/`selectExpr`'s naive epoch (`1704074400`); 6 cells are `unaliased_ttype_ntz|select` whose dtype moved from wrong `timestamp` to Spark's `timestamp_ntz` while the unaliased display name stays carded.

## Pins

- **Facade pin (`python/repark/tests/timestamps/test_fexpr_timestamp_ntz_1.py`):** 100 parametrized cases across all doors (`select`, `withColumn`, `filter`, `selectExpr`, `sql`), zones (`America/New_York`, `Asia/Kolkata`, `UTC`), ANSI modes (`true`/`false`), zone changes, mixing, Iceberg/Parquet writes, and neighbours — **76 failed, 24 passed on main**; **100 passed on `b9add8f73`**.
- **Neighbour suites:** `test_date_part_session_zone_1.py` and `test_timestamp_type.py` (171 passed on main) plus all 5,979 tests in `python/repark/tests/timestamps/` pass on `b9add8f73`.
- **Rust unit tests:** deferral config (`timestamp_type.rs`), explicit LTZ marker / NTZ `TRY_CAST` / LTZ-to-NTZ wall conversion / stale `Union` rebuild / NTZ-session DML-into-LTZ store cast (`instant_ts/tests.rs`), string-source NTZ cast nullability (`timestamp_ntz_cast/tests.rs`), Iceberg conform stage LTZ marker (`conform_stage/tests.rs`), fragment cast & `TIMESTAMP_LTZ` literal lowering (`ntz_cast_lower.rs`, `timestamp_ltz_literal.rs`), and `F.expr` lowering (`expr_tests.rs`).
