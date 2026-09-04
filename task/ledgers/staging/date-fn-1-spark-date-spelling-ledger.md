# Unit ledger — DATE-FN-1 · Spark SQL `date()` spelling

**Retires:** this ledger moves to `../completed/` in the unit's last commit.
This file closes when DATE-FN-1 merges, or when the owner closes the slate row.

**Unit:** DATE-FN-1 · **Date:** 2026-09-04 · **Executor:** Grok (grok-4.6), Actor ·
**Branch:** `fix/date-fn-1-spark-date-spelling` · **Base:** `main` `8cb965f` + SQL-HARDEN-1 merge
**Model:** grok-4.6
**risk_tier:** standard.

Spark is the oracle. Live PySpark 4.1.2, zulu-17, `TZ=UTC`, ANSI on unless named, 2026-09-04.
Registry cell `CUTOVER-DATE-1` matched; no HALT.

## Proposition ledger

| ID | Clause | Proof obligation | Verdict |
|---|---|---|---|
| C-001 | Live cells for `date(ts)`, `date(string)`, `date(date)`, `date(NULL)`, invalid string under both ANSI modes, and `unix_timestamp` on timestamp / string / NULL; SQL door and facade (`F.date`?). | Oracle table below. | **PROVEN** |
| C-002 | Register `date` as the SQL-door spelling with Spark's CAST semantics; S6 gold fact + agg tables build; rows Spark-equal; no new dependency. | Kernels + dispatch + S6 golden. | **PROVEN** |
| C-003 | S6 golden updated; unit pins for each measured cell; live co-collected leg; mutation unregister `date` → red (`N` red of `M`). | Pins + live test + mutation table. | **PROVEN** |
| C-004 | `CUTOVER-DATE-1` FIXED 2026-09-04 (DATE-FN-1); matrix row; STATUS h2 one line; this ledger + maps lockstep. | Registry + matrix + STATUS + maps. | **PROVEN** |

## Oracle (live PySpark 4.1.2, 2026-09-04, JDK 17, `TZ=UTC`)

| Cell | Spark | repark before |
|---|---|---|
| `DATE(TIMESTAMP '2024-06-15 03:00:00')` | `2024-06-15` date32 non-null | `Invalid function 'date'` |
| `DATE('2024-06-15')` | `2024-06-15` date32 nullable | `Invalid function 'date'` |
| `DATE('2024-06-15 03:00:00')` | `2024-06-15` date32 nullable | `Invalid function 'date'` |
| `DATE(DATE '2024-06-15')` | `2024-06-15` date32 non-null | `Invalid function 'date'` |
| `DATE(NULL)` / null ts / null string | NULL date32 nullable | `Invalid function 'date'` |
| `DATE('not-a-date')` ANSI on | `CAST_INVALID_INPUT` | `Invalid function 'date'` |
| `DATE('not-a-date')` ANSI off | NULL | `Invalid function 'date'` |
| `DATE('06/15/2024')` ANSI on | `CAST_INVALID_INPUT` | `Invalid function 'date'` |
| `DATE('06/15/2024')` ANSI off | NULL | `Invalid function 'date'` |
| `F.date` | **absent** | absent |
| `unix_timestamp(TIMESTAMP '2024-06-15 12:00:00')` UTC | `1718452800` int64 non-null | `Invalid function 'unix_timestamp'` |
| `unix_timestamp(TIMESTAMP '1969-12-31 23:30:00')` | `-1800` | `Invalid function 'unix_timestamp'` |
| `unix_timestamp('2024-06-15 12:00:00')` UTC | `1718452800` | `Invalid function 'unix_timestamp'` |
| `unix_timestamp('2024-06-15')` ANSI on | `CANNOT_PARSE_TIMESTAMP` | `Invalid function 'unix_timestamp'` |
| `unix_timestamp(NULL)` / null ts / null string | NULL int64 nullable | `Invalid function 'unix_timestamp'` |
| `unix_timestamp('not-a-timestamp')` ANSI on | `CANNOT_PARSE_TIMESTAMP` | `Invalid function 'unix_timestamp'` |
| `unix_timestamp('not-a-timestamp')` ANSI off | NULL | `Invalid function 'unix_timestamp'` |
| S6 gold join | fact `(s1, 10, 15), (s2, 20, 40), (s3, 10, 15)`; agg two rows | `Invalid function 'date'` |

## Kernels

| Name | Layer |
|---|---|
| `date` | `timestamp_cast.rs` `SparkDate`; `invoke_local_dates`; ANSI invalid string `CAST_INVALID_INPUT` / NULL |
| `unix_timestamp` | `timestamp_cast.rs` `SparkUnixTimestamp`; timestamp floor epoch; string `yyyy-MM-dd HH:mm:ss` in session zone; 0-arg now; alias `to_unix_timestamp` |
| facade | `_scalar("unix_timestamp")`; no `F.date` (PySpark has none) |
| S6 CTAS | `conform_batch_retaining_unmapped_columns` identity arm attaches the write schema field ids so a multi-table join cannot leak `PARQUET:field_id` |

## Mutation

| Knob | Red of M |
|---|---|
| skip `ctx.register_udf(timestamp_cast::date_udf())` | 13 red of 25 (`test_date_fn_1.py` DATE cells + S6 CTAS; unix_timestamp pins stayed green) |
| restore 1-row array for zero-arg `unix_timestamp()` | 1 red of 25 (`test_unix_timestamp_zero_arg_repeats_once_per_input_row`) |

## S6 after the fix

| Probe | repark | Spark | Agree |
|---|---|---|---|
| statements | 14 OK | 14 OK | yes |
| fact rows | `(s1, 10, 15), (s2, 20, 40), (s3, 10, 15)` | same | yes |
| fact schema | string/int32 nullable | same | yes |
| agg rows | `(10, 2026-01-01, Thursday, 1, 1), (20, 2026-01-02, Friday, 1, 1)` | same | yes |
| agg `num_surveys` | int64 non-null | int64 nullable | no |
| META zstd | absent | `write.parquet.compression-codec = zstd` | no (`V3-COV-7`) |

S6 verdict stays **DIVERGES**; registry citation moves to `V3-COV-7`. `CUTOVER-DATE-1` is FIXED.

## 9. Delivery template

| Item | Path |
|---|---|
| Registry | `docs/spark-sql-iceberg-parity.md` `CUTOVER-DATE-1` FIXED 2026-09-04 (DATE-FN-1); `B-TZ-1` FIXED |
| Live leg | `test_parity_live.py::test_live_date_fn_1_date_and_unix_timestamp` on `spark_engine` |
| S6 golden | `_sql_harden_cutover_repark.py` fact/agg rows; `VERDICTS` still DIVERGES |
| Maps | lockstep on every touched directory |

```yaml
COVERAGE_ATTESTATION:
  pr_unit: date-fn-1-spark-date-spelling
  complete: true
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Live PySpark 4.1.2 cells recorded before code; pins assert Spark answers.
      artifacts: [python/repark/tests/test_date_fn_1.py, python/repark/tests/test_parity_live.py]
    - id: AT-2
      status: ATTACKED
      evidence: Controls cover timestamp, string, date, NULL, invalid ANSI on/off, unix_timestamp ts/string/NULL.
      artifacts: [python/repark/tests/test_date_fn_1.py]
    - id: AT-3
      status: ATTACKED
      evidence: Invalid date string ANSI on CAST_INVALID_INPUT; invalid unix string CANNOT_PARSE_TIMESTAMP.
      artifacts: [python/repark/tests/test_date_fn_1.py]
    - id: AT-4
      status: N/A
      justification: Scalar UDFs are immutable and have no shared mutable state.
    - id: AT-5
      status: ATTACKED
      evidence: No AWS, IAM, secrets, .github, or dependency-file change.
      artifacts: [crates/repark-functions/src/timestamp_cast.rs]
    - id: AT-6
      status: ATTACKED
      evidence: date and unix_timestamp are named Spark spellings; no unnamed helpers.
      artifacts: [crates/repark-functions/src/timestamp_cast.rs]
    - id: AT-7
      status: ATTACKED
      evidence: Tests in the same change; S6 golden rewritten to the measured post-fix answer.
      artifacts: [python/repark/tests/_sql_harden_cutover_repark.py]
    - id: AT-8
      status: ATTACKED
      evidence: map.md lockstep on every touched directory.
      artifacts: [crates/repark-functions/src/map.md, python/repark/tests/map.md]
    - id: AT-9
      status: ATTACKED
      evidence: Mutation skip date_udf register reds the DATE pins.
      artifacts: [task/ledgers/staging/date-fn-1-spark-date-spelling-ledger.md]
    - id: AT-10
      status: ATTACKED
      evidence: Live co-collect on spark_engine beside test_live_disclosure_still_diverges.
      artifacts: [python/repark/tests/test_parity_live.py]
DELIVERY_SIGNOFF:
  pr_unit: date-fn-1-spark-date-spelling
  artifacts_verified:
    ledger: PASS (C-001..C-004 PROVEN)
    coverage_attestation: PASS (AT-1..AT-10)
    findings_ledger: PASS (none open)
    shipped_flag_register: PASS (count 0)
  done_gate: PASS
  status_update: STATUS.md h2 DATE-FN-1 one line; CUTOVER-DATE-1 FIXED
  verdict: ACCEPTED
  rejection_route: N/A
SHIPPED_FLAG_REGISTER:
  pr_unit: date-fn-1-spark-date-spelling
  flags: []
  count: 0
```
