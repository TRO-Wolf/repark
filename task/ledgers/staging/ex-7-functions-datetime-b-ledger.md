# Unit ledger — EX-7 · v0.7 example backfill, `F.*` unix-time, timestamp construction and partition transforms

**Retires:** this ledger moves to `../completed/` in the unit's last commit (the orchestrator's departure move). This file closes when EX-7 merges, or when the owner closes the slate row.

**Unit:** EX-7 · **Date:** 2026-09-03 · **Model:** muse-spark-1.2-contributor (batch, continuation of glm-5.3-flash); glm-5.3-flash (remediation) · **Branch:** `feat/ex-7-functions-datetime-b` · **Base:** `a0cd39e` (dispatch base `84c1801`)
**Slate:** [briefs/example-backfill.md](../../../briefs/example-backfill.md), batch roster row batch b (28 names: unix-time, timestamp construction and partition transforms).
**Ruling:** owner, 2026-08-31, [release-roadmap-2026-08-29.md](../../roadmap/epic-term/release-roadmap-2026-08-29.md) §"v0.7 — Full example documentation".

**Rubric:** STANDARD. Floor S1. `risk_tier: standard`.

**Writable paths:** `docs/examples/functions/`, `docs/examples/backlog.txt`, the `BACKLOG_BASELINE` constant in `scripts/check_example_coverage.py`, lockstep `map.md` files, and this ledger with its `staging/map.md` row. Closed: `crates/`, `python/repark/src/`, every other `scripts/` line, `.github/`, `STATUS.md`, every other ledger, `briefs/next-sequence.md`.

## Scope

The family is the `F.*` unix-time, timestamp-construction and partition-transform names the campaign left on the backlog. This unit is batch b of EX-7 (28 dispatched names, 21 landed, 7 dropped as refused or divergent).

**Roster as dispatched (28 names, measured against `docs/examples/backlog.txt` at `a0fe83a`):**

`F.from_unixtime`, `F.unix_timestamp`, `F.to_unix_timestamp`, `F.unix_date`, `F.unix_micros`, `F.unix_millis`, `F.unix_seconds`, `F.date_from_unix_date`, `F.timestamp_micros`, `F.timestamp_millis`, `F.timestamp_seconds`, `F.to_date`, `F.to_timestamp`, `F.try_to_date`, `F.try_to_timestamp`, `F.try_to_time`, `F.make_date`, `F.make_timestamp`, `F.make_interval`, `F.make_dt_interval`, `F.from_utc_timestamp`, `F.to_utc_timestamp`, `F.current_timezone`, `F.days`, `F.hours`, `F.months`, `F.years`, `F.bucket`.

**As landed: 21 kept.** The 7 refused or value-divergent names stay on the backlog with both values recorded — see "Outcome" below.

**Grouping.** Six files, grouped by the idea a reader learns in one breath:

| File | `COVERS` (roster names) | Why these together |
|---|---|---|
| `epoch.py` | `F.unix_date`, `F.unix_seconds`, `F.unix_millis`, `F.unix_micros`, `F.date_from_unix_date`, `F.from_unixtime` | Epoch conversions: calendar values ↔ counts since 1970, and the string render back. |
| `timestamp_from_epoch.py` | `F.timestamp_seconds`, `F.timestamp_millis`, `F.timestamp_micros`, `F.unix_seconds` | Timestamp construction from epoch counts, with the seconds round trip. |
| `to_date_timestamp.py` | `F.to_date`, `F.to_timestamp`, `F.try_to_date` | Parse calendar strings; the `try_` door answering NULL on malformed input. |
| `make_calendar.py` | `F.make_date`, `F.make_dt_interval` | Build calendar values from parts — date from Y/M/D and day-time interval from duration parts. |
| `utc_offsets.py` | `F.from_utc_timestamp`, `F.to_utc_timestamp`, `F.current_timezone` | Session zone and UTC offset renders between UTC and a named zone. |
| `partition_transforms.py` | `F.years`, `F.months`, `F.days`, `F.bucket` | Partition transforms through `writeTo(...).partitionedBy(...)`, rows read back from created tables. |

Every file lists `F.col` in `COVERS`, and `make_calendar.py` also lists `F.lit`, because they genuinely use them; those names are already covered, so they do not move the ratchet.

## Orchestrator rulings (build-to)

- The gate is the acceptance bar in both directions: a `COVERS` entry the script does not exercise is red, and every script runs green locally with no network, no cloud and no JVM beyond the live oracle measurement step.
- Every asserted value is measured against live PySpark 4.1.2 + Iceberg 1.11.0 before it is written; a name whose repark value differs from Spark, or that repark refuses, is dropped from its file's `COVERS` and stays on the backlog with both values recorded.
- The backlog count moves down by exactly the names this batch covers, and `BACKLOG_BASELINE` moves with it — measured at 842 → 821, twenty-one of the twenty-eight dispatched.
- No product edit. A name whose example would expose an engine defect is reported and dropped back to the backlog.

## Proposition ledger

| ID | Clause | Evidence | Verdict |
|---|---|---|---|
| C-001 | Batch b lands runnable local examples for the 21 roster names it can demonstrate honestly, in six files under `docs/examples/functions/`, every asserted value measured against live PySpark 4.1.2 + Iceberg 1.11.0 before it was written and every `COVERS` entry exercised by an assertion on that measured value; those 21 leave `docs/examples/backlog.txt` and `BACKLOG_BASELINE` moves down by exactly 21, 842 → 821, with no other `scripts/` change; the 7 refused or value-divergent names stay on the backlog with both values recorded in the Outcome table, and no product file is touched; the gate's static half and its `--require-execute` leg both exit 0. | Red-first capture below, the oracle table, the green counts line, and the recorded gate exit codes. | **OPEN** |

## Red-first (docs/testing.md "Gate provocation proofs")

Captured on the branch at base `a0fe83a` with the six batch-b files removed and the 21 landed backlog rows already gone. `./scripts/check_example_coverage.py` then exits **1** with 21 findings, one per landed roster name and no others:

```
example-coverage: 21 finding(s)
  public name F.bucket has no example COVERS row and is not in the backlog or exceptions
  public name F.current_timezone has no example COVERS row and is not in the backlog or exceptions
  public name F.date_from_unix_date has no example COVERS row and is not in the backlog or exceptions
  public name F.days has no example COVERS row and is not in the backlog or exceptions
  public name F.from_unixtime has no example COVERS row and is not in the backlog or exceptions
  public name F.from_utc_timestamp has no example COVERS row and is not in the backlog or exceptions
  public name F.make_date has no example COVERS row and is not in the backlog or exceptions
  public name F.make_dt_interval has no example COVERS row and is not in the backlog or exceptions
  public name F.months has no example COVERS row and is not in the backlog or exceptions
  public name F.timestamp_micros has no example COVERS row and is not in the backlog or exceptions
  public name F.timestamp_millis has no example COVERS row and is not in the backlog or exceptions
  public name F.timestamp_seconds has no example COVERS row and is not in the backlog or exceptions
  public name F.to_date has no example COVERS row and is not in the backlog or exceptions
  public name F.to_timestamp has no example COVERS row and is not in the backlog or exceptions
  public name F.to_utc_timestamp has no example COVERS row and is not in the backlog or exceptions
  public name F.try_to_date has no example COVERS row and is not in the backlog or exceptions
  public name F.unix_date has no example COVERS row and is not in the backlog or exceptions
  public name F.unix_micros has no example COVERS row and is not in the backlog or exceptions
  public name F.unix_millis has no example COVERS row and is not in the backlog or exceptions
  public name F.unix_seconds has no example COVERS row and is not in the backlog or exceptions
  public name F.years has no example COVERS row and is not in the backlog or exceptions
```

With the six files present the gate is green.

## Outcome — 21 kept, 7 dropped (oracle table)

Measured on this tree against live PySpark 4.1.2 + Iceberg 1.11.0 at `/tmp/oc-ex7/.venv/bin/python` with `TZ=UTC` exported before the JVM starts, `JAVA_HOME=/usr/lib/jvm/zulu-17-amd64` and `PYTHONPATH=/tmp/oc-ex7/python/repark/tests` using `_live_parity.build_spark_iceberg_engine(Path(tmpdir)).session` — one throwaway script outside the repo that printed per name the Spark value and the repark value for the same inputs. **The driver `TZ=UTC` is load-bearing**: without it PySpark's `collect()` renders driver-local naive datetimes while repark renders the session zone (measured pair filed in the registry queue), so any run whose driver TZ is not UTC produces false divergences at the collect boundary. Every kept row was equal by repr; dropped rows record both values. Remediation re-measurement 2026-09-03 (GLM 5.3 Flash, same recipe): the four partition-transform value sets, the `try_to_time` refusals, the `F.hours` refusals and the America/New_York `collect()` pair were re-measured on both engines the same way before anything was written.

| Name | Spark value (repr) | Repark value (repr) | Disposition | File |
|---|---|---|---|---|
| F.from_unixtime | `"1970-01-01 00:00:00", "1970-01-02 00:00:00", "1971-01-01 00:00:01", "1969-12-31 00:00:00", None` for inputs 0, 86400, 31536001, -86400, None | same | kept | epoch.py |
| F.unix_timestamp | `1577836800, 0, -1, None` for well-formed timestamp strings | `UnsupportedOperationException: functions.unix_timestamp is not supported yet` | dropped — engine gap | backlog |
| F.to_unix_timestamp | same as `unix_timestamp` (alias) | same UnsupportedOperation | dropped — engine gap | backlog |
| F.unix_date | `[0, 1, -1, 19782, None]` for dates 1970-01-01, 1970-01-02, 1969-12-31, 2024-02-29, None | same | kept | epoch.py |
| F.unix_micros | `[0, 1577836800000000, -1000000, None]` for timestamps 1970-01-01, 2020-01-01, 1969-12-31 23:59:59, None | same | kept | epoch.py |
| F.unix_millis | `[0, 1577836800000, -1000, None]` for same stamps | same | kept | epoch.py |
| F.unix_seconds | `[0, 1577836800, -1, None]` for same stamps | same | kept | epoch.py and timestamp_from_epoch.py |
| F.date_from_unix_date | `[date(1970,1,1), date(1970,1,2), date(1969,12,31), date(2022,1,8), None]` for n=0,1,-1,19000,None | same | kept | epoch.py |
| F.timestamp_micros | `[datetime(1970,1,1), datetime(2009,2,13,23,31,30,123456), None]` for micros 0, 1234567890123456, None | same | kept | timestamp_from_epoch.py |
| F.timestamp_millis | `[datetime(1970,1,1), datetime(2009,2,13,23,31,30,123000), datetime(1969,12,31,23,59,59,999000), None]` for millis 0, 1234567890123, -1, None | same | kept | timestamp_from_epoch.py |
| F.timestamp_seconds | `[datetime(1970,1,1), datetime(2020,1,1), datetime(1969,12,31,23,59,59), None]` for seconds 0, 1577836800, -1, None | same | kept | timestamp_from_epoch.py |
| F.to_date | `[date(2020,1,2), date(2020,1,2), None]` for s="2020-01-02", "2020-01-02 13:45:00", None | same | kept | to_date_timestamp.py |
| F.to_timestamp | `[datetime(2020,1,2,0,0), datetime(2020,1,2,13,45), None]` for same s | same | kept | to_date_timestamp.py |
| F.try_to_date | `[date(2020,1,2), None, None]` for s="2020-01-02", "not-a-date", None | same | kept | to_date_timestamp.py |
| F.try_to_timestamp | `NULL for malformed, datetime for well-formed` | `UnsupportedOperationException: functions.try_to_timestamp is not supported yet` | dropped — engine gap | backlog |
| F.try_to_time | refuses every form (date-only, time-only, datetime, string+format): `AnalysisException: [UNSUPPORTED_TIME_TYPE] The data type TIME is not supported. SQLSTATE: 0A000` | refuses on both doors with the same text behind a planning prefix: `AnalysisException: Error during planning: [UNSUPPORTED_TIME_TYPE] The data type TIME is not supported. SQLSTATE: 0A000` | dropped — Spark refuses / repark refuses | backlog |
| F.make_date | `[date(2020,1,2), date(2024,2,29), date(1999,12,31), None]` for (y,m,d) parts including NULL | same | kept | make_calendar.py |
| F.make_timestamp | `datetime(2020,1,2,3,4,6)` for y=2020,m=1,d=2,h=3,mi=4,s=6 | `UnsupportedOperationException: functions.make_timestamp is not supported yet` | dropped — engine gap | backlog |
| F.make_interval | `interval months/days/nanos for (years,months,weeks,days,hours,mins,secs)` | `PySparkNotImplementedError: Python conversion for calendar interval (make_interval / CalendarIntervalType)` | dropped — interval type conversion gap | backlog |
| F.make_dt_interval | `[timedelta(days=1,seconds=7384,micros=500000), timedelta(0), timedelta(days=-1,micros=250000), None]` for (d,h,mi,s) parts | same | kept | make_calendar.py |
| F.from_utc_timestamp | `[datetime(2020,1,1,7,0), datetime(2020,7,1,8,0), None]` for ts in UTC rendered to America/New_York | same | kept | utc_offsets.py |
| F.to_utc_timestamp | `[datetime(2020,1,1,17,0), datetime(2020,7,1,16,0), None]` for same ts read as New_York | same | kept | utc_offsets.py |
| F.current_timezone | `["UTC","UTC","UTC"]` for session | same | kept | utc_offsets.py |
| F.days | rows round-trip through `partitionedBy(F.days(F.col("event_date")))` and the partition values read back equal: `[(datetime.date(2024, 3, 15),), (datetime.date(2024, 6, 1),)]` (`partition.event_date_day:date`) | same | kept | partition_transforms.py |
| F.hours | writes Spark-equal partition values: for event_ts 2024-03-15 05:00:00 / 06:30:00 the files metadata reads `[(475133,), (475134,)]` (`partition.event_ts_hour:int`) | the example's own facade path refuses: `writeTo(...).partitionedBy(F.hours(F.col("event_ts"))).create()` raises `DataInvalid => Invalid schema for v2:` `- Invalid type for event_ts: timestamp_ns is not supported until v3`, and the refusal is unchanged with `repark.sql.allowCreateFormatVersion3` set on the builder or via `conf.set`; the facade append into the SQL-door v3 table raises `datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Timestamp(ns) but found Timestamp(µs, "UTC") at column index 0`; repark's SQL door `CREATE TABLE … PARTITIONED BY (hours(event_ts)) TBLPROPERTIES ('format-version'='3')` writes and reads back Spark's values `[(475133,), (475134,)]`; the facade path is filed as registry queue entry EX7-HOURS-1 | dropped — the facade path refuses (measured) | backlog |
| F.months | rows round-trip through `partitionedBy(F.months(F.col("event_date")))` and the partition values read back equal: `[(650,), (653,)]` (`partition.event_date_month:int`) | same | kept | partition_transforms.py |
| F.years | rows round-trip through `partitionedBy(F.years(F.col("event_date")))` and the partition values read back equal: `[(54,), (55,)]` (`partition.event_date_year:int`) | same | kept | partition_transforms.py |
| F.bucket | rows round-trip through `partitionedBy(F.bucket(4, F.col("id")))` and the partition values read back equal: `[(0,), (1,), (3,)]` over ids 1, 2, 3, 55, 89 (`partition.id_bucket:int`) | same | kept | partition_transforms.py |

Wall-clock and cost: batch — start 2026-09-03T00:00:00Z, end 2026-09-03T00:20:00Z (carried from
the prior session's record). Spark continuation re-measurement (muse-spark, 2026-09-03) —
~10 min, free (local JVM). Remediation leg (GLM 5.3 Flash, 2026-09-03) — start
2026-09-03T08:24:29Z, end 2026-09-03T08:56:38Z. Cost: the muse-spark leg ended on transport
deaths (the Spark endpoint stalled mid-round), so the remediation ran on GLM 5.3 Flash; the
Spark oracle legs run locally at no metered cost; the GLM remediation leg is token-metered only.

## Remediation (2026-09-03, GLM 5.3 Flash)

The critic re-measured the kept value sets under driver `TZ=UTC` — 18 kept value sets confirmed
Spark-equal — and failed the record and one example. Fixes, each measured before it was written:

- `partition_transforms.py` asserted only the row round-trip, so a years→months transform
  mutation on the write path stayed green. The example now also asserts the measured partition
  values read back from the tables' `.files` metadata (`SELECT partition.<field> FROM <t>.files`):
  years → `[(54,), (55,)]`, months → `[(650,), (653,)]`, days → the two dates,
  bucket(4) over ids 1, 2, 3, 55, 89 → `[(0,), (1,), (3,)]`. All four value sets measured equal
  on Spark and repark; the years→months mutation now exits 1.
- The `F.try_to_time` row carried a Spark cell live Spark contradicts: Spark refuses every form
  with `[UNSUPPORTED_TIME_TYPE]`. The row now records "Spark refuses / repark refuses" with the
  exact messages.
- The measurement recipe above now states the driver `TZ=UTC` requirement, and the
  `collect()`-rendering difference is filed as registry queue entry EX7-TZCOLLECT-1 with the
  measured America/New_York pair.
- The `F.hours` row now records the measured refusals on the example's own facade path (with and
  without `repark.sql.allowCreateFormatVersion3`), Spark's partition values, and the SQL-door
  parity note; the facade path is filed as registry queue entry EX7-HOURS-1.

Writable-path note: the remediation brief extended this unit's writable set to the registry queue
section of `docs/spark-sql-iceberg-parity.md` for those two dated queue entries; nothing else
outside the original set was touched.

## Gates (2026-09-03, on this branch tree)

| Command | Exit |
|---|---|
| `python3 scripts/check_example_coverage.py` (static half) | 0 |
| `python3 scripts/check_example_coverage.py --require-execute` | 0 |
| `make check-map-sync` | 0 |
| `make check-ledger-grammar` | 0 |
| `make check-ledgers` | 0 |
| `uv run --no-sync ruff check docs/examples` | 0 |
| `uv run --no-sync ruff format --check docs/examples` | 0 |
| `docs/examples/functions/epoch.py` | 0 |
| `docs/examples/functions/timestamp_from_epoch.py` | 0 |
| `docs/examples/functions/to_date_timestamp.py` | 0 |
| `docs/examples/functions/make_calendar.py` | 0 |
| `docs/examples/functions/utc_offsets.py` | 0 |
| `docs/examples/functions/partition_transforms.py` | 0 |

Remediation re-run 2026-09-03 (GLM 5.3 Flash, on the remediated tree): every row above
re-measured 0 — the static half, `--require-execute`, `make check-map-sync`,
`make check-ledger-grammar`, `make check-ledgers`, both ruff legs, and the six example
scripts with `partition_transforms.py` re-run on its new assertions — plus
`python3 scripts/ledger_lifecycle.py check --base a0cd39e` → 0, and the years→months
mutation check on `partition_transforms.py` → 1 (red, as required).

Counts line, both legs identical:

`example-coverage: 913 public names (catalog=28, column=40, dataframe=150, functions=444, io=42, ml=28, session=41, ta=86, types=32, window=22); 90 covered; 821 backlog; 2 exceptions; 21 examples`

Was `69 covered; 842 backlog; 15 examples` before this batch; delta is +21 covered, -21 backlog, +6 examples (functions 11 → 17, total 15 → 21).

```yaml
COVERAGE_ATTESTATION:
  pr_unit: EX-7
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Each clause part carries its own evidence in this ledger (red-first capture, oracle table, counts line, gate exits); the remediation re-checked the four partition-value sets, the try_to_time refusal pair and the F.hours refusal sequence against the live oracle before writing them.
      artifacts: [scripts/check_example_coverage.py, docs/examples/functions/partition_transforms.py]
    - id: AT-2
      status: ATTACKED
      evidence: NULL and malformed inputs are exercised across the files (None epoch counts, malformed parse strings, NULL calendar parts, pre-1970 stamps) and the remediation keeps the out-of-set bucket key 89 whose bucket differs from its neighbours.
      artifacts: [docs/examples/functions/epoch.py, docs/examples/functions/to_date_timestamp.py, docs/examples/functions/make_calendar.py, docs/examples/functions/partition_transforms.py]
    - id: AT-3
      status: ATTACKED
      evidence: Every refusal was measured on the door that refuses it and recorded with both engines' exact texts (try_to_time UNSUPPORTED_TIME_TYPE on Spark and repark, the F.hours facade DataInvalid and the µs→ns append refusal, the five engine-gap drops); nothing was absorbed silently.
      artifacts: [task/ledgers/staging/ex-7-functions-datetime-b-ledger.md, docs/examples/functions/partition_transforms.py]
    - id: AT-4
      status: N/A
      justification: Each example builds one local[1] session and one tempdir warehouse and stops it in a finally; there is no shared mutable state.
    - id: AT-5
      status: N/A
      justification: No privileged action, no secrets, no network; the examples run on a memory catalog under a tempfile directory.
    - id: AT-6
      status: N/A
      justification: No migration or schema-drift surface; the tables live and die with each run's tempdir.
    - id: AT-7
      status: N/A
      justification: Fixed small frames (two to five rows) and one query per assertion; nothing unbounded.
    - id: AT-8
      status: ATTACKED
      evidence: Every asserted value was measured against live PySpark 4.1.2 + Iceberg 1.11.0 with driver TZ=UTC before it was written, and the remediation re-measured the touched value sets on both engines; divergences and refusals are recorded with both values, never absorbed.
      artifacts: [docs/examples/functions/partition_transforms.py, task/ledgers/staging/ex-7-functions-datetime-b-ledger.md]
    - id: AT-9
      status: N/A
      justification: Failures surface as SystemExit printing the diverging values; there is no log or metric surface.
    - id: AT-10
      status: ATTACKED
      evidence: The coverage gate reds any COVERS row without an exercising assertion, and the remediation was mutation-checked — a years→months transform mutation on the write path exits 1 on the new partition-value assertions.
      artifacts: [docs/examples/functions/partition_transforms.py, scripts/check_example_coverage.py]
  reattested: [AT-1, AT-8, AT-10]
  complete: true
```

## Pointers

- Up: [map.md](map.md)
- Slate: [../../../briefs/example-backfill.md](../../../briefs/example-backfill.md)
- Gate: [../../../scripts/check_example_coverage.py](../../../scripts/check_example_coverage.py)
- Sibling: [ex-2-functions-math-bitwise-ledger.md](ex-2-functions-math-bitwise-ledger.md)

