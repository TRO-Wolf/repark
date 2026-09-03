# Unit ledger — EX-7 · v0.7 example backfill, `F.*` unix-time, timestamp construction and partition transforms

**Retires:** this ledger moves to `../completed/` in the unit's last commit (the orchestrator's departure move). This file closes when EX-7 merges, or when the owner closes the slate row.

**Unit:** EX-7 · **Date:** 2026-09-03 · **Model:** muse-spark-1.2-contributor (continuation of glm-5.3-flash) · **Branch:** `feat/ex-7-functions-datetime-b` · **Base:** `a0fe83a`
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

Every file lists `F.col` (and `epoch.py` etc. `F.lit` where used) in `COVERS` because they genuinely use them; those names are already covered, so they do not move the ratchet.

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

Measured on this tree against live PySpark 4.1.2 + Iceberg 1.11.0 at `/tmp/oc-ex7/.venv/bin/python` with `JAVA_HOME=/usr/lib/jvm/zulu-17-amd64` and `PYTHONPATH=/tmp/oc-ex7/python/repark/tests` using `_live_parity.build_spark_iceberg_engine(Path(tmpdir)).session` — one throwaway script under `/tmp/oc-ex7-oracle/` (not in the repo) that printed per name the Spark value and the repark value for the same inputs. Every kept row was equal by repr; dropped rows record both values.

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
| F.try_to_time | `NULL or TIME value for valid input` | `AnalysisException: [UNSUPPORTED_TIME_TYPE] The data type TIME is not supported` | dropped — TIME type not supported | backlog |
| F.make_date | `[date(2020,1,2), date(2024,2,29), date(1999,12,31), None]` for (y,m,d) parts including NULL | same | kept | make_calendar.py |
| F.make_timestamp | `datetime(2020,1,2,3,4,6)` for y=2020,m=1,d=2,h=3,mi=4,s=6 | `UnsupportedOperationException: functions.make_timestamp is not supported yet` | dropped — engine gap | backlog |
| F.make_interval | `interval months/days/nanos for (years,months,weeks,days,hours,mins,secs)` | `PySparkNotImplementedError: Python conversion for calendar interval (make_interval / CalendarIntervalType)` | dropped — interval type conversion gap | backlog |
| F.make_dt_interval | `[timedelta(days=1,seconds=7384,micros=500000), timedelta(0), timedelta(days=-1,micros=250000), None]` for (d,h,mi,s) parts | same | kept | make_calendar.py |
| F.from_utc_timestamp | `[datetime(2020,1,1,7,0), datetime(2020,7,1,8,0), None]` for ts in UTC rendered to America/New_York | same | kept | utc_offsets.py |
| F.to_utc_timestamp | `[datetime(2020,1,1,17,0), datetime(2020,7,1,16,0), None]` for same ts read as New_York | same | kept | utc_offsets.py |
| F.current_timezone | `["UTC","UTC","UTC"]` for session | same | kept | utc_offsets.py |
| F.days | rows round-trip through `partitionedBy(F.days(F.col("event_date")))` | same | kept | partition_transforms.py |
| F.hours | partition transform `hours(col)` applied via `partitionedBy` | repark `F.hours` is partition-transform only; scalar `select(F.hours(col))` is `PARTITION_TRANSFORM_EXPRESSION_NOT_IN_PARTITIONED_BY` and the example family teaches the partition-transform form only for years/months/days/bucket | dropped — not a scalar example in this batch | backlog |
| F.months | rows round-trip through `partitionedBy(F.months(F.col("event_date")))` | same | kept | partition_transforms.py |
| F.years | rows round-trip through `partitionedBy(F.years(F.col("event_date")))` | same | kept | partition_transforms.py |
| F.bucket | rows round-trip through `partitionedBy(F.bucket(4, F.col("id")))` | same | kept | partition_transforms.py |

Wall-clock: start 2026-09-03T00:00:00Z, end 2026-09-03T00:20:00Z (carried from prior session; continuation wall-clock measured below).

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

Counts line, both legs identical:

`example-coverage: 913 public names (catalog=28, column=40, dataframe=150, functions=444, io=42, ml=28, session=41, ta=86, types=32, window=22); 90 covered; 821 backlog; 2 exceptions; 21 examples`

Was `69 covered; 842 backlog; 15 examples` before this batch; delta is +21 covered, -21 backlog, +6 examples (functions 11 → 17, total 15 → 21).

## Pointers

- Up: [map.md](map.md)
- Slate: [../../../briefs/example-backfill.md](../../../briefs/example-backfill.md)
- Gate: [../../../scripts/check_example_coverage.py](../../../scripts/check_example_coverage.py)
- Sibling: [ex-2-functions-math-bitwise-ledger.md](ex-2-functions-math-bitwise-ledger.md)

