# R-4 — TZ-8 `CAST(ts AS DATE)` / `to_date` read the session zone

Unit ledger. **Base:** `origin/main` @ `fddf1bc4840ade68274ca5c55993dda0fb182a61` (#94 freeze).
**Branch:** `grok/r4-tz8`. **Worktree:** `/tmp/grok-r4`. **Date:** 2026-08-14.

Closes the date-cast half of registry TZ-8: Spark's `CAST(ts AS DATE)` / `to_date(ts)` take an
LTZ timestamp's date in `spark.sql.session.timeZone`; NTZ stays the stored wall. Arrow /
DataFusion `CAST` and DataFusion `to_date` both read the array's UTC annotation, so a New
York session answered a day late. This ledger never edits
`docs/spark-sql-iceberg-parity.md` (R-7 deferred); §6 is paste-true.

`datediff` was chartered CLOSED as the trio's third leg. It **converged as a consequence**
of the CAST rewrite (`datafusion-spark` `SparkDateDiff::simplify` lowers to Date32
subtraction of `CAST(ts AS DATE)`). `last_day` / `date_add` over TIMESTAMP stay residual.

## 0. Altitude

- **CAST(ts AS DATE)** — `SparkExprSemantics` CAST dispatcher (B-TZ-4 mold). New DATE arm
  after numeric (TZ-5, byte-stable) and string (B-TZ-4, declined). Replaces the cast with
  embedded `__repark_timestamp_to_date__`. No new `AnalyzerRule`. `analyzer_rules()` order
  untouched.
- **to_date** — registered overwrite of DataFusion's `to_date` (comments that said
  datafusion-spark were stale). TIMESTAMP args share `datetime::invoke_local_dates`;
  Date/string/numeric keep arrow Date32. One argument (Java-pattern `format=` already
  refused at the facade).
- **Kernel** — existing `invoke_local_dates`: LTZ (`Timestamp(_, Some(_))`) → session-zone
  calendar; NTZ / DATE / string → stored wall / identity. `coerce_to_date32` unchanged
  (`add_months` / `trunc` already correct).
- **CLOSED:** `timestamp_cast.rs` string shape; `instant_ts.rs`; `types.py` /
  interchange; `_live_parity.py`; `/0` and STRING CAST arms.

## 1. Oracle first — live Spark 4.1.2 (the spec)

Recorded under `/tmp/grok-jvm-record.lock` (`MARKER=r4-record`) after FIFO (R-1 → R-2 →
R-3 → R-4). Driver: throwaway `/tmp/r4-record-tz8.py` (not in-tree). Basis: `local[2]`,
`spark.sql.ansi.enabled=true`, `spark.sql.shuffle.partitions=2`, UI off, zulu-17,
`SPARK_LOCAL_IP=127.0.0.1`, PySpark **4.1.2**.

**The recorded dates ARE the spec. No inference.** V-3/V-4 already held the headline
cells; this unit re-records the full midnight-crossing / NTZ / epoch matrix.

### 1a. LTZ — session zone moves the date

| Instant | Zone | CAST AS DATE | to_date |
|---|---|---|---|
| `2024-06-15T03:00:00Z` | America/New_York | `2024-06-14` | `2024-06-14` |
| same | UTC | `2024-06-15` | `2024-06-15` |
| same | Asia/Tokyo | `2024-06-15` | `2024-06-15` |
| `2023-12-31T16:30:00Z` | Asia/Tokyo | `2024-01-01` | `2024-01-01` |
| `2024-01-01T04:30:00Z` | America/New_York | `2023-12-31` | `2023-12-31` |
| `1970-01-01T00:00:00Z` | America/New_York | `1969-12-31` | `1969-12-31` |
| same | UTC | `1970-01-01` | `1970-01-01` |
| `CAST(NULL AS TIMESTAMP)` | any | `NULL` | `NULL` |

### 1b. NTZ — zone-independent wall

`to_timestamp_ntz('2024-06-15 12:00:00')` / `TimestampNTZType` 12:00 under UTC, NY, Tokyo:
**`2024-06-15`**.

### 1c. datediff rides CAST

`datediff(to_timestamp('2024-06-15T03:00:00Z'), DATE '2024-06-01')` under NY: **13**
(Spark). Mechanism: `SparkDateDiff::simplify` → `CAST(end AS Date32) - CAST(start AS
Date32)`. Not a dedicated datediff kernel.

### 1d. Still residual

`last_day(ts)` / `date_add(ts, 1)` over a TIMESTAMP still fail to plan (Spark answers
`2024-05-31` / `2024-06-01` for `2024-06-15T03:00:00Z` under NY).

## 2. The fix

Three files of engine change, all in `repark-functions` (crate-DAG tier 3). No
`analyzer_rules()` append.

**`datetime.rs`.** `invoke_local_dates` is `pub(crate)` — the shared kernel. Comment
update only otherwise (`coerce_to_date32` contract unchanged).

**`timestamp_cast.rs`.** Fourth embedded UDF `__repark_timestamp_to_date__` → `Date32`,
`Volatility::Volatile`. Registered `to_date` overwrite (same kernel for TIMESTAMP;
arrow Date32 for everything else). Kernel pin
`ltz_date_is_session_zone_and_ntz_is_stored_wall`.

**`analyzer.rs`.** `rewrite_timestamp_casts` dispatches DATE after string declines.
`rewrite_timestamp_to_date_cast` replaces `CAST(ts AS Date32)` with the UDF. Numeric and
STRING arms untouched. A1 pin flip: `non_numeric_timestamp_casts_are_untouched` is now
`timestamp_to_timestamp_cast_is_untouched`; DATE is
`timestamp_cast_to_date_is_spark_date32`.

**`lib.rs`.** `register_all` overwrites DataFusion `to_date` after the date shims.

**`expr_fn.rs` + `repark-python` `column.rs`.** `F.to_date` embeds the same UDF (call_scalar
used DataFusion's built-in; SQL `to_date` used the overwrite). `to_date` is always
nullable (Spark).

## 3. Pins

| Pin | What it holds |
|---|---|
| `timestamp_cast::ltz_date_is_session_zone_and_ntz_is_stored_wall` | NY 03:00Z → 14th vs NTZ 15th |
| `analyzer::timestamp_cast_to_date_is_spark_date32` | type Date32 + rewrite + idempotent |
| `analyzer::timestamp_to_timestamp_cast_is_untouched` | identity TIMESTAMP fence |
| `session_timezone::timestamp_to_date_paths_read_the_session_zone` | Spark-door CAST + to_date + datediff |
| `session_timezone::last_day_and_date_add_over_a_timestamp_still_refuse` | named residual |
| `test_session_timezone_parity.py` (5 new equality rows + DataFrame-API pin) | facade SQL / NTZ / epoch / Tokyo |
| `test_partition_value_audit.py` `[tz8_cast_ts_as_date_…]` + `[tz8_to_date_…]` | V-4 date-key rows flipped to equality |

## 4. Corpus flips

- `test_session_timezone_parity.py`: 5 new TZ-8 equality rows (NY CAST+to_date+datediff,
  UTC control, Tokyo forward, NTZ, epoch). G1 19→23; G16 10→11; equality 28→33.
  Standalone `test_dataframe_api_timestamp_to_date_reads_the_session_zone`.
- `test_partition_value_audit.py`: both TZ-8 date-key rows `repark_data` / `repark_meta`
  → `None` (equality onto the recorded Spark `2023-12-31` / `2024-06-15` slots).
- Spark-door: old disclosure `timestamp_to_date_paths_outside_this_crate_still_read_the_stored_zone`
  renamed and flipped. `datediff` 14→13. Residual test renamed to last_day/date_add.

## 5. What stayed closed

- Registry + STATUS (R-7 deferred; §6 only).
- `_live_parity.py` / `test_parity_live.py`.
- `timestamp_cast.rs` string shape; `instant_ts.rs`; `types.py` / interchange.
- `/0` path and STRING CAST arm.
- `analyzer_rules()` order.
- Lockfiles.

## 6. Registry handoff (paste-true; R-7 owns the file)

- **TZ-8** — `CAST(TIMESTAMP AS DATE)` / `to_date` / `datediff` take the date in
  `spark.sql.session.timeZone` for LTZ; NTZ stays the stored wall.
  - **repark** — `CAST(to_timestamp('2024-06-15T03:00:00Z') AS DATE)` and
    `to_date(to_timestamp('2024-06-15T03:00:00Z'))` answer `2024-06-14` under
    `America/New_York` and `2024-06-15` under UTC; NTZ 12:00 is `2024-06-15` in every
    zone. `datediff(ts, DATE '2024-06-01')` is 13 under NY (rides CAST). As an identity
    partition key under NY, `CAST(ts AS DATE)` / `to_date(ts)` write Spark's
    `2023-12-31` for `2024-01-01T04:30:00Z`.
  - **Apache Spark** — same dates. *(oracle: recorded 2026-08-14, PySpark 4.1.2,
    zulu-17, `local[2]`, ANSI on.)*
  - **Pin** —
    `python/repark/tests/test_session_timezone_parity.py::test_session_timezone_row_matches_spark_or_still_diverges[timestamp_to_date_cast_and_to_date_under_new_york_session]`
    + `python/repark/tests/test_partition_value_audit.py::test_partition_value_row[tz8_cast_ts_as_date_identity_new_york_ctas]`
    + `…[tz8_to_date_ts_identity_new_york_ctas]`. Rust
    `crates/repark-spark/tests/session_timezone.rs::timestamp_to_date_paths_read_the_session_zone`.
    One row, two citations (SQL date + partition date).
  - **Rationale** — class TZ-8 (date-cast). `last_day` / `date_add` over TIMESTAMP stay
    residual (fail to plan).

TZ-8 progress text: **CAST / to_date landed (R-4 / this PR); datediff converged via CAST;
last_day/date_add remain.**

## 7. Deviations

- `datediff` was chartered as a named residual. It converged because
  `SparkDateDiff::simplify` is CAST-to-Date32 subtraction. Not a dedicated datediff
  kernel; named in §6 as closed-via-CAST.
- `to_date(str, java_format)` stays refused (Chrono/Java gap; facade `format=`).
- Live-record transcript pasted after the JVM lock is acquired (FIFO after R-3).

## 8. ACC / C4 (sequential hat-switch)

Risk tier: **standard**. `claims_critic=true`. max_cycles=2, floor S1.

**Actor.** Oracle recorded first (20 cells, PySpark 4.1.2). Kernel
`invoke_local_dates` + CAST DATE arm + registered `to_date` + `expr_fn`/`F.to_date`
embed + facade equality + partition-date flip. `datediff` measured as CAST-ride
(13). `last_day`/`date_add` still refuse. `make verify` 0; `make preflight` 0
(3059 facade passed).

**Critic-1 (Quality).** Context break executed; attacking artifacts, not memory.
Attacked: spec vs recorded table (NY 03:00Z → 14th, UTC 15th, Tokyo 15th, Tokyo
forward 2024-01-01, epoch NY 1969-12-31, NTZ 15th all zones); LTZ/NTZ branch
(`is_instant`); CAST vs `to_date` same kernel; `F.to_date` was DF built-in
(fixed via `expr_fn`); `to_date` always-nullable (live `date_math` golden);
mutation-proof facade rows; DATE rewrite idempotent; `/0`+STRING arms
untouched; `analyzer_rules()` order untouched; map.md lockstep; file-size
ceilings. Residual S3: `last_day`/`date_add` over TIMESTAMP still refuse
(recorded Spark last_day=2024-06-30, date_add=2024-06-15 for this fixture).
Verdict: **CLEAN**.

**Critic-2 (Security/Safety).** Context break executed. Attacked panic surface
(out-of-chrono → NULL, no unwrap in prod), no secrets, no `unsafe`, session
zone from validated carrier, Volatile UDF (no UTC const-fold), no AWS.
Atomicity N/A. Verdict: **CLEAN**.

**Critic-4 (Claims).** Context break executed. Inventoried: CAST/to_date FIXED;
datediff CONVERGED via CAST (not a dedicated kernel); last_day/date_add
NAMED residual; registry not edited; `_live_parity.py` not edited; lock
events vs files; CL-IDENTITY at commit. Verdict: **CLEAN** pending commit
identity.

## 9. Lock events

| When | Event |
|---|---|
| 2026-08-13T21:21:58-04:00 | observed R-1 holding `/tmp/grok-jvm-record.lock` (`MARKER=r1-record` pid 2727182, later 2788473) |
| 2026-08-13T21:45:41-04:00 | R-1 pid 2788473 dead; lock age ~16 min — did **not** stale-rm (<30 min) |
| 2026-08-13T21:53:22-04:00 | lock free; no r1/r2/r3 marker; no local SparkSubmit |
| 2026-08-13T21:53:31-04:00 | acquired `MARKER=r4-record` pid 3079659 lane=r4-tz8 (noclobber) |
| 2026-08-13T21:53:31–21:55+ | oracle probe `/tmp/r4-record-tz8.py`, PySpark 4.1.2, 20 cells, 0 errors |
| 2026-08-13T21:55+ | released after marker-verify `MARKER=r4-record` (`rm /tmp/grok-jvm-record.lock`) |

No other `rm` of the lock.

## 10. Gates

| Gate | EC | Notes |
|---|---|---|
| `make verify` | 0 | cd-fused `/tmp/grok-r4` |
| `make preflight` | 0 | cd-fused `/tmp/grok-r4`; facade 3059 passed, 71 skipped |
