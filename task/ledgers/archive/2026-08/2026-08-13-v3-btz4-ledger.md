# V-3 — B-TZ-4 `CAST(TIMESTAMP AS STRING)` pin-first

Unit ledger. **Base:** `origin/main` @ `8d325d4` (V-wave freeze). **Branch:**
`grok/v3-btz4`. **Date:** 2026-08-13.

Closes the *string-cast* half of TZ-4 PR-3: Spark's `CAST(TIMESTAMP AS STRING)` is a
session-zone space-separated Arrow `string`; repark emitted `string_view` with an ISO-`T`
stored-zone instant. Registry row **B-TZ-4**. This ledger never edits
`docs/spark-sql-iceberg-parity.md` (V-5 owns it); §6 is paste-true for the landing increment.

TZ-8 (`CAST(ts AS DATE)`) is **not** tonight. Date-cast evidence captured cheaply during the
record run is a handoff only.

## 1. Oracle first — live Spark 4.1.2 (the spec)

Recorded 2026-08-13T16:36:31-04:00 under `/tmp/grok-jvm-record.lock`
(`MARKER=v3-record`, pid 4189237), after `/tmp/grok-v1-first-released` existed and the lock
was free. Driver: throwaway `/tmp/v3-record-cast-string.py` (not in-tree) plus the committed
`_record_timestamp_cast_goldens.py` recipe shape. Basis: `local[2]`,
`spark.sql.ansi.enabled=true`, `spark.sql.shuffle.partitions=2`, UI off, zulu-17,
`SPARK_LOCAL_IP=127.0.0.1`, PySpark **4.1.2**. Lock released 16:37:18-04:00 (marker-verified
`MARKER=v3-record` before `rm`).

**The recorded strings ARE the spec. No inference.**

### 2a. Shape (every LTZ/NTZ row)

| Field | Spark 4.1.2 recorded |
|---|---|
| Arrow type | `string` (`pa.string()`, not `string_view`) |
| Separator | space (`yyyy-MM-dd HH:mm:ss`), never ISO-`T` |
| Nullability | `true` for `to_timestamp` / `CAST(NULL AS TIMESTAMP)`; `false` for `make_timestamp` literals |

### 2b. Fractional seconds — trailing-zero shape (NY session, instant 12:00Z + fraction)

| Input (UTC) | **Spark 4.1.2** |
|---|---|
| `.5` / `.50` / `.500000` | `2024-06-15 08:00:00.5` |
| `.123400` | `2024-06-15 08:00:00.1234` |
| `.123456` | `2024-06-15 08:00:00.123456` |
| `.100000` | `2024-06-15 08:00:00.1` |
| `.000001` | `2024-06-15 08:00:00.000001` |
| `.123000` | `2024-06-15 08:00:00.123` |
| whole second | `2024-06-15 08:00:00` (no decimal) |

### 2c. LTZ — session zone moves the wall

| Instant | Zone | **Spark 4.1.2** |
|---|---|---|
| `2024-06-15T12:00:00Z` | America/New_York | `2024-06-15 08:00:00` |
| same | Asia/Tokyo | `2024-06-15 21:00:00` |
| same | UTC | `2024-06-15 12:00:00` |
| `1970-01-01T00:00:00Z` | America/New_York | `1969-12-31 19:00:00` |
| same | Asia/Tokyo | `1970-01-01 09:00:00` |
| same | UTC | `1970-01-01 00:00:00` |
| `1969-12-31T23:30:00Z` | America/New_York | `1969-12-31 18:30:00` |
| same | Asia/Tokyo | `1970-01-01 08:30:00` |
| `CAST(NULL AS TIMESTAMP)` | any | `NULL` |

### 2d. NTZ — zone-independent wall

`to_timestamp_ntz('2024-06-15 12:00:00')` and `TimestampNTZType` DataFrame column, all three
zones: **`2024-06-15 12:00:00`**. Fraction `.123400` → `.1234` under every zone.

### 2e. Year shape (`make_timestamp` / `to_timestamp`)

| Input | **Spark 4.1.2** |
|---|---|
| `to_timestamp('0001-01-01 00:00:00')` | `0001-01-01 00:00:00` |
| `make_timestamp(0, 1, 1, 0, 0, 0)` | `0000-01-01 00:00:00` |
| `make_timestamp(-1, 1, 1, 0, 0, 0)` | `-0001-01-01 00:00:00` |
| `make_timestamp(10000, 1, 1, 0, 0, 0)` | `+10000-01-01 00:00:00` |

`make_timestamp` is an engine gap on repark (`functions.make_timestamp` refuses).
`to_timestamp('0001-01-01 …')` is also unusable on the facade: DataFusion's ns
intermediate cannot represent year 1 (`ArrowInvalid` 1677–2262). Year shape
(0001 / 0000 / −0001 / +10000) is a kernel pin
(`spark_timestamp_string_year_shape_matches_iso_local_date`).

### 2f. Doors

| Door | Zone | **Spark 4.1.2** |
|---|---|---|
| `F.col('ts').cast('string')` LTZ aware 12:00Z | NY | `2024-06-15 08:00:00` |
| `F.expr('CAST(to_timestamp(…Z) AS STRING)')` | NY | `2024-06-15 08:00:00` |
| `F.col('ts').cast('string')` `TimestampNTZType` 12:00 | NY | `2024-06-15 12:00:00` |
| `createDataFrame` naive datetime + `TimestampType` | NY | `2024-06-15 12:00:00` (localize then render = identity) |

### 2g. TZ-8 date-cast handoff (no code)

`CAST(to_timestamp('2024-06-15T03:00:00Z') AS DATE)`:

| Zone | Spark | Note |
|---|---|---|
| America/New_York | `2024-06-14` | session-zone date (EDT) |
| Asia/Tokyo | `2024-06-15` | |
| UTC | `2024-06-15` | |

Repark still reads the stored/UTC date (registry TZ-8). Not tonight.

## 3. The fix

Two files of engine change, both in `repark-functions` (crate-DAG tier 3). No `lib.rs`
`analyzer_rules()` append — the rewrite lives inside existing `SparkExprSemantics`.

**`timestamp_cast.rs`.** Third embedded UDF `__repark_timestamp_to_string__` → `Utf8`,
`Volatility::Volatile` (const-eval would fold a session-zone render against a default UTC
carrier). LTZ (`Timestamp(_, Some(_))`) → session-zone wall via the existing
`SessionTimeZoneConfig` carrier. NTZ (`Timestamp(_, None)`) → stored wall (ticks as UTC
digits). Format = recorded ISO_LOCAL_DATE + space + `HH:mm:ss` + fraction with trailing
zeros stripped. Ticks floor to microseconds first (Spark's resolution). Out-of-chrono →
NULL, no panic.

**`analyzer.rs`.** `rewrite_timestamp_casts` dispatches: numeric arm **untouched**
(`rewrite_timestamp_to_numeric_cast`); string arm
`rewrite_timestamp_to_string_cast` replaces `CAST(ts AS Utf8/Utf8View/LargeUtf8)` with
the UDF. DATE / TIMESTAMP targets stay declined. A1 pin flip:
`non_numeric_timestamp_casts_are_untouched` is DATE-only; STRING is
`timestamp_cast_to_string_is_spark_utf8` (`Utf8` + one UDF + `"2024-06-15 12:00:00"`).

## 4. Pins

| Pin | What it holds |
|---|---|
| `timestamp_cast::spark_timestamp_string_trims_trailing_fraction_zeros` | recorded fraction shape |
| `timestamp_cast::spark_timestamp_string_year_shape_matches_iso_local_date` | 0001 / 0000 / −0001 / +10000 |
| `timestamp_cast::ltz_renders_in_the_session_zone_and_ntz_does_not` | NY 12:00Z → 08:00 vs NTZ 12:00 |
| `analyzer::timestamp_cast_to_string_is_spark_utf8` | type Utf8 + rewrite + value |
| `analyzer::non_numeric_timestamp_casts_are_untouched` | DATE still Date32, no string UDF |
| `test_timestamp_cast_parity.py` (12 new equality rows) | facade SQL / DataFrame / expr |
| `test_the_class_is_covered_per_entry_point_and_per_edge` | STRING + NTZ + three zones |

## 5. A5 / A6 overflow

| Grant | Disposition |
|---|---|
| **A5** `timestamp_cast_ansi_door.rs` | **not taken.** Existing pins are `CAST(ts AS BIGINT)` only; they stay green. Native G11 session CLOSED (no native-ANSI rewrite). |
| **A5 overflow** `crates/repark-spark/tests/timestamp_cast_seconds.rs` `casts_outside_the_class_are_untouched` | **taken — STRING cell only.** Spark-extended session, Spark door. Forced red: expected `Utf8View`, engine now emits Spark `Utf8`. DATE / TIMESTAMP cells unchanged. Same flip as A1's analyzer `:849` STRING cell. |
| **A6** `test_session_timezone_parity.py` `tz_aware_to_naive_round_trip` | **note-only.** Value still `2024-06-15 12:00 UTC` (render 08:00 NY + PR-2 localize = identity). Note updated so it no longer says "B-TZ-4 is a later PR". TZ-6 / TZ-7 rows not reopened. |
| **A6** `_live_parity.py` SCENARIOS | **not taken.** No existing scenario is `CAST(ts AS STRING)`. Count stays **42**. DISCLOSURES / LIFECYCLE_* / `test_parity_live.py` CLOSED. No new live scenario. |

## 6. Registry handoff (paste-true; V-5 owns the file)

- **B-TZ-4** — `CAST(TIMESTAMP AS STRING)` is Spark's session-zone space-separated Arrow `string`.
  - **repark** — `Utf8` (`string`), space-separated wall in `spark.sql.session.timeZone` for
    LTZ; stored wall for NTZ; trailing-zero fractions stripped (`.123400` → `.1234`); year
    −1 is `-0001`, year 10000 is `+10000`.
  - **Apache Spark** — same strings, Arrow `string`. *(oracle: recorded 2026-08-13, PySpark
    4.1.2, zulu-17, `local[2]`, ANSI on.)*
  - **Pin** — `python/repark/tests/test_timestamp_cast_parity.py::test_timestamp_cast_row_matches_spark_or_still_diverges[timestamp_to_string_ltz_under_new_york]`
    (and the 11 sibling STRING rows in `ROWS`); Rust
    `crates/repark-functions/src/timestamp_cast.rs::spark_timestamp_string_trims_trailing_fraction_zeros`
    + `ltz_renders_in_the_session_zone_and_ntz_does_not`; analyzer
    `timestamp_cast_to_string_is_spark_utf8`.
  - **Rationale** — class B-TZ-4 (string-cast render). TZ-8 date-cast stays disclosed.

B-TZ-4 progress text for the TZ-4 family row: **string-cast landed (V-3 / this PR).**

## 7. Deviations

- `make_timestamp` year 0 / −1 / 10000 not on the facade (engine gap R-FN-BATCH3). Kernel
  pins hold the recorded year shape.
- `to_timestamp_ntz` is not a repark SQL function; the NTZ facade row uses
  `createDataFrame` + `TimestampNTZType` (the same spelling TZ-4 PR-2 already pins).
- TZ-8 date-cast recorded, not fixed.

## 8. ACC / C4 (sequential hat-switch)

Risk tier: **standard**. `claims_critic=true`.

**Actor.** Oracle recorded first. Kernel + analyzer rewrite + 12 facade equality rows.
`make verify` 0; `make preflight` 0 (2992 facade passed). ANSI door STRING value not
newly pinned (same `SparkExprSemantics` rule; A5 did not force an ansi_door red).

**Critic-1 (Quality).** Context break executed; attacking artifacts, not memory.
Attacked spec, LTZ/NTZ branch, fraction trim vs recorded table, year-shape kernel,
mutation-proof facade rows, DATE-untouched pin, A5 spark-door flip, map.md lockstep.
Null reports: failure/partial-failure N/A (pure expression rewrite); concurrency N/A.
Residual S3: ANSI-door STRING *value* not a new pin (type+value covered on Spark
door + facade; rewrite is session-scoped). Verdict: **CLEAN**.

**Critic-2 (Security/Safety).** Context break executed. Attacked panic surface
(out-of-chrono → NULL), no secrets, no `unsafe`, session zone from validated carrier,
no AWS. Atomicity N/A. Verdict: **CLEAN**.

**Critic-4 (Claims).** Context break executed. Inventoried 12-vs-13 counts (year-0001
dropped after ns-range red), §6 template, map.md both-adds, lock events vs files.
CL-IDENTITY checked at commit time (`%ae`). Verdict: **CLEAN** pending commit identity.

## 9. Lock events

| When | Event |
|---|---|
| 2026-08-13T16:36:23-04:00 | observed `/tmp/grok-v1-first-released` |
| 2026-08-13T16:36:31-04:00 | acquired `/tmp/grok-jvm-record.lock` `MARKER=v3-record` pid 4189237 (lock was absent) |
| 2026-08-13T16:36:31–16:37:18 | oracle probe (69 JSONL lines, 0 errors) |
| 2026-08-13T16:37:18-04:00 | released after marker-verify `MARKER=v3-record` |
| 2026-08-13T17:08:13 | acquired `MARKER=v3-parity-live` (pid 406820); record driver failed (`pyspark` dropped by preflight `uv sync`); leftover own marker removed |
| 2026-08-13T17:08:35 | re-acquired `MARKER=v3-parity-live` pid 409367 |
| 2026-08-13T17:08:35+ | `_record_timestamp_cast_goldens.py`: **31 rows, 0 mismatches** (19 TZ-5 + 12 B-TZ-4) |
| 2026-08-13T17:08–17:12 | `make parity-live PARITY_LIVE_JAVA_HOME=/usr/lib/jvm/zulu-17-amd64`: 3085 passed, 3 skipped, **1 failed** = Y-7 `test_udf_with_collated_string_types` (expected base red). No SCENARIO golden flip. Count stays 42. |
| after parity-live | released `v3-parity-live` (marker-verified) |
