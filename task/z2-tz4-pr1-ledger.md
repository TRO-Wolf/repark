# Z-2 TZ-4 PR-1 ledger — µs+UTC instant producers + Iceberg `timestamptz`

**Date:** 2026-08-13 · **Lane:** Z-2 · **Branch:** `grok/z2-tz4-pr1` · **Base:**
`9b2dce3c73af402e8705923135d7de014da5501f`
**Charter:** `BRIEF-z2-tz4-pr1.md` + `TZ4-DESIGN.md` §5.1 + Y-8 addendum 2026-08-13
(Q1=A, Q2=A).

---

## 0. A7 first oracle — live Spark+Iceberg CREATE

**Lock:** acquired `/tmp/grok-jvm-record.lock` `MARKER=z2-tz4-probe` pid=3708807
time=2026-08-13T08:01:43-04:00. First acquire process exited after venv check (dead pid,
marker still ours; lock **not** stale-rm'd — age < 30 min). Probe 2 used the same lock.
**Released** 2026-08-13T08:03:26-04:00 after CREATE+CTAS smoke. Sentinel
`/tmp/grok-z2-probe-released` written. No `/tmp/grok-z2-halted`. **No lock leftover.**

**Stale-rm events:** none.

**Environment:** PySpark 4.1.2, `JAVA_HOME=/usr/lib/jvm/zulu-17-amd64`,
`SPARK_LOCAL_IP=127.0.0.1`, GAV `org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0`,
Hadoop catalog, warehouse `/tmp/z2-tz4-probe-wh-d4eishf3`.
`spark.sql.session.timeZone=America/New_York`. `spark.sql.timestampType=TIMESTAMP_LTZ`.
Full transcript: `/tmp/z2-tz4-probe.log`.

| Probe | Spark type | Arrow | Iceberg metadata `type` |
|---|---|---|---|
| `CREATE TABLE (ts TIMESTAMP) USING iceberg` | `TimestampType()` | `timestamp[us, tz=UTC]` | **`timestamptz`** |
| `CREATE TABLE (ts TIMESTAMP_NTZ)` | `TimestampNTZType()` | `timestamp[us]` | `timestamp` |
| CTAS `current_timestamp()` | `TimestampType()` | `timestamp[us, tz=UTC]` | **`timestamptz`** |
| CTAS `to_timestamp('…Z')` | `TimestampType()` | `timestamp[us, tz=UTC]` | **`timestamptz`** |
| identity-partitioned CTAS on `ts` | `timestamp` (partition col) | — | **`timestamptz`** + identity spec |

**Ruling:** Q2 lean CONFIRMED. Spark's Iceberg type **is** `timestamptz`. No HALT. Mapping flip
authorized.

Insert of `TIMESTAMP '2024-06-15 12:00:00'` under NY session stored `2024-06-15 16:00 UTC`
(session-zone localization — TZ-7 / PR-2, not this PR).

---

## 0b. Blast-radius sweep (before representation commit)

| Family | Expected | Disposition |
|---|---|---|
| tz corpus equality rows (ints/dates/strings) | no value flip | CONVERGED stay |
| TZ-4 type disclosures (`to_timestamp` Z, `date_trunc` return, DF-API `date_trunc`) | type → `us+UTC` | **flip to equality** |
| `tz_aware_to_naive_round_trip` | type wrap of leftover ns CAST | **flip to equality** (value already matched; B-TZ-4 string shape not asserted) |
| TZ-6 / TZ-7 (3+1) | stay disclosures | **stay** (PR-2) |
| `current_timestamp` type pin | ns-naive → us+UTC | **flip to equality** |
| timestamp-cast `[bigint_to_timestamp_reads_seconds]` | type half | **flip to equality** |
| live 13 tz SCENARIOS | ints; count 42 | expect still-green; **no `_live_parity.py` edit unless red** |
| interchange / boundary | loose type pins | **re-read only** (A5) |
| `test_a3_cast_vocab` | `TimestampType()` stays naive us | **no edit** (`types.py` CLOSED) |
| dogfood ns residuals + SQL/expr CTAS reject | flip to success | **flip** |
| `cast(TimestampType())` strips tz | stay | **stay** (PR-2) |
| window ns pins | A5 | **named morning** |
| `analyzer.rs` | Z-3 file-disjoint | **untouched**; CAST wrap is a new rule in `instant_ts.rs` |
| ANSI `repark-sql/src/create_table.rs` | A11 probe after Spark-door flip | **no grant unless second mapping writes `timestamp`** |

---

## 1. Decisions

1. **Representation (Q1=A).** Instant-typed producers emit `Timestamp(µs, UTC)`:
   - SQL `now()` / `current_timestamp()` — new UDFs that simplify to a µs+UTC literal
     (same statement-stable contract as DataFusion `NowFunc`; copy of `F.current_timestamp`).
   - `to_timestamp` — wrap DataFusion's `ToTimestampFunc` (timezone unset, Q9) then
     type-only CAST to µs+UTC. One return type: zoneless inputs keep UTC ticks (TZ-7).
   - `date_trunc` return type + output array timezone → UTC.
   - Leftover `Timestamp(ns, _)` expressions (folded `CAST(<int> AS TIMESTAMP)`,
     `TIMESTAMP` literals) get a type-only wrap. **Do not retarget** `CAST(int AS TIMESTAMP)`
     onto µs — DataFusion would then read the integer as microseconds, not seconds.
2. **Write mapping (Q2=A).** Spark-door `TIMESTAMP` (no TZ) → `PrimitiveType::Timestamptz`.
   `TIMESTAMP_NTZ` stays `Timestamp`.
3. **CTAS type smoke.** `current_timestamp` / `to_timestamp(Z)` / identity-partitioned CTAS
   store `timestamptz`.
4. **TZ-6 / TZ-7 stay disclosed.** No zoneless localization. D-B5 extractors unchanged
   (`session_time_zone.rs` CLOSED). `types.py` / `column.rs` CLOSED.
5. **Registry.** §6 handoff only: TZ-4 progress. TZ-6/TZ-7 retire **only in PR-2**.
   B-TZ-4 untouched until PR-3.

---

## 2. Files

| Path | Why |
|---|---|
| `crates/repark-functions/src/instant_ts.rs` | **new** — `now` / `current_timestamp` / `to_timestamp` + CAST wrap rule |
| `crates/repark-functions/src/lib.rs` | register + analyzer_rules |
| `crates/repark-functions/src/datetime.rs` | `date_trunc` return µs+UTC |
| `crates/repark-spark/src/create_table.rs` | TIMESTAMP → timestamptz |
| `crates/repark-spark/src/tests/create_table.rs` | pin + CTAS type smoke |
| `crates/repark-spark/tests/session_timezone.rs` | date_trunc type pins |
| `crates/repark-spark/tests/timestamp_cast_seconds.rs` | CAST AS TIMESTAMP type pin |
| `python/repark/tests/test_session_timezone_parity.py` | 7 type flips + shape pin |
| `python/repark/tests/test_timestamp_cast_parity.py` | reverse-cast type flip |
| `python/repark/tests/test_dogfood_gaps.py` | ns residual / CTAS reject flips |
| maps + this ledger | lockstep |

---

## 3. Gates

| Gate | Exit | Notes |
|---|---|---|
| `make verify` | **0** | `/tmp/z2-verify.log` |
| `make preflight` | **0** | facade **2901 passed**, 71 skipped; `/tmp/z2-preflight.log` |
| `make parity-live` | **2** | 2994 passed, 1 failed: Apache smoke `test_udf_with_collated_string_types` classified FAIL-MISSING because Y-7 (#71) refuses `string collate fr` on UDF `returnType`. **Not TZ-4** — `functions.py` / smoke_suite untouched this lane. Pin has been PASS since phase-3 PR-4; Y-7 landed on the freeze SHA without flipping it. Named residual. `/tmp/z2-parity-live.log` |

---

## 4. Named morning deferrals (A5)

- Window-ns type pins (A5 union after this PR lands).
- Interchange/boundary: re-read only. Pandas/polars timestamp cells use wall-clock /
  accept ns\|us and tz None\|UTC (same loose pin as Arrow). No new type-parity claim.
- `make parity-live` Apache smoke `test_udf_with_collated_string_types` — pre-existing
  Y-7 (#71) vs phase-3 PASS pin. Not this unit.
- `types.py` `TimestampType()` / facade `cast("timestamp")` still naive us
  (`test_a3_cast_vocab`, `test_current_timestamp_cast_timestamptype_strips_tz`).
- TZ-6 / TZ-7 / TZ-8 / B-TZ-4 / TZ-2/TZ-3/B-TZ-5.
- `spark.sql.timestampType`, `conf.set` timezone, DISCLOSURES/mirrors, registry file.
- Fork commits: none.

---

## 5. ANSI-door probe (A11)

Native `AnsiDialect` session (no `SparkExtension`). `CREATE TABLE ice.sales.ts_ddl (ts timestamp)`
→ `DataInvalid: Invalid schema for v2: Invalid type for ts: timestamp_ns is not supported until v3`.

Did **not** write Iceberg `timestamp` (naive). Grant to `repark-sql/src/create_table.rs` **not
taken**. Named morning: native ANSI column-def TIMESTAMP still `CAST(NULL AS TIMESTAMP)` → ns
→ `timestamp_ns`. Pin:
`crates/repark-sql/tests/session_wiring.rs::ansi_column_def_timestamp_still_rejects_ns_on_v2`.

Spark-door (facade) CREATE/CTAS is `timestamptz`. Product type is door-neutral; the ANSI
column-def path is a second mapping that still emits ns.

---

## 6. Registry handoff (paste-true; do **not** land here)

Z-5 owns `docs/spark-sql-iceberg-parity.md`. Ready-to-paste **progress** for TZ-4 only.
**TZ-6 / TZ-7 retire only in PR-2 — say so. B-TZ-4 untouched until PR-3.**

- **TZ-4 (progress, not retired).** Instant-typed producers (SQL `current_timestamp`/`now`,
  `to_timestamp`, `date_trunc` return, `CAST(<integer> AS TIMESTAMP)` type) now export
  `timestamp[us, tz=UTC]`. Spark-door DDL `TIMESTAMP` maps to Iceberg `timestamptz`
  (live Spark 4.1.2 CREATE probe 2026-08-13). Residues: zoneless input (TZ-7), NTZ
  distinction (TZ-6), `TimestampType()` Python mapping, B-TZ-4 string-cast.
  - **Pin:** `python/repark/tests/test_session_timezone_parity.py::test_current_timestamp_type_and_zone_disclosure`
  - **Pin:** `python/repark/tests/test_session_timezone_parity.py::test_session_timezone_row_matches_spark_or_still_diverges[to_timestamp_of_zone_suffixed_string]`
  - **Pin:** `crates/repark-spark/src/tests/create_table.rs::column_def_temporary_refuse_testing_create_ref_and_types`
  - **Pin:** `crates/repark-spark/src/tests/create_table.rs::ctas_of_instant_producers_stores_timestamptz`

---

## 7. Lock events (A10)

| Event | Detail |
|---|---|
| acquire | `MARKER=z2-tz4-probe` pid=3708807 time=2026-08-13T08:01:43-04:00 |
| stale-rm | none |
| probe-1 | died on invalid `spark.sql.timestampType` default `'<unset>'`; lock kept |
| probe-2 | CREATE+CTAS+NTZ+partition smoke; `PROBE_DONE` |
| release | 2026-08-13T08:03:26-04:00; rm lock (marker-verified) |
| sentinel | `/tmp/grok-z2-probe-released` written; no `/tmp/grok-z2-halted` |
| leftover | **no** |

Later corpus re-records take the lock FIFO, RELEASE-ON-EXIT.

---

## 8. Deviations

- `CAST(int AS TIMESTAMP)` type wrap is **not** a target-type retarget (that reads the
  integer as microseconds). It is a wrap of the ns-from-seconds result. Same net type,
  correct ticks.
- `tz_aware_to_naive_round_trip` flipped because leftover ns CAST output is wrapped
  (type-only). B-TZ-4 string rendering is still unpinned.
- `analyzer.rs` untouched (conductor-5 file-disjoint vs Z-3). New analyzer rule lives in
  `instant_ts.rs`.
