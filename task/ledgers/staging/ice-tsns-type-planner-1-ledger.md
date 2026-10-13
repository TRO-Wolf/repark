# Unit ledger — ICE-TSNS-TYPE-PLANNER-1 · step 0 inventories every nanosecond producer

**Scope (2026-10-13):** measurement only, on main `be011012c1b6f3ae20d7bb8dadc35af472553341`.
No product file changes on the unit branch: nothing under `crates/*/src` or
`python/repark/src` moves; the H2 probe lives on the throwaway branch
`probe/type-planner-h2`, never merged. Spark 4.1.2 is the oracle for function
names.

**Date:** 2026-10-13 · **Branch:** `fix/ice-tsns-type-planner-1` · **Base:** `be011012` ·
**Model:** Muse Spark (muse-spark-1.3-contributor) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Order:** the card
[ice-tsns-type-planner-1-card-2026-10-13.md](../../roadmap/mid-term/ice-tsns-type-planner-1-card-2026-10-13.md),
the sketch [sketch.md](../../wo/ice-tsns-type-planner-1/sketch.md) sections 2, 3, 4 and 6,
parity row ICE-TSNS-SQL-1-R-017.
**This ledger closes when the unit's pull request merges.** Step 0 does not merge that change.

## PROPOSITION LEDGER — ICE-TSNS-TYPE-PLANNER-1 — 2026-10-13

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | The P5 walk runs over every registered scalar, aggregate and window function of the Spark-door session and its printed output is committed. | The test plus its evidence file. | **PROVEN** | `crates/repark-spark/tests/ice_tsns_type_planner_1_p5.rs`; `task/wo/ice-tsns-type-planner-1/evidence/p5-ns-returns.txt`: 23 ns lines over 443 scalar, 60 aggregate, 92 window functions, six probe inputs. |
| C-002 | The H3 walk runs the six operators over all sixteen ordered type pairs on a plain session and on the Spark-door session and its lattice is committed. | The test plus its evidence file. | **PROVEN** | `crates/repark-spark/tests/ice_tsns_type_planner_1_h3.rs`; `task/wo/ice-tsns-type-planner-1/evidence/h3-coercion-lattice.txt`: 192 lines, plain and RePark identical modulo the array spelling. |
| C-003 | The H2 probe (marker planner at both builders, probe-branch release module, eight spelling outcomes) is recorded with the probe sha. | The evidence file. | **PROVEN** | `task/wo/ice-tsns-type-planner-1/evidence/h2-probe.md`; probe branch `probe/type-planner-h2` at `42ed8b55`, never merged. |
| C-004 | The H4 reader table and the P1 to P15 producer table in section 1 are recorded from the release module of main. | The evidence file plus section 1. | **PROVEN** | `task/wo/ice-tsns-type-planner-1/evidence/h4-reader-types.md`; section 1 below. One new producer row (P15) carries an empty disposition. |
| C-005 | Step 1 (planner, deletions, analyzer, P5 dispositions) opens only after the ruling on H1 to H3. | The ruling. | **OPEN** | Halts recorded in section 2; no product file moves on the unit branch. |

## 1. The completed producer table (P1 to P15)

Measured from the release module of main on the unit branch head. Spark door
is a local `ReparkSession`; ANSI door is the process-wide native `repark.sql`
session; frame door is the DataFrame API on the Spark session. The wall used
is `2024-01-02 03:04:05`. The disposition column repeats the sketch unchanged,
except row P15, which is new and empty.

| # | Producer | Spark door | ANSI door | Disposition |
|---|---|---|---|---|
| P1 | `CAST(x AS TIMESTAMP)`, `CAST(x AS TIMESTAMP_NTZ)` | `CAST('…' AS TIMESTAMP)` answers `timestamp[us, tz=UTC]`; `TIMESTAMP_NTZ` answers `timestamp[us]` | `CAST('…' AS TIMESTAMP)` answers `timestamp[ns]`; `TIMESTAMP_NTZ` refuses (`Unsupported SQL type TIMESTAMP_NTZ`) | Planner maps to µs; the rewrite arms for ns targets are deleted |
| P2 | Typed literals `TIMESTAMP '…'`, `TIMESTAMP_NTZ '…'` | `TIMESTAMP '…'` answers `timestamp[us, tz=UTC]`; `TIMESTAMP_NTZ '…'` answers `timestamp[us]` | `TIMESTAMP '…'` answers `timestamp[ns]`; `TIMESTAMP_NTZ '…'` refuses | Planner maps to µs; `wrap_ns_literal` shrinks to the cases step 0 still finds |
| P3 | `CAST(x AS timestamp_ns)` / `timestamptz_ns` | lowered UDF call answers `timestamp[ns]` / `timestamp[ns, tz=UTC]` | refuses (`Unsupported SQL type timestamp_ns`, `timestamptz_ns`) | Planner maps to ns types; the analyzer wraps the wall kernel by target type |
| P4 | Planner-inserted coercion casts | `coalesce(CAST('…' AS timestamp_ns), NULL)` answers `timestamp[us, tz=UTC]` (the R-017 defect) | refuses on the unknown `timestamp_ns` name | Stays as DataFusion emits it; means ns. Pinned by the 9,600-cell census |
| P5 | DataFusion built-ins RePark does not override | eight names print ns lines, zero aggregate or window lines; per-name table below | same registry, same lines | Each by name: override, refuse, or keep (per-name table) |
| P6 | `now()`, `current_timestamp`, `localtimestamp` | `now()` and `current_timestamp()` answer `timestamp[us, tz=UTC]`; bare `localtimestamp` refuses (`UNRESOLVED_COLUMN`) | `now()` and `current_timestamp()` answer `timestamp[ns]` | Stays; step 0 confirms the override wins registration order |
| P7 | Interval and date arithmetic | `TIMESTAMP '…' + INTERVAL 1 DAY` answers `timestamp[us, tz=UTC]` | same spelling answers `timestamp[ns]` | Unit of the timestamp operand; stays, ns only when the operand is ns |
| P8 | Frame type names | `.cast("timestamp")` and `.cast(TimestampType())` answer `timestamp[us, tz=UTC]` | frame door only | The second builder takes the same planner; one pin per spelling |
| P9 | Arrow ingest: pandas/polars ns | `datetime64[ns]` pandas and polars frames read `timestamp[us]` | frame door only | Stays (R-019, FIXED 10-13) |
| P10 | Readers | CSV/JSON read `string` (CSV with inference reads `timestamp[s]`, see P15); Parquet `us`/`ns`/zoned-`ns` and Iceberg v2/v3 read their stored units | reader door only | Real ns stays ns; any inferred ns from text readers is a finding with its own row (none: H4 NOT MET) |
| P11 | Python UDF returns | scalar `pandas_udf` over a `TIMESTAMP` answers `timestamp[us, tz=UTC]` | frame door only | Out of this unit (NS-ARRAY-FLOOR-1) |
| P12 | `VALUES` rows and `INSERT … VALUES` conform | v3 `timestamp_ns` table stores `INSERT VALUES` and `INSERT SELECT` and reads `timestamp[ns]` | not measured (native catalog path untested) | Re-measured; expected to shrink (R-018(1)) |
| P13 | The ANSI door | the ANSI column of P1 to P4, P6, P7 | — | Same planner, registered once in `df_guards.rs`; the ANSI door's own type names checked by step 0 |
| P14 | Cached frames | a cached `CAST('…' AS timestamp_ns)` frame reads `timestamp[ns]` | frame door only | Re-measured; the planner should make the cache a pure pass-through |
| P15 | CSV-inferred `timestamp[s]` column (new) | `read_csv` with `inferSchema` and `header` reads `timestamp[s]` | reader door only |  |

### P5 per-name table

Probe inputs are `ts_us_tz`, `ts_us`, `utf8`, `int64`, `date32`, `null`
(single-argument `return_type`, else `return_field_from_args`). Spark 4.1.2
answers are live `DESCRIBE FUNCTION` outcomes. No aggregate or window function
prints an ns line.

| Name | Lines | Spark 4.1.2 | Replacement | Step-1 disposition |
|---|---|---|---|---|
| `__repark_cast_timestamp_ns__` | 5 (`date32`, `null`, `ts_us`, `ts_us_tz`, `utf8` to naive ns) | no (RePark-internal) | wall kernel family in `repark-functions` | retire the by-name lowering; the analyzer wraps by target type |
| `__repark_cast_timestamptz_ns__` | 5 (same inputs to zoned ns) | no (RePark-internal) | wall kernel family in `repark-functions` | same as above |
| `__repark_session_end__` | 1 (`date32` to naive ns) | no (RePark-internal) | `session_end_udf` in `crates/repark-functions/src/spark_session_window.rs` | keep; the `date32` to ns arm is reviewed in step 1 |
| `generate_series` | 2 (`ts_us`, `ts_us_tz` to lists of ns) | no (Spark spells it `sequence`) | none (DataFusion default) | refuse by name |
| `range` | 2 (`ts_us`, `ts_us_tz` to lists of ns) | yes (`DESCRIBE FUNCTION range` answers) | none in `datafusion_spark` or `crates/repark-functions` | H1, see section 2 |
| `session_window` | 1 (`date32` to a struct of naive ns) | yes | `SparkSessionWindow` in `crates/repark-functions/src/spark_session_window.rs` | keep the override; its `date32` to ns arm is reviewed in step 1 |
| `to_local_time` | 1 (`null` to naive ns) | no | none (DataFusion default) | refuse by name |
| `to_timestamp_nanos` | 6 (every input to naive ns) | no | none (DataFusion default) | refuse by name |

`date_bin` prints no ns line over the six probe inputs; Spark 4.1.2 has no
`date_bin` either, so it joins the refuse-by-name list if an ns input ever
reaches it. P6 holds: `now`, `current_timestamp` and `to_timestamp` print no
ns line on the Spark door, so the RePark overrides win registration order
there; the ANSI door answers `ns` for `now()` and `current_timestamp()`
(row P6).

## 2. The four halt criteria

| Halt | Verdict | Evidence and count |
|---|---|---|
| H1 | **MET** | `task/wo/ice-tsns-type-planner-1/evidence/p5-ns-returns.txt`. One name: `range` returns nanoseconds (a list of ns for `ts_us` and `ts_us_tz`), exists in Spark 4.1.2, and has no Spark-semantics replacement in `datafusion_spark` or `crates/repark-functions`. Nuance: Spark's `range` is a relation over integers while the probe called the scalar `return_type` with one timestamp argument. |
| H2 | **MET** | `task/wo/ice-tsns-type-planner-1/evidence/h2-probe.md`. One SQL-text spelling of eight does not reach the planner: the DDL column type in `CREATE TABLE … (c TIMESTAMP)` resolves through the catalog mapping (the table still creates a real `timestamp[us, tz=UTC]` column on the probe branch). The four Spark expression spellings and both ANSI statements fail on the marker; `F.col("x").cast("timestamp")` resolves through the type-name map and is not an SQL-text spelling. |
| H3 | **MET** | `task/wo/ice-tsns-type-planner-1/evidence/h3-coercion-lattice.txt` (192 lines, plain and RePark identical modulo the array spelling). Twenty cells narrow to microseconds: `coalesce`, `UNION ALL` and `CASE` take the first argument's unit (the four ordered pairs with a microsecond type first); `array` takes microseconds when any member is microsecond (all eight mixed ordered pairs). `=` and join keys carry both operand types uncoerced at analyze; every analyzed join carries the condition in `filter` (equi-key extraction is an optimizer step). |
| H4 | **NOT MET** | `task/wo/ice-tsns-type-planner-1/evidence/h4-reader-types.md`. Zero text readers infer nanoseconds: CSV reads `string` (or `timestamp[s]` with inference), JSON reads `string`. |

## 3. What was not measured

- Postgres `timestamp` / `timestamptz` reads: `make pg-up` fails in this lane (no Docker daemon socket), recorded in the H4 evidence file.
- Optimized-plan join keys: the lattice reads analyzed plans only.
- Multi-argument `return_type` probes: the P5 walk calls with one argument per probe input.
- Nanosecond probe inputs: the six P5 inputs hold no ns type, so e.g. `date_bin` over ns inputs is unmeasured.
- The `array` spelling on the plain session and `make_array` on the RePark session: each session ran the spelling its registry resolves.
- ANSI-door DDL and catalog writes, and `VALUES` conformance on the ANSI door.
- A cached narrowing expression: the cached frame holds a plain `timestamp_ns` literal.
- Write doors beyond the P12 smoke (`UPDATE`, `MERGE`, `OVERWRITE`, `BY NAME`, CTAS).
- Non-default session zones: every run used the default zone.
- Spark-internal names (`__repark_*`): no `DESCRIBE` by construction.

## Q. Questions

Step 0 is measured; step 1 needs three rulings before it opens:

- Q1 (H1): does the scalar `range`-over-timestamps row halt the unit, or does Spark's integer-relation `range` clear it? Lean: clear it by refusing the scalar `range`/`generate_series` names with the other DataFusion-only names, since no Spark spelling calls them with timestamps.
- Q2 (H2): does the DDL column-type row halt the unit, or does the planner proceed while DDL keeps resolving through the catalog mapping? Lean: proceed; the sketch never claims DDL for the planner.
- Q3 (H3): what is the one custom timestamp-lattice coercion rule (name and placement) that step 1 adds for the twenty narrowing cells? Lean: a single analyzer rule before coercion that widens any microsecond/nanosecond mix to nanoseconds, by type, on `coalesce`, `UNION`, `array`, `CASE` and join keys, leaving comparisons to execution.
