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
| C-006 | The fixture is committed before any product code, its expectations are derived by the harness from the rule, and its red and green counts on main's module are recorded. | The fixture commit plus section 4. | **OPEN** | `python/repark/tests/test_ice_tsns_type_planner_1.py`, `_ice_tsns_type_planner_1_rule.py`; counts in section 4. Stays open until the head is green. |
| C-007 | Every census cell (9,600) and every cache cell (420) stores what its typed twin stores. | The census and cache tests green on the head. | **OPEN** | `test_census_cell_equals_its_typed_twin`, `test_cached_frame_stores_what_the_frame_stores`. |
| C-008 | The 117 reads and the 92 consumer-door cells answer what their typed twin, or the plain column, answers. | The two tests green on the head, or the halt they hit. | **OPEN** | `test_read_over_a_site_equals_its_typed_twin`, `test_consumer_door_over_a_site_equals_the_column`. |
| C-009 | The parking repros of R-017's seven verifier rounds, by name, equal their twins. | The repro test green on the head. | **OPEN** | `test_parking_repros_of_the_seven_rounds`. |
| C-010 | The amended pins hold: twenty mixed-unit cells on both doors in three zones, every lattice position, the H1 refusals, the H2 microsecond pins, P15, R-010, R-013, R-018(1), the Postgres reads. | The pin tests green on the head. | **OPEN** | `test_mixed_units_widen_to_the_finest_on_both_doors`, `test_every_lattice_position_equals_its_typed_twin`, `test_refusals_microsecond_pins_and_registry_rows`, `test_postgres_timestamps_read_as_microseconds`. |
| C-011 | One planner types the five written names on both doors, and a join key in the optimized plan carries one unit. | The Rust pins green on the head. | **OPEN** | `crates/repark-sql/tests/ice_tsns_type_planner_1_lattice.rs`. |
| C-012 | `SparkTimestampUnitWiden` runs before type coercion on every door and widens mixed timestamp units to the finest, by the types at the position, never narrowing and never touching equal units. | The rule's unit tests plus the lattice and position cells on the head. | **OPEN** | `crates/repark-functions/src/timestamp_unit_widen.rs` (7 unit tests); the fixture cells of C-010. |
| C-013 | No rule reads a `Timestamp(ns, _)` cast target or literal as the SQL `TIMESTAMP`: the three `rewrite_cast` arms, their NTZ-mode twins, the nanosecond-literal rewrites and the AST lowering of the two nanosecond names are deleted. | The deletion commit plus the census green on the head. | **OPEN** | `crates/repark-functions/src/instant_ts.rs`, `crates/repark-spark/src/keyword_lower.rs`; `test_census_cell_equals_its_typed_twin`. |
| C-014 | The wall kernels are applied by the analyzer from the type of a cast target on every door, so a written, a coerced and a widened cast to a nanosecond type are one expression. | The lattice cells on both doors and halt H7's Rust cell green on the head. | **OPEN** | `crates/repark-functions/src/timestamp_ns_cast.rs` `wall_kernel_by_target`; `test_mixed_units_widen_to_the_finest_on_both_doors`. |

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

## 4. Step 1, the fixture (commit 0), red first

**Model:** Claude (claude-opus-5-5), executor lane A, 2026-10-13. Base of step 1: `d997e2cc`.
Main's release module: `293896ce`, the profile of the lane's own builds.

Every expectation is derived by `_ice_tsns_type_planner_1_rule.py`: a cell is one statement
with markers, its twin is that statement with the NULL typed, or the coarser operand cast,
to the nanosecond type of the position, and the cell is green when both give one outcome
(error class, or Arrow types and ticks, or Parquet type and stored ticks). The census, cache
and repro stores read the Parquet files. No cell names an expected value; the harness holds
no category copied from #1027 or #1042. The twin is spelled `CAST(… AS timestamp_ns)`, not
the kernel name, so it survives the retirement of the by-name registration.

Counts on main's module (`_record_ice_tsns_type_planner_1.py --jobs 8 --main 293896ce`):

| Family | Cells | Green on main | Red on main |
|---|---|---|---|
| census (5 zones, 6 target and source pairs, 5 forms, 64 doors) | 9,600 | 2,619 | 6,981 |
| cache (21 spellings, 2 targets, 2 sources, 5 zones) | 420 | 60 | 360 |
| reads (the 117 of #1042's verify-slice1b) | 117 | 0 | 117 |
| consumer doors (46 cells, 2 zones) | 92 | 0 | 92 |
| parking repros (87 named statements, 2 sources, 2 zones) | 348 | 135 | 213 |
| lattice (20 cells, 2 doors, 3 zones) | 120 | 0 | 120 |
| positions (32 positions, 3 siblings, 2 sources, 3 zones) | 576 | 193 | 383 |
| doors (H1, H2, P15, R-010, R-013, R-018(1)) | 15 | 5 | 10 |
| Postgres (`timestamp`, `timestamptz`) | 2 | 2 | 0 |
| Rust pins, both doors (`ice_tsns_type_planner_1_lattice.rs`) | 6 tests | 1 | 5 |

Notes on the counts, as measured:

- The 420 cache cells are derived, not copied: the fifteen spellings of #1027's matrix that
  narrow beside an untyped NULL and its six beside a typed NULL or a microsecond value, two
  nanosecond targets, two sources, the five zones of that matrix. A cached append is
  compared with the same frame uncached and with its typed twin.
- The consumer cells compare a door over `coalesce(c, NULL)` with the same door over `c`:
  by the first clause the site is the column. On main all 92 differ.
- 938 of 11,275 statements error on main's module
  (`ice_tsns_type_planner_1_main.json`); the fixture fails a cell whose statement answered
  on main and errors on the head.
- The green cells on main are controls: the written `TIMESTAMP` casts, the NULLs that are
  not timestamp operands, the positions main already types by the finest unit (`nvl`,
  `ifnull`, `array_append`, `array_prepend`, `array_position`, the comparisons beside a
  NULL), the two microsecond pins, the integer `range` relation, `repark.sql` not
  resolving the kernel name (the Python native door never registered it; the Rust
  `AnsiDialect` door does, and its cell is red), and both Postgres cells.
- The P15 cell through `spark.read.options(...).csv(...)` is green on main: that spelling
  already reads microseconds. `spark.read_csv(path, options)` reads `timestamp[s]` and is
  red.
- The Postgres cells ran against `make pg-up` on the rootless socket, in plaintext
  (`sslmode=disable`); without `REPARK_PG_URL` they skip.
- The join-key pin reads the optimized plan in Rust (`into_optimized_plan`), on both doors,
  for `=`, `<` and `<=>`.

## Q. Questions

Step 0 is measured; step 1 needs three rulings before it opens:

- Q1 (H1): does the scalar `range`-over-timestamps row halt the unit, or does Spark's integer-relation `range` clear it? Lean: clear it by refusing the scalar `range`/`generate_series` names with the other DataFusion-only names, since no Spark spelling calls them with timestamps.
- Q2 (H2): does the DDL column-type row halt the unit, or does the planner proceed while DDL keeps resolving through the catalog mapping? Lean: proceed; the sketch never claims DDL for the planner.
- Q3 (H3): what is the one custom timestamp-lattice coercion rule (name and placement) that step 1 adds for the twenty narrowing cells? Lean: a single analyzer rule before coercion that widens any microsecond/nanosecond mix to nanoseconds, by type, on `coalesce`, `UNION`, `array`, `CASE` and join keys, leaving comparisons to execution.
- **Ruled 2026-10-13 (owner):** Q1 to Q3 are answered by
  [amendment 1](../../wo/ice-tsns-type-planner-1/amendment-1.md): scalar `range` and
  `generate_series` over timestamps refuse by name; the planner proceeds with DDL and
  `Column.cast` pinned at microseconds; the lattice rule is `SparkTimestampUnitWiden`. Step 1
  opens from this head after the owner's triage.
