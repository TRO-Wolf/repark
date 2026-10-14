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

## 5. Step 1, the build: four product commits, then a halt

Commits on `fix/ice-tsns-type-planner-1` over `d997e2cc`, in the ordered sequence:
`17a6b171` the fixture; `aeed9261` the planner; `4c28bc5b` `SparkTimestampUnitWiden`;
`6c3688af` the deletions; `07cbe563` the wall kernels by cast target type. Commits 5
(P5 dispositions and P15) and 6 (registry rows, cost table) are **not built**: the unit
stopped at the halt below.

**How the planner is registered.** `repark-core` sits below `repark-functions`, so
`df_guards.rs` cannot name `SparkTypePlanner`, and `Cargo.toml` was not to be edited. The
session builder registers the planner once and each door hands it in: `SparkExtension`
(`SessionExtension::type_planner`), `AnsiDialect` (`SqlDialect::type_planner`), and the
Python native door's own extension (the door behind `repark.sql` builds with no dialect,
and the binding cannot implement the async dialect trait without a new dependency). The
two traits each gained one defaulted hook; `SqlDialect` also gained
`configure_analyzer_rules`, which is how the ANSI dialect places the lattice rule.

**Order of commits.** The order of the brief holds, with one move: the by-name
registration of the two wall kernels was to go with the deletions and did not go at all.
The Iceberg store doors in `repark-iceberg` look the kernel up with `ctx.udf(name)` and
render it in generated SQL; that crate cannot link `repark-functions`, and moving those
doors to the cast spelling is R-009's surface. R-013 keeps its row.

### 5.1 The fixture on the head (`07cbe563`, release module)

| Family | Cells | Green on main | Green on head | Red on head |
|---|---|---|---|---|
| census | 9,600 | 2,619 | 9,570 | 30 |
| cache | 420 | 60 | 420 | 0 |
| reads | 117 | 0 | 117 | 0 |
| consumer doors | 92 | 0 | 92 | 0 |
| parking repros | 348 | 135 | 288 | 60 |
| lattice, both doors, three zones | 120 | 0 | 120 | 0 |
| positions | 576 | 193 | 473 | 103 |
| doors | 19 | 5 of 15 | 11 | 8 |
| Postgres | 2 | 2 | 2 | 0 |

The door family grew by four out-of-range cells after commit 0 (a microsecond value
outside the nanosecond range beside a nanosecond value, both ANSI modes); main's count is
of the original fifteen.

What is red on the head, by cause, as measured:

- **Census, 30:** door `df_functions_append`, form `array`, every zone and pair. The
  statement stores what the other doors store; its **twin** errors
  (`array(UserDefined)`) because the twin goes through the expression-string session
  with a written nanosecond cast inside `array(…)`. A twin-side refusal, not a narrowed
  value.
- **Positions, 103:** `nvl2` (18, `nvl2(UserDefined)` on statement or twin),
  `array_contains` (18), `array_concat` (12), `array_append` (8), `array_prepend` (8),
  `array_position` and `array_remove` beside a NULL (12), `INTERSECT` and `EXCEPT`
  beside a microsecond branch (24), `UNION` beside a `TIMESTAMP` branch for the naive
  source (3, an internal DataFusion error; these three answered on main). The
  list-and-element functions are RePark's own overrides with their own coercion; the
  rule's cast does not reach a result there, or the override refuses the widened pair.
- **Parking repros, 60:** the six container shapes of #1027 verify 1
  (`coalesce(array(ns), array(ts))`, `array_append`, `array_prepend`, `array_union`,
  `flatten`, `CASE` over arrays: 24 cells) stay narrowed; `array_except` and
  `array_intersect` (8); the three-branch `UNION ALL` with a NULL first branch (8, still
  microseconds); `aggregate` with a typed microsecond NULL (4, now refused);
  `concat_ws(c, NULL)` (4, errors on main too); a map-element site (4, the
  Arrow `list array` panic of R-018(2)); the qualified `UPDATE` with a scalar subquery
  (4, a schema error on both roles' text); four single cells.
- **Doors, 8:** the four H1 refusals and the `read_csv` P15 cell (commit 5, not built);
  the out-of-range cell with ANSI off, twice (the widened value is NULL, as the written
  nanosecond cast is on main with ANSI off: the kernel follows the ANSI mode, the
  amendment asks for a raise in both); one R-010 `DELETE` cell whose harness reads every
  Parquet file under the table and so cannot see a copy-on-write delete (a fixture
  defect, not a product reading).
- **Rust pins, both doors:** 5 of 6 green
  (`cargo test -p repark-sql --test ice_tsns_type_planner_1_lattice`); the R-013 cell is
  red, as ruled above.

### 5.2 The halt

**Halt: a statement main answers that the head errors on and that is not a ruled
refusal.** 218 statements of the non-census families answered on main's module and error
on the head; none of the 9,600 census statements does. All 218 are one shape: a NULL or a
microsecond value beside a nanosecond value is now nanoseconds (the first and third
clauses), and the consumer above it refuses nanoseconds.

| Consumer | Cells | Head's error |
|---|---|---|
| `sequence(site, …)` in SQL and the functions API (the 117 reads: 95; repros: 8) | 103 | `ListArray expected data type Timestamp(ns) got Timestamp(µs, "UTC")` |
| pandas and Arrow doors over a site: `applyInPandas`, cogrouped `applyInPandas`, `mapInArrow`, `mapInPandas`, `pandas_udf`, `arrow_udf`, `applyInArrow` (reads: 20; consumer cells: 88) | 108 | the door's own schema or conversion error |
| `aggregate(array(ns), CAST(NULL AS TIMESTAMP), …)` | 4 | `DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE` |
| `UNION` of a naive nanosecond column and a `TIMESTAMP` column | 3 | an internal DataFusion error |

Each of the first three rows equals its typed twin, and the twin errors on main too:
`sequence(ns, ns)` and the pandas doors over a plain `timestamp_ns` column refuse on
main. The fixture's own cells for these families are therefore green (statement equals
twin) while its main-answers check is red. This is the finding #1042's verify-slice1b
parked on (G-1), reached now by the type itself rather than by a rewrite. The unit did
not design around it: no consumer was taught nanoseconds and no site was narrowed back.

### 5.3 Other suites on the head

- Rust, the five touched crates
  (`cargo test --no-fail-fast -p repark-functions -p repark-sql -p repark-spark -p repark-core -p repark-python`):
  5,930 passed, 55 failed. By module: `tests::ntz_values_mix` 19, `tests::ntz_store` 15,
  `tests::v3_timestamp_ns_door` 6, `pins_report` 3, `spark_nullability::tests` 2, and one
  each in `u9_timestamp_ltz`, `session_write_conf`, `plan_partitioning`, `ltz_store`,
  `extension::tests`, three integration files, and the R-013 cell. Not triaged one by
  one: the `VALUES` conform (P12), the store doors and the pins that spell the lowered
  kernel name are the expected movers; the three `pins_report` tests and the two
  benchmark-shaped tests may be unrelated.
- The facade suite was **not** run to completion on any head of this unit: one run on
  the planner commit was stopped in its long tail to free the module for the next build.

### 5.4 Not built, not measured

Commit 5 (P5 dispositions, the H1 refusal text, P15); commit 6 (registry rows, the cost
table); the eight mutants; the cost ratios; the whole facade suite; live Spark cells; the
`TIMESTAMP_NTZ` session mode beyond the unit tests; the 842-cell Spark and Iceberg
matrix; the parity-live job.

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
- **Step 1 questions (2026-10-13, executor lane A), open:**
  - Q4 (halt): the 218 statements of section 5.2. Do `sequence` and the pandas and Arrow
    doors learn to take a nanosecond value (their own units), does the site stay
    nanoseconds and the refusal stand as ruled, or does the rule not apply under those
    consumers? Lean: the type stays nanoseconds and each consumer gets its own unit of
    work; a consumer-by-consumer exception in the rule would be the reader accounting the
    owner refused.
  - Q5 (H5): the rule's table does not reach a list beside a list (`coalesce`, `CASE`,
    `array_union`, `flatten`, `array_except`, `array_intersect` over arrays of mixed
    units) and RePark's own list-and-element overrides. Extend the table to an element
    type, or leave these narrowed and registered? Lean: extend to the element type of a
    list operand at the positions already listed, nothing else.
  - Q6: out of range with ANSI off. The widened cast is the wall kernel, which answers
    NULL with ANSI off exactly as a written `CAST(us AS timestamp_ns)` does on main; the
    amendment asks for a raise in both modes and halt H7 asks the widened value to equal
    the written cast. Both cannot hold. Lean: the kernel raises in both modes for a
    timestamp source, written or widened.
  - Q7: the zone label. The sketch's table says `Timestamp(µs, Some(session zone))`; the
    planner types `Timestamp(µs, "UTC")`, the instant type the engine already uses
    everywhere. Lean: keep `"UTC"`.
  - Q8: the ANSI doors. `TIMESTAMP` is now the microsecond instant there (a string cast
    reads the wall as UTC, where main answered a naive nanosecond wall), `TIMESTAMP_NTZ`,
    `TIMESTAMP_LTZ` and the two nanosecond names now plan in expressions where main
    refused them, and `CREATE TABLE` keeps main's mapping and refusals. Is that the
    intended ANSI door? Lean: yes for the two nanosecond names and `TIMESTAMP`, as the
    sketch's table says; the owner should see it stated.
  - Q9: R-013. Retiring the by-name registration means moving the Iceberg store doors to
    the cast spelling. In this unit, or in R-009's? Lean: R-009's.
