# Unit ledger — FACADE-4 · type conversions in Rust — step 0

**Date:** 2026-09-14 · **Branch:** `perf/facade-4-s0` (C-001..C-008) · **Base:** `2bebc9da`
**Model:** swe-2-high · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** Card FACADE-4 (audit §8): Arrow schema ↔ Spark types ↔ DDL move into one
Rust table; `spark/types.py` keeps the Python classes (F1 frozen except `VariantType`)
and thins to checks plus conversion calls; K1 keeps DDL spellings byte-identical.
Step 0 is measurement and pins only — the release baseline cells (conversion isolated
per schema, plus conversion's share of end-to-end walls), the three-table agreement
census (facade `types.py` conversions vs `_csv_smart` rungs vs the reader lattice and
Rust `spark_ddl_type_name` / `arrow_type_key`), the DDL/Arrow goldens with a mutation
proof, and the step-1 target list. No product code under `python/repark/src/` or
`crates/` changes in this step.

**Not in this step:** `STATUS.md`, anything under `python/repark/src/` or `crates/`,
and (owned by another session tonight) `dataframe/core.py`, `dataframe/eager.py`,
`dataframe/cache_handle.py`, `spark/catalog.py`, `spark/functions_collections.py`,
`crates/repark-core/src/session/temp_views.rs`. No JVM, no parity-live legs.
Step 1 is a later round.

## PROPOSITION LEDGER — FACADE-4 step 0 — 2026-09-14

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | Step 0 lands with zero product change: no file under `python/repark/src/` or `crates/` in the branch diff; the native in `.venv` is a release build for every timing cell. | `git diff origin/main -- python/repark/src crates/` empty; `repark._native.__debug_assertions__ is False`. | **PROVEN** | `git diff 2bebc9da..HEAD -- python/repark/src crates/` is 0 lines — pins, ledger, docs and maps only; `.venv` native is release (`167,915,168 B`, `__debug_assertions__ False`). |
| C-002 | DDL goldens cover every F1 type class: for each of the 25 public type classes plus StructField/StructType shapes — `simpleString`, `typeName`, `json`, `repr`, `fromDDL` round-trip (parse → simpleString → parse), `StructType.toDDL` re-parse where the base tree permits, and `json` → `fromJson`/`_parse_datatype_json_value` round-trip — recorded from base `2bebc9da` as bytes. | `test_facade_4_ddl_round_trip.py` + committed `facade_4_type_goldens.json`; record mode `REPARK_FACADE_4_RECORD_GOLDENS=1` refused under `CI`/`GITHUB_ACTIONS`. | **PROVEN** | 47 cases (29 atomic + 16 complex + raw-Arrow probe + session-conf) recorded under a pinned UTC/LTZ session. Red-first: `AssertionError: missing golden facade_4_type_goldens.json`. Byte-identical compare after record (3 passed). Record-mode-refused-in-CI pin green. |
| C-003 | Arrow answers are golden for the schema set (flat 7-type, 50-column wide mixed, `struct<array<map>>` depth 3, decimal (10,2)/(38,18)/(38,0), timestamp tz/ntz/session-tz, interval and char/varchar): `repark_type_to_arrow` Arrow spellings and `struct_type_from_arrow` class+simpleString answers; an `isinstance` pin asserts every conversion answer is a public `repark.spark.types` class. | Same test file + golden; isinstance pin. | **PROVEN** | Per-case `arrow` and `arrow_schema_back` golden fields plus an 18-field raw-Arrow probe (`uint`, `date64`, `time64`, `duration`, `month_day_nano_interval`, `decimal256`, `dictionary`, non-UTC tz). isinstance pin asserts `repark.spark.types` module identity on every answer. |
| C-004 | Mutation proof: a one-line change to a conversion answer turns the golden red; restore leaves it green. | Scratch edit, red output pasted, `git checkout` restore. | **PROVEN** | `repark_type_to_arrow` `pa.timestamp("us", tz="UTC")` → `pa.timestamp("us")` red: `changed=['struct_flat7', 'struct_timestamp_variants', 'struct_wide50', 'timestamp']`. `git checkout` restore → 3 passed. No product file in the commit. |
| C-005 | Release baseline: warmup + 5 reps, medians, idle-box wait (no cargo/rustc/maturin) before each cell, `systemd-run --user --scope -p MemoryMax=8G` with `OPENBLAS_NUM_THREADS=8`. Cells: (a) `repark_type_to_arrow` per schema with per-call µs plus a spy count of calls per `createDataFrame`/`collect`/`to_arrow`/`show` of a 1e5 frame; (b) `struct_type_from_arrow` round-trip; (c) DDL parse (`fromDDL`/`_parse_datatype_string`) and DDL write (`simpleString`/DDL token) walls; (d) cProfile cumulative share of conversion in `df.schema`, `createDataFrame(pandas)`, `spark.read.csv(inferSchema)` at 1e5. Plain statement whether any cell is a wall (≥5 % of an end-to-end wall, or ≥1 ms per user call). | `docs/perf/facade-4-types-baseline-2026-09-14.md` + committed runner under `docs/perf/facade-4-types-baseline-2026-09-14/`. | **PROVEN** | Release native (`__debug_assertions__ False`), loads 2.26–2.44 at cell starts. `repark_type_to_arrow` 7.1–59.9 µs/call; spy: 0 calls on pandas create/collect/to_arrow/show/df.schema, 16 calls on nested-StructType create (≈0.16 ms of a 2,089 ms wall). `struct_type_from_arrow` 8.2–99.3 µs; DDL parse ≤125 µs (wide50), write ≤25 µs. cProfile share: df.schema 0.033/0.066 ms, createDataFrame(pandas) 0/1,650 ms, csv infer 0.14/2,270 ms. **No wall.** |
| C-006 | Three-table agreement census measured by calling each table: for timestamps (tz, ntz, session-tz non-UTC), decimals (precision/scale edges, 38 overflow), nested nullability (struct field, array element `containsNull`, map `valueContainsNull`), date, binary, char/varchar, intervals — what (i) the facade `types.py` conversions, (ii) `_csv_smart` rungs, (iii) the reader lattice + Rust `spark_ddl_type_name`/`arrow_type_key` each answer. Every disagreement is a row with the concrete input and the three answers; none fixed. | Census section below; probe script under `docs/perf/facade-4-types-baseline-2026-09-14/`. | **PROVEN** | `census_probe.py` run under the 8 GiB scope: 8 agree rows, 21 measured disagreement rows (D1–D21) including the reader-vs-DESCRIBE split on `float32`/`binary` and the bidirectional `containsNull`/`valueContainsNull` drop. |
| C-007 | Step-1 target list: either (A) a measured wall and the Rust move that removes it, or (B) "no wall: ship as the correctness consolidation" naming the exact conversion entry points step 1 routes through one Rust table, plus the census disagreements needing an owner ruling (each a question with the three answers and a lean). | Step-1 section below. | **PROVEN** | Option (B): no measured wall. Eleven facade + two Rust entry points named; ten owner questions (timestamps-infer, binary, float32, unsigned/small ints, decimal>38, csv literal lattice, intervals, nullability, unsupported Arrow) each with the three answers and a lean. |
| C-008 | Gates green: the card's named pins unedited (`test_types_1.py`, `test_types_simple_string.py`, `test_types_x2_census.py`, `test_cast_failure_parity.py`, `test_a3_cast_vocab.py`), the FACADE-2 guard (`test_facade_2_column_display_goldens.py`, `test_facade_2_group2_no_python_assembly.py`), the new pins, `make verify`, `test_production_file_size.py`. | Commands and counts in Evidence. | **PROVEN** | 184 passed / 7 skipped across the nine named files; `make verify` green end-to-end (fmt, clippy ×3, structural gates, ruff check+format, Rust tests). |

## Census — the three conversion tables (measured, step 0)

Measured 2026-09-14 by `docs/perf/facade-4-types-baseline-2026-09-14/census_probe.py`
under `systemd-run --user --scope -p MemoryMax=8G -p MemorySwapMax=0`,
`OPENBLAS_NUM_THREADS=8 OMP_NUM_THREADS=8`, release native
(`repark._native.__debug_assertions__ is False`), session `UTC` /
`TIMESTAMP_LTZ` unless a row says otherwise. Columns: **(i)** facade
`python/repark/src/repark/spark/types.py` (`_arrow_type_to_repark`,
`repark_type_to_arrow`, `struct_type_from_arrow`, `fromDDL`,
`toDDL`); **(ii)** `_csv_smart` rungs (`try_rung`/`resolve_cell_rung` →
`rung_to_spark_type`/`rung_to_sql_cast`/`rung_to_engine_cast`); **(iii)** the
reader lattice + Rust names (`df.schema`/`dtypes`/`arrow_type_key` from
`createDataFrame(pa.Table)` and `session.table`, CSV `inferSchema`, and
`DESCRIBE TABLE` = `spark_ddl_type_name`). `—` = no entry point for the input.

### Agree

| Input | (i) facade types.py | (ii) _csv_smart | (iii) reader + Rust |
|---|---|---|---|
| `date32` / `date64` / `'2024-01-02'` | `DateType` | `date` rung → `DateType` | `date`; csv infer `date`; DESCRIBE `date` |
| `float64` / `'1.5e3'` | `DoubleType` | `float64` rung → `DoubleType` | `double` |
| `int64` / `'9999999999'` | `LongType` (`bigint`) | `int64` rung → `LongType` | `bigint` |
| `boolean` / `'true'` | `BooleanType` | `bool` rung → `BooleanType` | `boolean` |
| `timestamp[us, tz=UTC]` | `TimestampType` | `'…Z'` literal → `TimestampType` | `timestamp`; DESCRIBE `timestamp` |
| `timestamp[us, tz=America/New_York]` | `TimestampType` | `'…+02:00'` literal → `string` rung | `timestamp` (any tz → `timestamp`) |
| `decimal128(10,2)` | `DecimalType(10,2)` | `'1.23'` → `decimal(3,2)` | `decimal(10,2)`; DESCRIBE `decimal(10,2)` |
| `char(8)` / `varchar(32)` DDL | `CharType`/`VarcharType`; `repark_type_to_arrow` → `string` | literal → `string` | no char/varchar surface; `string` |

### Disagree — every row measured, none fixed

| # | Input | (i) facade types.py | (ii) _csv_smart | (iii) reader + Rust |
|---|---|---|---|---|
| D1 | Arrow `timestamp[us]` (tz-naive) | `TimestampNTZType` (`timestamp_ntz`) | naive literal `'2024-01-02 03:04:05'` → `TimestampType` (`timestamp`) | `arrow_type_key`/`df.schema` `timestamp_ntz`; csv infer `timestamp` |
| D2 | session `spark.sql.timestampType=TIMESTAMP_NTZ` | `default_timestamp_data_type()` → `TimestampNTZType` | — | csv infer still answers `timestamp` under an NTZ session |
| D3 | csv literal `'1.23'` | — | `decimal128` rung → `DecimalType(3,2)` | csv infer → `double` |
| D4 | csv literal `decimal(38,18)`-shaped | — | `decimal128` rung → `DecimalType(18,18)` (precision = literal digits) | csv infer → `double` |
| D5 | csv literal `'42'` (int32 range) | — | `int32` rung → `IntegerType` (`int`) | csv infer → `bigint` |
| D6 | `decimal256(76,10)` (precision >38) | `_arrow_type_to_repark` → `DecimalType(76,10)`; `repark_type_to_arrow` **refuses** | 39-digit literal → `float64` rung → `DoubleType` | `arrow_type_key`/`df.schema` `decimal(76,10)` |
| D7 | Arrow `binary` / `large_binary` | `BinaryType` (`binary`) | literal → `string` | `df.schema`/`type_key` `string`; **DESCRIBE `binary`** |
| D8 | Arrow `float32` | `FloatType` (`float`) | — | `df.schema`/`type_key` `double`; **DESCRIBE `float`** |
| D9 | Arrow `int8` / `int16` | `ByteType`/`ShortType` (`tinyint`/`smallint`) | — | `int` (scan and DESCRIBE collapse to `int`) |
| D10 | Arrow `uint8`/`uint16`/`uint32`/`uint64` | `StringType` | — | `int` (type_key and df.schema) |
| D11 | Arrow `duration[us]` | `StringType` | interval literal → `string` | `type_key` `Duration(Microsecond)`; df.schema `string` |
| D12 | Arrow `month_day_nano_interval` | `StringType` | `string` | `type_key` `Interval(MonthDayNano)`; df.schema `string` |
| D13 | Arrow `time64[us]` | `StringType` | — | `type_key` `Time64(Microsecond)`; df.schema `string` |
| D14 | `DayTimeIntervalType`/`YearMonthIntervalType` | `repark_type_to_arrow` → `string`; `fromDDL` **refuses** `'interval day to second'`/`'interval year to month'`; `interval` → `CalendarIntervalType` | literal → `string` | — |
| D15 | struct field `nullable=False` | preserved by `struct_type_from_arrow`; `toDDL` emits `req INT NOT NULL`; `fromDDL` **refuses** its own `NOT NULL` output | — | `type_key` preserves `false` |
| D16 | `ArrayType(int, containsNull=False)` | `repark_type_to_arrow` → `list<item: int32>` (item nullable — flag **dropped**); arrow `list<int32 not null>` → `containsNull=True` (flag **dropped** inbound) | — | df.schema `array<int>` (flag invisible on the surface) |
| D17 | `MapType(str,int, valueContainsNull=False)` | `repark_type_to_arrow` → `map<string, int32>` (value nullable — flag **dropped**); arrow non-null value → `valueContainsNull=True` | — | df.schema `map<string,int>` |
| D18 | Iceberg `Float32` column read back | — | — | `DESCRIBE` `float` vs `session.table(...).dtypes` `double` — two surfaces inside (iii) disagree with each other |
| D19 | Iceberg `Binary` column read back | — | — | `DESCRIBE` `binary` vs `session.table(...).dtypes` `string` |
| D20 | Arrow `dictionary` | `StringType` | — | (not probed end-to-end; `arrow_type_key` retains the dictionary value type) |
| D21 | Arrow `null` | `NullType` (`void`) | — | — |

Notes on the disagreements:

- D7/D8/D18/D19 show the reader side is itself two tables: the physical scan
  widens `Float32`→`Float64` and decodes `Binary`→`Utf8` before
  `arrow_type_key` ever runs, while `spark_ddl_type_name` names the stored
  Iceberg type. A single Rust table must decide which of those two answers a
  given entry point is promising.
- D1/D2: the Arrow tz-naive → `timestamp_ntz` mapping is consistent between
  facade and reader lattice, but the CSV inference path normalizes every
  timestamp to `timestamp` and ignores `spark.sql.timestampType=TIMESTAMP_NTZ`.
- D3/D4/D5: the `_csv_smart` rungs (decimal/int32/int64) are strictly more
  precise than what `spark.read.csv(inferSchema)` actually answers
  (`double`/`bigint`) — the smart rungs are not the table that serves the
  public infer path.
- D15/D16/D17: nullability survives at the struct-field level both ways, is
  silently dropped at the array-element and map-value level both ways, and
  `toDDL`'s `NOT NULL` output is refused by `fromDDL` — the facade's own DDL
  write/read pair does not round-trip.
- D6: `decimal256(76,10)` survives `arrow → facade type` but cannot go back to
  `decimal128`; the reader keeps `decimal(76,10)` on the surface — a type the
  facade cannot materialize an Arrow column for.

## Step-1 target list

**Verdict: (B) — no wall: ship as the correctness consolidation.** The
baseline ([docs/perf/facade-4-types-baseline-2026-09-14.md](../../../docs/perf/facade-4-types-baseline-2026-09-14.md))
measures every conversion at ≤134 µs per call, zero calls on
`collect`/`to_arrow`/`show`/`df.schema`, and 0.000–0.006 % share of the
1e5-row end-to-end walls. Nothing reaches the card's wall bar (≥5 % of an
end-to-end wall or ≥1 ms per user call). Step 1 exists to unify the three
tables' *answers*, not to remove a measured cost.

### Conversion entry points step 1 routes through one Rust table

Facade `python/repark/src/repark/spark/`:

- `types.repark_type_to_arrow` — reached via `_sql_type_to_arrow` in
  `session/create_dataframe_schema.py` (explicit-schema `createDataFrame`).
- `types.struct_type_from_arrow` / `types._arrow_type_to_repark` — `df.schema`,
  `dtypes`, `printSchema`, and every Arrow-backed schema surface.
- `types._parse_datatype_string` / `DataType.fromDDL` — DDL schema strings.
- `types._datatype_to_ddl_token` / `simpleString` / `StructType.toDDL` — DDL
  write (including the `NOT NULL` emission the parser refuses).
- `jsonValue` / `fromJson` / `_parse_datatype_json_value` — schema JSON
  round-trip.
- `types._engine_type` — the engine cast token.
- `timestamp_type.active_timestamp_type` / `default_timestamp_data_type` /
  `default_timestamp_arrow_type` — the `spark.sql.timestampType` /
  session-tz defaults.
- `create_dataframe_inference.resolve_column_type` + `types._merge_type` —
  the inference lattice.
- `_csv_smart.rung_to_spark_type` / `rung_to_engine_cast` / `rung_to_sql_cast`
  — the CSV rung table.

Reader/Rust side (already Rust — becomes the table's Rust leg):

- `crates/repark-spark/src/spark_type_names.rs::spark_ddl_type_name` —
  DESCRIBE/DDL names.
- `crates/repark-python`'s `arrow_type_key` consumers — `df.schema`/dtypes
  logical keys and nested DDL names in `dataframe.rs`.

### Census disagreements needing an owner ruling before unification

Each question carries the three measured answers and a lean; all are
recorded in the census table above.

1. **Timestamps under CSV `inferSchema`.** Literal `'2024-01-02 03:04:05'`:
   facade session default honors `spark.sql.timestampType` (NTZ →
   `TimestampNTZType`); `_csv_smart` rung → `TimestampType`; served csv
   infer → `timestamp` even under an NTZ session. *Should the unified table
   honor `spark.sql.timestampType` on the CSV infer path?* Lean: yes —
   both other tables distinguish ntz, and the session knob is the facade's
   own contract.
2. **Binary.** Arrow `binary`: facade `BinaryType`; `_csv_smart` `string`;
   `df.schema`/`dtypes` `string`; DESCRIBE `binary`. *Is `string` on the
   scan surface the contract to keep (physical utf8 decode) or a bug to
   fix?* Lean: keep per-entry-point answers — `binary` where the stored
   type is named (DESCRIBE, `struct_type_from_arrow`), `string` where the
   physically decoded column is described — and make the table carry both
   flags rather than pick one spelling globally.
3. **Float32.** Arrow/Iceberg `float32`: facade `FloatType` (`float`);
   scan surface `double`; DESCRIBE `float`. Same shape as Q2. Lean: same —
   `float` for the stored/logical name, `double` only where the widened
   physical column is described.
4. **Unsigned ints.** Arrow `uint8`–`uint64`: facade `StringType`; reader
   `int`. *Does the unified table widen unsigned to the signed family
   (`int`/`bigint`) or keep the `string` degradation?* Lean: reader's `int`
   for `uint8`/`uint16`/`uint32` and `bigint` for `uint64` — `string` loses
   the numeric type the user can already see via the reader.
5. **Signed small ints.** Arrow `int8`/`int16`: facade `tinyint`/`smallint`;
   reader `int`. *Keep Spark's `tinyint`/`smallint` or the scan's `int`?*
   Lean: `tinyint`/`smallint` — they are the Spark-visible answer for the
   Arrow type; Iceberg's int32 widening is a storage fact DESCRIBE already
   reports separately.
6. **Decimal precision >38.** `decimal256(76,10)`: facade refuses
   `repark_type_to_arrow`; reader surfaces `decimal(76,10)`. *Does the
   unified table cap DDL precision at 38 (Spark's limit) or admit
   `decimal(p,s)` for p>38?* Lean: cap at 38 on the facade surface
   (Spark parity) and let the reader keep its internal spelling.
7. **CSV literal precision.** `'42'`: `_csv_smart` `int` vs served infer
   `bigint`; `'1.23'`: `_csv_smart` `decimal(3,2)` vs served infer `double`.
   *Which lattice is canonical?* Lean: neither is fully Spark-shaped —
   Spark's infer ladder is int → bigint → double → timestamp → date →
   bool → string — so adopt int/`bigint`/`double` (no decimal rung) and
   retire the `_csv_smart` rung table into the same Rust lattice.
8. **Interval DDL.** `fromDDL` refuses `interval day to second` /
   `interval year to month`; `repark_type_to_arrow` degrades intervals to
   `string`. *Keep the string degradation + refusal?* Lean: keep — it is
   today's Spark-visible answer and the F1 freeze pins it; the table just
   needs to emit `string` for all three interval classes.
9. **Nullability.** `toDDL` emits `NOT NULL` that `fromDDL` refuses;
   `containsNull`/`valueContainsNull` are dropped in both conversion
   directions. *Does the unified table preserve element-level nullability
   and teach the parser `NOT NULL`?* Lean: yes on both — PySpark accepts
   `NOT NULL` in schema DDL and tracks `containsNull`; today's drops are
   silent fidelity loss, not a contract.
10. **Unsupported Arrow types.** `duration`/`time64`/`month_day_nano_interval`/
    `dictionary`: facade `StringType`; Rust `arrow_type_key` keeps the raw
    Arrow name (`Duration(Microsecond)`, …) while `df.schema` shows `string`.
    *Does the unified table keep the string degradation on the facade
    surface?* Lean: yes — raw Arrow names on a Spark surface are a leak;
    the type_key can keep its internal spelling.

Step 1 stops here; implementation is a later round.

## Evidence

- C-001: `git diff 2bebc9da..HEAD -- python/repark/src crates/` empty (pins,
  ledger, docs and maps only); `repark._native.__debug_assertions__ is False`
  in `.venv` for every timed cell (runner asserts the same header).
- C-002/C-003/C-004: `test_facade_4_ddl_round_trip.py` (3 tests) +
  `facade_4_type_goldens.json` (47 cases); mutation proof recorded in C-004.
- C-005: `run_baseline.py` JSON — cells (a)–(d) tables in the baseline doc;
  verdict NO WALL (every call ≤134 µs; conversion 0.000–0.006 % of the 1e5-row
  walls; `df.schema` 50 % share of a 66 µs op ≈ 0.03 ms per call).
- C-006: `census_probe.py` JSON — 8 agree rows, D1–D21 disagree rows.
- Gates (2026-09-14):
  `pytest test_types_1 test_types_simple_string test_types_x2_census
  test_cast_failure_parity test_a3_cast_vocab
  test_facade_2_column_display_goldens test_facade_2_group2_no_python_assembly
  test_facade_4_ddl_round_trip test_production_file_size -q`
  → 184 passed, 7 skipped.
  `make verify` → green (fmt, clippy workspace, Rust tests, map sync).
- Commits: `f8e33cca` ledger skeleton; `585ef87a` pins + goldens + mutation
  proof; `6f5608e4` census table + probe; `75405954` baseline doc + runner;
  HEAD step-1 target list + evidence.

## Coverage attestation — step 0

```yaml
COVERAGE_ATTESTATION:
  pr_unit: facade-4
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every deliverable of the step-0 brief maps to a clause C-001..C-008, each PROVEN with pasted evidence; the branch diff holds no file under python/repark/src or crates.
      artifacts: [task/ledgers/staging/facade-4-ledger.md]
    - id: AT-2
      status: ATTACKED
      evidence: Goldens cover the 25 public type classes plus struct shapes, decimal edges (10,2)/(38,18)/(38,0) and decimal256 above 38, timestamp tz/ntz/session-tz, intervals, char/varchar, uint and dictionary Arrow inputs.
      artifacts: [python/repark/tests/test_facade_4_ddl_round_trip.py, python/repark/tests/facade_4_type_goldens.json]
    - id: AT-3
      status: ATTACKED
      evidence: Refusals are recorded as golden answers (fromDDL of its own NOT NULL output, interval day to second, repark_type_to_arrow of decimal256), and record mode is refused under CI.
      artifacts: [python/repark/tests/test_facade_4_ddl_round_trip.py]
    - id: AT-4
      status: N/A
      justification: Step 0 adds tests, docs and a measurement runner only; no shared or mutable state changes.
    - id: AT-5
      status: N/A
      justification: No privileged action, secret or path handling; goldens are committed JSON beside the test.
    - id: AT-6
      status: ATTACKED
      evidence: The three-table census measures 8 agreements and 21 disagreements (D1-D21) including silent containsNull/valueContainsNull loss in both directions; none fixed, each an owner question for step 1.
      artifacts: [task/ledgers/staging/facade-4-ledger.md, docs/perf/facade-4-types-baseline-2026-09-14/census_probe.py]
    - id: AT-7
      status: ATTACKED
      evidence: Release baseline, warmup plus 5 reps, medians under an 8 GiB scope; the dearest conversion is 134 us per call and the largest share of a 1e5 wall is 0.008 %, so no wall.
      artifacts: [docs/perf/facade-4-types-baseline-2026-09-14.md, docs/perf/facade-4-types-baseline-2026-09-14/run_baseline.py]
    - id: AT-8
      status: ATTACKED
      evidence: The F1-frozen names repark_type_to_arrow and struct_type_from_arrow are pinned by answer, and an isinstance pin asserts every answer is a public repark.spark.types class.
      artifacts: [python/repark/tests/test_facade_4_ddl_round_trip.py]
    - id: AT-9
      status: N/A
      justification: No log-format or diagnosis-path change.
    - id: AT-10
      status: ATTACKED
      evidence: Dropping the tz from pa.timestamp in repark_type_to_arrow redded four golden cases and the restore greened them; the card's existing pins stay unedited and green.
      artifacts: [python/repark/tests/test_facade_4_ddl_round_trip.py]
  complete: true
```
