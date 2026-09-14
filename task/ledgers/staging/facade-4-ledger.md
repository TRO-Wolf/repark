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
| C-001 | Step 0 lands with zero product change: no file under `python/repark/src/` or `crates/` in the branch diff; the native in `.venv` is a release build for every timing cell. | `git diff origin/main -- python/repark/src crates/` empty; `repark._native.__debug_assertions__ is False`. | **OPEN** | Pending gates. |
| C-002 | DDL goldens cover every F1 type class: for each of the 25 public type classes plus StructField/StructType shapes — `simpleString`, `typeName`, `json`, `repr`, `fromDDL` round-trip (parse → simpleString → parse), `StructType.toDDL` re-parse where the base tree permits, and `json` → `fromJson`/`_parse_datatype_json_value` round-trip — recorded from base `2bebc9da` as bytes. | `test_facade_4_ddl_round_trip.py` + committed `facade_4_type_goldens.json`; record mode `REPARK_FACADE_4_RECORD_GOLDENS=1` refused under `CI`/`GITHUB_ACTIONS`. | **PROVEN** | 47 cases (29 atomic + 16 complex + raw-Arrow probe + session-conf) recorded under a pinned UTC/LTZ session. Red-first: `AssertionError: missing golden facade_4_type_goldens.json`. Byte-identical compare after record (3 passed). Record-mode-refused-in-CI pin green. |
| C-003 | Arrow answers are golden for the schema set (flat 7-type, 50-column wide mixed, `struct<array<map>>` depth 3, decimal (10,2)/(38,18)/(38,0), timestamp tz/ntz/session-tz, interval and char/varchar): `repark_type_to_arrow` Arrow spellings and `struct_type_from_arrow` class+simpleString answers; an `isinstance` pin asserts every conversion answer is a public `repark.spark.types` class. | Same test file + golden; isinstance pin. | **PROVEN** | Per-case `arrow` and `arrow_schema_back` golden fields plus an 18-field raw-Arrow probe (`uint`, `date64`, `time64`, `duration`, `month_day_nano_interval`, `decimal256`, `dictionary`, non-UTC tz). isinstance pin asserts `repark.spark.types` module identity on every answer. |
| C-004 | Mutation proof: a one-line change to a conversion answer turns the golden red; restore leaves it green. | Scratch edit, red output pasted, `git checkout` restore. | **PROVEN** | `repark_type_to_arrow` `pa.timestamp("us", tz="UTC")` → `pa.timestamp("us")` red: `changed=['struct_flat7', 'struct_timestamp_variants', 'struct_wide50', 'timestamp']`. `git checkout` restore → 3 passed. No product file in the commit. |
| C-005 | Release baseline: warmup + 5 reps, medians, idle-box wait (no cargo/rustc/maturin) before each cell, `systemd-run --user --scope -p MemoryMax=8G` with `OPENBLAS_NUM_THREADS=8`. Cells: (a) `repark_type_to_arrow` per schema with per-call µs plus a spy count of calls per `createDataFrame`/`collect`/`to_arrow`/`show` of a 1e5 frame; (b) `struct_type_from_arrow` round-trip; (c) DDL parse (`fromDDL`/`_parse_datatype_string`) and DDL write (`simpleString`/DDL token) walls; (d) cProfile cumulative share of conversion in `df.schema`, `createDataFrame(pandas)`, `spark.read.csv(inferSchema)` at 1e5. Plain statement whether any cell is a wall (≥5 % of an end-to-end wall, or ≥1 ms per user call). | `docs/perf/facade-4-types-baseline-2026-09-14.md` + committed runner under `docs/perf/facade-4-types-baseline-2026-09-14/`. | **OPEN** | Pending. |
| C-006 | Three-table agreement census measured by calling each table: for timestamps (tz, ntz, session-tz non-UTC), decimals (precision/scale edges, 38 overflow), nested nullability (struct field, array element `containsNull`, map `valueContainsNull`), date, binary, char/varchar, intervals — what (i) the facade `types.py` conversions, (ii) `_csv_smart` rungs, (iii) the reader lattice + Rust `spark_ddl_type_name`/`arrow_type_key` each answer. Every disagreement is a row with the concrete input and the three answers; none fixed. | Census section below; probe script under `docs/perf/facade-4-types-baseline-2026-09-14/`. | **PROVEN** | `census_probe.py` run under the 8 GiB scope: 8 agree rows, 21 measured disagreement rows (D1–D21) including the reader-vs-DESCRIBE split on `float32`/`binary` and the bidirectional `containsNull`/`valueContainsNull` drop. |
| C-007 | Step-1 target list: either (A) a measured wall and the Rust move that removes it, or (B) "no wall: ship as the correctness consolidation" naming the exact conversion entry points step 1 routes through one Rust table, plus the census disagreements needing an owner ruling (each a question with the three answers and a lean). | Step-1 section below. | **OPEN** | Pending. |
| C-008 | Gates green: the card's named pins unedited (`test_types_1.py`, `test_types_simple_string.py`, `test_types_x2_census.py`, `test_cast_failure_parity.py`, `test_a3_cast_vocab.py`), the FACADE-2 guard (`test_facade_2_column_display_goldens.py`, `test_facade_2_group2_no_python_assembly.py`), the new pins, `make verify`, `test_production_file_size.py`. | Commands and counts in Evidence. | **OPEN** | Pending. |

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

Pending — filled by C-007.

## Evidence

Pending — filled as clauses close.
