# Unit ledger — FACADE-4 · type conversions in Rust — steps 0+1

**Date:** 2026-09-14 · **Branch:** `perf/facade-4-s0` (C-001..C-008), `perf/facade-4-s1` (C-009..) · **Base:** `2bebc9da`
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
| C-002 | DDL goldens cover every F1 type class: for each of the 25 public type classes plus StructField/StructType shapes — `simpleString`, `typeName`, `json`, `repr`, `fromDDL` round-trip (parse → simpleString → parse), `StructType.toDDL` re-parse where the base tree permits, and `json` → `fromJson`/`_parse_datatype_json_value` round-trip — recorded from base `2bebc9da` as bytes. | `test_facade_4_ddl_round_trip.py` + committed `facade_4_type_goldens.json`; record mode `REPARK_FACADE_4_RECORD_GOLDENS=1` refused under `CI`/`GITHUB_ACTIONS`. | **PROVEN** | 45 cases (30 atomic + 13 complex + raw-Arrow probe + session-conf) re-recorded under a pinned UTC/LTZ session; json/fromDDL answers now record `containsNull`/`valueContainsNull`. Red-first: `AssertionError: missing golden facade_4_type_goldens.json`. Byte-identical compare after record (3 passed). Record mode refused under any non-empty `CI`/`GITHUB_ACTIONS` (`CI=1` pinned). |
| C-003 | Arrow answers are golden for the schema set (flat 7-type, 50-column wide mixed, `struct<array<map>>` depth 3, decimal (10,2)/(38,18)/(38,0), timestamp tz/ntz/session-tz, interval and char/varchar): `repark_type_to_arrow` Arrow spellings and `struct_type_from_arrow` class+simpleString answers; an `isinstance` pin asserts every conversion answer is a public `repark.spark.types` class. | Same test file + golden; isinstance pin. | **PROVEN** | Per-case `arrow` and `arrow_schema_back` golden fields — `arrow_schema_back` snapshots `struct_type_from_arrow` of the F1 structs' own fields with recursive nullability — plus a 23-field raw-Arrow probe (`int8`/`int16`/`float32`/`uint16`/`uint32`/`uint64`, `date64`, `time64`, `duration`, `month_day_nano_interval`, `decimal256`, `dictionary`, non-UTC tz, `null`, non-null list/map items). Session-tz case runs on a fresh `America/New_York`+NTZ session and records `active_session_time_zone()` = `America/New_York`. isinstance pin asserts `repark.spark.types` module identity on every answer. |
| C-004 | Mutation proof: a one-line change to a conversion answer turns the golden red; restore leaves it green. | Scratch edit, red output pasted, `git checkout` restore. | **PROVEN** | `repark_type_to_arrow` `pa.timestamp("us", tz="UTC")` → `pa.timestamp("us")` red: `changed=['struct_flat7', 'struct_timestamp_variants', 'struct_wide50', 'timestamp']`. `git checkout` restore → 3 passed. Committed `mutation_probe.py` proves each critic counterexample red: `fromJson` forcing flags `True` → `changed=['array_int_no_null','map_str_int_no_null','struct_nested3']`; inbound `int8`/`int16` → `IntegerType`, preserving Arrow item nullability, and dropping inner struct nullability → each `changed=['arrow_probe_schema_back']`; restore → `baseline_matches_golden: true`. No product file in the commit. |
| C-005 | Release baseline: warmup + 5 reps, medians, idle-box wait (no cargo/rustc/maturin) before each cell, `systemd-run --user --scope -p MemoryMax=8G` with `OPENBLAS_NUM_THREADS=8`. Cells: (a) `repark_type_to_arrow` per schema with per-call µs plus a spy count of calls per `createDataFrame`/`collect`/`to_arrow`/`show` of a 1e5 frame; (b) `struct_type_from_arrow` round-trip; (c) DDL parse (`fromDDL`/`_parse_datatype_string`) and DDL write (`simpleString`/DDL token) walls; (d) cProfile cumulative share of conversion in `df.schema`, `createDataFrame(pandas)`, `spark.read.csv(inferSchema)` at 1e5. Plain statement whether any cell is a wall (≥5 % of an end-to-end wall, or ≥1 ms per user call). | `docs/perf/facade-4-types-baseline-2026-09-14.md` + committed runner under `docs/perf/facade-4-types-baseline-2026-09-14/`. | **PROVEN** | Release native (`__debug_assertions__ False`), loads 2.26–2.44 at cell starts. `repark_type_to_arrow` 7.1–59.9 µs/call; six-name spy (`--spy-only` re-run): nested-StructType create = 16 `repark_type_to_arrow` + 6 each `_sql_type_to_arrow`/`fromDDL`/`_parse_datatype_string` (≈0.16 ms of a 2,089 ms wall); `df.schema` = 1 `fromDDL` + 1 `_parse_datatype_string` (timestamp field); pandas create/collect/to_arrow/show = 0 on all six names. `struct_type_from_arrow` 8.2–99.3 µs; DDL parse ≤125 µs (wide50), write ≤25 µs. cProfile share: df.schema 0.033/0.066 ms, createDataFrame(pandas) 0/1,650 ms, csv infer 0.14/2,270 ms. **No wall.** |
| C-006 | Three-table agreement census measured by calling each table: for timestamps (tz, ntz, session-tz non-UTC), decimals (precision/scale edges, 38 overflow), nested nullability (struct field, array element `containsNull`, map `valueContainsNull`), date, binary, char/varchar, intervals — what (i) the facade `types.py` conversions, (ii) `_csv_smart` rungs, (iii) the reader lattice + Rust `spark_ddl_type_name`/`arrow_type_key` each answer. Every disagreement is a row with the concrete input and the three answers; none fixed. | Census section below; probe script under `docs/perf/facade-4-types-baseline-2026-09-14/`. | **PROVEN** | `census_probe.py` re-run under the 8 GiB scope (`census_answers.json`): 18 agree rows (one concrete input each), 24 measured disagreement rows (D1–D24) including the `uint64`→`bigint` split, `_csv_smart`→`TimestampNTZType` under NTZ, the reader `null`→`void` surface, and the offset-literal `string`/`timestamp` split. |
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

One concrete input per row; `—` = the table has no entry point for that
input. Full probe JSON: `census_answers.json` beside the probe.

| Input | (i) facade types.py | (ii) _csv_smart | (iii) reader + Rust |
|---|---|---|---|
| Arrow `timestamp[us, tz=UTC]` | `TimestampType` | `—` | `timestamp`; DESCRIBE `timestamp` |
| Arrow `timestamp[us, tz=America/New_York]` | `TimestampType` | `—` | `timestamp` (any tz → `timestamp`) |
| Arrow `timestamp[s, tz=UTC]` | `TimestampType` | `—` | `timestamp` |
| Arrow `date32` | `DateType` | `—` | `date`; DESCRIBE `date` |
| Arrow `date64` | `DateType` | `—` | `date` |
| Arrow `int64` | `LongType` (`bigint`) | `—` | `bigint` (type_key `long`); DESCRIBE `bigint` |
| Arrow `float64` | `DoubleType` | `—` | `double`; DESCRIBE `double` |
| Arrow `bool` | `BooleanType` | `—` | `boolean` |
| Arrow `string` | `StringType` | `—` | `string` |
| Arrow `decimal128(10,2)` | `DecimalType(10,2)` | `—` | `decimal(10,2)`; DESCRIBE `decimal(10,2)` |
| csv literal `'2024-01-02'` | `—` | `date` rung → `DateType` | infer `date` |
| csv literal `'true'` | `—` | `bool` rung → `BooleanType` | infer `boolean` |
| csv literal `'1.5e3'` | `—` | `float64` rung → `DoubleType` | infer `double` |
| csv literal `'9999999999'` | `—` | `int64` rung → `LongType` | infer `bigint` |
| csv literal `'abcdefgh'` | `—` | `string` rung → `StringType` | infer `string` |
| csv literal `'DEADBEEF'` | `—` | `string` rung → `StringType` | infer `string` |
| csv literal `'interval 1 year'` | `—` | `string` rung → `StringType` | infer `string` |
| csv literal 39-digit integer | `—` | `float64` rung → `DoubleType` (precision >38 refuses decimal) | infer `double` |

`char(8)`/`varchar(32)` DDL tokens exist only on table (i) (`fromDDL` →
`CharType`/`VarcharType`; `repark_type_to_arrow` → `string`) — `_csv_smart`
reads such a token as string content and the reader has no char/varchar
surface — so there is no cross-table agree row; the facade answers are bound
by the goldens.

### Disagree — every row measured, none fixed

| # | Input | (i) facade types.py | (ii) _csv_smart | (iii) reader + Rust |
|---|---|---|---|---|
| D1 | Arrow `timestamp[us]` (tz-naive) | `TimestampNTZType` (`timestamp_ntz`) | naive literal `'2024-01-02 03:04:05'` → `TimestampType` (`timestamp`) | `arrow_type_key`/`df.schema` `timestamp_ntz`; csv infer `timestamp` |
| D2 | session `spark.sql.timestampType=TIMESTAMP_NTZ`, naive literal `'2024-01-02 03:04:05'` | `default_timestamp_data_type()` → `TimestampNTZType` | `timestamp` rung → `TimestampNTZType` (measured under the NTZ session) | csv infer still answers `timestamp` under an NTZ session |
| D3 | csv literal `'1.23'` | — | `decimal128` rung → `DecimalType(3,2)` | csv infer → `double` |
| D4 | csv literal `decimal(38,18)`-shaped | — | `decimal128` rung → `DecimalType(18,18)` (precision = literal digits) | csv infer → `double` |
| D5 | csv literal `'42'` (int32 range) | — | `int32` rung → `IntegerType` (`int`) | csv infer → `bigint` |
| D6 | Arrow `decimal256(76,10)` (precision >38) | `_arrow_type_to_repark` → `DecimalType(76,10)`; `repark_type_to_arrow` **refuses** | — | `arrow_type_key`/`df.schema` `decimal(76,10)` |
| D7 | Arrow `binary` / `large_binary` | `BinaryType` (`binary`) | — | `df.schema`/`type_key` `string`; **DESCRIBE `binary`** |
| D8 | Arrow `float32` | `FloatType` (`float`) | — | `df.schema`/`type_key` `double`; **DESCRIBE `float`** |
| D9 | Arrow `int8` / `int16` | `ByteType`/`ShortType` (`tinyint`/`smallint`) | — | `int` (scan and DESCRIBE collapse to `int`) |
| D10 | Arrow `uint8`/`uint16`/`uint32` | `StringType` | — | `int` (type_key and df.schema) |
| D11 | Arrow `duration[us]` | `StringType` | — | `type_key` `Duration(Microsecond)`; df.schema `string` |
| D12 | Arrow `month_day_nano_interval` | `StringType` | — | `type_key` `Interval(MonthDayNano)`; df.schema `string` |
| D13 | Arrow `time64[us]` | `StringType` | — | `type_key` `Time64(Microsecond)`; df.schema `string` |
| D14 | `DayTimeIntervalType`/`YearMonthIntervalType` | `repark_type_to_arrow` → `string`; `fromDDL` **refuses** `'interval day to second'`/`'interval year to month'`; `interval` → `CalendarIntervalType` | — | — |
| D15 | struct field `nullable=False` | preserved by `struct_type_from_arrow`; `toDDL` emits `req INT NOT NULL`; `fromDDL` **refuses** its own `NOT NULL` output | — | `type_key` preserves `false` |
| D16 | `ArrayType(int, containsNull=False)` | `repark_type_to_arrow` → `list<item: int32>` (item nullable — flag **dropped**); arrow `list<int32 not null>` → `containsNull=True` (flag **dropped** inbound) | — | df.schema `array<int>` (flag invisible on the surface) |
| D17 | `MapType(str,int, valueContainsNull=False)` | `repark_type_to_arrow` → `map<string, int32>` (value nullable — flag **dropped**); arrow non-null value → `valueContainsNull=True` | — | df.schema `map<string,int>` |
| D18 | Iceberg `Float32` column read back | — | — | `DESCRIBE` `float` vs `session.table(...).dtypes` `double` — two surfaces inside (iii) disagree with each other |
| D19 | Iceberg `Binary` column read back | — | — | `DESCRIBE` `binary` vs `session.table(...).dtypes` `string` |
| D20 | Arrow `dictionary` | `StringType` | — | (not probed end-to-end; `arrow_type_key` retains the dictionary value type) |
| D21 | Arrow `null` | `NullType` (`void`) | — | type_key `Null`; `df.schema`/`dtypes` `void` |
| D22 | Arrow `uint64` | `StringType` | — | `bigint` (type_key `long`; df.schema `bigint`) |
| D23 | csv literal `'2024-01-02 03:04:05+05:00'` (offset timestamp) | — | `string` rung → `StringType` | csv infer → `timestamp` |
| D24 | csv literal 38-digit integer | — | `decimal128` rung → `DecimalType(38,0)` | csv infer → `double` |

Notes on the disagreements:

- D7/D8/D18/D19 show the reader side is itself two tables: the physical scan
  widens `Float32`→`Float64` and decodes `Binary`→`Utf8` before
  `arrow_type_key` ever runs, while `spark_ddl_type_name` names the stored
  Iceberg type. A single Rust table must decide which of those two answers a
  given entry point is promising.
- D1/D2: under an NTZ session the facade default and the `_csv_smart`
  timestamp rung agree (`TimestampNTZType`), and the reader lattice agrees on
  Arrow `timestamp[us]` → `timestamp_ntz`; only the CSV `inferSchema` path
  normalizes every timestamp literal to `timestamp`, ignoring
  `spark.sql.timestampType=TIMESTAMP_NTZ`.
- D3/D4/D5/D24: the `_csv_smart` rungs (decimal/int32/int64) are strictly
  more precise than what `spark.read.csv(inferSchema)` actually answers
  (`double`/`bigint`) — the smart rungs are not the table that serves the
  public infer path. D23 is the same shape for timestamps: the offset
  literal `'2024-01-02 03:04:05+05:00'` is `string` to `_csv_smart` but
  `timestamp` to the served infer path.
- D10/D22: the reader widens `uint8`/`uint16`/`uint32` to `int` but `uint64`
  to `bigint` (type_key `long`) — a single "unsigned → int" answer would
  misstate `uint64`.
- D15/D16/D17: nullability survives inbound at every struct-field level
  (`struct_type_from_arrow`/`_arrow_type_to_repark` keep `field.nullable`)
  and outbound where a schema field carries `pa.field(nullable=…)`, but is
  silently dropped at the array-element and map-value level both ways, and
  for nested struct fields outbound (`repark_type_to_arrow(StructType)`
  emits `pa.struct` tuples without nullability). `toDDL`'s `NOT NULL`
  output is refused by `fromDDL` — the facade's own DDL write/read pair
  does not round-trip.
- D6: `decimal256(76,10)` survives `arrow → facade type` but cannot go back to
  `decimal128`; the reader keeps `decimal(76,10)` on the surface — a type the
  facade cannot materialize an Arrow column for.

## Step-1 target list

**Verdict: (B) — no wall: ship as the correctness consolidation.** The
baseline ([docs/perf/facade-4-types-baseline-2026-09-14.md](../../../docs/perf/facade-4-types-baseline-2026-09-14.md))
measures every conversion at ≤134 µs per call, zero calls on
`collect`/`to_arrow`/`show` across all six spied names (`df.schema` makes one `fromDDL` and one
`_parse_datatype_string` call, L-008), and 0.000–0.006 % share of the
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

1. **Timestamps under CSV `inferSchema`.** Literal `'2024-01-02 03:04:05'`
   under an NTZ session: facade session default → `TimestampNTZType`;
   `_csv_smart` rung → `TimestampNTZType` (it calls
   `default_timestamp_data_type()`); served csv infer → `timestamp` even
   under an NTZ session. The split is infer vs the other two tables.
   *Should the unified table honor `spark.sql.timestampType` on the CSV
   infer path?* Lean: yes — both other tables distinguish ntz, and the
   session knob is the facade's own contract.
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
4. **Unsigned ints.** Arrow `uint8`/`uint16`/`uint32`: facade `StringType`;
   reader `int`. Arrow `uint64`: facade `StringType`; reader `bigint`
   (type_key `long`). *Does the unified table widen unsigned to the signed
   family (`int`/`bigint`) or keep the `string` degradation?* Lean:
   reader's `int` for `uint8`/`uint16`/`uint32` and `bigint` for `uint64` —
   `string` loses the numeric type the user can already see via the reader.
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

## Critic remediation — Grok Critic-3 review of PR #579 (NEEDS_REMEDIATION)

Report: `/tmp/oc-worker/f-crit4/report.md` (read-only probes beside it). All
eleven findings remediated in place on this branch; product code untouched.

| Finding | Severity | Defect | Fix commit |
|---|---|---|---|
| L-001 | P1 | D10 collapsed `uint64` into `int` | `0df5b17b` — D10 split; D22 carries `uint64` → `bigint`/`long`; all four widths in the reader lattice |
| L-002 | P1 | D2 (ii) `—`; Q1 misattributed the split | `0df5b17b` — `csv_smart_rungs_ntz` measured after the NTZ flip: `TimestampNTZType`; D2/Q1 rewritten (infer vs the other two) |
| L-003 | P1 | D21 (iii) `—` though the reader surfaces null | `0df5b17b` — `pa.null()` probed: type_key `Null`, `df.schema`/`dtypes` `void` |
| L-004 | P1 | Agree rows mixed inputs; offset-literal disagreement hidden | `0df5b17b` — 18 single-input Agree rows; D23 (offset literal `string` vs `timestamp`), D24 (38-digit `decimal(38,0)` vs `double`) added |
| L-005 | P2 | `containsNull`/`valueContainsNull` not golden | `02f83bf7` — flags recorded on json/fromDDL answers and array/map fields; fromJson-forces-True mutation reds |
| L-006 | P2 | inbound element/inner-struct nullability not golden | `02f83bf7` — recursive `_struct_shape`; `arrow_schema_back` snapshots F1 struct fields directly; preserve/drop mutations red |
| L-007 | P2 | inbound `int8`/`int16` unpinned | `02f83bf7` — `pa.int8()`/`pa.int16()`/`pa.float32()`/`pa.uint16()`/`pa.uint32()` in the probe; int8/int16→IntegerType mutation reds |
| L-008 | P2 | spy covered one name; 0-calls non-binding | `afe8745c` — six-name spy on all module bindings + `--spy-only` leg; `df.schema` = 1 `fromDDL`/`_parse_datatype_string`, nested create = 16/6/6/6, doc corrected |
| L-009 | P2 | session-tz golden never carried `America/New_York` | `02f83bf7` — fresh NY+NTZ session after the UTC stop; records `active_session_time_zone()` = `America/New_York` + a session-zone wall clock |
| L-010 | P3 | case count `47` vs JSON's `45` keys | `02f83bf7` — C-002 corrected to 45 (30 atomic + 13 complex + 2) |
| L-011 | P3 | CI refusal exact-`"true"` only | `02f83bf7` — any non-empty `CI`/`GITHUB_ACTIONS` counts; `CI=1` and `CI=""` legs pinned |

Re-check (Grok critic-logic round 2 on `f93c6928`): **PASS**, report `/tmp/oc-worker/f-crit4/report.md`. Residual P3
notes, carried to step 1 and not fixed here:

| Finding | Severity | Note | Disposition |
|---|---|---|---|
| R2-P3-1 | P3 | D1 annotates a naive CSV literal in column (ii) of an Arrow `timestamp[us]` row; under LTZ that literal's `_csv_smart` and infer answers agree, and the NTZ split is D2 | done — step-1 D1 pins only the Arrow `timestamp[us]` input; the naive literal's NTZ legs are pinned on D2 (`rung_ntz`, `csv_infer_ntz`) |
| R2-P3-2 | P3 | D20 dictionary is still not probed through the reader lattice; the facade and golden pin inbound `StringType` | done — the step-1 D20 pin probes `pa.dictionary` through the reader lattice (type_key `Dictionary(Int32, Utf8)`, `df.schema`/`dtypes` `string`) |
| R2-P3-3 | P3 | Q2 still names `_csv_smart` `string` beside Arrow `binary`; the hex literal is a different input | wording fixed with the Q2 ruling |
| R2-P3-4 | P3 | the step-1 blurb claimed zero calls on `df.schema` | fixed in this commit |
| R2-P3-5 | P3 | inbound `valueContainsNull=False` is pinned on the Spark `MapType` JSON path, not on a raw Arrow non-null map value field | done — the step-1 D17 pin adds a raw `pa.map_` whose value field is non-nullable and asserts the inbound `valueContainsNull=True` drop |

## PROPOSITION LEDGER — FACADE-4 step 1 — 2026-09-14

Step 1 is the correctness consolidation: one Rust conversion table every path
reads, with **every surface keeping today's answer byte-for-byte** — no D-row
is unified, no refusal class, message or DDL spelling changes. Residue rows
name the entry points left on their Python path.

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-009 | S1-0 pins first: one parametrised characterization case per census Agree row AND per D1–D21 row, measuring the answer each table gives today (facade `_arrow_type_to_repark`/`repark_type_to_arrow`/`fromDDL`/`toDDL`/`struct_type_from_arrow`, `_csv_smart` rungs, reader `logical_schema_fields`/`df.schema`/`dtypes`/csv `inferSchema`/`DESCRIBE`, NTZ session conf). Green on the step-0 base; a scratch mutation per family (timestamp, decimal, nullability) turns a row red. | `test_facade_4_census_pins.py` green on base; three pasted reds; restore green. | **PROVEN** | 29 rows green on the s0 base. Mutations: `TimestampNTZType()`→`TimestampType()` reds D1 (`'TimestampType:timestamp' != 'TimestampNTZType:timestamp_ntz'`); `DecimalType(p,s)`→`DecimalType(10,2)` reds D6; `field.nullable`→`True` reds D15. All restored. |
| C-010 | S1a table + shims: `crates/repark-spark/src/type_table.rs` owns the `SparkDataType`/`SparkField` descriptor (mirrors the 25 public facade classes), Arrow↔descriptor conversion in both directions, DDL/simpleString parse and write, and the distinct surfaces (`Describe` vs `LogicalKey` Arrow names, engine cast token, DDL token, SQL marker, CSV rung row, timestamp defaults). `spark_ddl_type_name`/`spark_ddl_type_name_at_depth` and `arrow_type_key` delegate to it; pure Rust, no PyO3, no `repark-sql` edge. | `cargo test -p repark-spark -p repark-python` green unedited; DAG check green. | **PROVEN** | `cargo test -p repark-spark` → 942+5+13+1+1+7+23+10 passed, 0 failed; `cargo test -p repark-python` → 74+25 passed. Existing `spark_type_names`/`arrow_type_key` assertions unedited — byte-identical outputs. |
| C-011 | S1a Arrow legs through the table: `type_bridge.rs` binds descriptor↔dict and Arrow↔`"arrow_schema"` capsules both directions; `types._arrow_type_to_repark`, `struct_type_from_arrow`, `repark_type_to_arrow` route through `spark_descriptor_from_arrow_type`/`_schema`/`arrow_type_capsule_from_descriptor`. Public classes and `isinstance` identity unchanged; FFI-uncarriable inputs keep Python paths (C-015). | Step-0 goldens + census pins + `test_types_*` green unedited on the rewired path. | **PROVEN** | `test_facade_4_ddl_round_trip` (3) + `test_facade_4_census_pins` (29) + `test_types_1` + `test_types_simple_string` + `test_types_x2_census` + `test_cast_failure_parity` + `test_a3_cast_vocab` + FACADE-2 guard: 202 passed, 7 skipped on the release native. |
| C-012 | S1b DDL parse/write through the table: `_parse_datatype_string`/`DataType.fromDDL`, `simpleString`, `_engine_type`, `_datatype_to_ddl_token`, `StructType.toDDL` and `create_dataframe_values._data_type_to_sql_type` delegate to `spark_descriptor_from_ddl`/`simple_string`/`engine_token`/`ddl_token`/`struct_field_ddl`/`sql_marker`. Every refusal keeps its class and message; `json`/`fromJson`/`_parse_datatype_json_value` stays Python (residue — C-015). | Golden bytes + census refusal rows green unedited; error-path probes match base spellings. | **PROVEN** | 47-case golden corpus byte-identical; probes: `'a int, b string not null'`, `'bogus'`, `'map<int>'`, ``'`weird name` int'``, `'a:x'`, `'a b c'` all refuse `ValueError: cannot parse datatype: …` / `cannot parse map type: …` exactly as base; `'a int'` → `struct<a:int>`, `''` → `struct<>` as base. |
| C-013 | S1c the other two tables read it: `_csv_smart.rung_to_spark_type`/`rung_to_engine_cast`/`rung_to_sql_cast` read `csv_rung_descriptor`/`csv_sql_cast_token`; `timestamp_type.default_timestamp_arrow_type`/`default_timestamp_data_type` read `default_timestamp_descriptor` + Arrow capsule; `create_dataframe_inference._sql_type_to_arrow` resolves flat atomic tokens through `sql_token_to_arrow_capsule`. D3–D5/D1–D2 pins stay green. | Census pins + facade suite green. | **PROVEN** | Rung probes: `int64` → spark `bigint`/engine `long`/sql `bigint` (D3); `decimal128` p38s18 → `decimal(38,18)`; `bool`/`int32`/`float64`/`date`/`timestamp`/unknown all match base. `default_timestamp_arrow_type` → `timestamp[us, tz=UTC]` under LTZ. |
| C-014 | S1d no regression: `run_baseline.py` re-run on this branch and on the base `/tmp/f-types4` release native, same box, back to back, idle builders before each cell, 8 GiB scope. Bar: no end-to-end cell slower by >5 %, no per-call conversion slower by >1 ms. Thinned line counts of `types.py`/`_csv_smart.py`/`timestamp_type.py` recorded; the two refreshed `test_production_file_size` body-hash baselines named (`_data_type_to_sql_type`, `_sql_type_to_arrow`). | `docs/perf/facade-4-types-step1-2026-09-14.md` before/after table; bars hold. | **PROVEN** | `docs/perf/facade-4-types-step1-2026-09-14.md`: both runs under the 8 GiB scope on one box, medians of 5 behind idle-box waits. Every end-to-end wall within ±5 % (worst +3.5 % `to_arrow`, most negative); worst per-call conversion +196.5 µs (wide50 round-trip) vs the 1 ms bar; DDL parse faster than base on every schema. Line counts: `types.py` 1834→1833, `_csv_smart.py` 899→868, `timestamp_type.py` 97→99. Body-hash baselines refreshed for the two moved bodies only. First-pass `fromDDL` 100–300× regression traced to per-call regex compilation, fixed with a `OnceLock` pattern cache inside the step. |
| C-015 | Residue rows — every entry point left on its Python path, with the reason byte-identity cannot ride the table: (1) `json`/`jsonValue`/`fromJson`/`_parse_datatype_json_value` — JSON shape plus `__COLLATIONS` metadata walk is facade policy; (2) descriptor trees containing a foreign `DataType` subtype — `_nested_token_python`/`_ddl_token_python`/`_sql_type_token_python` keep today's container spellings and leaf fallbacks; (3) decimals outside the Arrow FFI scale envelope (`pa.decimal128(10,300)` accepts where arrow-rs cannot represent) — `_arrow_type_to_repark_python`/`_struct_type_from_arrow_python`/`_repark_type_to_arrow_python` keep pyarrow's own acceptance/refusal bytes; (4) collation refusal — `refuse_evaluated_collation` stays Python policy in `_data_type_to_sql_type`; (5) `_sql_type_to_arrow` decimal and nested spellings — the `PySparkTypeError` wrap stays on the Python parse route. | Census pins cover each residue class; fallbacks named in the residue table below. | **PROVEN** | D16 (`list<item: int32 not null>` arrow-in), D6/D9 (decimal edges) and the unknown-subtype `toDDL`/`simpleString` paths are pinned and green; no refusal class or message moved. |

| C-016 | S1-remediation P1-DTYPES (Grok S2-21): `dtypes` regressed +106 % wide50 / +80 % flat7 / +16.7 % nested3 because every `simpleString()` paid a descriptor-dict build + PyO3 crossing + Rust re-parse. Fix: the atomic classes' `simpleString`/`_engine_type` answers are Python-side constants and parameter derivations in `_type_table.py::_ATOMIC_TYPE_ROWS`, byte-identical to the Rust table (MRO scan so foreign subclasses keep the inherited base answer); nested trees compose in Python over the same answers because one nested descriptor FFI (~15 µs on nested3 `mid`) alone breaks the +5 % bar. Agreement pin: `test_atomic_tokens_agree_with_rust_table` checks every atomic class against `simple_string_from_descriptor`/`engine_token_from_descriptor`. | `dtypes`/`df.schema`/`printSchema`/DESCRIBE within +5 % of base on flat7/wide50/nested3 (measured in the step-1 perf doc surface table); the agreement pin green. | **PROVEN** | Surfaces re-measured base vs branch back to back (see the S1d doc's surface table): flat7 dtypes 10.45→10.35 µs (−1 %), wide50 60.48→61.67 µs (+2 %), nested3 32.00→26.69 µs (−17 %); `df.schema` and `printSchema` within +5 % on all three; DESCRIBE unchanged (Rust-side). 30 census pins + focused type gates green; `types.py` baseline ratcheted 1833→1639 after the bridge moved to `_type_table.py`. |
| C-017 | Step-0 round-2 dispositions R2-P3-1/2/5 land on the corrected D1–D24 census: D1 rewritten to the single Arrow `timestamp[us]` input; D2 gains the `_csv_smart` rung leg measured under an NTZ session (`rung_ntz` surface); `uint64` moved out of D10 into new D22 (reader `bigint`/type_key `long` vs facade `StringType`); D17 gains the raw `pa.map_` non-null-value-field leg (inbound `valueContainsNull=True`); D20's dictionary column is probed through the reader lattice; D21 gains its reader leg (`Null` type_key, `void` schema); new D23 (offset literal `'2024-01-02 03:04:05+05:00'`: `string` rung vs `timestamp` infer) and D24 (38-digit literal: `decimal(38,0)` rung vs `double` infer); missing Agree rows added for `timestamp[s, tz=UTC]`, Arrow `string` and the 39-digit literal (A9–A11). | All 36 census-pin tests green (11 Agree + 24 disagree + the atomic agreement pin); every expected answer measured live on this branch. | **PROVEN** | `test_facade_4_census_pins.py` — 36 passed; every expected tuple measured against the release native before pinning. |
| C-018 | S1-remediation P2-DICT (Grok S2-21): the descriptor crossed PyO3 as a `PyDict` tree with un-interned keys/kind tags and owned `String` extraction per node. Fix: every key and kind tag interned via `pyo3::intern!` on both directions of `type_bridge.rs`; inbound `kind`/collation extract as `Cow<'_, str>` (borrowed compare); the missing-key refusal keeps its `type-table descriptor is missing key "…"` bytes. Companion fix found by the re-measurement: `_type_table.py`'s decode rebuilt its kind→class map and re-imported `types` per recursive node (~30 µs on nested3's `mid`, `df.schema` +50 %) — the maps and module handle now cache. | `dtypes`/`df.schema`/`printSchema` within +5 % of base on flat7/wide50/nested3; reviewer micro table and tracemalloc peaks re-measured back to back; census pins green. | **PROVEN** | Surface table (branch vs base, back to back, load ~1.9): flat7 dtypes −0.3 %, wide50 +1.8 %, nested3 −17.8 %; `df.schema` −1.2 %/+0.7 %/−47.1 %; `printSchema` −1.0 %/+0.8 %/−29.0 % — all inside +5 %. Micro (µs, base→branch): `repark_type_to_arrow` 9.4→27.3 flat7 / 60.2→170.6 wide50 / 16.5→31.0 nested3; `struct_type_from_arrow` 14.7→25.8 / 99.0→156.5 / 20.3→30.5; `fromDDL` 16.4→11.5 / 125.4→69.5 / 32.6→13.7 (faster than base everywhere); `toDDL` 3.3→15.1 / 24.3→97.4 / 6.4→16.7; `simpleString` 3.0→3.9 / 18.5→21.5 / 2.7→8.4. Tracemalloc peaks/200 calls (base→branch): `struct_type_from_arrow` 2.0→2.2 KB flat7 / 12.3→34.8 KB wide50 / 2.5→2.8 KB nested3; `repark_type_to_arrow` 0.4→2.0 / 1.5→20.9 / 0.6→5.8 KB — the `PyDict` tree is the wire format's inherent allocation. |
| C-019 | S1-remediation P3-COLLATION (Grok S2-21): `SparkString.collation` was an owned `String` allocated `to_string()` per string node. Fix: the field is `Cow<'static, str>`; `DEFAULT_COLLATION` is `pub` and every default path (Arrow `Utf8`/`LargeUtf8`/`Utf8View` and catch-all arms, `atomic_type_from_name`, `sql_type_from_token` fallback, `csv_rung_type` fallback, the bridge inbound default) borrows it; only a parsed custom collation owns. | Byte-identical outputs: `simple_string`/`ddl_token`/`engine_token` answers unchanged for default and `string collate NAME` paths; census pins and Rust tests green. | **PROVEN** | `StringType("UTF8_LCASE").simpleString()` → `string collate UTF8_LCASE`, default → `string`; `fromDDL("string collate UTF8_LCASE")` round-trips; 36 census-pin tests and `cargo test -p repark-spark -p repark-python` green (0 failed). |
| C-020 | L-001 (critic, `/tmp/oc-worker/f-crit4s1/report.md`): `parse_ddl`/`sql_type_from_token` saturated integer parameters above `i64::MAX`. Fix: `parse_py_int` uses checked multiply/add and returns `TypeTableError::IntegerOverflow` ("integer parameter beyond i64 range"); `parse_atomic_token` propagates instead of `.ok()`-swallowing; `type_bridge::table_error_to_py` maps it to `PyOverflowError`; `_type_table._parse_datatype_string` routes that refusal (and any non-printable text) to `_parse_datatype_string_python`, so value, `simpleString` and refusal equal base's unbounded Python parse. | `test_facade_4_step1_remediation.py::test_fromddl_integer_params_beyond_i64` green — 9 cases including field-list and boundary legs. | **PROVEN** | Red-first: all 5 original repros failed pre-fix (`decimal(9223372036854775808,0)` → `decimal(9223372036854775807,0)` saturation). Post-fix `fromDDL` answers `decimal(9223372036854775808,0)`/`char(9×40)`/`time(…)`/`varchar(…)` and `struct<a:decimal(9223372036854775808,0)>` byte-identical to base; `decimal(9223372036854775807,0)` stays on the Rust path. |
| C-021 | L-002 (critic): `_atomic_token` answered the canonical table token for pass-through subclasses where base's dynamic `type(self).typeName()` answered e.g. `mybin`. Fix: the atomic table carries five per-class answers (descriptor head, `simpleString`, `_engine_type`, SQL marker, DDL marker); an MRO scan resolves the tabled ancestor's row; `_inherited_type_name` reproduces the dynamic fallback only for classes without an explicit `simpleString`/`typeName` override; `_leaf_simple`/`_leaf_engine`/`_leaf_ddl` dispatch nested fallback leaves identically. | `test_subclass_tokens_match_base_golden` green — pass-through subclass of every one of the 25 public classes across `simpleString`/`typeName`/`_engine_type`/`jsonValue`/`toDDL`-leaf. | **PROVEN** | Golden recorded from `/tmp/f-types4` (base release): `MyBinary.simpleString()`→`mybinary`/`_engine_type`→`binary`/DDL leaf `BINARY`; `MyNull`→`void` (constant `typeName`); `StructField` subclass keeps the `TypeError` answer. 26 parametrised cases green; red-first showed 18 of 25 classes diverging on `simpleString`. |
| C-022 | L-003 (critic): `_sql_type_token_python`'s leaf emitted engine tokens (`int`, `long`) where base's `isinstance` chain emitted SQL markers (`INT`, `BIGINT`, `DECIMAL(10,2)`, `VOID`, `ARRAY<INT>`, `STRUCT<a:INT>`). Fix: the leaf consults the row table's SQL-marker column (`_atomic_token(data_type, 2)`) before the engine/`callable`/`raise` tail, mirroring base's marker chain including its no-arm classes. | `test_sql_type_leaf_markers_for_subclasses` green — 7 reproductions. | **PROVEN** | Red-first: `INT`/`BIGINT`/`TINYINT`/`VOID`/`DECIMAL(10,2)`/`ARRAY<INT>`/`STRUCT<a:INT>` pins failed as engine tokens. Post-fix byte-identical to base's chain; classes base had no arm for still fall to `_engine_type()`. |
| C-023 | L-004 (critic): `python_repr` differed from Python `repr()` on control characters in refusal messages. Fix: `_parse_datatype_string` routes any input containing a non-`isprintable()` code point to the Python parse residue, so every refusal message carries `repr()`'s exact bytes by construction; `python_repr` becomes unreachable for non-printable input and its printable-domain quote logic already matched `repr`. | `test_fromddl_refusal_repr_bytes` green — 14 code-point cases. | **PROVEN** | Red-first: C0 controls, `x\x1cy`, `\x7f`, C1 `\x80`/`\x9f`, U+200B and the three quote-shape cases failed. Post-fix `cannot parse datatype: '\x00'` … `'\\u200b'` / `"it's"` byte-identical to base. |
| C-024 | L-005 (critic): `repark_type_to_arrow` of an `ArrayType` nested ≥64 deep refused where base returned the type (PyArrow C-Data recursion ceiling). Fix: the outbound call wraps the FFI export in `try/except → _repark_type_to_arrow_python`; inbound `_arrow_type_to_repark`/`struct_type_from_arrow` already had the equivalent fallback. | `test_arrow_nesting_depth_ceiling_fallback` green — depths 63/64/70 on `repark_type_to_arrow`, `_arrow_type_to_repark` and `struct_type_from_arrow`. | **PROVEN** | Red-first: depths 64 and 70 refused (`Recursion level in ArrowSchema struct exceeded`); 63 passed. Post-fix all three depths return identical types on all three surfaces. |
| C-025 | L-006 (critic): mutation proof that C-021's MRO contract bites — removing the `cls.__mro__[1:]` scan from `_atomic_token` must turn the subclass golden and marker pins red. | Pasted red run in evidence; restore green. | **PROVEN** | Mutation `row = rows.get(cls); if row is None: return None` → `test_subclass_tokens_match_base_golden` red on 18 classes + `test_sql_type_leaf_markers_for_subclasses` red on all 7 cases (`STRUCT<a:myint>` vs `STRUCT<a:INT>`); restore → 61 passed. |
| C-026 | PY-P2-002 (Python reviewer, `/tmp/oc-worker/f-rev4-py/report.md`): read-side conversions built a `PyDict` descriptor tree in Rust then walked per-node dict lookups in Python. Fix: `type_bridge` emits tagged tuples (`("array", element, contains_null)` etc.) and `_descriptor_to_datatype` indexes positions — no per-node key lookup. Nested `_sql_type_to_arrow` keeps the Python route: the only native token-to-Arrow call (`sql_token_to_arrow_capsule`) answers `string` for nested spellings through its catch-all arm rather than refusing, and `arrow_type_capsule_from_descriptor` reads the dict wire shape the DDL emitter no longer produces — no correct native nested route exists. | Census pins + DDL round-trip + facade suite green on the tuple wire shape. | **PROVEN** | 36 census + 3 round-trip + 61 remediation tests green; `timestamp_type.default_timestamp_arrow_type` rerouted through `default_timestamp_data_type` + `repark_type_to_arrow` because its native descriptor feed became a tuple. |
| C-027 | PY-P2-003 (Python reviewer): `rung_to_engine_cast`/`rung_to_sql_cast` constructed a public `DataType` only to re-encode it. Fix: new `csv_engine_token` `#[pyfunction]` answers the engine token straight from `csv_rung_type`; `rung_to_sql_cast` binds `csv_sql_cast_token` through the cached accessor; `rung_to_spark_type` still returns the class. | `test_csv_rung_cast_tokens_agree_with_rust_table` green — 9 rung rows. | **PROVEN** | Every rung's `csv_engine_token` answer equals `rung_to_spark_type(resolution)._engine_type()` and `csv_sql_cast_token(engine)` — including the two decimal-rung shapes and `unknown`. |
| C-028 | PY-P3-001/002/004 (Python reviewer): parameterised `jsonValue` formats locally instead of dispatching `simpleString`; native calls bind once through `_type_table._native_function` (cached `getattr` on the lazy module); the wide-decimal and nested-decimal pre-walks drop because FFI refusals cover the same inputs with identical fallback answers (measured: p=50 outbound and decimal256 scale-200 inbound refuse identically through the C interface); `repark_type_to_arrow` checks the `DecimalType` precision/scale FFI bound before building a descriptor, message unchanged. | `test_wide_decimal_conversions_take_python_fallback` + census pins green; subclass golden unchanged. | **PROVEN** | `DecimalType(10,300)` → `decimal128(10, 300)`, `DecimalType(76,10)` → `ValueError: precision should be between 1 and 38`, `pa.decimal256(76,200)` → `DecimalType(76,200)` — all byte-identical to base through the FFI-refusal fallback. |

## Step-1 remediation findings — Grok S2-21 (`/tmp/oc-worker/f-rev4-rs/report.md`)

| Finding | Severity | Defect | Fix commit | Clause |
|---|---|---|---|---|
| P1-DTYPES | P1 | per-column `simpleString` descriptor FFI regressed `dtypes` +106 % on wide50 | `f71b02fa` | C-016 |
| P2-DICT | P2 | descriptor dict tree used un-interned keys/kind tags and owned `String` extraction per node | `f0ada689` | C-018 |
| P3-COLLATION | P3 | `SparkString.collation` allocated a `String` per node for the default collation | `f0ada689` | C-019 |
| P3-PARSE-DEPTH | P3 | `parse_ddl` recurses with no depth cap while the name surface bounds at 32 | not changed — filed as card DDL-DEPTH-1 (ruling Q-R13-13: no new refusal in this run) | — |
| P3-ERR-DOUBLE | P3 | spaced refusal paths (`interval day to second`, `NOT NULL`) attempt `parse_field_list` first, then `parse_complex_or_atomic`, allocating a discarded error message | not changed — removing the discarded allocation needs an `Option`-returning duplicate of the fallible parse family, not a local change; both paths already beat base (`fromDDL` −30 % to −58 %) | — |

## Step-1 round-2 findings — critic logic (Grok, `/tmp/oc-worker/f-crit4s1/report.md`)

| Finding | Severity | Defect | Fix commit | Clause |
|---|---|---|---|---|
| L-001 | P1 | `parse_ddl` saturated integer parameters above `i64::MAX` | `4c8d25af` | C-020 |
| L-002 | P1 | `_atomic_token` returned canonical tokens for pass-through subclasses | `4c8d25af` | C-021 |
| L-003 | P2 | `_sql_type_token_python` leaf emitted engine tokens, not SQL markers | `4c8d25af` | C-022 |
| L-004 | P2 | `python_repr` differed from Python `repr()` on control characters | `4c8d25af` | C-023 |
| L-005 | P2 | `repark_type_to_arrow` refused Arrow nesting depth ≥64 where base returned | `4c8d25af` | C-024 |
| L-006 | P2 | mutation proof required for the C-016/C-021 MRO contract | proof in C-025 | C-025 |

## Step-1 round-2 findings — Python reviewer (`/tmp/oc-worker/f-rev4-py/report.md`)

| Finding | Severity | Defect | Fix commit | Clause |
|---|---|---|---|---|
| PY-P2-002 | P2 | read-side descriptor tree paid per-node `PyDict` lookups; nested `_sql_type_to_arrow` double-hopped through classes | `d82c5cc6`; nested kept Python — no correct native route exists | C-026 |
| PY-P2-003 | P2 | `rung_to_engine_cast`/`rung_to_sql_cast` built a `DataType` only to re-encode | `d82c5cc6` | C-027 |
| PY-P3-001 | P3 | parameterised `jsonValue` dispatched through `simpleString` | `4c8d25af` | C-028 |
| PY-P3-002 | P3 | per-call `_native` attribute lookups; FFI-covered wide-decimal pre-walks | `4c8d25af` | C-028 |
| PY-P3-003 | P3 | perf doc cell (c) `toDDL` rows labelled a refusal series | the measurement commit | C-014 evidence refresh |
| PY-P3-004 | P3 | `DecimalType(76,10)` refusal built a descriptor before the bound check | `4c8d25af` | C-028 |

## Rulings applied (run 14b)

Owner standing instruction (act on the report recommendations), applied by the run-14b orchestrator on 2026-09-14.
Step 1 stays the byte-identical consolidation (R13-D-5); the D-row rulings below are binding targets for the unification
slices that follow step 1, each a one-row table change with its census pin.

| Ruling | Applied as |
|---|---|
| Q-R13-1 timestamps (D1/D2) | `read.csv(inferSchema)` honors `spark.sql.timestampType=TIMESTAMP_NTZ` — unification slice |
| Q-R13-2 binary (D7/D19) | Spark oracle measured 2026-09-14 (PySpark 4.1.2, Parquet): `binary` on schema, dtypes and DESCRIBE → `binary` on every surface — unification slice |
| Q-R13-3 float32 (D8/D18) | `float` on every Spark surface (oracle: `float` on schema, dtypes, DESCRIBE) — unification slice |
| Q-R13-4 unsigned ints (D10/D22) | widen to the signed family: `uint8`/`uint16` → `int`, `uint32`/`uint64` → `bigint` — unification slice |
| Q-R13-5 small signed ints (D9) | `tinyint`/`smallint` — unification slice |
| Q-R13-6 decimal > 38 (D6) | cap at 38 on the facade surface and refuse beyond it as Spark does; the reader keeps its internal spelling — unification slice |
| Q-R13-7 CSV literal lattice (D3–D5, D23, D24) | adopt Spark's ladder, retire the `_csv_smart` rung table into the Rust lattice — its own pinned slice |
| Q-R13-8 intervals (D14) | keep the `string` degradation and the `fromDDL` refusal; Spark-shaped interval classes are their own card |
| Q-R13-9 nested nullability (D15–D17) | preserve `containsNull`/`valueContainsNull`; `fromDDL` accepts `NOT NULL` — unification slice |
| Q-R13-10 unsupported Arrow types (D11–D13, D20, D21) | keep `string` on the facade surface; `type_key` keeps its internal spelling |
| Q-R13-11 critic-logic on step 0 | applied by run 13 (critic PASS before #579 merged) |
| Q-R13-12 FACADE-5 zero-column phantom rows | fix in FACADE-5 step 1 as its own pinned slice with a changelog line (not this unit) |
| Q-R13-13 DDL parse depth cap | no new refusal in this run; filed as a card (DDL-DEPTH-1) |
| Q-R13-14 µs-scale per-call cost of one table | accepted only while every end-to-end cell stays within ±5 % of main on a release build; measured numbers below |

Owner question carried to the hand-back: should the DDL parser gain a
recursion-depth cap? Premise: base's Python parser accepts DDL nested deeper
than 32 (the 32 bound belongs to the `typeName` name surface, not the parser),
so any cap changes a refusal byte-for-byte. Lean: keep uncapped for step 1 —
the recursion is linear and heap-bound, matching base.

## Evidence

- C-001: `git diff 2bebc9da..HEAD -- python/repark/src crates/` empty (pins,
  ledger, docs and maps only); `repark._native.__debug_assertions__ is False`
  in `.venv` for every timed cell (runner asserts the same header).
- C-002/C-003/C-004: `test_facade_4_ddl_round_trip.py` (3 tests) +
  `facade_4_type_goldens.json` (45 cases = 30 atomic + 13 complex +
  `arrow_probe_schema_back` + `session_conf_ny_ntz`); mutation proofs
  recorded in C-004 (`mutation_probe.py` JSON: all four critic mutations
  red, baseline and restore green).
- C-005: `run_baseline.py` JSON — cells (a)–(d) tables in the baseline doc;
  verdict NO WALL (every call ≤134 µs; conversion 0.000–0.006 % of the 1e5-row
  walls; `df.schema` 50 % share of a 66 µs op ≈ 0.03 ms per call). The cell
  (a) call-count leg was re-run as `--spy-only` with the six-name spy:
  `df.schema` calls `DataType.fromDDL`/`_parse_datatype_string` once per op,
  nested create calls all three nested-schema entry points; collect/
  to_arrow/show/pandas-create stay 0 on all six.
- C-006: `census_probe.py` + committed `census_answers.json` — 18 agree
  rows, D1–D24 disagree rows.
- Gates (2026-09-14):
  `pytest test_types_1 test_types_simple_string test_types_x2_census
  test_cast_failure_parity test_a3_cast_vocab
  test_facade_2_column_display_goldens test_facade_2_group2_no_python_assembly
  test_facade_4_ddl_round_trip test_production_file_size -q`
  → 184 passed, 7 skipped.
  `make verify` → green (fmt, clippy workspace, Rust tests, map sync).
- Commits: `f8e33cca` ledger skeleton; `585ef87a` pins + goldens + mutation
  proof; `6f5608e4` census table + probe; `75405954` baseline doc + runner;
  `fcee0a8c` step-1 target list + evidence; `7b423721` coverage attestation;
  `0df5b17b` census remediation (L-001..L-004); `02f83bf7` pin remediation
  (L-005..L-007, L-009..L-011); `afe8745c` spy remediation (L-008); HEAD
  findings table.

## Coverage attestation — step 0

```yaml
COVERAGE_ATTESTATION:
  pr_unit: facade-4
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Step 0's deliverables map to C-001..C-008 and step 1's to C-009..C-015 (pins, table, Arrow legs, DDL legs, CSV/timestamp tables, residue, perf bar), each PROVEN with pasted evidence.
      artifacts: [task/ledgers/staging/facade-4-ledger.md]
    - id: AT-2
      status: ATTACKED
      evidence: Goldens cover the 25 public type classes plus struct shapes, decimal edges (10,2)/(38,18)/(38,0) and decimal256 above 38, timestamp tz/ntz/session-tz, intervals, char/varchar, uint and dictionary Arrow inputs.
      artifacts: [python/repark/tests/test_facade_4_ddl_round_trip.py, python/repark/tests/facade_4_type_goldens.json]
    - id: AT-3
      status: ATTACKED
      evidence: Refusals are recorded as golden answers (fromDDL of its own NOT NULL output, interval day to second, repark_type_to_arrow of decimal256), and record mode is refused under any non-empty CI or GITHUB_ACTIONS value (CI=1 pinned).
      artifacts: [python/repark/tests/test_facade_4_ddl_round_trip.py]
    - id: AT-4
      status: ATTACKED
      evidence: Step 1's only shared mutable state is the table's process-local OnceLock<Mutex<BTreeMap>> regex cache — a pure-function memo behind one lock, poison-recovering, carrying no session state and reading no env; no global mutable state in the Session sense.
      artifacts: [crates/repark-spark/src/type_table.rs]
    - id: AT-5
      status: N/A
      justification: No privileged action, secret or path handling; goldens are committed JSON beside the test.
    - id: AT-6
      status: ATTACKED
      evidence: The three-table census measures 18 agreements and 24 disagreements (D1-D24) including silent containsNull/valueContainsNull loss in both directions, the uint64 bigint split, csv_smart honoring NTZ while infer does not, and the offset-literal string/timestamp split; none fixed, each an owner question. Step 1 pins every Agree and D-row — 11 agree rows plus D1-D24 — by calling each surface today and changing no answer, refusal class or spelling.
      artifacts: [task/ledgers/staging/facade-4-ledger.md, docs/perf/facade-4-types-baseline-2026-09-14/census_probe.py, docs/perf/facade-4-types-baseline-2026-09-14/census_answers.json, python/repark/tests/test_facade_4_census_pins.py]
    - id: AT-7
      status: ATTACKED
      evidence: Step-1 before/after on the real base release native, same box back to back, medians of 5 under an 8 GiB scope: every end-to-end wall within ±5 % and the worst per-call conversion +196.5 us vs the 1 ms bar.
      artifacts: [docs/perf/facade-4-types-step1-2026-09-14.md, docs/perf/facade-4-types-baseline-2026-09-14/run_baseline.py]
    - id: AT-8
      status: ATTACKED
      evidence: The F1-frozen names are pinned by answer; descriptor-to-class reconstruction in types.py preserves isinstance identity for every public class including VariantType.
      artifacts: [python/repark/tests/test_facade_4_ddl_round_trip.py, python/repark/tests/test_facade_4_census_pins.py]
    - id: AT-9
      status: N/A
      justification: No log-format or diagnosis-path change.
    - id: AT-10
      status: ATTACKED
      evidence: Dropping the tz from pa.timestamp in repark_type_to_arrow redded four golden cases and the restore greened them; the committed mutation_probe.py additionally proves fromJson flag-forcing, inbound int8/int16 widening, Arrow item-nullability preservation, and inner-struct nullability drops each red; step-1 scratch mutations redded the pins in each family — timestamp tz (D1), decimal (D6), list nullability (D15) — and restores greened them; the card's existing pins stay unedited and green.
      artifacts: [python/repark/tests/test_facade_4_ddl_round_trip.py, python/repark/tests/test_facade_4_census_pins.py]
  complete: true
```
