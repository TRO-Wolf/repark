# Unit ledger — EX-22 · v1.1 example backfill, the `types` surface and `DataFrameWriterV2`

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands (the
orchestrator's departure move). This file closes when EX-22 merges, or when the owner closes the
slate row.

**Unit:** EX-22 · **Date:** 2026-09-04 · **Model:** glm-5.3-flash · **Branch:** `docs/ex-22-types-writerv2` · **Base:** `b5827be6`
**Slate:** [briefs/example-backfill.md](../../../briefs/example-backfill.md), EX-22 lane brief (43 roster names). **Ruling:** owner, 2026-08-31, [release-roadmap-2026-08-29.md](../../roadmap/epic-term/release-roadmap-2026-08-29.md) §"v1.1 — Full example documentation (was v0.7)".

**Rubric:** STANDARD. Floor S1. `risk_tier: standard`.

**Writable paths:** `docs/examples/types/`, `docs/examples/io/`, `docs/examples/backlog.txt`,
the `BACKLOG_BASELINE` constant in `scripts/check_example_coverage.py`,
`docs/spark-sql-iceberg-parity.md` §7, `python/repark/tests/test_examples_window_catalog.py`,
lockstep `map.md` files, and this ledger with its `staging/map.md` row. Closed: `crates/`,
`python/repark/src/`, every other `scripts/` line, `.github/`, `STATUS.md`, every other ledger,
`briefs/next-sequence.md`.

## Scope

The roster is all 28 `types.*` backlog names plus all 15 `DataFrameWriterV2.*` backlog names at
base `b5827be6` (camelCase and snake_case aliases are one example each, both names in `COVERS`).
Eleven files cover the 42 names the live oracle measured Spark-equal: eight under
`docs/examples/types/` (the directory and its `map.md` are new) and three under
`docs/examples/io/`. `DataFrameWriterV2.overwrite` has no agreeing arm — repark refuses
conditionally overwrites outright — so it stays on the backlog with §7 `EX-W2-1`; the
empty-source `overwritePartitions` arm is §7 `EX-W2-2` and the `option`/`options` branch/tag arm
is §7 `EX-W2-3`; all three are pinned in `python/repark/tests/test_examples_window_catalog.py`.
Every snake_case spelling and both Arrow helpers measured `hasattr` `False` on live PySpark 4.1.2
and are covered as repark extensions. WriterV2 examples write to a local memory-catalog Iceberg
table (`register_memory_catalog("local", …)` + `CREATE NAMESPACE local.ns`, then
`writeTo("local.ns.…")` on repark; the parity harness's Iceberg engine builder with a local
Hadoop warehouse on the oracle) and read the table back; no cloud catalog is needed, so no name
stays on the backlog for that reason. The four flagged names (`VariantType`, `TimeType`,
`CharType`/`VarcharType`) measured Spark-equal on every construction/display arm.

**Roster (43):** `types.ArrayType`, `types.BinaryType`, `types.BooleanType`, `types.ByteType`,
`types.CalendarIntervalType`, `types.CharType`, `types.DataType`, `types.DateType`,
`types.DayTimeIntervalType`, `types.DecimalType`, `types.DoubleType`, `types.FloatType`,
`types.IntegerType`, `types.LongType`, `types.MapType`, `types.NullType`, `types.ShortType`,
`types.StringType`, `types.StructField`, `types.StructType`, `types.TimeType`,
`types.TimestampNTZType`, `types.TimestampType`, `types.VarcharType`, `types.VariantType`,
`types.YearMonthIntervalType`, `types.repark_type_to_arrow`, `types.struct_type_from_arrow`,
`DataFrameWriterV2.append`, `DataFrameWriterV2.create`, `DataFrameWriterV2.createOrReplace`,
`DataFrameWriterV2.create_or_replace`, `DataFrameWriterV2.option`, `DataFrameWriterV2.options`,
`DataFrameWriterV2.overwrite`, `DataFrameWriterV2.overwritePartitions`,
`DataFrameWriterV2.overwrite_partitions`, `DataFrameWriterV2.partitionedBy`,
`DataFrameWriterV2.partitioned_by`, `DataFrameWriterV2.replace`,
`DataFrameWriterV2.tableProperty`, `DataFrameWriterV2.table_property`,
`DataFrameWriterV2.using`.

**Grouping (11 files, each named for one breath):**

| File | `COVERS` (roster names) | Why these together |
|---|---|---|
| `types/atomic_numeric.py` | `types.IntegerType`, `types.LongType`, `types.ShortType`, `types.ByteType`, `types.FloatType`, `types.DoubleType` | The six numerics: short-form `simpleString` vs `typeName`, the classmethod form, `jsonValue`/`json`, repr. |
| `types/string_and_bool.py` | `types.StringType`, `types.CharType`, `types.VarcharType`, `types.BinaryType`, `types.BooleanType` | The string family: default and collated `StringType`, fixed/variable lengths, binary, boolean. |
| `types/temporal_types.py` | `types.DateType`, `types.TimestampType`, `types.TimestampNTZType`, `types.CalendarIntervalType` | Date/timestamp display names plus the measured day-ordinal conversion round trip. |
| `types/interval_types.py` | `types.TimeType`, `types.DayTimeIntervalType`, `types.YearMonthIntervalType` | Time-of-day precision and the ANSI interval field ranges and display strings. |
| `types/decimal_null_variant.py` | `types.DecimalType`, `types.NullType`, `types.VariantType` | The parameterized decimal, the `void` marker, the `variant` marker. |
| `types/complex_types.py` | `types.ArrayType`, `types.MapType`, `types.StructField`, `types.StructType` | Array/map descriptors, field access (`["v"]`, `[0]`, `fieldNames`, `len`), `add` chaining, `toDDL`, `treeString`, equality, and the explicit-schema `createDataFrame`. |
| `types/datatype_from_ddl.py` | `types.DataType` | The base `typeName` classmethod and three `fromDDL` parse arms. |
| `types/arrow_schema_roundtrip.py` | `types.repark_type_to_arrow`, `types.struct_type_from_arrow` | The two repark-only Arrow helpers: repark type → `pyarrow` type, `pa.Schema` → `StructType`, and the `pa.struct` round trip. |
| `io/writerv2_create.py` | `DataFrameWriterV2.using`, `DataFrameWriterV2.tableProperty`, `DataFrameWriterV2.table_property`, `DataFrameWriterV2.partitionedBy`, `DataFrameWriterV2.partitioned_by`, `DataFrameWriterV2.create` | Builder-chain creates on `local.ns.*` with table properties and identity partitions, read back plus a partition-filter arm. |
| `io/writerv2_replace.py` | `DataFrameWriterV2.createOrReplace`, `DataFrameWriterV2.create_or_replace`, `DataFrameWriterV2.replace` | Full-table rebuilds: createOrReplace twice, the snake spelling, replace-on-existing, each read back. |
| `io/writerv2_append_overwrite.py` | `DataFrameWriterV2.append`, `DataFrameWriterV2.overwritePartitions`, `DataFrameWriterV2.overwrite_partitions`, `DataFrameWriterV2.option`, `DataFrameWriterV2.options` | By-name append from a reordered frame, both dynamic-overwrite spellings, and the storage-option chains. |

## Proposition ledger

| ID | Clause | Proof obligation | Verdict |
|---|---|---|---|
| C-001 | Eleven runnable files under `docs/examples/types/` (new directory with its `map.md`) and `docs/examples/io/` land local examples for the 42 Spark-equal roster names — construction, `typeName`/`simpleString`/`jsonValue`/repr, StructType field access, the Arrow helpers, and an explicit-schema `createDataFrame` — every asserted value measured against live PySpark 4.1.2 before it was written (WriterV2 arms on the harness's Iceberg engine); each script exits 0 under `python <path>` with no network and no JVM; no product file is touched. | The oracle table (43 rows, one per roster name), the eleven scripts each exiting 0 locally, and the recorded gate exit codes. | **PROVEN** |
| C-002 | The 42 covered names leave `docs/examples/backlog.txt` and `BACKLOG_BASELINE` moves down by exactly 42 — 374 → 332 at the dispatch base `b5827be6`, 340 → 298 as shipped after the EX-21 merge — with no other `scripts/` change; `DataFrameWriterV2.overwrite` stays listed; the gate's static half and its `--require-execute` leg both exit 0 (613 covered; 298 backlog; 165 examples). | The gate's own counts line at the dispatch base (537/374/138), at the merged base (571/340/154), and on the shipped tree (613/298/165), plus the red-first provocation below. | **PROVEN** |
| C-003 | A name whose repark answer differs from Spark's is not papered over: `DataFrameWriterV2.overwrite` stays on the backlog with §7 row `EX-W2-1`, the empty-source `overwritePartitions` arm is §7 `EX-W2-2`, the `option`/`options` branch-tag arm is §7 `EX-W2-3`, and repark's current answer for each is pinned in `python/repark/tests/test_examples_window_catalog.py` (one-line forced docstrings, 7 tests passing); no `types.*` name measured divergent. | The three registry rows, the three new pin tests (7 passed), and the oracle table's dropped/arm rows. | **PROVEN** |
| C-004 | This ledger records the roster, the grouping, the red-first provocation, the name-by-name oracle table, and the gates; `staging/map.md` gains the EX-22 row; `docs/examples/map.md`, `docs/examples/io/map.md`, and the new `docs/examples/types/map.md` move in lockstep with the files. | The ledger itself and the lockstep map diffs in the same commits. | **PROVEN** |
| C-005 | The `overwritePartitions`-on-an-unpartitioned-table divergence measured in the round-2 review is filed, not papered over: §7 row `EX-W2-4` records repark's parser leak (`ParseException` where Spark replaces the whole table with no error) with status OPEN and follow-up `WRITERV2-OVERWRITE-UNPART-1` as the fix unit; repark's current answer — the raise and the table still answering `[(1,'a')]` — is pinned in `python/repark/tests/test_examples_window_catalog.py`; the runnable example does not teach the leak (`overwritePartitions` stays covered through its partitioned arm). | The registry row, the new pin test, and the pin's red-first provocation below. | **PROVEN** |

`LOGIC_SCORE` = **5/5 `PROVEN`**.

## Red-first (docs/testing.md "Gate provocation proofs")

Captured on this tree with the eleven example files held outside `docs/examples/` and
`docs/examples/backlog.txt` + `scripts/check_example_coverage.py` restored to main's state at
the merge (the 43 roster rows still listed, `BACKLOG_BASELINE=340`): the static gate exits **0**
(`571 covered; 340 backlog; 154 examples`). **Provocation:** delete all 43 roster rows and set
`BACKLOG_BASELINE` to 297 (`340 − 43`, as if the whole roster were covered) with no example
files present; the gate exits **1** with exactly **43 findings**, one per roster name and no
others. Restoring the unit state — the 43 rows deleted, `BACKLOG_BASELINE` 298, the eleven
files back — returns the gate to **0** (`613 covered; 298 backlog; 165 examples`). Re-run
2026-09-04 on the shipped tree after the EX-21 merge, in the round-2 review.
`pins: ex-22-types-writerv2/C-001, C-002`

Pin provocation, same protocol, for the round-2 `EX-W2-4` pin
(`test_writerv2_overwrite_partitions_unpartitioned_leak`): with the test's `pytest.raises`
guard replaced by a direct `overwritePartitions()` call asserting Spark's `[(5,'z')]` answer,
`.venv/bin/python -m pytest
python/repark/tests/test_examples_window_catalog.py::test_writerv2_overwrite_partitions_unpartitioned_leak -q`
exits **1** — the raised `ParseException('SQL error: ParserError("Expected: an expression,
found: ) at Line: 1, Column: 57")')` fails the test. Restoring the guard returns the run to
**0** (1 passed). The column offset varies with the fixture's table-name length; the pin
matches the stable prefix.

## Oracle (live PySpark 4.1.2, ANSI on, UTC, local[2], JDK zulu-17, TZ=UTC)

Measured with `.venv/bin/python`, `JAVA_HOME=/usr/lib/jvm/zulu-17-amd64`, throwaway scripts under
`scratch/ex22-oracle/` (gitignored, never committed). Leg 1 (JVM-free): the 43-name `hasattr`
classification — all 26 `types.*` classes and the 10 camelCase `DataFrameWriterV2` methods exist
on Spark; the 4 snake_case spellings and `repark_type_to_arrow`/`struct_type_from_arrow` do not
(repark extensions). Legs 2/2b/5: the types value table, identical probes on both engines —
every construction, `typeName` (instance and classmethod), `simpleString`, `jsonValue`, `json`,
repr, collation, `needConversion`, decimal precision/scale, interval field-range, `toDDL`,
`treeString`, `add`-chain, `fromDDL`, and StructType-equality arm matched byte-for-byte, as did
the explicit-schema `createDataFrame` (rows `[(1, Decimal('1.50'), ['a']), (2,
Decimal('2.25'), ['b','c'])]`, schema `struct<k:int,v:decimal(10,2),w:array<string>>`). Legs
3/3b: the WriterV2 table on the Iceberg Hadoop catalog `local` over a local warehouse, identical
arms on both engines — create, createOrReplace, replace, by-name append, dynamic partition
overwrite, storage-option chains, table properties, identity partitions, `using`; the three
measured refusals and the error-class arms are per-row below. `Spark`'s `fromDDL`/`toDDL` need
an active session; repark's are JVM-free with identical answers.
`pins: ex-22-types-writerv2/C-001, C-003`

| Name | Spark value (repr) | repark value (repr) | Kept / dropped | File | Note |
|---|---|---|---|---|---|
| `types.ArrayType` | `'array<int>'`; `ArrayType(IntegerType(), False)`; `{'type': 'array', 'elementType': 'integer', 'containsNull': True}` | same | kept | `types/complex_types.py` | |
| `types.BinaryType` | `'binary'` | same | kept | `types/string_and_bool.py` | |
| `types.BooleanType` | `'boolean'` | same | kept | `types/string_and_bool.py` | |
| `types.ByteType` | `'tinyint'` | same | kept | `types/atomic_numeric.py` | |
| `types.CalendarIntervalType` | `'interval'`; `typeName()` `'interval'` | same | kept | `types/temporal_types.py` | |
| `types.CharType` | `'char(5)'`; `CharType(5)` | same | kept | `types/string_and_bool.py` | flagged name; measured Spark-equal |
| `types.DataType` | `typeName()` `'data'`; `fromDDL('k int, v string')` → `'struct<k:int,v:string>'`, `fromDDL('array<decimal(8,3)>')` → `'array<decimal(8,3)>'`, `fromDDL('struct<a:int,b:string>')` → `'struct<a:int,b:string>'` | same | kept | `types/datatype_from_ddl.py` | Spark `fromDDL`/`toDDL` need an active session; repark answers identically JVM-free |
| `types.DateType` | `'date'`; `toInternal(date(2024,3,15))` `19797`; `fromInternal(19797)` `datetime.date(2024, 3, 15)`; `needConversion()` `True` | same | kept | `types/temporal_types.py` | |
| `types.DayTimeIntervalType` | `'interval day to second'`; fields `(0, 3)`; `DAY` `0`; `(2,3)` → `'interval minute to second'`, `DayTimeIntervalType(2, 3)` | same | kept | `types/interval_types.py` | |
| `types.DecimalType` | `'decimal(10,0)'`; `(10,4)` → `'decimal(10,4)'`, `DecimalType(10,4)`; `precision`/`scale` `10`/`4`; `json` `'"decimal(10,2)"'` | same | kept | `types/decimal_null_variant.py` | |
| `types.DoubleType` | `'double'` | same | kept | `types/atomic_numeric.py` | |
| `types.FloatType` | `'float'` | same | kept | `types/atomic_numeric.py` | |
| `types.IntegerType` | `'int'` simpleString, `'integer'` `typeName`/`jsonValue`, `'"integer"'` json, `IntegerType()` | same | kept | `types/atomic_numeric.py` | |
| `types.LongType` | `'bigint'`; `typeName()` `'long'` | same | kept | `types/atomic_numeric.py` | |
| `types.MapType` | `'map<string,int>'`; `MapType(StringType(), IntegerType(), True)` | same | kept | `types/complex_types.py` | |
| `types.NullType` | `'void'`; `typeName()` `'void'` | same | kept | `types/decimal_null_variant.py` | |
| `types.ShortType` | `'smallint'` | same | kept | `types/atomic_numeric.py` | |
| `types.StringType` | `'string'`; collated `'string collate UTF8_LCASE'`, `StringType('UTF8_LCASE')`; `collation` `'UTF8_BINARY'`; `isUTF8BinaryCollation()` `False` collated | same | kept | `types/string_and_bool.py` | |
| `types.StructField` | `StructField('k', IntegerType(), False)`; `'k:int'`; `{'name': 'k', 'type': 'integer', 'nullable': False, 'metadata': {}}` | same | kept | `types/complex_types.py` | `typeName()` raises on both (Spark `INVALID_TYPENAME_CALL`, repark bare `TypeError` — raise-behavior agrees, error-class text not asserted; out-of-scope note) |
| `types.StructType` | `'struct<k:int,v:decimal(10,2),w:array<string>>'`; full repr; `fieldNames`; `["v"]`/`[0]`/`len`; `toDDL`; `treeString`; `add` chain; equality `True`; explicit-schema `createDataFrame` rows | same | kept | `types/complex_types.py` | |
| `types.TimeType` | `'time(6)'`; `(9)` → `'time(9)'`, `TimeType(9)` | same | kept | `types/interval_types.py` | flagged name; measured Spark-equal |
| `types.TimestampNTZType` | `'timestamp_ntz'`; `typeName()` `'timestamp_ntz'` | same | kept | `types/temporal_types.py` | |
| `types.TimestampType` | `'timestamp'` | same | kept | `types/temporal_types.py` | |
| `types.VarcharType` | `'varchar(10)'` | same | kept | `types/string_and_bool.py` | flagged name; measured Spark-equal |
| `types.VariantType` | `'variant'`; `typeName()` `'variant'` | same | kept | `types/decimal_null_variant.py` | flagged name; measured Spark-equal |
| `types.YearMonthIntervalType` | `'interval year to month'`; `(1,1)` → `YearMonthIntervalType(1, 1)` | same | kept | `types/interval_types.py` | |
| `types.repark_type_to_arrow` | `hasattr` `False` (extension) | `pa.int8()`, `pa.list_(pa.int32())`, `pa.decimal128(10, 2)`, `pa.timestamp('us', tz='UTC')`, `pa.timestamp('us')`, `pa.map_(pa.string(), pa.int32())` | kept | `types/arrow_schema_roundtrip.py` | repark extension, measured on repark |
| `types.struct_type_from_arrow` | `hasattr` `False` (extension) | `'struct<k:int,v:string>'`, non-nullable `k` preserved, `pa.struct([('k', pa.int32()), ('v', pa.string())])` round trip | kept | `types/arrow_schema_roundtrip.py` | repark extension, measured on repark |
| `DataFrameWriterV2.append` | by-name: reordered `(b,a)` frame lands `[(1, 10), (2, 20)]` | same | kept | `io/writerv2_append_overwrite.py` | |
| `DataFrameWriterV2.create` | rows `[(1,'a'),(2,'b')]`; re-create raises `TABLE_OR_VIEW_ALREADY_EXISTS` | same | kept | `io/writerv2_create.py` | exists-arm measured, pinned not taught |
| `DataFrameWriterV2.createOrReplace` | `[(9, 'z')]` | same | kept | `io/writerv2_replace.py` | |
| `DataFrameWriterV2.create_or_replace` | `hasattr` `False` (extension) | `[(11, 'q')]` | kept | `io/writerv2_replace.py` | snake spelling |
| `DataFrameWriterV2.option` | storage-option arm rows `[(1,'a')]`; branch arm: no error, row lands on the default branch, `b1` unchanged | rows same; branch arm raises `UnsupportedOperationException` | kept | `io/writerv2_append_overwrite.py` | repark warns once on non-branch options (loud); branch arm is §7 `EX-W2-3` |
| `DataFrameWriterV2.options` | storage-option arm rows `[(2,'b')]` | same | kept | `io/writerv2_append_overwrite.py` | shares `option`'s branch refusal |
| `DataFrameWriterV2.overwrite` | `overwrite(F.col('id') == 1)` answers `[(1,'aa'), (2,'b')]` | raises `UnsupportedOperationException` unconditionally | dropped | §7 `EX-W2-1` | no agreeing arm exists |
| `DataFrameWriterV2.overwritePartitions` | populated arm `[(2,'b'),(9,'a')]`; empty-source arm: no-op, rows unchanged | populated arm same; empty-source arm raises `AnalysisException` | kept | `io/writerv2_append_overwrite.py` | empty-source arm is §7 `EX-W2-2` |
| `DataFrameWriterV2.overwrite_partitions` | `hasattr` `False` (extension) | `[(2, 'b'), (8, 'a')]` | kept | `io/writerv2_append_overwrite.py` | snake spelling |
| `DataFrameWriterV2.partitionedBy` | identity-partition rows `[(1,'a'),(2,'b')]` + `WHERE cat='a'` filter | same | kept | `io/writerv2_create.py` | |
| `DataFrameWriterV2.partitioned_by` | `hasattr` `False` (extension) | `[(1, 'x')]` | kept | `io/writerv2_create.py` | snake spelling |
| `DataFrameWriterV2.replace` | rows `[(2,'b')]`; missing table raises `TABLE_OR_VIEW_NOT_FOUND` | same | kept | `io/writerv2_replace.py` | missing-arm measured, pinned not taught |
| `DataFrameWriterV2.tableProperty` | rows `[(1,'a'),(2,'b')]` through two chained properties | same | kept | `io/writerv2_create.py` | |
| `DataFrameWriterV2.table_property` | `hasattr` `False` (extension) | `[(1, 'x')]` | kept | `io/writerv2_create.py` | snake spelling |
| `DataFrameWriterV2.using` | `using('iceberg')` chain rows `[(1,'u')]` | same | kept | `io/writerv2_create.py` | non-iceberg refusal is product-disclosed, not taught |

## Out-of-scope observations (measured, not acted on)

- `F.col('s').cast(ByteType())` keeps Spark's values but repark's resulting schema says `int`
  where Spark says `tinyint`. Cast-schema work, not a `types.*` name answer; the cast arms were
  dropped from the examples and no registry row was filed from this unit.
- `DataFrame.to_arrow()` marks every field nullable where Spark's `toArrow()` preserves
  non-nullability (`k: int32 not null`). The DataFrame round-trip arm was dropped from
  `arrow_schema_roundtrip.py` (the direct `pa.schema` arm preserves nullability on repark);
  the `DataFrame.toArrow` roster row belongs to the DataFrame family's measurement.
- `StructField.typeName()` error text differs (Spark `INVALID_TYPENAME_CALL` message vs repark's
  plain `TypeError` text); both raise `TypeError`-family errors, and the example asserts only
  the raise class family's agreement by not asserting the message.

## Gates (2026-09-04, on this tree)

| Command | Exit |
|---|---|
| `.venv/bin/python scripts/check_example_coverage.py --require-execute` | **0** |
| `.venv/bin/python -m pytest python/repark/tests/test_examples_window_catalog.py -q` | **0** (7 passed) |
| `make check-map-sync` | **0** |
| `make check-ledger-grammar` | **0** |
| `make check-ledgers` | **0** |
| `make check-docs-compaction` | **0** |
| `python3 scripts/ledger_lifecycle.py check --base origin/main` | **0** |
| `typos .` | **0** |
| `.venv/bin/ruff check docs/examples python/repark/tests` | **0** |
| `.venv/bin/ruff format --check docs/examples python/repark/tests` | **0** |

Round-2 re-run (2026-09-04, on the shipped tree, after findings F1–F5 landed):

| Command | Exit |
|---|---|
| `.venv/bin/python scripts/check_example_coverage.py --require-execute` | **0** (`613 covered; 298 backlog; 2 exceptions; 165 examples`) |
| `.venv/bin/python -m pytest python/repark/tests/test_examples_window_catalog.py python/repark/tests/test_qi1_idents.py -q` | **0** (42 passed; the pin module carries all four WriterV2 pins) |
| `make check-map-sync` | **0** |
| `make check-ledger-grammar` | **0** |
| `make check-ledgers` | **0** |
| `make check-docs-compaction` | **0** |
| `python3 scripts/ledger_lifecycle.py check --base origin/main` | **0** |
| `typos .` | **0** |
| `.venv/bin/ruff check docs/examples python/repark/tests scripts` | **0** |
| `.venv/bin/ruff format --check docs/examples python/repark/tests scripts` | **0** |

Two structural checks on the branch diff: `git diff origin/main --numstat -- docs/spark-sql-iceberg-parity.md docs/examples/map.md` reads `62 0` on the registry (additions only) and `6 3` on the map (small); added `#` lines in `*.py` beyond `# noqa`: none.

The system `python3` in this clone cannot import `repark._native`; the `--require-execute` leg
runs under `.venv/bin/python`, which resolves `repark` to the sibling checkout of the same base
SHA `b5827be6` (expected for this lane).

Counts line (execute leg):

`example-coverage: 913 public names (catalog=28, column=40, dataframe=150, functions=444, io=42, ml=28, session=41, ta=86, types=32, window=22); 613 covered; 298 backlog; 2 exceptions; 165 examples`

Before this unit: `537 covered; 374 backlog; 138 examples` (at the dispatch base `b5827be6`);
main's tree at the merge: `571 covered; 340 backlog; 154 examples`. On this unit's shipped
tree: `613 covered; 298 backlog; 165 examples` (`BACKLOG_BASELINE` 340 → 298 shipped; 374 → 332
at the dispatch base) — exactly the 42 kept names; `DataFrameWriterV2.overwrite` stays listed.

## Review-gap table (round-1 findings, resolved in-lane)

| Finding | Disposition |
|---|---|
| `register_memory_catalog` refused the examples' relative warehouse path ("must start with `/`") | caught by the examples' own failure on the first local run; the three WriterV2 examples now anchor on `Path.cwd() / name`, which is absolute under the gate's per-script temp cwd |
| the leg-3 `overwrite` probe passed a string condition; Spark parsed it as a column name and raised `UNRESOLVED_COLUMN`, which would have misrecorded Spark's answer | re-measured in leg 3b with `overwrite(F.col("id") == 1)`; Spark answers the conditional overwrite `[(1,'aa'), (2,'b')]` |
| the leg-3 branch arm read only the branch table, leaving Spark's answer ambiguous (row unseen) | leg 3b read main and branch: the row lands on the default branch, `b1` keeps its seed rows; EX-W2-3's Spark bullet states that measured shape |
| leg-4 probe used `arrow_schema.simpleString()`, which `pyarrow.Schema` does not have | the nullability arms (the finding that mattered) printed before the probe error; the example asserts only repark-measured direct arms |

## Review-gap table (round-2 findings, resolved in-lane)

| Finding | Disposition |
|---|---|
| F1 (S1) — eleven `expect()` helpers carried a docstring off the house form | deleted all eleven (eight `types/` examples, three `io/writerv2_*` examples); the house form is one module docstring plus the `main()` one-liner, helpers bare |
| F2 (S2) — the merge unioned `docs/examples/map.md` Contents: io/, session/, window/, catalog/ rows twice, plus the stale combined `types/, ml/` row | resolved to exactly one row per family in main's order with main's text; the branch's `types/` row and the `ml/`-only remaining-EX-1-family row kept; the stale row and every duplicate deleted |
| F3 (S2) — ledger and map counts recorded the pre-merge tree (579/332/149, baseline 374 → 332) | re-recorded against the shipped tree (613/298/165; `BACKLOG_BASELINE` 340 → 298 shipped, 374 → 332 at the dispatch base) in C-002, the counts section, `scripts/map.md`, and `staging/map.md`; the red-first provocation re-run on the shipped tree and its measured numbers recorded (base 571/340/154 at main's merge state; provocation exits 1 with exactly 43 findings; restore 613/298/165 at exit 0) |
| F4 (S2) — the `overwritePartitions`-on-an-unpartitioned-table control was missing and hid an unfiled divergence | filed §7 `EX-W2-4` (OPEN, follow-up `WRITERV2-OVERWRITE-UNPART-1` as the fix unit) directly after EX-W2-3; pinned `test_writerv2_overwrite_partitions_unpartitioned_leak` (asserts the raise on the stable message prefix and the table still answering `[(1,'a')]`); the pin's red-first provocation recorded with new clause C-005 (5/5 PROVEN); the runnable example does not teach the leak. This lane's verbatim re-measure reproduces the leak and its message prefix; the parser column offset follows the fixture's table-name length (57 with the pin's `t_pin_unpart`, 53 with `t_unpart`; the filed row quotes the reviewer's oracle run) |
| F5 (S2) — three measured-equal controls were absent from the examples | the nested array-of-struct `simpleString` arm in `complex_types.py`; the `decimal(39,0)`/`decimal(5,7)` arms in `decimal_null_variant.py`; a second `append()` arm in `writerv2_append_overwrite.py` whose table answers `[(1,'a'),(2,'b'),(3,'c')]` ordered by id; the `--require-execute` gate re-ran green |

## Cost

The GLM (glm-5.3-flash) leg started 2026-09-04: read the contract, the corpus, and the merged
EX-20 ledger; ran five oracle legs (leg 1 `hasattr` JVM-free; legs 2/2b/5 the types value table
and its session-gated extras, one Spark JVM per leg; legs 3/3b the WriterV2 table on the Iceberg
harness engine); wrote the eleven example files, the three registry rows, the pins, the backlog
ratchet and the maps, then committed in slices. Base `b5827be6`.

## Disk

Pickup: `df -h` 634 GB free of 1.8 TB. The oracle scratch lives under the gitignored `scratch/`
(probe scripts plus captured outputs, left gitignored at close); the Spark and repark warehouse
residues land under `scratch/ex22-oracle/wh_*` and the per-run temp dirs (gitignored, removed at
close). `.venv` and the sibling-checkout native module reused; no cargo build, `make develop`
not run.

## Dual-wire

Unchanged by this unit. Static half: `make check-example-coverage` and ci.yml python job
(`./scripts/check_example_coverage.sh`). Execute half: wheels.yml smoke
`python -I scripts/check_example_coverage.py --require-execute` after the packaged wheel is
installed. EX-22 moves only the inventory/backlog ratchet and example files; it moves no wire,
and `.github/` is closed to this unit.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: ex-22-types-writerv2
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: The AST walk emits 913 names across ten families; the 42 Spark-equal roster names are covered by eleven new example files and the oracle table records both engines' values for all 43 roster rows.
      artifacts: [scripts/check_example_coverage.py, docs/examples/inventory.txt, docs/examples/types/atomic_numeric.py, docs/examples/types/string_and_bool.py, docs/examples/types/temporal_types.py, docs/examples/types/interval_types.py, docs/examples/types/decimal_null_variant.py, docs/examples/types/complex_types.py, docs/examples/types/datatype_from_ddl.py, docs/examples/types/arrow_schema_roundtrip.py, docs/examples/io/writerv2_create.py, docs/examples/io/writerv2_replace.py, docs/examples/io/writerv2_append_overwrite.py]
    - id: AT-2
      status: ATTACKED
      evidence: The red-first provocation deleted all 43 roster rows and set the baseline to 297 with no example files; the gate exited 1 with exactly 43 findings, and the backlog is an exact baseline 298 with DataFrameWriterV2.overwrite still listed.
      artifacts: [scripts/check_example_coverage.py, docs/examples/backlog.txt]
    - id: AT-3
      status: ATTACKED
      evidence: The gate's use-check binds types.* on the types door alias and the WriterV2 methods on repark-rooted locals; the examples bind every COVERS name through the real receiver (writer locals, the T alias).
      artifacts: [scripts/check_example_coverage.py, docs/examples/types/complex_types.py, docs/examples/io/writerv2_create.py]
    - id: AT-4
      status: N/A
      justification: The gate is a read-only process over source files and example scripts; no shared mutable engine state.
    - id: AT-5
      status: N/A
      justification: No new execution surface beyond the eleven local examples and the three pin tests; example children drop AWS_* and PYTHONPATH and run against local filesystem only.
    - id: AT-6
      status: N/A
      justification: No engine or python/repark/src product change; the backfill walks public names that already exist.
    - id: AT-7
      status: ATTACKED
      evidence: The static gate is AST-only; example execution is skipped when the native module is absent and required when --require-execute is passed; the red-first provocation ran the AST-only half with exactly 43 findings.
      artifacts: [scripts/check_example_coverage.py]
    - id: AT-8
      status: ATTACKED
      evidence: make ci stays native-build-free with the new examples; the walk adds no import of the facade.
      artifacts: [Makefile, scripts/check_example_coverage.py]
    - id: AT-9
      status: N/A
      justification: Findings print to stderr through the existing reporter; no new log or metric surface.
    - id: AT-10
      status: ATTACKED
      evidence: The pins citation for C-001/C-003 lives in task/ledgers/staging/map.md beside the prior example batches, and the pin tests cite the registry rows in their one-line docstrings.
      artifacts: [task/ledgers/staging/map.md, python/repark/tests/test_examples_window_catalog.py, docs/examples/types/complex_types.py, docs/examples/io/writerv2_create.py, docs/examples/io/writerv2_replace.py, docs/examples/io/writerv2_append_overwrite.py]
  reattested: []
  complete: true
```

## Pointers

- Up: [map.md](map.md)
- Slate: [../../../briefs/example-backfill.md](../../../briefs/example-backfill.md)
- Gate: [../../../scripts/check_example_coverage.py](../../../scripts/check_example_coverage.py)
- Pins: [../../../python/repark/tests/test_examples_window_catalog.py](../../../python/repark/tests/test_examples_window_catalog.py)
- Registry: [../../../docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md) §7 `EX-W2-1`, `EX-W2-2`, `EX-W2-3`
- Siblings: [ex-20-window-catalog-ledger.md](ex-20-window-catalog-ledger.md), [ex-18-dataframe-c-ledger.md](ex-18-dataframe-c-ledger.md)

```yaml
SHIPPED_FLAG_REGISTER:
  pr_unit: ex-22-types-writerv2
  flags: []
  count: 0

DELIVERY_SIGNOFF:
  pr_unit: ex-22-types-writerv2
  artifacts_verified:
    ledger: PASS (C-001..C-005 PROVEN)
    coverage_attestation: PASS (AT-1..AT-10, complete true)
    findings_ledger: PASS (review-gap table carries the four in-lane round-1 resolutions)
    shipped_flag_register: PASS (count 0)
  done_gate: PASS (gates table)
  status_update: v1.1 example backfill, types + DataFrameWriterV2 batch — 42 covered, one divergent stays
  verdict: PENDING
  rejection_route: N/A
```
