# Unit ledger — ICE-TSTZNS-WALL-1 · a `timestamptz_ns` target stores one instant for one wall on every door

**Date:** 2026-10-10 · **Branch:** `fix/ice-tstzns-wall-1` · **Base:** `9b230aed` ·
**Model:** Claude Opus 5.5 (`claude-opus-5-5`) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.** Target: v1.5.5.

**Order:** the owner's ruling of 2026-10-10 (Frontier D9, adopted): fix parity row
[ICE-TSNS-SQL-1-R-008](../../../docs/spark-sql-iceberg-parity.md) as a small follow-up under
the acceptance criteria of ICE-TSNS-MERGE-WALL-1 (R-007), whose ledger held it as the open
clause C-013 and question Q1
([ice-tsns-merge-wall-1-ledger.md](ice-tsns-merge-wall-1-ledger.md)).
**This ledger closes when the unit's pull request merges.**

**Why.** The mirror of R-007. Into a `timestamptz_ns` column INSERT localises a naive wall in
the session zone. `INSERT OVERWRITE`, MERGE insert and update, `overwritePartitions` and
`insertInto(overwrite=True)` stored the same wall as if it were UTC, so one table held two
instants for one input, and `UPDATE` with no `WHERE` raised a raw Arrow error.

## PROPOSITION LEDGER — ICE-TSTZNS-WALL-1 — 2026-10-10

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | Main `9b230aed` is measured before any change and reproduces the defect: 38 cells differ from INSERT and 6 raise a raw Arrow error in the R-007 unit's own matrix; in the R-007 verifier's door matrix over five zones 348 cells differ and 60 raise the raw error; 50 cells panic on an out-of-range `DATE` into a required column. | §1, the scripts and counts. | **PROVEN** | §1. |
| C-002 | Spark 4.1.2 with Iceberg 1.11.0 cannot write the type; the reference is what INSERT stores, and a `zoneinfo` rule reproduces INSERT on main in five zones, gap and overlap walls included. | §2: 300 INSERT cells of the verifier's matrix against the rule. | **PROVEN** | §2: 0 of 300 differ. |
| C-003 | At head every write door stores, for the same source value and session zone, the instant INSERT stores, at full nanosecond precision, in UTC, America/New_York, Asia/Kolkata, Asia/Kathmandu and Australia/Lord_Howe, with the values read from the Parquet files. | The facade matrix (nineteen doors, five zones, four source types) and the gap and overlap test, both read with `pyarrow`; the Rust door matrix (ten doors, both row-level modes, five zones); the verifier's matrix re-run (§4). | **PROVEN** | §4: 0 cells differ from INSERT. |
| C-004 | `UPDATE t SET v = <column>` with no `WHERE` from a `TIMESTAMP`, `TIMESTAMP_NTZ` or `DATE` column stores the rule's instant; main raised the raw Arrow `arguments need to have the same data type`. No raw or un-named error remains on a supply. | The `update_column` cells of the facade matrix; the Rust door `update with no where`; the verifier's matrix (§4). | **PROVEN** | §4: 0 raw errors of 60. |
| C-005 | A value past the nanosecond range answers as INSERT answers on every door: `[CAST_OVERFLOW]` naming `"TIMESTAMPTZ_NS"` under ANSI, NULL without it in a nullable column; into a required column the write refuses and no door panics. | The Rust overflow pin (three source types, ten doors, both ANSI modes, nullable and required); the facade DataFrame pin; the verifier's overflow and required-column probes (§5). | **PROVEN** | §5: 0 panics of 50. |
| C-006 | Controls are identical to main: every `timestamp_ns` cell, every microsecond `TIMESTAMP` and `TIMESTAMP_NTZ` cell on v3 and v2, values, logical type and error text. | The verifier's door matrix over five zones and its overflow and required-column probes, head against main (§6); the R-007 facade file, whose microsecond controls are held against main's recorded cells. | **PROVEN** | §6: 15,630 control cells, 0 moved. |
| C-007 | A nested `timestamptz_ns` leaf is not converted and stores what it stored on main, from every door. | The nested probes on main and at head (§7); the `nested_leaf_cast_sql` unit pin; the R-007 Rust control `a_nested_zoned_or_microsecond_leaf_is_not_guarded`. | **PROVEN** | §7: identical on every door. |
| C-008 | One function names the store kernel of a target (`ntz_store::store_kernel_udf_name`), and the shared conform and store sites ask it; no door is listed. The zoned kernel's return field is nullable where an overflow can answer NULL. | The name and SQL unit pins; the kernel's nullability pin; the diff (§3). | **PROVEN** | §3. |
| C-009 | The native ANSI door resolves the zoned kernel the shared MERGE and UPDATE sites now emit. | An ANSI-door UPDATE, MERGE update and MERGE insert into `timestamptz_ns`. | **PROVEN** | §3.4; mutant M6. |
| C-010 | Hand mutants are each killed by a named pin. | §8. | **PROVEN** | §8. |
| C-011 | The unit's gates are green with real exit codes. | §9. | **PROVEN** | §9. |
| C-012 | Main's recorded cells show the defect the pins fix: in the fixture of the R-007 unit, INSERT follows the rule and 19 cells of the control zone read a wall as UTC, 2 raise the raw error. | `test_the_rule_is_what_insert_stores_and_main_broke_it_on_38_cells`. | **PROVEN** | §1. |
| C-013 | A nested `timestamptz_ns` leaf stores the instant a top-level column stores. | Out of this unit by the brief: nested leaves are not converted. Measured (§7). | **OPEN** | Q1: every door reads a wall as UTC there, one reading, no door disagreeing. Card ICE-TSNS-NESTED-1. |
| C-014 | A NULL into a required column is refused by a named text. | Out of this unit: every column type, INSERT included (§5). | **OPEN** | Q2: Arrow's `declared as non-nullable but contains null values`, on main and at head. |
| C-015 | Under ANSI, `UPDATE t SET v = <out-of-range TIMESTAMP literal>` with no `WHERE` reports INSERT's error class. | The mirror of R-012, which is open for the naive type (§5). | **OPEN** | Q3: `[CAST_INVALID_INPUT]` where INSERT raises `[CAST_OVERFLOW]`; main raised the raw Arrow error. |

## 1. Main, measured before any change (C-001, C-012)

Build of `9b230aed`, values read back from the Parquet files by the scripts named.

**The R-007 unit's own matrix** (`_record_ice_tsns_merge_wall_1_main.py --whole`, three zones,
nineteen doors, five source types): of the 285 door cells of the `timestamptz_ns` target,
38 store an instant that differs from INSERT's, 6 raise the raw Arrow error, and the rest
equal INSERT or take a named refusal (a string source through a door that refuses strings,
and the cast spellings of parity row R-010). The 38 are nineteen door and source pairs in the
two zones that are not UTC:

| Door | Sources that differ |
|---|---|
| `INSERT OVERWRITE` | `TIMESTAMP_NTZ`, `timestamp_ns` |
| MERGE insert (column, `*`, literal) | `TIMESTAMP_NTZ`, `timestamp_ns` |
| MERGE update (column, `*`, literal) | `TIMESTAMP_NTZ`, `timestamp_ns` |
| `overwritePartitions`, `insertInto(overwrite=True)` | `TIMESTAMP_NTZ`, `timestamp_ns` |
| `UPDATE … SET v = <column>` with no `WHERE` | `timestamp_ns` |

The 6 are `UPDATE … SET v = <column>` with no `WHERE` from a `TIMESTAMP` and a
`TIMESTAMP_NTZ` column, in each zone: `Arrow error: Invalid argument error: arguments need to
have the same data type`. Example, America/New_York, the wall
`2026-01-02 03:04:05.123456789`: INSERT stores `1767341045123456789`, MERGE stores
`1767323045123456789`.

**The R-007 verifier's door matrix** (`mx.py`, target `tz_ns`; 47 doors, 7 source types, 15
moments, UTC, America/New_York, Asia/Kolkata, Asia/Kathmandu, Australia/Lord_Howe; 1,645
cells), each cell against `INSERT … SELECT` of the same source in the same session:

| Door family | Equals INSERT | Differs | Raw Arrow error | Named refusal |
|---|---|---|---|---|
| INSERT, append, `REPLACE WHERE` | 250 | 0 | 0 | 90 |
| `INSERT OVERWRITE`, `INSERT … BY NAME` | 52 | 48 | 0 | 40 |
| MERGE insert | 78 | 72 | 0 | 60 |
| MERGE update and upsert | 156 | 144 | 0 | 120 |
| `UPDATE` with a `WHERE` | 78 | 32 | 0 | 170 |
| `UPDATE` with no `WHERE` | 24 | 16 | 60 | 40 |
| DataFrame overwrite | 39 | 36 | 0 | 30 |
| **Total** | **677** | **348** | **60** | **550** |

Ten more cells have no INSERT reference (INSERT refuses that source). The 348 are 87 cells
in each of the four zones that are not UTC, from `TIMESTAMP_NTZ`, `timestamp_ns` and `DATE`
sources; UTC has none. The 60 raw errors are the unfiltered `UPDATE` from a `TIMESTAMP`, a
`TIMESTAMP_NTZ` and a `DATE` column, 20 each.

**Out of range** (`misc2.py`, five zones, both ANSI modes): a `DATE` past the nanosecond
range into a required `timestamptz_ns` column panics (`attempt to multiply with overflow`,
caught at the Python boundary) through `INSERT OVERWRITE`, `INSERT … BY NAME`, MERGE insert
(column and literal) and `overwritePartitions`: 50 cells. A `TIMESTAMP` or `TIMESTAMP_NTZ`
past the range raises a raw Arrow `Arithmetic overflow` on those doors where INSERT raises
`[CAST_OVERFLOW]`.

## 2. The reference (C-002)

Spark 4.1.2 with Iceberg 1.11.0 cannot create, read or write the type (the R-007 ledger's
§2), so the reference is INSERT, which did not change. The pins compute it with `zoneinfo`:
an instant source is kept; a wall is read in the session zone with the offset in force
before a transition, so a wall inside a gap moves forward by the gap and a wall inside an
overlap takes its earlier instant. That rule was checked against what INSERT stores on main
in the verifier's matrix: 300 cells (`INSERT … VALUES` and `INSERT … SELECT`, `timestamp_ns`
and `TIMESTAMP_NTZ` sources, fifteen moments, five zones), 0 differ, Lord Howe's half-hour
gap and overlap included.

## 3. The fix (C-008, C-009)

### 3.1 One function names the store kernel

`ntz_store::wall_cast_udf_name` named the kernel of a wall target: microsecond
`TIMESTAMP_NTZ` and, since R-007, `timestamp_ns`. A `timestamptz_ns` target fell through it
at every shared site to a plain Arrow cast, which reads a naive wall as UTC.
`ntz_store::store_kernel_udf_name` now answers for the three types, and for
`Timestamp(ns, Some(_))` it names INSERT's own kernel, `__repark_cast_timestamptz_ns__`
(`NS_INSTANT_CAST_UDF_NAME`, pinned equal to the registered function). No kernel was added.
The sites ask it: `update_cast::store_assignment_cast_sql` (MERGE UPDATE SET, top-level
assignment), the MERGE INSERT projection, the identity-UPDATE projection, and
`zone_store_frame` (the overwrite doors).

### 3.2 Why naming the type at the matchers was not the whole fix

Four more things had to change, each found by measuring, not by reading:

- `zone_stores` builds its targets from the Iceberg schema and did not map
  `PrimitiveType::TimestamptzNs` at all, so the overwrite doors never reached a matcher.
  `wall_kernel_reads` also had to accept a zoned nanosecond target.
- The INSERT hook conformed an `UPDATE` plan for naive targets only (a filter R-007 added to
  hold this type as a control). That filter is what left `UPDATE` with no `WHERE` on the raw
  Arrow error; it is removed.
- The zoned kernel declared its result non-nullable for a non-null source. An out-of-range
  literal with ANSI off answers NULL, so once the other doors reached the kernel fifteen
  cells of the overflow matrix raised Arrow's `declared as non-nullable but contains null
  values` where INSERT stores NULL. The return field is now nullable unless the source is
  already a zoned nanosecond timestamp, the rule R-007 gave the naive kernel.
- A nested field assignment (`SET t.st.v = …`) reaches the same store-cast function with the
  leaf's type. Naming the zoned type there would have converted a nested leaf through one
  door only. `update_cast::nested_leaf_cast_sql` keeps what main did for a leaf, and
  `merge/nested_assign.rs` calls it below the top level (§7).

### 3.3 The narrowing refusal names the type

The unfiltered `UPDATE` refuses a `SET` value narrowed from nanoseconds to microseconds
(R-017, from R-007). It now runs for a zoned target too and names it:
`Cannot safely cast … "TIMESTAMP" to "TIMESTAMPTZ_NS" … give the NULL beside it the type
timestamptz_ns`.

### 3.4 The ANSI door

`AnsiDialect::on_session_built` registers the zoned kernel beside the naive one; without it
an UPDATE or MERGE into `timestamptz_ns` on that door would answer `UNRESOLVED_ROUTINE`
where main stored a value (`update_and_merge_into_timestamptz_ns_store_the_instant`).

## 4. Head over the matrices (C-003, C-004)

Build of the fix, the same scripts as §1, five zones.

| Matrix | Cells | Equals INSERT | Differs | Raw Arrow error | Named refusal |
|---|---|---|---|---|---|
| The verifier's `mx.py`, target `tz_ns`, main | 1,645 | 677 | 348 | 60 | 550 |
| The same at head | 1,645 | 1,085 | **0** | **0** | 550 |
| The R-007 unit's matrix, target `tz_ns`, main (three zones) | 285 | 178 | 38 | 6 | 60 |
| The same at head | 285 | 222 | **0** | **0** | 60 |

In each matrix the cells that moved are exactly the ones that were wrong or raw: 408 of the
verifier's (348 + 60) and 44 of the unit's (38 + 6). The 550 named refusals are main's, cell
for cell: a string source through a door that refuses strings, and the cast spellings of
parity row R-010. Ten cells of the verifier's matrix and three of the unit's have no INSERT
reference (INSERT refuses that source through `SELECT`); they did not move.

The pins read the values from the Parquet files: the facade matrix holds nineteen doors in
five zones against the `zoneinfo` rule, with non-zero digits below the microsecond for the
nanosecond sources; the Rust door file holds ten doors in both row-level modes in five zones
against `INSERT … SELECT` in the same session. The merge-on-read `UPDATE … WHERE` cells, which
differed on main for wall sources while the copy-on-write ones did not, are among the fixed.

## 5. Out of range (C-005, C-014, C-015)

The verifier's overflow matrix (`bx.py`) and required-column probe (`misc2.py`), five zones,
both ANSI modes, zoned target:

| Answer | Main | Head |
|---|---|---|
| A Rust panic caught at the Python boundary | 230 | **0** |
| A raw Arrow overflow or type error | 1,040 | **0** |
| `[CAST_OVERFLOW]` naming `"TIMESTAMPTZ_NS"` | 423 | 1,137 |

The 230 panics are the 50 of §1 (a required column) and 180 of the overflow matrix (a `DATE`
past the range into a nullable column through the same doors). Under ANSI every door now
raises INSERT's `[CAST_OVERFLOW]`. With ANSI off a nullable column stores NULL on every door,
as INSERT does; no cell of the overflow matrix raises Arrow's non-nullable error for a
nullable column (15 did on the first draft, before the kernel's return field was widened,
§3.2).

**A required column** (600 cells): 120 store, 175 raise `[CAST_OVERFLOW]`, 300 refuse the
NULL (Arrow's `declared as non-nullable but contains null values`, or `UPDATE cannot assign
NULL to required column` on the unfiltered `UPDATE`), 5 are C-015. No panic and no raw
overflow remains. Refusing a NULL in a required column by Arrow's text is INSERT's answer on
main for every column type, an explicit NULL included (C-014, Q2). Six INSERT cells per ANSI-off
zone changed text and nothing else: `INSERT … SELECT` and `append` of an out-of-range value
into a required `timestamptz_ns` column end with the sentence `INSERT … VALUES` already ended
with (`… contains null valuesfailed to convert uuid text columns into bytes`), because the
kernel now declares a nullable result. They refused before and refuse now.

**C-015.** Under ANSI, `UPDATE t SET v = TIMESTAMP '3000-01-01 …'` with no `WHERE` raises
`[CAST_INVALID_INPUT]` on the literal's text, as the naive type does (parity row R-012); main
raised the raw Arrow error.

## 6. Controls (C-006)

Head against main, the same scripts and zones, compared cell for cell in value, Arrow type and
error text:

| Script | Control targets | Cells | Moved |
|---|---|---|---|
| `mx.py`, five zones | `timestamp_ns`; `TIMESTAMP_NTZ` and `TIMESTAMP` on v3 and v2 | 8,225 | 0 |
| `bx.py`, five zones, both ANSI modes | `timestamp_ns` | 3,360 | 0 |
| `misc2.py`, five zones, both ANSI modes | required and nullable `timestamp_ns`, required `TIMESTAMP_NTZ` and `TIMESTAMP`, four side probes | 2,440 | 0 |
| The R-007 unit's matrix, three zones, carries included | the same five targets | 1,605 | 0 |

15,630 control cells, **0 moved**. The R-007 facade file still holds the microsecond targets
against main's recorded cells; `timestamptz_ns` left its control list because it is this
unit's subject.

## 7. Nested `timestamptz_ns` leaves (C-007, C-013)

Measured on main before the change, as the brief asks, with the verifier's `nx.py` (shape
`struct<v: timestamptz_ns, a: array<timestamptz_ns>>`, nineteen doors, five source types,
America/New_York, Asia/Kolkata, Asia/Kathmandu) and a struct-only probe that adds field
assignment (`struct<v: timestamptz_ns, n: int>`, seven doors):

| Source | What every answering door stores in the leaf |
|---|---|
| `TIMESTAMP`, `timestamptz_ns` | the instant |
| `TIMESTAMP_NTZ`, `timestamp_ns` | the wall read as UTC, in every zone |
| `DATE` | midnight UTC |

**The doors do not disagree and nothing is cut**: one reading per source, the same in three
zones, nine digits kept from a nanosecond source. So the brief's halt condition is not met.
The refusals are main's own: MERGE refuses the array-bearing column by its store-assignment
text, and `UPDATE` with no `WHERE` raises the raw Arrow error for any nested column (parity
row R-014).

The one reading is not the top-level rule: a top-level column localises a wall in the
session zone and a nested leaf reads it as UTC. That is recorded, not changed (C-013, Q1).

**At head:** the same two probes, 355 cells in the three zones, are identical to main in
every cell. The field-assignment door is the one that would have moved: it reaches the store
cast with the leaf's type, so it has its own function (`nested_leaf_cast_sql`), a unit pin
and a door pin (`a_nested_zoned_leaf_keeps_the_one_reading_main_has_on_every_door`, mutant
M7).

## 8. Mutants (C-010)

Each applied to the committed tree `971a4ce7`, run against the suites named, restored.

| Mutant | Killed by |
|---|---|
| M1 the store kernel is not named for a zoned nanosecond target | both tests of the Rust door file; the `update_cast` and `ntz_store` unit pins |
| M2 the overwrite doors do not map an Iceberg `timestamptz_ns` column to a target | both tests of the Rust door file |
| M3 a zoned nanosecond target takes instants only, as a microsecond one does | both tests of the Rust door file; the `ntz_store` unit pin |
| M4 the `UPDATE` conform skips zoned targets again | both tests of the Rust door file |
| M5 the zoned kernel's return field keeps its old nullability | `the_return_field_is_nullable_where_an_overflow_answers_null` |
| M6 the ANSI door does not register the zoned kernel | `update_and_merge_into_timestamptz_ns_store_the_instant` |
| M7 a nested leaf takes the top-level store cast | `a_nested_zoned_leaf_keeps_the_one_reading_main_has_on_every_door` |
| M8 the identity `UPDATE` projection names walls only | both tests of the Rust door file |
| M9 the MERGE INSERT projection names walls only | both tests of the Rust door file |
| M10 the top-level store-assignment cast names walls only | both tests of the Rust door file; the `update_cast` unit pin |

The facade pins were not run under the mutants; they hold the same doors in five zones from
the Parquet files on the unmutated build.

## 9. Gates (C-011)

Run 2026-10-10 on the fix commit, one cargo command at a time under the build lock on cores
32-47; the document gates on the tree of the records commit. The pins were red first: on
`c32d692c`, over main's code, both tests of the Rust door file fail (the second by a panic in
Arrow's cast), the kernel's return-field pin fails, and 70 of the 133 facade tests fail.

| Command | Exit | Result |
|---|---|---|
| `cargo fmt --all -- --check` | 0 | |
| `make rust-clippy` | 0 | |
| `make rust-panic-ban` | 0 | |
| `cargo test --locked -p repark-functions --lib` | 0 | 903 passed, 1 ignored |
| `cargo test --locked -p repark-iceberg --lib` | 0 | |
| `cargo test --locked -p repark-spark --lib` | 0 | |
| `cargo test --locked -p repark-sql --lib` | 0 | 394 passed |
| `cargo test --locked -p repark-spark --test timestamptz_ns_wall_doors` | 0 | 3 passed |
| `cargo test --locked -p repark-spark --test timestamp_ns_wall_doors --test timestamp_ns_nested_shapes` | 0 | 7 + 6 passed |
| `make develop` | 0 | |
| `pytest python/repark/tests/test_ice_tstzns_wall_1.py python/repark/tests/test_ice_tsns_merge_wall_1.py -q -n 8` | 0 | 487 passed |
| `pytest python/repark/tests -q -n 8 -k "iceberg or v3 or merge or timestamp or nested or struct"` | 0 | 3680 passed, 140 skipped, 11 xfailed |
| `ruff check .` and `ruff format --check .` (0.15.22) | 0 | |
| `make check-rust-file-size`, `make check-lib-rs` | 0 | |
| the map, ledger, link, spelling and compaction gates | 0 | |

## Q. Questions for a ruling

- **Q1 (C-013, OWNER).** Should a nested `timestamptz_ns` leaf localise a wall in the session
  zone, as a top-level column does? *Premise:* every door reads a wall as UTC there, on main
  and at head, so the doors agree with each other and disagree with the top-level rule; a
  naive nested leaf is refused outright since R-007. *Lean:* decide it inside
  ICE-TSNS-NESTED-1, with the naive leaf: the conversion is the same walk and the same
  pairing and layout questions apply.
- **Q2 (C-014, RULING).** Should a NULL into a required column be refused by a named text?
  *Premise:* with ANSI off an out-of-range value answers NULL; into a required column every
  door, INSERT included, then raises Arrow's `Column 'v' is declared as non-nullable but
  contains null values`, for every column type, on main and at head. This unit removed the
  panic and the raw overflow on these doors; what is left is INSERT's own answer. Six INSERT
  cells of a required `timestamptz_ns` column changed text only: `INSERT … SELECT` and
  `append` now end with the sentence `INSERT … VALUES` already ended with (§5). *Lean:* a
  separate small unit that names the refusal once for every type.
- **Q3 (C-015, RULING).** The mirror of R-012: should the unfiltered `UPDATE` of an
  out-of-range `TIMESTAMP` literal raise `[CAST_OVERFLOW]` instead of `[CAST_INVALID_INPUT]`?
  *Premise:* main raised the raw Arrow error there; the door now answers through the same
  peel as the naive type. *Lean:* fix with R-012.
