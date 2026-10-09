# Unit ledger — ICE-TSNS-MERGE-WALL-1 · every write door stores the same nanosecond wall

**Date:** 2026-10-09 · **Branch:** `fix/ice-tsns-merge-wall-1` · **Base:** `40fc916f` ·
**Model:** Claude Opus 5.5 (`claude-opus-5-5`) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.** Target: the next bug release (v1.5.4).

**Order:** the readiness review of 2026-10-09 (finding R-007, parity row
[ICE-TSNS-SQL-1-R-007](../../../docs/spark-sql-iceberg-parity.md)), owner priority the same
day. **This ledger closes when the unit's pull request merges.**

**Why.** In a session whose zone is not UTC, MERGE wrote a `TIMESTAMP` into a `timestamp_ns`
column as the instant's UTC wall, while `INSERT` stored the session-zone wall. One table then
holds two different walls for one input.

## PROPOSITION LEDGER — ICE-TSNS-MERGE-WALL-1 — 2026-10-09

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | The review's first test, ported unchanged in substance, is red on main `40fc916f`. | `cargo test -p repark-spark --test timestamp_ns_wall_doors` on the base. | **PROVEN** | §1: `INSERT=1767323045123456000 MERGE=1767341045123456000`, `assertion left == right failed`. |
| C-002 | Spark 4.1.2 with Iceberg 1.11.0 cannot create, describe, read or write `timestamp_ns`, so no Spark answer exists; the reference is INSERT's ratified rule (ICE-TSNS-SQL-1 clause 2 and its A-1). | The probe of §2, verbatim. | **PROVEN** | §2. |
| C-003 | Main is measured over the whole matrix (1926 cells) and its answers are recorded; of the 321 `timestamp_ns` cells, 38 store a wrong value and 15 refuse where a precise answer exists. | `_record_ice_tsns_merge_wall_1_main.py 40fc916f`; the classification of §3. | **PROVEN** | §3; `python/repark/tests/ice_tsns_merge_wall_1_main.json`. |
| C-004 | At head every door stores the rule's value in a `timestamp_ns` column: the 38 cells main got wrong are right. | The facade matrix against `expected_wall`; eight doors on the Rust door in three zones; the review's test green. | **PROVEN** | §4, §5: 38 of 38; `test_every_door_stores_the_session_wall_in_timestamp_ns` (57 tests), `timestamp_ns_wall_doors.rs` (3). |
| C-005 | At head no cell main answers correctly has moved: the 1605 control cells and the 268 `timestamp_ns` cells main answered right or refused by the ratified gate are identical, and every carry (DELETE, sibling UPDATE and MERGE, three maintenance rewrites, both modes) reads back the seeded ticks. | The facade matrix against main's recorded cells. | **PROVEN** | §5: 1926 cells, 47 moved, all 47 on the `timestamp_ns` target; that count is this matrix's only and C-021 restates it. Pins after fold 1 (§9.5): `test_control_targets_answer_what_main_answered` (95, one zone), the Rust door's `untouched_rows_carry_their_nanosecond_ticks`. |
| C-006 | `UPDATE t SET v = <column or literal>` with no `WHERE` into a `timestamp_ns` column answers by the rule from a `TIMESTAMP`, `TIMESTAMP_NTZ` or `timestamptz_ns` source, with every nanosecond digit; main raised a raw Arrow error (9 matrix cells). | The `update_column` cells; the Rust door's `update` door; the literal and NULL edge pins. | **PROVEN** | §4.2, §5; `test_update_with_no_where_stores_a_literal`. |
| C-007 | No door drops a digit below the microsecond from a true nanosecond input (`timestamp_ns` column, `timestamptz_ns` column, nine-digit casts, an Arrow nanosecond array), INSERT included. | The matrix moments all carry such digits; `test_true_nanosecond_inputs_keep_every_digit`. | **PROVEN** | §3, §5: no truncating cell on main or at head for a `timestamp_ns` target. |
| C-008 | One conversion, one place: a nanosecond wall target reaches INSERT's own kernel `__repark_cast_timestamp_ns__` from every site, through one function (`ntz_store::wall_cast_udf_name`); no kernel was added and `timestamp_ntz_cast.rs` is unchanged. | The name pin, the per-unit SQL pin, the diff. | **PROVEN** | §4.1; `ns_wall_udf_name_matches_the_registered_udf`, `a_wall_target_casts_through_the_kernel_of_its_unit`. |
| C-009 | The native ANSI door resolves the kernel the shared MERGE and UPDATE sites now emit. | An ANSI-door UPDATE, MERGE update and MERGE insert into `timestamp_ns`. | **PROVEN** | §4.3; `repark-sql` `update_and_merge_into_timestamp_ns_store_the_wall`; mutant M4. |
| C-010 | Out-of-range inputs answer as INSERT answers on the doors of §6's probe: an instant past 2262 from a `TIMESTAMP` column is `[CAST_OVERFLOW]` under ANSI and NULL without it; a NULL and a refused integer are unchanged from main. | The edge probe on main and at head (§6) and its pins. | **PROVEN** | §6: 56 probe cells, 40 identical, 16 moved from a raw Arrow error; `test_an_instant_past_2262_*`. The verify showed the first wording ("every door") false on three classes; fold 1 fixes two (C-018, C-019) and leaves one OPEN (C-020). |
| C-011 | Five hand mutants are each killed by a named pin. | §7. | **PROVEN** | §7: M1..M5 red, sources restored. |
| C-012 | The unit's gates are green with real exit codes. | §8. | **PROVEN** | §8. |
| C-013 | A `timestamptz_ns` target stores one instant for one wall on every door. | Out of this unit by the brief's control rule; measured (O-1, O-6). | **OPEN** | Q1: 38 cells read a wall as UTC and 6 raise a raw Arrow error on main and at head. Parity row ICE-TSNS-SQL-1-R-008. |
| C-014 | A nanosecond source narrows into a microsecond column the same way on every door. | Out of this unit by the brief's control rule; measured (O-2, O-3). | **OPEN** | Q2: 72 cells truncate toward zero or read a wall as UTC. Parity row ICE-TSNS-SQL-1-R-009. |
| C-015 | `UPDATE` and `DELETE` lower the nanosecond cast spellings. | Out of this unit: a statement-lowering gap, not a stored value (O-5). | **OPEN** | Q3: 6 `timestamp_ns` cells stay refused. Parity row ICE-TSNS-SQL-1-R-010. |
| C-016 | Fold 1. A `timestamp_ns` leaf nested in a struct, a list or a map (struct in struct, array of struct, array, struct of array, map value, map of struct) stores the wall a top-level column stores, from every door that answers; no door stores a second wall; what refuses, refused on main. | The nested matrix on main, at `1be18c26` and at head (§9.1); the Rust door pins; the kernel's unit pins. | **PROVEN** | §9.1: 4680 cells; 2142 answer, all by the rule (main: 556 of them a UTC wall; `1be18c26`: two walls in one table); 2538 refuse exactly as on main. |
| C-017 | A nested microsecond `TIMESTAMP_NTZ` field stores one wall for one input from every door. | Out of this fold by the ruling: a microsecond control. Measured, six nested doors and 22 more, three zones (§9.2). | **OPEN** | Q4: 28 cells (field assignment) store the session wall, 106 (every other door) the UTC wall, on main and at head. Parity row ICE-TSNS-SQL-1-R-011. |
| C-018 | Fold 1. With ANSI off, an out-of-range `TIMESTAMP` or `DATE` literal through `UPDATE … WHERE` (both row-level modes) and `MERGE … INSERT VALUES` stores NULL, as INSERT does, the two edge instants whose session wall overflows included. | The verify's 42 cells re-measured (§9.3); the Rust door pin; the return-field unit pin. | **PROVEN** | §9.3: 42 of 42, from `declared as non-nullable but contains null values` at `1be18c26`. |
| C-019 | Fold 1. A `TIMESTAMP_NTZ` or `DATE` past the nanosecond range through `INSERT OVERWRITE`, `INSERT … BY NAME`, `overwritePartitions` and `insertInto(overwrite=True)` answers as `INSERT … SELECT`: `[CAST_OVERFLOW]` under ANSI, NULL without; no Rust panic. | The verify's 96 cells re-measured (§9.3); the Rust door and facade pins; the `wall_kernel_reads` unit pin. | **PROVEN** | §9.3: 96 of 96, from a raw Arrow overflow and a caught panic on main and at `1be18c26`. |
| C-020 | Under ANSI, `UPDATE t SET v = <out-of-range TIMESTAMP literal>` with no `WHERE` reports INSERT's error class. | Not the site of C-018 or C-019 (§9.3): the unfiltered `UPDATE`'s planner-cast peel reaches the literal's string. A dated row by the ruling. | **OPEN** | Q5: 16 cells answer `[CAST_INVALID_INPUT]` where INSERT answers `[CAST_OVERFLOW]`; main raised a raw Arrow error. Parity row ICE-TSNS-SQL-1-R-012. |
| C-021 | The count of cells that moved from main is restated against the verify's wider matrix; every moved cell is a `timestamp_ns` target cell and none is a control. | The verify's matrix, probes and overflow matrix re-run on head and compared with its own main outputs (§9.4). | **PROVEN** | §9.4. |
| C-022 | On the ANSI door the kernel's name is not user-visible. | Measured by the verify: after `on_session_built`, `SELECT __repark_cast_timestamp_ns__('…')` resolves and `information_schema.routines` lists it, as it lists the NTZ kernel. A dated row by the ruling. | **OPEN** | Q6. Parity row ICE-TSNS-SQL-1-R-013. |
| C-023 | The facade module is cut to the 57 `timestamp_ns` door tests and one zone of controls; mutants M5 and X2 still die; carry is pinned at the Rust door. | Collect the module; re-run M5 and X2 (§9.5, §9.6). | **PROVEN** | §9.5: 179 tests from 464, the fixture 195 kB from 501 kB. |
| C-024 | Six hand mutants of the fold are each killed by a named pin. | §9.6. | **PROVEN** | §9.6. |
| C-025 | The fold's gates are green with real exit codes. | §9.7. | **PROVEN** | §9.7. |
| C-026 | A nested column answers, or refuses by name, wherever a top-level column answers. | Out of this fold: none of these stores a value (§9.1). | **OPEN** | Q7: `UPDATE` with no `WHERE` on any nested column, `INSERT … VALUES` of `array(TIMESTAMP '…')` and `INSERT OVERWRITE … VALUES` of a struct holding a `TIMESTAMP` literal raise raw Arrow errors on main and at head. Parity row ICE-TSNS-SQL-1-R-014. |

## 1. The red test (C-001)

`crates/repark-spark/tests/timestamp_ns_wall_doors.rs::insert_and_merge_store_the_same_nanosecond_wall_time`
is the review's `readiness_review_probe.rs` first test, in the directory of the other
whole-session tests. Its second test (an encrypted-table refusal) belongs to another lane and
is not ported. On the base, in an America/New_York session:

```text
running 1 test
INSERT=1767323045123456000 MERGE=1767341045123456000
assertion `left == right` failed: same input must store the same wall time across write doors
  left: 1767323045123456000
 right: 1767341045123456000
test insert_and_merge_store_the_same_nanosecond_wall_time ... FAILED
```

The literal `TIMESTAMP '2026-01-02 03:04:05.123456789'` is a microsecond value, so both doors
store `…123456000`: the test catches the wall, not truncation. True nanosecond inputs are in
the matrix of §3.

## 2. Spark does not support the type (C-002)

PySpark 4.1.2 (`/tmp/sparkenv`), `iceberg-spark-runtime-4.1_2.13-1.11.0.jar`, zulu-17, a Hadoop
catalog, session zone America/New_York, 2026-10-09. The SQL DDL refuses the type name. A
format-v3 table created through the Iceberg Java API (`Types.TimestampNanoType.withoutZone()` /
`.withZone()`) exists, and Spark then refuses every statement on it:

```text
FAIL create ddl ParseException 
FAIL create ddl tz ParseException 
OK   java api created v3 table with timestamp_ns / timestamptz_ns
FAIL describe UnsupportedOperationException Cannot convert unsupported type to Spark: timestamp_ns
FAIL select UnsupportedOperationException Cannot convert unsupported type to Spark: timestamp_ns
FAIL insert UnsupportedOperationException Cannot convert unsupported type to Spark: timestamp_ns
FAIL merge UnsupportedOperationException Cannot convert unsupported type to Spark: timestamp_ns
iceberg version 1.11.0
```

The probe prints the first line of each message, which is empty for the two parse errors;
Spark's own error log of the same run carries them:
`[UNSUPPORTED_DATATYPE] Unsupported data type "TIMESTAMP_NS". SQLSTATE: 0A000` and the same
for `"TIMESTAMPTZ_NS"`.

So the reference is the rule INSERT already follows, ratified by ICE-TSNS-SQL-1 (ruling
Q-21c-6 clause 2, ledger A-1): a `timestamp_ns` column is a wall clock; an instant written
into it stores its wall in the session zone; a wall is stored as it is; nothing below a
nanosecond is dropped. The pins compute that rule with Python's `zoneinfo`, an implementation
independent of the engine, and main's INSERT doors agree with it in every cell (§3).

## 3. Main over the matrix (C-003)

The matrix is `python/repark/tests/_ice_tsns_merge_wall_1_doors.py`. A cell is one session
zone × one target column type × one door × one source type, and writes eight moments, each
with non-zero digits below the microsecond:

| # | Moment (nine digits, read as UTC for an instant) | Why |
|---|---|---|
| 1 | `2026-01-02 03:04:05.123456789` | the review's value |
| 2 | `1969-12-31 23:59:59.999999999` | before the epoch: floor and truncation differ |
| 3 | `2026-03-08 02:30:00.000000001` | a wall inside New York's spring gap |
| 4 | `2026-11-01 01:30:00.000000001` | a wall inside New York's autumn overlap |
| 5, 6 | `2026-03-08 06:59:59.999999999`, `07:00:00.000000001` | instants on each side of the gap |
| 7, 8 | `2026-11-01 05:30:00.000000001`, `06:30:00.000000001` | the two instants of one overlap wall |

- **Zones:** UTC, Asia/Kolkata (not UTC, no DST), America/New_York (DST).
- **Targets:** `timestamp_ns`; controls `timestamptz_ns`, `TIMESTAMP_NTZ` and `TIMESTAMP` on
  format v3, `TIMESTAMP_NTZ` and `TIMESTAMP` on format v2.
- **Sources:** `TIMESTAMP` (`l`), `TIMESTAMP_NTZ` (`n`), `timestamp_ns` (`ns`),
  `timestamptz_ns` (`tzns`), string (`s`). Column sources come from a table written through
  the DataFrame door from Arrow arrays of the exact type; literal sources are
  `TIMESTAMP '…+00:00'`, `TIMESTAMP_NTZ '…'`, `CAST('…' AS timestamp_ns)`,
  `CAST('…+00:00' AS timestamptz_ns)` and a bare string.
- **Doors (19):** `INSERT … VALUES`, `INSERT … SELECT`, `INSERT OVERWRITE`,
  `INSERT … REPLACE WHERE`; MERGE insert (column list, `INSERT *`, literals); MERGE update
  (`SET`, `SET *`, literals); `UPDATE … SET` of literals with a `WHERE`, and of a sibling
  column with no `WHERE`; the DataFrame writers `writeTo().append()`,
  `.overwritePartitions()`, `.overwrite(condition)` (the DataFrame spelling of replaceWhere),
  `write.insertInto()` with and without `overwrite`, `write.mode("append").saveAsTable()`, and
  an in-memory Arrow array of the source type through `createDataFrame(…).writeTo().append()`.
- **Carries (6, each copy-on-write and merge-on-read):** `DELETE` of one row, `UPDATE` of a
  sibling column, a MERGE that deletes one row and updates a sibling column,
  `rewrite_data_files`, `rewrite_manifests`, `rewrite_position_delete_files`. The table is
  seeded with exact values of the target's own type and the untouched rows must read back
  unchanged.

3 zones × 6 targets × (19 doors × 5 sources + 6 carries × 2 modes) = **1926 cells**, recorded
on main `40fc916f` in `python/repark/tests/ice_tsns_merge_wall_1_main.json`.

**The 321 `timestamp_ns` cells on main:**

| Class | Cells | Which |
|---|---|---|
| right value | 178 | every `INSERT` (VALUES, SELECT, REPLACE WHERE), `append`, `overwrite(condition)`, `insertInto`, `saveAsTable`, the Arrow array, and every door from a wall source or in UTC |
| carry unchanged | 36 | all six carries, both modes, all zones |
| refused, ratified | 54 | a string column, and a string literal outside `INSERT … VALUES`: the WI-2 store-assignment gate, as for a `TIMESTAMP` column |
| **wrong value** | **38** | an instant source (`l`, `tzns`) in Asia/Kolkata and America/New_York stores its UTC wall through `INSERT OVERWRITE`, MERGE insert (three spellings), MERGE update (three spellings), `overwritePartitions`, `insertInto(overwrite=True)` (9 doors × 2 sources × 2 zones = 36), and `UPDATE … SET` of a `TIMESTAMP` literal (2) |
| **refused, a precise answer exists** | **15** | `UPDATE t SET v = c` with no `WHERE` from a `TIMESTAMP`, `TIMESTAMP_NTZ` or `timestamptz_ns` column raises the raw `Arrow error: Invalid argument error: arguments need to have the same data type` (9); `UPDATE … SET v = CAST('…' AS timestamp_ns)` raises `Unsupported SQL type timestamp_ns` / `timestamptz_ns` (6) |

No door truncates a true nanosecond input into a `timestamp_ns` column on main: INSERT from a
`timestamp_ns` column, from a `timestamptz_ns` column, from the nine-digit casts, from a bare
string and from an Arrow nanosecond array all keep every digit.

**Observed on the controls, recorded and not changed by this unit** (they are the questions of
§Q):

- O-1 — `timestamptz_ns` target, a wall source (`TIMESTAMP_NTZ`, `timestamp_ns`), a zone other
  than UTC: `INSERT` reads the wall in the session zone; `INSERT OVERWRITE`, the MERGE arms,
  `overwritePartitions` and `insertInto(overwrite=True)` store the wall as if it were UTC.
  38 cells. This is the mirror of R-007.
- O-2 — `TIMESTAMP` (microsecond) target, a `timestamp_ns` or `timestamptz_ns` source:
  `INSERT OVERWRITE`, MERGE insert, `overwritePartitions` and `insertInto(overwrite=True)`
  truncate toward zero (`-1 ns` stores `0 µs`; INSERT floors to `-1 µs`) and do not read a
  wall in the session zone. 36 cells on each format version, UTC included.
- O-3 — `TIMESTAMP_NTZ` (microsecond) target, a `timestamp_ns` source: every door, INSERT
  included, truncates toward zero (`-1 ns` stores `0 µs`).
- O-4 — `TIMESTAMP_NTZ '<wall>'` refuses `'__repark_timestamp_ntz__' expects an Int64 wall,
  got -1` (and `got 0`) when the wall is within about 36 minutes of the epoch, in a plain
  `SELECT` too. The matrix spells that one moment `to_timestamp_ntz('…')`.
- O-5 — `UPDATE` and `DELETE` do not lower the `timestamp_ns` / `timestamptz_ns` cast
  spellings, in `SET` or in `WHERE` (`Unsupported SQL type timestamp_ns`).
- O-6 — `UPDATE t SET v = c` with no `WHERE` into a `timestamptz_ns` column from a
  `TIMESTAMP` or `TIMESTAMP_NTZ` column raises the same raw Arrow error. 6 cells.

## 4. The fix

### 4.1 One function names the kernel of the target's unit (C-008)

The review's cause holds: `is_ntz_wall_target` matched `Timestamp(µs, None)` only, so at the
four store sites a `timestamp_ns` target took the instant path, a plain Arrow cast that keeps
the UTC reading. The review's warning also holds: the NTZ kernel returns microsecond arrays,
so widening the matcher alone would truncate.

The brief asks to extend `timestamp_ntz_cast.rs` to carry the target's unit, or to say why
not. **Why not:** the nanosecond wall conversion already exists.
`repark-functions/src/timestamp_ns_cast.rs` holds `__repark_cast_timestamp_ns__`, and it is the
kernel INSERT's conform runs, which is the ratified reference of §2. Giving the NTZ kernel a
unit would add a second nanosecond conversion beside the one INSERT uses, which is what the
rule forbids. So no kernel changed and none was added:

- `repark-iceberg/src/write/ntz_store.rs`: `wall_cast_udf_name(target)` answers the NTZ kernel
  for `Timestamp(µs, None)` and the nanosecond kernel for `Timestamp(ns, None)`;
  `wall_cast_sql` renders the call (it replaces `ntz_wall_cast_sql`).
- The four sites ask it: `update_cast.rs::store_assignment_cast_sql` (MERGE UPDATE SET and the
  nested-assignment fold), `merge/insert.rs::zone_wrapping_stream_sql` (MERGE INSERT),
  `predicate_dml/lineage.rs::update_projection_sql` (identity UPDATE), and
  `ntz_store.rs::zone_stores` (`INSERT OVERWRITE`, `overwritePartitions`,
  `insertInto(overwrite=True)`), which now names an Iceberg `timestamp_ns` column as a wall
  target.
- `is_ltz_instant_target` and `refuse_ntz_writes` are unchanged, so a `timestamptz_ns` target
  and the microsecond refusals do not move.

### 4.2 `UPDATE` with no `WHERE` (C-006)

An `UPDATE` with no `WHERE` is not an identity DML. DataFusion plans it as `Dml(Update)` over
a projection that casts each `SET` value to the column type, and the fork's update node
raised `arguments need to have the same data type`. The first attempt wrapped the value after
analysis only, and the Rust-door pin caught it truncating: the session analyzer had already
narrowed `CAST(<timestamptz_ns> AS Timestamp(ns))` to microseconds
(`__repark_narrow_timestamp_ns__`), and a `TIMESTAMP_NTZ` source in a DST zone went wall →
instant → wall. INSERT avoids both by peeling the planner cast **before** analysis, so
`repark-spark/src/insert_timestamp_ns.rs` now runs both of its hooks for an `UPDATE` plan as
well, on naive `timestamp_ns` targets only. No fork change.

### 4.3 The ANSI door (C-009)

`repark_functions::register_all` runs only under the Spark extension. The ANSI door registered
the NTZ kernel by hand for the same sites (WO NTZ-1), and not the nanosecond one, so the fix
of §4.1 would have turned an ANSI-door MERGE or UPDATE into a `timestamp_ns` column from a
stored value into `UNRESOLVED_ROUTINE`. `repark-sql/src/dialect.rs` now registers it. The
matrix could not see this: it runs on the Spark door.

## 5. Head over the matrix (C-004, C-005, C-006, C-007)

Recorded with the same recorder against the head build (`--output`, outside the repository)
and compared cell by cell with `ice_tsns_merge_wall_1_main.json` (2026-10-09):

| | main | head |
|---|---|---|
| `timestamp_ns`: right value | 178 | 225 |
| `timestamp_ns`: carry unchanged | 36 | 36 |
| `timestamp_ns`: refused, ratified string gate | 54 | 54 |
| `timestamp_ns`: wrong value | 38 | 0 |
| `timestamp_ns`: refused, a precise answer exists | 15 | 6 (O-5, Q3) |
| control cells identical to main | — | 1605 of 1605 |
| cells moved | — | 47, all `timestamp_ns`: 38 wrong → right, 9 raw Arrow error → right |

The facade pins hold every one of the 1926 cells: `test_ice_tsns_merge_wall_1.py`, 454 matrix tests and 10 edge tests.

## 6. Edge probe, main against head (C-010)

56 cells in America/New_York, ANSI on and off, a `timestamp_ns` target, twelve door spellings
plus a `days(v)` partitioned MERGE, from a `DATE` column and from a `TIMESTAMP` column holding
`3000-01-01 00:00:00.000001Z`, a normal value and a NULL. 40 cells are identical. The 16 that
moved were all errors on main:

- `UPDATE t SET v = <TIMESTAMP literal>` and `SET v = NULL` with no `WHERE` (8 cells): the raw
  Arrow error → the session wall, and NULL.
- The instant past 2262 through `INSERT OVERWRITE`, MERGE insert, MERGE update and the
  partitioned MERGE (8 cells): a raw `Arithmetic overflow: Overflow happened on:
  32503680000000001 * 1000` in both modes → what `INSERT … SELECT` answers on main and at
  head: `[CAST_OVERFLOW] The value 32503680000000001 of the type "TIMESTAMP" cannot be cast
  to "TIMESTAMP_NS"` under ANSI, NULL (with the other rows stored) without it.

Unchanged: an integer refuses with the same store-assignment text; `v + INTERVAL 1 DAY`; a
NULL; the `days(v)` partition values. A `DATE` stores midnight of its day on every door **of
this probe**; the first version of this sentence said "on every door", and the verify measured
one where it was not so on main: `UPDATE … SET v = DATE '2026-01-02' WHERE …` stored midnight
shifted by the zone (`1767330000000000000` in America/New_York). At head it stores midnight,
as INSERT does; that cell is in the restated count of §9.4.

## 7. Mutants (C-011)

Each applied by hand, run, and restored (2026-10-09).

| # | Mutation | Killed by |
|---|---|---|
| M1 | `wall_cast_udf_name` loses its nanosecond arm (the base's matcher) | `a_wall_target_casts_through_the_kernel_of_its_unit` (`arrow_cast((s.v), 'Timestamp(ns)')`); `insert_and_merge_store_the_same_nanosecond_wall_time` (`1767323045123456000` vs `1767341045123456000`); `every_door_stores_a_nanosecond_instant_as_its_session_wall` |
| M2 | `zone_stores` does not name `TimestampNs` as a wall target | `every_door_stores_a_nanosecond_instant_as_its_session_wall` at `insert overwrite` (the UTC walls) |
| M3 | the nanosecond conform returns `None` for `WriteOp::Update` | the same test at `update`: `Arrow error: Invalid argument error: arguments need to have the same data type` |
| M4 | the ANSI door does not register the nanosecond kernel | `update_and_merge_into_timestamp_ns_store_the_wall` |
| M5 | the `UPDATE` conform also takes zoned targets (`WriteOp::Update => false`) | `test_control_targets_answer_what_main_answered[*-tz_ns-update_column]` in all three zones: the control moved |

## 8. Gates (C-012)

Run 2026-10-09 on the head tree, one cargo command at a time.

| Command | Exit | Result |
|---|---|---|
| `cargo fmt --check` | 0 | |
| `make rust-clippy` | 0 | |
| `make rust-panic-ban` | 0 | |
| `cargo test --locked -p repark-iceberg --lib` | 0 | 963 passed |
| `cargo test --locked -p repark-spark --lib` | 0 | 2631 passed, 5 ignored |
| `cargo test --locked -p repark-sql --lib` | 0 | 393 passed |
| `cargo test --locked -p repark-functions --lib` | 0 | 902 passed, 1 ignored |
| `cargo test --locked -p repark-spark --test timestamp_ns_wall_doors --test dml_sessions --test session_timestamp_type` | 0 | 3 + 1 + 7 passed |
| `cargo test --locked -p repark-sql --test ansi_ntz_wall_cast --test ansi_update_cast --test session_wiring` | 0 | 3 + 4 + 4 passed |
| `make develop` | 0 | |
| `pytest python/repark/tests -q -n 8 -k "ntz or timestamp_ns or tsns or merge or update"` | 0 | 1703 passed, 38 skipped |
| `python3 scripts/sync_map_md.py --check` | 0 | |
| `bash scripts/check_map_md.sh --base origin/main` | 0 | |
| `make check-ledger-grammar` | 0 | |

Disk: 454 GB free on `/` before the first build; the unit used the clone's existing `target/`
and made no worktree. Scratch (the recorder outputs, probe scripts, mutant log) is in
`/tmp/oc-worker/tsns-wall/`, outside the repository.

## 9. Fold 1, after the Opus verify of `1be18c26` (C-016 to C-026)

The verify (2026-10-09, an independent matrix of 16,383 cells) held the headline for
top-level columns and failed the unit on one S1 and two S2. The orchestrator's rulings of the
same day: fix the nested split forward, do not touch the nested microsecond control, fix both
overflow classes, restate the count, cut the pin budget. Order kept: red pins first
(`ca7be55e`), then the fixes.

### 9.1 Nested `timestamp_ns` leaves (C-016, C-026)

**Fixed forward**, not the fallback. The cause was as the verify named it:
`store_assignment_cast_sql` is also the nested-assignment fold's leaf cast, so field
assignment reached the leaf kernel while every other nested door kept a plain struct cast.

The conform is the kernel INSERT already runs, given a second argument
(`__repark_cast_timestamp_ns__(value, <typed NULL of the target type>)`): it walks a struct,
list or map and converts each leaf whose paired target leaf is `Timestamp(ns, None)` with the
one-argument conversion, and leaves everything else to the store cast that already followed.
No kernel was added. Sites: `store_assignment_cast_sql` (whole-struct MERGE UPDATE SET, the
nested fold's non-struct leaves), the MERGE INSERT projection, the identity-UPDATE projection,
`zone_stores` (the overwrite doors and `INSERT … BY NAME`), and INSERT's `after_analysis`
hook, which also reaches the cast `INSERT … VALUES` keeps inside its rows. Reasons are in the
maps of `repark-functions/src/timestamp_ns_cast/`, `repark-iceberg/src/write/` and
`repark-spark/src/`.

**The nested matrix** (scratch, outside the repository): three zones × nine shape spellings ×
22 to 28 doors (the 19 of §3 without the Arrow array, plus `INSERT … BY NAME`,
`INSERT OVERWRITE … VALUES`, whole-value and field `UPDATE` with and without a `WHERE`, each
row-level door in both modes) × five sources (`TIMESTAMP`, `TIMESTAMP_NTZ`, `timestamp_ns`,
`timestamptz_ns`, `DATE`), four moments with digits below the microsecond; the leaf is read
back as int64 through Arrow. 4680 `timestamp_ns` cells:

| Shape | Cells | Answer, by the rule: main | `1be18c26` | head | Refuse (all three builds) |
|---|---|---|---|---|---|
| `struct<v: timestamp_ns, n: int>` | 660 | 379 of 513 | 407 of 513 | 513 of 513 | 147 |
| `struct<i: struct<v, n>, m: int>` | 660 | 379 of 513 | 407 of 513 | 513 of 513 | 147 |
| `array<struct<v, n>>` | 480 | 167 of 225 | 167 of 225 | 225 of 225 | 255 |
| `array<timestamp_ns>` | 480 | 166 of 222 | 166 of 222 | 222 of 222 | 258 |
| `struct<a: array<timestamp_ns>, n: int>` | 480 | 167 of 225 | 167 of 225 | 225 of 225 | 255 |
| `map<string, timestamp_ns>`, one literal key | 480 | 73 of 99 | 73 of 99 | 99 of 99 | 381 |
| `map<string, struct<v, n>>`, one literal key | 480 | 49 of 63 | 49 of 63 | 63 of 63 | 417 |
| `map<string, timestamp_ns>`, a key per row | 480 | 99 of 135 | not run | 135 of 135 | 345 |
| `map<string, struct<v, n>>`, a key per row | 480 | 107 of 147 | not run | 147 of 147 | 333 |
| **all** | **4680** | **1586 of 2142** | | **2142 of 2142** | **2538** |

Every cell main answered wrong (556) stored an instant's UTC wall in a zone other than UTC.
At `1be18c26` the 56 field-assignment cells of the two struct shapes had moved to the rule
and the other 424 measured there had not: two walls in one table. At head no answering cell differs from the
rule, and the set of refusing cells is main's, cell for cell.

**What refuses, and how** (head, the same on main):

| Class | Cells | Named? |
|---|---|---|
| MERGE INSERT or UPDATE SET of an array-bearing column: `cannot store-assign column … not ANSI-store-assignable`, a `List(Timestamp(ns))` source into the same type included | 540 | named |
| `UPDATE … SET` of a `CAST(… AS timestamp_ns)` literal: `Unsupported SQL type` | 108 | named, R-010 |
| `UPDATE` with no `WHERE` on a nested column, any source: `arguments need to have the same data type` / `column types must match schema types` | 480 | raw, R-014 |
| `INSERT … VALUES` of `array(TIMESTAMP '…')`, `INSERT OVERWRITE … VALUES` of a nested value holding a `TIMESTAMP` literal: `column types must match schema types` | 24 | raw, R-014 |
| the probe's own `map('k', <column>)` and `map(…, CAST(NULL AS …))` spellings, refused by the `map` function before any store (`map requires key and value lists to have the same length`), and a row constructor without an `id` | 1262 | not a store |
| `map(…)` of a `TIMESTAMP_NTZ`, `timestamp_ns` or `timestamptz_ns` literal: `list array`, a Rust panic caught at the Python boundary | 124 | raw; a `map` construction defect, seen on main, not a store |

The last two classes trade a few cells between runs: a merge-on-read whole-map `MERGE` from
a column answers the `map` function's error in one run and the caught panic in the next, on
main as at head.

The fold changed no refusal. The raw ones store nothing and are parity row R-014 (C-026, Q7);
the caught panic is in the hand-back as out of scope.

**Controls of the nested matrix:** `struct<v: TIMESTAMP_NTZ>`, `struct<v: timestamptz_ns>`,
`struct<v: TIMESTAMP>`, and the struct-in-struct and array-of-struct shapes with a
`TIMESTAMP_NTZ` leaf: 3120 cells, 0 moved from main.

### 9.2 The nested microsecond `TIMESTAMP_NTZ` control (C-017)

Not changed, by ruling. Measured on main and at head, `struct<v: TIMESTAMP_NTZ, n: INT>`,
28 doors × 5 sources × 3 zones = 660 cells, identical on both builds:

| | Cells |
|---|---|
| a wall source, or a UTC session: every door agrees | 379 |
| refused (the classes of §9.1) | 147 |
| an instant in Asia/Kolkata or America/New_York, **field assignment**: the session wall | 28 |
| the same instant, **every other door**: the UTC wall | 106 |

The 28: `MERGE … UPDATE SET t.st.v = s.<column>` (8, both modes), the same with a literal
(8), `UPDATE … SET st.v = <column> WHERE` (8), `UPDATE … SET st.v = <literal> WHERE` (4, a
`TIMESTAMP` literal; the nanosecond literal spellings refuse, R-010). The 106: nested
`INSERT … VALUES`, `SELECT`, `BY NAME`, `OVERWRITE`, `REPLACE WHERE`, the five DataFrame
writers, MERGE INSERT (three spellings), whole-struct MERGE UPDATE (three spellings) and
whole-struct `UPDATE … WHERE`. The six doors of the verify's repro, one input
(`TIMESTAMP '2026-01-02 03:04:05.123456+00:00'`), microsecond ticks of `st.v`:

| Door | UTC | America/New_York | Asia/Kolkata |
|---|---|---|---|
| `INSERT … VALUES` | 1767323045123456 | 1767323045123456 | 1767323045123456 |
| `INSERT … SELECT` | 1767323045123456 | 1767323045123456 | 1767323045123456 |
| `MERGE … UPDATE SET t.st.v = s.x` | 1767323045123456 | **1767305045123456** | **1767342845123456** |
| `MERGE … UPDATE SET t.st = named_struct(…)` | 1767323045123456 | 1767323045123456 | 1767323045123456 |
| `UPDATE … SET st.v = <literal> WHERE` | 1767323045123456 | **1767305045123456** | **1767342845123456** |
| `MERGE … INSERT` | 1767323045123456 | 1767323045123456 | 1767323045123456 |

Pinned as it is by `a_nested_microsecond_ntz_field_keeps_the_split_main_has`. Parity row
ICE-TSNS-SQL-1-R-011; question Q4.

### 9.3 Out-of-range inputs (C-018, C-019, C-020)

The verify's overflow matrix (`bx.py`: three zones × ANSI on and off × two targets × 17 door
spellings, row-level ones in both modes × 14 edge inputs = 4032 cells) re-run on head and
compared with its recorded outputs for main and for `1be18c26`:

| | Cells |
|---|---|
| `timestamptz_ns` target, moved from main | 0 of 2016 |
| `timestamp_ns` target, moved from `1be18c26` | 138, each from a raw error to INSERT's answer |
| — a literal through `UPDATE … WHERE` (both modes) or `MERGE … INSERT VALUES`, ANSI off: `declared as non-nullable but contains null values` → NULL | 42 |
| — a `TIMESTAMP_NTZ` or `DATE` column through `INSERT OVERWRITE`, `INSERT … BY NAME`, `overwritePartitions`, `insertInto(overwrite=True)`: raw Arrow overflow or a caught panic → `[CAST_OVERFLOW]` (48, ANSI on) and NULL (48, ANSI off) | 96 |
| `timestamp_ns` door cells (1848, the two INSERT doors being the reference) that answer as INSERT for the same input: main 824, `1be18c26` 1646, head | 1784 |
| `timestamp_ns` door cells that still differ from INSERT's answer | 64 |
| — `UPDATE` with no `WHERE` of a `TIMESTAMP` literal under ANSI: `[CAST_INVALID_INPUT]` for `[CAST_OVERFLOW]` (C-020, R-012) | 16 |
| — `UPDATE … SET v = CAST(… AS timestamp_ns / timestamptz_ns) WHERE`, both modes: `Unsupported SQL type` (C-015, R-010) | 48 |

So of the verify's 154 overflow-class cells that differed, 138 now answer as INSERT and 16
stay, with a dated row.

**C-018, the site.** The kernel declared its return field non-nullable for a non-null
argument that was not a string. Its answer to an overflow with ANSI off is NULL, so a literal
(a non-nullable scalar) broke the batch. `return_field_from_args` now answers nullable for
every source that is not already the target type.

**C-019, the site.** `zone_store_frame` handed a column to the kernel only when the source was
an instant; a wall source took the plain Arrow cast, whose overflow is raw and, for `DATE`
under ANSI, a panicking multiplication. `ntz_store::wall_kernel_reads` now hands a nanosecond
wall target every temporal source that is not already the target type. A microsecond NTZ
target keeps the instant-only rule: widening it is the verify's mutant X2, which moves
microsecond controls. In range the kernel and the cast give the same ticks, and no in-range
cell moved (§9.4).

**C-020, not the same site.** The unfiltered `UPDATE` goes through INSERT's `before_analysis`
peel, which strips the planner cast and the typed literal's own cast and hands the kernel the
literal's text; text past the range does not parse, hence "malformed". `INSERT … VALUES`
skips typed cells. A fix belongs to that peel and would touch the six in-range cells the peel
makes right, so it is a dated row (R-012) and question Q5.

### 9.4 What moved from main, restated (C-021)

The first ledger's "47 moved" is true of its own 1926-cell matrix and of nothing wider; the
verify reproduced exactly 47 there and found more on its own. Its three instruments re-run on
head and compared with its recorded outputs for main `40fc916f`:

| Instrument | Cells | Moved from main, all on a `timestamp_ns` target | Control cells moved | Differs from `1be18c26` |
|---|---|---|---|---|
| this unit's matrix (`--whole`) | 1926 | 47: 38 values, 9 raw errors → values | 0 of 1605 | 0 |
| the verify's door matrix (`mx.py`: five zones with Australia/Lord_Howe and `+05:45`, six targets, 47 door spellings with merge-on-read, `INSERT BY NAME`, `mergeInto`, `NOT MATCHED BY SOURCE`, seven sources with `DATE` and a zoned string; Parquet values, logical type and manifest bounds compared) | 9870 | 324 of 1645: 244 values, 80 raw errors → values | 0 of 8225 | 0 |
| the verify's side-effect probes in America/New_York (`px.py`; 2439 of its 2487 cells re-run, the 48-cell cross-build read probe was not) | 2439 | 247: partitioned 100, `UPDATE` spellings 36, nested 30, pruning 27, after `ADD COLUMN` 22, maintenance after each door 18, sorted 11, `SET TIME ZONE` 3 | 0 | 28, all nested `timestamp_ns` cells (§9.1) |
| the verify's overflow matrix (`bx.py`, §9.3) | 4032 | 976 of 2016 | 0 of 2016 | 138 (§9.3) |
| the nested matrix of §9.1 | 4680 + 3120 controls | 556 values | 0 of 3120 | 424 values of the 3720 cells measured there |

The probe cells carry the path of a metadata file, which differs between any two runs; it is
left out of the comparison, as the verify left it out.

Read against the verify's sentence ("324 of 1645 `timestamp_ns` cells moved, plus 100
partitioned, 22 after ADD COLUMN, 2 nested"): the 324, the 100 and the 22 are unchanged by
the fold, cell for cell; the nested count is 30, every answering instant-source cell of its
nested probe, where it was 2, because the fold moved the nested INSERT, MERGE INSERT, whole-value and overwrite doors
to the wall field assignment already stored. The classes the first ledger did not list are
all in the 324: a `DATE` source through `UPDATE … WHERE` (main stored midnight shifted by the
zone), `INSERT … BY NAME`, `INSERT OVERWRITE … VALUES` and `BY NAME`, DataFrame `mergeInto`,
`WHEN NOT MATCHED BY SOURCE THEN UPDATE`, and every merge-on-read variant. Each stores what
`INSERT … VALUES` stores for the same input; the verify checked all 1095 answering cells
against INSERT and against `zoneinfo`, and head equals `1be18c26` on every one of them.

**No control moved on any instrument:** 1605 + 8225 + 2016 + 3120 cells, and the probes'
`timestamptz_ns` and microsecond tables.

### 9.5 The pin budget (C-023)

| | Before | After |
|---|---|---|
| `test_ice_tsns_merge_wall_1.py`, tests | 464 | 179 |
| `ice_tsns_merge_wall_1_main.json` | 500,749 bytes, 1926 cells | 194,635 bytes, 760 cells |

Kept: the 57 `timestamp_ns` door tests (19 doors × 3 zones); the control doors in one zone,
America/New_York (19 doors × 5 control targets = 95), which is where M5 and X2 die; the
fixture check, the nanosecond-digit pins, the 10 edge pins. Dropped: the controls of UTC and
Asia/Kolkata (190) and the 108 carry tests. The carry tests go because a Rust-level pin now
covers carry: `untouched_rows_carry_their_nanosecond_ticks` runs DELETE, a sibling UPDATE, a
sibling MERGE, `rewrite_data_files`, `rewrite_manifests` and
`rewrite_position_delete_files` in both row-level modes over `timestamp_ns` and
`timestamptz_ns` tables seeded with digits below the microsecond. Added (13): the nested
struct through five DataFrame writers, and the out-of-range `TIMESTAMP_NTZ` and `DATE`
through the two DataFrame overwrite doors under ANSI on and off; those doors exist on the
facade only. The fixture is the recorded cells filtered by `doors.in_fixture`, not a new
recording; `--whole` on the recorder still measures all 1926 cells.

### 9.6 Mutants of the fold (C-024)

Each applied by hand to the committed tree, run, and restored (2026-10-09).

| # | Mutation | Killed by |
|---|---|---|
| F1 | `nested_wall_conform_sql` answers `None`: the SQL-text sites lose the nested conform | `nested_struct_doors_store_one_wall` (whole-struct MERGE and MERGE INSERT back on `1767323045123456000` in America/New_York); `a_nested_nanosecond_leaf_stores_the_session_wall_at_any_depth`; `a_wall_target_casts_through_the_kernel_of_its_unit` (`arrow_cast((s.st), 'Struct("v": Timestamp(ns))')`) |
| F2 | INSERT's hook never names a nested target | `nested_struct_doors_store_one_wall` (`INSERT … VALUES` and `INSERT … SELECT` back on the UTC wall); the any-depth pin |
| F3 | the kernel's return field is nullable for a string source only, as at `1be18c26` | `an_overflowing_literal_stores_null_without_ansi_as_insert_does` (`declared as non-nullable but contains null values` at `UTC update where`); `the_return_field_is_nullable_where_an_overflow_answers_null` |
| F4 | `wall_kernel_reads` hands a nanosecond target an instant source only, as at `1be18c26` | `an_overflowing_wall_source_answers_as_insert_select_on_the_overwrite_doors` (`Arithmetic overflow: Overflow happened on: 32503680000000001 * 1000` at `UTC ansi=true insert overwrite n`); `the_kernel_reads_what_its_target_cannot_take_by_a_plain_cast` |
| F5 | `zone_store_frame` skips a nested target | the any-depth pin (an overwrite door back on `1767323045123456000`) |
| F6 | nested fields always pair by position | `nested_fields_pair_by_name_and_else_by_position` (a reordered struct keeps its instant leaf). The nine door tests stay green: every door spelling they use names its fields in the target's order, so only the unit pin sees this one. |

F1 to F6 were applied to `1b341d3a`; `5ceb607f` after it restructures one match and names one
helper with no behaviour change, and the whole gate set of §9.7 ran on it.

**The two mutants the pin cut had to keep dying** (C-023), re-run on `5ceb607f`:

| # | Mutation | Killed by |
|---|---|---|
| M5 (the lane's) | the `UPDATE` conform also takes zoned targets (`WriteOp::Update => false`) | the facade control `test_control_targets_answer_what_main_answered[tz_ns-update_column]`: 1 failed, 94 passed, in the one zone kept |
| X2 (the verify's) | `wall_kernel_reads` hands a microsecond NTZ target every source | the facade controls `[ntz_v3 and ntz_v2]-[insert_overwrite, df_overwrite_partitions, df_insert_into_overwrite]`: 6 failed, 89 passed; and now a Rust pin, `the_kernel_reads_what_its_target_cannot_take_by_a_plain_cast`. The nine Rust door tests stay green under it, as the verify found. |

### 9.7 Gates of the fold (C-025)

Run 2026-10-09 on the head tree, one cargo command at a time, under the build lock on cores
48-63.

| Command | Exit | Result |
|---|---|---|
| `cargo fmt --check` | 0 | |
| `make rust-clippy` | 0 | |
| `make rust-panic-ban` | 0 | |
| `cargo test --locked -p repark-functions --lib` | 0 | 906 passed, 1 ignored |
| `cargo test --locked -p repark-iceberg --lib` | 0 | 965 passed |
| `cargo test --locked -p repark-spark --lib` | 0 | 2631 passed, 5 ignored |
| `cargo test --locked -p repark-sql --lib` | 0 | 393 passed |
| `cargo test --locked -p repark-spark --test timestamp_ns_wall_doors --test dml_sessions --test session_timestamp_type` | 0 | 9 + 1 + 7 passed |
| `cargo test --locked -p repark-sql --test ansi_ntz_wall_cast --test ansi_update_cast --test session_wiring` | 0 | 3 + 4 + 4 passed |
| `make develop` | 0 | |
| `pytest python/repark/tests/test_ice_tsns_merge_wall_1.py -q -n 8` | 0 | 179 passed, 32 s |
| `pytest python/repark/tests -q -n 8 -k "ntz or timestamp_ns or tsns or merge or update"` | 0 | 1418 passed, 38 skipped (1703 before the cut of 285) |
| `python3 scripts/sync_map_md.py --check` | 0 | |
| `bash scripts/check_map_md.sh --base origin/main` | 0 | |
| `make check-ledger-grammar` | 0 | |
| `./scripts/check_rust_file_size.sh` | 0 | `timestamp_ns_cast.rs` 930 lines, under the ceiling; the nested conform is its own file |

Two things about the run itself. The clone's `.venv` did not hold `polars`, a declared optional
extra of the facade that two test modules of the `-k` selection import, so the Python gate
first stopped on two collection errors; the locked version (`1.43.2`) was installed into the
clone's `.venv` and the gate re-run. And the first full run of these gates was discarded and
repeated from the start: a dry check of the mutant script had edited the working tree for a
few seconds while that run was compiling. The exits above are from the repeat, on `5ceb607f`.

Scratch (the nested matrix, the copies of the verify's scripts, the recorder outputs, the
mutant and gate logs) is in `/tmp/oc-worker/tsns-wall-f1/`, outside the repository.

## Q. Questions for a ruling

- **Q1 (C-013, RULING).** Should the mirror be fixed: a wall written into a `timestamptz_ns`
  column read in the session zone on every door, as INSERT reads it, and the unfiltered
  `UPDATE` answering instead of the raw Arrow error? *Premise:* 38 + 6 measured cells; the
  brief lists a `timestamptz_ns` target as a control that must not move and the parity row
  said the zoned type agreed everywhere, which holds for an instant source only. *Lean:* yes,
  as its own small unit: `is_ltz_instant_target` and `zone_stores` gain the zoned nanosecond
  target and route to `__repark_cast_timestamptz_ns__`, the `UPDATE` conform drops its
  `wall_only` filter; mutant M5 shows the pins that would move.
- **Q2 (C-014, RULING).** Should a nanosecond source narrow into a microsecond column by
  flooring (ruling Q-21c-8) on every door? *Premise:* 72 cells truncate toward zero or read a
  wall as UTC on the overwrite and MERGE-insert doors, and a `TIMESTAMP_NTZ` target truncates
  on every door, INSERT included. Only values before the epoch with digits below the
  microsecond differ by the truncation. *Lean:* yes, a separate unit; it changes microsecond
  controls, which this unit was told not to touch.
- **Q3 (C-015, OWNER).** Should `UPDATE` and `DELETE` lower `CAST(… AS timestamp_ns)`?
  *Premise:* they refuse `Unsupported SQL type`, in `SET` and in `WHERE`; a nanosecond column
  as the source works. *Lean:* yes, low priority; a lowering gap, not a wrong value.
- **Q4 (C-017, OWNER). First for the owner.** Should a nested microsecond `TIMESTAMP_NTZ`
  field store one wall from every door? *Premise:* on main and at head an instant stores its
  session wall through field assignment (28 measured cells) and its UTC wall through nested
  INSERT, MERGE INSERT, whole-struct assignment and the overwrite doors (106 cells), so one
  table can hold two walls for one input (§9.2); a top-level `TIMESTAMP_NTZ` column stores
  the session wall from every door since NTZ-STORE-DOORS-1. Fixing it rewrites what 106
  microsecond cells store today, which is why the ruling kept it out of a bug release's
  fold. *Lean:* yes, as its own unit, with the session wall as the one answer: the kernel's
  nested form of fold 1 needs only an NTZ twin (or a unit argument) and the same five sites;
  the release note should say that nested NTZ values written by INSERT before the fix hold
  the UTC wall.
- **Q5 (C-020, RULING).** Should the unfiltered `UPDATE` of an out-of-range `TIMESTAMP`
  literal raise `[CAST_OVERFLOW]` under ANSI instead of `[CAST_INVALID_INPUT]`? *Premise:* 16
  cells; the value outcome is right in both ANSI modes and only the error class differs; the
  site is INSERT's planner-cast peel, shared with `INSERT … SELECT <literal>`. *Lean:* yes,
  low priority, by making the peel stop at a typed literal as `INSERT … VALUES` does.
- **Q6 (C-022, OWNER).** Is it acceptable that the ANSI door resolves
  `__repark_cast_timestamp_ns__` by name and lists it in `information_schema.routines`?
  *Premise:* the shared MERGE and UPDATE sites render the kernel by name, so the door must
  register it; `__repark_cast_timestamp_ntz__` and `__repark_cast_map__` are already listed
  the same way. *Lean:* accept, and say so in the release note; hiding the three together is
  a separate change.
- **Q7 (C-026, RULING).** Should the three raw nested refusals of §9.1 answer or refuse by
  name? *Premise:* `UPDATE` with no `WHERE` on any nested column (480 cells), and a
  `TIMESTAMP` literal inside a nested value of `INSERT … VALUES` for an array or of
  `INSERT OVERWRITE … VALUES` (24 cells), raise raw Arrow type errors on main and at head;
  nothing is stored. *Lean:* yes, a separate unit: the literal cases are one more conform
  site each, the unfiltered nested `UPDATE` is a planning gap wider than timestamps.
- **Also seen, no question:** `TIMESTAMP_NTZ '<wall>'` refuses `expects an Int64 wall` when
  the wall is within about 36 minutes of the epoch, in a plain `SELECT` too (O-4). It is not a
  nanosecond or Iceberg defect.
