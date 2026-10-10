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
| C-016 | Fold 1. A `timestamp_ns` leaf nested in a struct, a list or a map (struct in struct, array of struct, array, struct of array, map value, map of struct) stores the wall a top-level column stores, from every door that answers; no door stores a second wall; what refuses, refused on main. | The nested matrix on main, at `1be18c26` and at head (§9.1); the Rust door pins; the kernel's unit pins. | **REJECTED** (2026-10-09, the split) | §11: the nested store is withdrawn by the split; a nested leaf refuses (C-040). Fold 1 measured 2142 answering cells, none wrong, and the re-verify then found the pairing and layout faults. Before the split: §9.1: 4680 cells; 2142 answer, all by the rule (main: 556 of them a UTC wall; `1be18c26`: two walls in one table); 2538 refuse exactly as on main. |
| C-017 | A nested microsecond `TIMESTAMP_NTZ` field stores one wall for one input from every door. | Out of this fold by the ruling: a microsecond control. Measured, six nested doors and 22 more, three zones (§9.2). | **OPEN** | Q4: 28 cells (field assignment) store the session wall, 106 (every other door) the UTC wall, on main and at head. Parity row ICE-TSNS-SQL-1-R-011. |
| C-018 | Fold 1. With ANSI off, an out-of-range `TIMESTAMP` or `DATE` literal through `UPDATE … WHERE` (both row-level modes) and `MERGE … INSERT VALUES` stores NULL, as INSERT does, the two edge instants whose session wall overflows included. | The verify's 42 cells re-measured (§9.3); the Rust door pin; the return-field unit pin. | **PROVEN** | §9.3: 42 of 42, from `declared as non-nullable but contains null values` at `1be18c26`. |
| C-019 | Fold 1. A `TIMESTAMP_NTZ` or `DATE` past the nanosecond range through `INSERT OVERWRITE`, `INSERT … BY NAME`, `overwritePartitions` and `insertInto(overwrite=True)` answers as `INSERT … SELECT`: `[CAST_OVERFLOW]` under ANSI, NULL without; no Rust panic. | The verify's 96 cells re-measured (§9.3); the Rust door and facade pins; the `wall_kernel_reads` unit pin. | **PROVEN** | §9.3: 96 of 96, from a raw Arrow overflow and a caught panic on main and at `1be18c26`. |
| C-020 | Under ANSI, `UPDATE t SET v = <out-of-range TIMESTAMP literal>` with no `WHERE` reports INSERT's error class. | Not the site of C-018 or C-019 (§9.3): the unfiltered `UPDATE`'s planner-cast peel reaches the literal's string. A dated row by the ruling. | **OPEN** | Q5: 16 cells answer `[CAST_INVALID_INPUT]` where INSERT answers `[CAST_OVERFLOW]`; main raised a raw Arrow error. Parity row ICE-TSNS-SQL-1-R-012. |
| C-021 | The count of cells that moved from main is restated against the verify's wider matrix; every moved cell is a `timestamp_ns` target cell and none is a control. | The verify's matrix, probes and overflow matrix re-run on head and compared with its own main outputs (§9.4). | **PROVEN** | §9.4. |
| C-022 | On the ANSI door the kernel's name is not user-visible. | Measured by the verify: after `on_session_built`, `SELECT __repark_cast_timestamp_ns__('…')` resolves and `information_schema.routines` lists it, as it lists the NTZ kernel. A dated row by the ruling. | **OPEN** | Q6. Parity row ICE-TSNS-SQL-1-R-013. |
| C-023 | The facade module is cut to the 57 `timestamp_ns` door tests and one zone of controls; mutants M5 and X2 still die; carry is pinned at the Rust door. | Collect the module; re-run M5 and X2 (§9.5, §9.6). | **PROVEN** | §9.5: 179 tests from 464, the fixture 195 kB from 501 kB. |
| C-024 | Six hand mutants of the fold are each killed by a named pin. | §9.6. | **PROVEN** | §9.6. |
| C-025 | The fold's gates are green with real exit codes. | §9.7. | **PROVEN** | §9.7. |
| C-026 | A nested column answers, or refuses by name, wherever a top-level column answers. | Out of fold 1: none of these stores a value (§9.1). | **OPEN** | Q7, narrowed by fold 2 (C-032): `UPDATE` with no `WHERE` assigning a nested `timestamp_ns` column is now refused by name and `INSERT … VALUES` of `array(TIMESTAMP '…')` stores; what stays is `UPDATE` with no `WHERE` on a nested column of any other type (raw on main) and `INSERT OVERWRITE … VALUES`, which is C-039. Parity row ICE-TSNS-SQL-1-R-014. |
| C-027 | Fold 2. The nested conform pairs struct fields exactly as the cast that follows it pairs them, the rule being an argument each site sets from the door's code: Arrow's cast (`cast`), DataFusion's struct cast (`name`), or names that must be the target's in order (`exact`). A struct with a renamed, recased or reordered field stores one wall from every door, or is refused by name. | The pairing pin at the Rust door (six spellings × six doors × two zones) and on the facade; the kernel's twenty-row pairing table; the re-verify's `nx.py` re-run (§10.1). | **REJECTED** (2026-10-09, the split) | §11: withdrawn by the split with the nested conform; the pairing question is R-016 and card ICE-TSNS-NESTED-1. Before the split: §10.1: the re-verify's 64 wrong-wall cells are 64 session walls; no answering cell of its 4750 differs from the rule. |
| C-028 | Fold 2. Every Arrow layout that can carry a timestamp inside a nested value is conformed or refused, in one `match` with no arm that stores by default: struct, map, list, large list, list view, large list view, fixed-size list, dictionary and run-end-encoded at any depth conform; a union and a non-temporal scalar are refused. | The layout unit pins; six layouts through two DataFrame doors on the facade; the re-verify's `misc2.py` re-run (§10.2). | **REJECTED** (2026-10-09, the split) | §11: withdrawn by the split; no layout is conformed, every layout refuses (C-040). Before the split: §10.2: list view, dictionary child and fixed-size list store the session wall on every door of `misc2.py`. |
| C-029 | Fold 2. For a nested `timestamp_ns` leaf a door stores the rule's session wall at full precision or refuses before any file is written, by one named text built in one function, with a dated registry row. No cell stores a second wall, a cut value or a NULL the source did not hold. | Every nested cell of the re-verify's matrix and of this unit's classified (§10.3); the refusal-text pin. | **REJECTED** (2026-10-09, the split) | §11: withdrawn by the split after the second re-verify found three routes that stored a cut value; replaced by C-040, where no nested cell stores. Before the split: §10.3. Registry row ICE-TSNS-SQL-1-R-015. The cells that are neither are statements whose own source does not evaluate (C-039), and a required leaf handed a NULL on the doors named in §10.3. |
| C-030 | Fold 2. A nested value narrowed from nanoseconds anywhere in its lineage is refused, not stored cut to microseconds, on every door that emits the nested call, MERGE through a subquery included; a typed NULL keeps nine digits, and a microsecond field beside the leaf may still be narrowed. | The narrowing pins at the Rust door (four spellings × five doors, three MERGE spellings, the sibling case). | **REJECTED** (2026-10-09, the split) | §11: withdrawn by the split; the lineage guard is deleted. Its top-level remainder is C-043. Before the split: §10.4: `INSERT … SELECT array(ns, NULL)` is refused; the re-verify's 44 `arr_empty` cells that stored a cut value on both builds are refusals. |
| C-031 | Fold 2. A required `timestamptz_ns` column, a control, refuses an out-of-range value with main's text exactly. | The re-verify's six cells re-measured; the Rust door pin. | **PROVEN** | §10.2: 0 of the `misc2.py` control cells differ from main. |
| C-032 | Fold 2. `UPDATE` with no `WHERE` assigning a column that holds a nested `timestamp_ns` leaf is refused by name, and an unrelated `UPDATE` with no `WHERE` on such a table still runs; `INSERT … VALUES` of `array(TIMESTAMP '…')` stores the session wall. | The unstorable pin; the any-depth pin's array row. | **REJECTED** (2026-10-09, the split) | §11: withdrawn by the split; replaced by C-040 (the refusal) and C-041 (the unrelated statement still runs). Before the split: §10.3: 480 cells of this unit's matrix from a raw Arrow error to the named refusal. |
| C-033 | The red check of pull request #1018 on `59462d6a` is found and fixed. | §10.6. | **PROVEN** | §10.6: the map lockstep guard, `crates/repark-functions/src/map.md`. |
| C-034 | The re-verify's six nested mutants and this fold's own are each killed by a Rust-level pin. | §10.7. | **REJECTED** (2026-10-09, the split) | §11: withdrawn by the split with the code the mutants broke; the split's mutants are C-047. Before the split: §10.7. |
| C-035 | The re-verify's `nx.py` and `misc2.py`, unchanged, and this unit's whole matrix are re-run on head; no control cell moved. | §10.1, §10.2, §10.5. | **PROVEN** | §10.5. |
| C-036 | The fold's gates are green with real exit codes. | §10.8. | **PROVEN** | §10.8. |
| C-037 | Struct fields pair on each door as Spark pairs them. | Out of this unit: every struct column, on main. Spark 4.1.2 measured for the rule on `TIMESTAMP_NTZ` leaves (§10.1). | **OPEN** | Q8: the INSERT family pairs by name where Spark pairs by position; `INSERT … BY NAME` and `overwritePartitions` pair by Arrow's rule where Spark pairs by name. Parity row ICE-TSNS-SQL-1-R-016. |
| C-038 | A nanosecond value beside an untyped NULL keeps its digits. | Out of this unit: expression typing, upstream of every store (§10.4). | **OPEN** | Q9: `coalesce(ns, NULL)` and four siblings are typed microseconds; a top-level `timestamp_ns` column stores the cut value on every door, as on main. Parity row ICE-TSNS-SQL-1-R-017. |
| C-039 | A statement whose `VALUES` list holds a nested `TIMESTAMP` literal, a mixed map row or a struct of the wrong arity evaluates or is refused by name. | Out of this unit: the source fails with no table involved (§10.3). | **OPEN** | Q10. Parity row ICE-TSNS-SQL-1-R-018. |
| C-040 | The split. A write that supplies a value for a column whose type holds a naive `timestamp_ns` leaf below the top level is refused before any file is written, on every door, by one named text that names the target column and leaf; one function decides and it reads the target type only. | The Rust door pin (nine shapes, seventeen doors, two zones, no file, no snapshot); the function's unit tests; the facade pin (fourteen layouts, eight routes); the verifier's nested matrices re-run (§11.4). | **PROVEN** | §11.1, §11.4: 0 nested cells store. Parity row R-015. |
| C-041 | The split. A statement that does not supply such a column still runs: an INSERT column list or `BY NAME` source that omits it, a bare `NULL` literal for the whole column, UPDATE and MERGE of other columns, DELETE, both row-level modes, compaction, manifest rewrite, a read and a time-travel read; rows already stored keep every nanosecond. | The Rust door pin `a_statement_that_does_not_supply_the_column_runs_and_carries_its_rows`. | **PROVEN** | §11.1. |
| C-042 | The split. A nested `timestamptz_ns` leaf and a nested microsecond leaf are not refused and answer as main answers. | The Rust door control; the verifier's control shapes re-run (§11.4). | **PROVEN** | §11.4: 0 control cells moved. |
| C-043 | The split. `UPDATE` with no `WHERE` whose `SET` value was narrowed from nanoseconds to microseconds (`CASE … ELSE NULL END`, `if(…, c, NULL)`, `array(c, NULL)[0]`) is refused by name and stores nothing; a plain column, a typed NULL, a zoned source and a literal store; the other doors are not refused. | The Rust door pin; `updtrunc.py` and `gq.py trunc` re-run (§11.2, §11.4). | **PROVEN** | §11.2. |
| C-044 | The split. A run-end-encoded or dictionary-encoded instant source stores the session wall at full precision through `INSERT OVERWRITE`, `INSERT … BY NAME`, `overwritePartitions` and `insertInto(overwrite=True)`. | The facade pin in America/New_York read from the Parquet file; the kernel and `wall_kernel_reads` unit pins; `lx.py` re-run (§11.3, §11.4). | **PROVEN** | §11.3. |
| C-045 | The split. Every top-level door has a pin that reads a value with non-zero digits below the microsecond from the Parquet file, not through SELECT. | `test_every_door_writes_the_digits_below_the_microsecond_into_the_parquet_file`: nineteen doors, three zones, two source types. | **PROVEN** | §11.3. |
| C-046 | The split. The verifier's `nx.py`, `nx2.py`, `lx.py`, `leak.py`, `updtrunc.py` and `gq.py` (door and trunc modes), unchanged, are re-run on head in America/New_York and Asia/Kolkata against its base outputs: no nested `timestamp_ns` cell stores, every answering top-level `timestamp_ns` cell equals INSERT, no control moved. | §11.4. | **PROVEN** | §11.4. |
| C-047 | The split. Hand mutants of the gate, one per container kind and per statement form, and of the two top-level fixes are each killed by a named pin. | §11.5. | **PROVEN** | §11.5. |
| C-048 | The split's gates are green with real exit codes. | §11.6. | **PROVEN** | §11.6. |
| C-049 | A nested `timestamp_ns` leaf stores the session wall on every door. | Out of this unit by the owner's amendment 3: card ICE-TSNS-NESTED-1. | **OPEN** | Q12. The refusal of C-040 stands until that unit. Parity row R-015. |

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

## 10. Fold 2, after the re-verify of `59462d6a` (C-027 to C-039)

The re-verify (2026-10-09, 22,495 cells against the merged main `ca5a062a`) held everything
claimed for top-level columns and failed the nested form on two S1 and one S2. The
orchestrator's ruling of the same day: for a nested `timestamp_ns` leaf a door stores the
rule's session wall at full precision or refuses before any file is written with one named
refusal; no third outcome. Order kept: red pins (`4fb9211b`), then the fix (`c01f764c`,
`6aef7f87`).

### 10.1 Pairing (C-027, C-037)

**The cause, as the re-verify named it.** Fold 1's conform paired struct fields by name as
soon as one source name matched a target name. The doors pair differently from one another,
so with one field renamed the conform converted one field and the door then stored another.

**Each door's real rule**, read from the code that runs after the conform and measured:

| Door | The cast that follows the conform | Rule | Word |
|---|---|---|---|
| `INSERT … VALUES` / `SELECT`, `REPLACE WHERE`, `writeTo().append()`, `insertInto()`, `saveAsTable` | the planner's `Expr::Cast`, DataFusion `nested_struct::cast_struct_column` | by name, case-sensitive; a target field with no source field is filled with NULL; no shared name, or a nullable field for a required one, refuses; the same rule continues under `List` into `List`, Arrow's rule under a `LargeList` source or a map | `name` |
| `INSERT OVERWRITE`, `INSERT … BY NAME`, `overwritePartitions`, `insertInto(overwrite=True)` | `positional_map_overwrite_batch`, Arrow `cast_struct_to_struct` | in order when the names are the target's in order; by name when every target name is among the source's; else by position | `cast` |
| MERGE UPDATE SET (whole value, `SET *`), NOT MATCHED BY SOURCE | `arrow_cast(…)` rendered by `store_assignment_cast_sql`, which DataFusion simplifies to the same `Expr::Cast` | as the first row; the nested-assignment fold has already rebuilt a struct it could pair by name (case as the session says) and refused the rest with Spark's texts | `name` |
| MERGE INSERT, `UPDATE … WHERE` | none of their own | the value has passed the store-assignment gate or been rebuilt to the target's names; anything else cannot be known here | `exact` |

**What a by-name door would have stored as NULL is refused.** `INSERT … SELECT
named_struct('q', x, 'b', x)` into `struct<a: timestamp_ns, b>` stored `a = NULL` on main and
at `59462d6a` and kept the row. Under the ruling a `timestamp_ns` leaf holds the value or the
statement is refused, so the conform refuses where its door's pairing leaves the leaf without
a source (R-015, condition 3). That is a change from main on those cells: a stored NULL
becomes a named refusal.

**Spark's rule, measured for the pairing only** (Spark 4.1.2, Iceberg 1.11.0,
`struct<a: TIMESTAMP_NTZ, b: TIMESTAMP>`, eight spellings × ten doors, 80 cells, scratch
`spark_pair.py`): `INSERT … VALUES` / `SELECT`, `INSERT OVERWRITE` and
`insertInto(overwrite=True)` pair **by position**; `INSERT … BY NAME`, MERGE, `UPDATE`,
`writeTo().append()` and `overwritePartitions()` pair **by name**, case-insensitive, and
refuse a field they cannot find (`CANNOT_FIND_DATA`); every door refuses a missing or an extra
field. RePark differs in kind on three doors, for every leaf type and on main: the INSERT
family (by name), `INSERT … BY NAME` and `overwritePartitions` (Arrow's rule). Not changed
here; parity row R-016, C-037, Q8.

### 10.2 Layouts and the controls of `misc2.py` (C-028, C-031)

`layout` in `nested.rs` is the one `match`; it names every Arrow type and no arm stores by
default.

| Source layout | Answer | Pin |
|---|---|---|
| `Struct` | conform, fields paired by the door's rule | Rust door and kernel |
| `Map` | conform, key and value | kernel, Rust door, facade |
| `List`, `LargeList` | conform | kernel, facade |
| `ListView`, `FixedSizeList` | cast to `List`, conform | kernel, facade |
| `LargeListView` | cast to `LargeList`, conform | kernel |
| `Dictionary`, `RunEndEncoded`, any depth | decode to the values, conform | kernel; dictionary child on the facade |
| `Timestamp`, `Date32`, `Date64`, `Utf8`, `LargeUtf8`, `Utf8View` | the leaf conversion | kernel, doors |
| `Null` | passes | kernel, Rust door |
| `Union` | refuse (R-015, condition 1) | kernel |
| every other scalar where the leaf belongs | refuse (R-015, condition 1) | kernel, Rust door |

**`misc2.py`, unchanged, on head** (America/New_York, ANSI on and off, 304 cells each):

| | Result |
|---|---|
| list view, dictionary-encoded struct child, fixed-size list, large list, plain list, struct, swapped struct, map value, map key through `append`, `overwritePartitions` and `mergeInto` | every answering cell holds the session wall (`1767305045123456000`); the fixed-size list's second element too |
| a struct with an out-of-range value under a NULL parent | stores, the row NULL; main raised a raw Arrow overflow |
| the same cases through MERGE for a list, and a string view | refused by the store-assignment gate's own text, as on main |
| required `timestamptz_ns`, 60 cells per ANSI mode | 60 identical to main, 0 moved (the re-verify's six are back) |
| required `TIMESTAMP_NTZ` and `TIMESTAMP`, 60 cells each per mode | identical to main |

### 10.3 Every nested cell: the rule's wall, or a named refusal (C-029, C-032, C-039)

Both nested matrices on head, every `timestamp_ns` cell put in one of three sets:

| Matrix | Cells | Stores the rule's wall | Named refusal | Neither |
|---|---|---|---|---|
| the re-verify's `nx.py`, unchanged (25 shapes with a `timestamp_ns` leaf × 19 door spellings × 5 sources × 2 zones) | 4750 | 2864 | 1864 | 22 |
| this unit's nested matrix (§9.1, 9 shape spellings × 22 to 28 doors × 5 sources × 3 zones) | 4680 | 2145 | 1188 | 1347 |
| **both** | **9430** | **5009** | **3052** | **1369** |

**No answering cell holds anything but the rule's wall:** 5009 of 5009; none a UTC wall, none
cut to microseconds, none a NULL the source did not hold. The Parquet values agree with the
`SELECT` in every cell of `nx.py`.

**The re-verify's matrix by shape** (cells: wall / named refusal / neither):

| Shape | Wall | Named | Neither |
|---|---|---|---|
| `multi`, `ns2`, `ns2_swapped`, `pair`, `pair_swapped`, `nullstruct`, `evo_field`, `evo_col`, `map_nskey`, `map_nsboth` (each) | 180 | 10 | 0 |
| `deep`, `arr_arr` (each) | 110 | 80 | 0 |
| `map_arr` | 108 | 80 | 2 |
| `arr_empty` | 66 | 124 | 0 |
| `pair_case`, `pair_case1` (each) | 130 | 60 | 0 |
| `pair_part2` | 90 | 100 | 0 |
| `pair_extra` | 80 | 100 | 10 |
| `pair_missing` | 40 | 140 | 10 |
| `pair_part1`, `pair_part3`, `pair_renamed`, `pair_unnamed`, `ns2_part` (each) | 40 | 150 | 0 |
| `req_leaf` | 0 | 190 | 0 |

**The named refusals, by text** (re-verify's matrix + this unit's):

| Refusal | Cells |
|---|---|
| R-015, the one text of this fold (`[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] … cannot be stored into this timestamp_ns leaf: …`) | 784 + 540 |
| MERGE and `UPDATE … WHERE`, a struct they cannot pair: `[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA]`, `EXTRA_STRUCT_FIELDS` (Spark's texts, as on main) | 800 + 0 |
| MERGE, an array-bearing column: `cannot store-assign column` (as on main) | 280 + 540 |
| `UPDATE … SET` of a `CAST(… AS timestamp_ns)` literal: `Unsupported SQL type` (R-010, as on main) | 0 + 108 |

What R-015 replaced on the re-verify's matrix: 64 wrong walls became walls, not refusals; 200
cells that stored a NULL leaf on main (a renamed, recased or missing field through a by-name
door) are refused; 44 `arr_empty` cells that stored a cut value on both builds are refused;
the raw `Unsupported CAST from Struct` of the by-name doors, the raw overwrite-door errors for
too few fields and every raw refusal of a required leaf handed a NULL are the named text. On
this unit's matrix 480 `UPDATE` with no `WHERE` cells went from a raw Arrow error to it, and 3
`INSERT … VALUES` of `array(TIMESTAMP '…')` cells from a raw error to the wall.

**The cells that are neither (1369), all of one kind: the statement's own source does not
evaluate, with or without a table, on main as at head (C-039, R-018).** No door is reached and
nothing is stored.

| What fails | Cells |
|---|---|
| this unit's probe spellings `map('k', <column>)` and `map(…, CAST(NULL AS …))`: the `map` function's `map requires key and value lists to have the same length`; a row constructor with no `id`; the caught `list array` panic for a multi-row `VALUES` of maps | 1326 |
| `INSERT OVERWRITE … VALUES` whose row holds a `TIMESTAMP` literal inside a struct, array or map: `column types must match schema types`, raised by a bare `VALUES (1, array(TIMESTAMP '…'))` too | 21 |
| `INSERT … VALUES` of a struct literal with fewer or more fields than the column: the planner's `type mismatch and can't cast to got Struct(…)` | 20 |
| a multi-row `VALUES` of `map(…)` mixing a `TIMESTAMP_NTZ` literal and `to_timestamp_ntz`: the caught `list array` panic, raised by a bare `SELECT … FROM (VALUES …)` too | 2 |

So the sentence the ruling asks for holds for every cell that reaches a write door, and not
for these 1369, 43 of them outside this unit's own probe spellings. Making them answer means
repairing how a `VALUES` list is typed and evaluated for every target, which moves control
cells from an error to a value; that is Q10.

### 10.4 No truncation (C-030, C-038)

`array(ns, NULL)` is typed `List(Timestamp(µs, "UTC"))` before any store sees it: the planner
casts the NULL to its `Timestamp(ns)`, the session's timestamp rule reads that cast as the SQL
`TIMESTAMP`, and the nanosecond argument is narrowed to match. On main the nested INSERT then
failed on an internal error; at `59462d6a` it stored `…123456000` for `…123456789`. The store
cannot recover the digits, so it refuses: the optimizer rule `NestedNanosecondGuard` finds
every nested call of the kernel in the plan and follows what feeds its `timestamp_ns` leaves,
through the call's own argument and down the plan under it, for a call of
`__repark_narrow_timestamp_ns__` or a cast from a nanosecond timestamp to a coarser one. One
rule guards every door that emits the nested call, MERGE through a subquery included. A field
beside the leaf may still be narrowed on purpose, and `array(ns, CAST(NULL AS timestamp_ns))`
stores nine digits.

**Not fixed, and not this unit's:** the typing itself. A top-level `timestamp_ns` column
stores the cut value for `INSERT … SELECT coalesce(ns, NULL)` on every door, on main and at
head (the re-verify measured it; scratch `t7.py` at head). Parity row R-017, C-038, Q9.

### 10.5 The matrices re-run (C-035)

| Instrument, on head | Cells | Result |
|---|---|---|
| the re-verify's `nx.py`, unchanged | 4750 + 570 controls | §10.3; 0 of 570 nested control cells moved from main; the microsecond and zoned leaves beside a `timestamp_ns` leaf are identical to main in every answering cell; no file schema moved |
| the re-verify's `misc2.py`, unchanged | 304 × 2 | §10.2 |
| this unit's nested matrix | 4680 + 3120 controls | §10.3; 0 of 3120 controls moved from main |
| this unit's whole matrix (`--whole`) | 1926 | 47 moved from main, all `timestamp_ns`; 0 of 1605 controls; identical to fold 1, cell for cell |
| the first verify's door matrix (`mx.py`) | 9870 | 324 moved from main, all `timestamp_ns`; 0 of 8225 controls; identical to fold 1, cell for cell |
| the first verify's overflow matrix (`bx.py`) | 4032 | 976 `timestamp_ns` cells moved from main, 0 of 2016 `timestamptz_ns`; identical to fold 1, cell for cell |
| the first verify's side-effect probes (`px.py`, America/New_York) | 2439 | differs from fold 1 on 17 cells, all on a nested `timestamp_ns` table: 16 raw errors (`UPDATE` with no `WHERE`, and a scalar subquery assigned to a field) are the named refusal, 1 raw error (`INSERT … VALUES` of an array literal) is the wall; 0 control probe cells moved from main |

**Controls moved: none**, on any instrument.

### 10.6 The red check of pull request #1018 (C-033)

`gh pr checks` could not be read from this clone (no credentials: `HTTP 401`). Every check of
`ci.yml` that needs no build was run locally on `59462d6a`; all passed against the clone's
`origin/main`, which was the stale `22cce0eb`. Against the main the branch had merged,
`ca5a062a`, the pull-request-only step of the `Repo guards` job fails:

```text
$ bash scripts/check_map_md.sh --base ca5a062a
ERROR: crates/repark-functions/src/map.md was not updated on this branch (map.md lockstep rule).
```

Fold 1 changed `crates/repark-functions/src/timestamp_ns_cast.rs` and wrote its notes in the
subdirectory's map only. Fold 2 adds the row (and the one `crates/repark-spark/src/merge/`
needed for this fold's own change); the guard exits 0 against `ca5a062a`. Whether that was the
only red check cannot be confirmed from here.

### 10.7 Mutants of the fold (C-034)

Sixteen, each applied by hand to `6aef7f87`, run against the kernel unit tests, the
`ntz_store` unit tests and both Rust door files, and restored (2026-10-09). The first eight
are the re-verify's six rewritten for the new code (two of them in two variants); every one
dies at the Rust level, where the re-verify found one that survived every suite and three
held by a single unit test or by the facade alone.

| # | Mutation | Killed by (Rust pins) |
|---|---|---|
| Y1 | Arrow's rule never pairs by position (a renamed field is refused instead) | door: `a_struct_source_pairs_as_the_door_that_stores_it_pairs`; kernel: `struct_fields_pair_as_the_door_named_in_the_argument_pairs`, `the_pairing_under_a_list_is_the_cast_the_door_runs_there`, `the_nested_form_reads_its_pairing_and_column_from_literals` |
| Y1b | Arrow's rule pairs by name as soon as one name matches (fold 1's rule) | the same four |
| Y2 | a map is not conformed | door: `a_nested_nanosecond_leaf_stores_the_session_wall_at_any_depth`; kernel: `a_nested_instant_takes_the_session_wall_at_any_depth` |
| Y3 | a large list and a large list view are not conformed (survived every suite at `59462d6a`) | kernel: `every_list_layout_is_stored_as_a_plain_list`, `the_pairing_under_a_list_is_the_cast_the_door_runs_there` |
| Y3b | a list view and a fixed-size list are not normalised | the same two |
| Y4 | `holds_nested_ns_wall` does not recurse | door: `a_struct_source_pairs_as_the_door_that_stores_it_pairs`; unit: `only_a_naive_nanosecond_leaf_makes_a_nested_wall_target` |
| Y5 | a `DATE` source does not reach the kernel on the overwrite doors | door: `an_overflowing_wall_source_answers_as_insert_select_on_the_overwrite_doors`; unit: `the_kernel_reads_what_its_target_cannot_take_by_a_plain_cast` |
| X2 | the kernel reads every source for a microsecond NTZ target | unit: `the_kernel_reads_what_its_target_cannot_take_by_a_plain_cast` (and six facade controls, §9.6) |
| G6 | a leaf a by-name door leaves without a source is not refused | door: the pairing pin, `the_refusal_names_the_leaf_and_the_reason`; kernel: the pairing table |
| G7 | the guard refuses only when both the argument and the plan under it narrow | door: `a_narrowed_nanosecond_value_is_refused_not_truncated`, `a_field_beside_the_leaf_may_be_narrowed_and_merge_is_guarded_too` |
| G7b | the guard does not follow the plan under the call | the same two (the subquery and MERGE spellings) |
| G8 | the zoned kernel's return field is nullable as at `59462d6a` | door: `a_required_zoned_column_refuses_an_overflow_as_main_does`; kernel: `the_return_field_is_nullable_where_an_overflow_answers_null` |
| G9 | under a list the by-name door is taken to run Arrow's cast | door: the pairing pin (one field renamed, in an array); kernel: `the_pairing_under_a_list_is_the_cast_the_door_runs_there` |
| G10 | a child is not masked by its NULL parent | kernel: `a_value_under_a_null_parent_is_not_read`, `a_null_typed_child_passes_under_a_null_parent` |
| G11 | `UPDATE` with no `WHERE` does not check that the nested column is carried | door: `what_cannot_feed_the_leaf_is_refused_by_name` |
| G12 | a union or a non-temporal scalar where the leaf belongs passes unconformed | kernel: `a_layout_that_cannot_carry_the_leaf_is_refused_by_name`, `a_string_leaf_is_read_as_insert_reads_a_literal`; door: `what_cannot_feed_the_leaf_is_refused_by_name` |

The facade pins were not run under the mutants (each needs the Python module rebuilt); they
hold the same layouts and pairing through the DataFrame doors on the unmutated build.

### 10.8 Gates of the fold (C-036)

Run 2026-10-09 on `6aef7f87`, one cargo command at a time under the build lock on cores
48-63; the Python and document gates on the tree of the fold's last commit.

| Command | Exit | Result |
|---|---|---|
| `cargo fmt --check` | 0 | |
| `make rust-clippy` | 0 | |
| `make rust-panic-ban` | 0 | |
| `cargo test --locked -p repark-functions --lib` | 0 | 916 passed, 1 ignored |
| `cargo test --locked -p repark-iceberg --lib` | 0 | 969 passed |
| `cargo test --locked -p repark-spark --lib` | 0 | 2634 passed, 5 ignored |
| `cargo test --locked -p repark-sql --lib` | 0 | 393 passed |
| `cargo test --locked -p repark-core --lib` | 0 | 1419 passed, 1 ignored |
| `cargo test --locked -p repark-spark --test timestamp_ns_wall_doors --test timestamp_ns_nested_shapes --test dml_sessions --test session_timestamp_type` | 0 | 9 + 7 + 1 + 7 passed |
| `cargo test --locked -p repark-sql --test ansi_ntz_wall_cast --test ansi_update_cast --test session_wiring` | 0 | 3 + 4 + 4 passed |
| `make develop` | 0 | |
| `pytest python/repark/tests/test_ice_tsns_merge_wall_1.py -q -n 6` | 0 | 195 passed |
| `pytest python/repark/tests -q -n 8 -k "ntz or timestamp_ns or tsns or merge or update"` | 0 | 1435 passed, 38 skipped |
| `python3 scripts/sync_map_md.py --check` | 0 | |
| `bash scripts/check_map_md.sh --base ca5a062a` (the merged main; §10.6) | 0 | |
| `make check-ledger-grammar` | 0 | |
| `./scripts/check_rust_file_size.sh` | 0 | the narrowing UDF moved to its own file to keep `timestamp_ns_cast.rs` under the ceiling |
| `./scripts/check_lib_rs.sh` | 0 | on the fold's last commit; see (4) below |
| the other checks of `ci.yml` that need no build (crate DAG, `lib.rs`, manifest, ledgers, docs compaction and links, owner ruling, dual wire, matrix-test liveness, Python lint, format, conventions, docstrings, example coverage, lock, TOML, spelling) | 0 | run on `59462d6a` for §10.6 and again on the fold's tree |

Four things about the run itself. (1) `c01f764c` passed these gates and still carried a
regression: a struct with a Null-typed child raised a raw Arrow error on every door. No pin
had such a child; the re-run of the re-verify's own matrix found it (180 cells of its
`nullstruct` shape), `6aef7f87` fixes it with a kernel pin and a four-door pin, and every gate
and matrix above was run again on that commit. (2) `gh` holds no credentials in this clone, so
the red check was found by running the workflow's steps (§10.6). (3) The matrices of §10.5
ran on the module built from `6aef7f87`, except the first verify's door matrix and this unit's
whole matrix, which exercise no nested value: those two are from the build of `c01f764c` and
were not repeated after the fix.

(4) One guard was not in the list above and failed after the ledger was first committed:
`./scripts/check_lib_rs.sh` (the `Repo guards` job) reported
`repark-functions src/lib.rs is 189 lines (ceiling 186)`, the three lines that added the
optimizer rule in `register_all`. A ceiling in the way is not edited: the fold's last commit
moves the registration to `repark-spark/src/extension.rs`, beside its call of `register_all`,
and `lib.rs` is back to 186 lines. Re-run on that commit: `cargo fmt --check`,
`make rust-clippy`, `make rust-panic-ban`, `make develop`, the `repark-functions`,
`repark-spark` and `repark-sql` lib suites (916, 2634, 393), the four Rust door files and the
three ANSI ones, the facade module and the Python gate, every no-build check of `ci.yml`
(all exit 0), and seven shapes of the re-verify's `nx.py` in both zones (1330 cells, identical
to the run on `6aef7f87`, the narrowing refusals among them). The mutants and the full
matrices were not repeated for it: the rule and every site are unchanged, only where the
session learns of the rule.

Scratch (the copies of both verifiers' scripts, their outputs for this head, the Spark pairing
probe, the mutant and gate logs) is in `/tmp/oc-worker/tsns-wall-f1/`, outside the
repository.

## 11. The split, after the second re-verify of `c6d947a3` (C-040 to C-049)

The second re-verify (2026-10-09, 72,077 cells, values read from the Parquet files, five
zones) held every top-level claim and failed the nested form again: one nested S1 (a value
cut to microseconds reached a nested leaf through an array element, a SQL temporary view and
a cached frame, none of which a lineage guard can see), one nested S2 (700 cells answered
with a planner error that has no name), and two top-level S1. The owner had adopted
amendment 3 beforehand: if fold 2 failed on any nested shape, the pull request splits. The
top-level fix ships; a nested `timestamp_ns` leaf refuses by name on every door; the nested
store becomes card ICE-TSNS-NESTED-1
([card](../../roadmap/mid-term/ice-tsns-nested-1-card-2026-10-09.md)). Order kept: red pins
(`65bb2042`), then the fix (`e8a862fb` and the commits after it).

### 11.1 One gate, by the target's type (C-040, C-041, C-042)

**Removed.** The nested conform of folds 1 and 2 is deleted, not disabled: the kernel's nested
call, the pairing words, the layout `match`, the lineage guard
`repark_nested_timestamp_ns_guard` and every site that emitted them. `nested.rs`,
`nested_tests.rs`, `lineage.rs` and `narrow.rs` are gone; the store sites in
`repark-iceberg` and the INSERT hook are as they were before fold 1, plus fold 1's two
top-level fixes (the wall kernel's nullable return field, `wall_kernel_reads`). No top-level
path used any of the removed code. Clauses C-016, C-027 to C-030, C-032 and C-034 are
REJECTED.

**Added.** `repark_iceberg::write::nested_ns_gate::refuse_nested_ns_supply` decides, and
nothing else does. It reads two things: the **target** column's type (is there a
`Timestamp(ns, None)` below the top level, in a struct, a list of any layout, a map key or a
map value) and the parsed statement (does it supply a value for that column). It never reads
the source's type, so a source shape, an Arrow layout, a view or a cached frame cannot pass
it. The Spark router calls it once at the top of `execute_inner`, before any door is chosen;
every SQL door and every DataFrame writer reaches the store through that function, so the
guard is one chokepoint and not a list of routes. It fires before the planner and before
every older gate. The text:

```text
[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incompatible data for the table
`ice`.`ns`.`t`: Cannot safely cast `st`.`a` to "TIMESTAMP_NS". A nested timestamp_ns leaf is
not writable yet: omit the column `st` or supply NULL for it. SQLSTATE: KD000
```

It names the target table, column and leaf, never the source expression (the last verify
found the old text naming `named_struct(a,ice.ns.src.l)` on `INSERT OVERWRITE`).

**Supplying a value.** Three answers per column: the statement does not name it; it gives a
bare `NULL` literal for the whole column in every row; anything else. Only the last is
refused. What the function cannot read counts as a value: `SELECT *`, a set operation, a
`PARTITION` clause, a MERGE star, a DataFrame written by name, an assignment to a field
inside the column.

**The decision on an untyped NULL: it stores.** `INSERT … VALUES (1, NULL, 0)`,
`INSERT … SELECT id, NULL, 0`, `UPDATE … SET st = NULL` and a MERGE clause that assigns or
inserts `NULL` for the whole column run. Nothing is converted, so no wall can be wrong.
`CAST(NULL AS …)` and a DataFrame `lit(None)` column are refused: the gate reads the
statement's text for the literal and does not evaluate types.

**Still allowed, each pinned** (`a_statement_that_does_not_supply_the_column_runs_and_carries_its_rows`,
both row-level modes): an INSERT column list or a `BY NAME` source that omits the column;
UPDATE and MERGE of other columns; DELETE; the copy-on-write and merge-on-read carry of rows
already stored, which read back every nanosecond; `rewrite_data_files` and
`rewrite_manifests`; a read; a time-travel read.

**Not gated, by decision.** `CREATE TABLE … AS SELECT` and `REPLACE TABLE … AS SELECT` (the
leaf takes the source's own type, nothing is converted; it is also how the carry pin seeds
its rows) and a column added by `ALTER TABLE` (the write that follows is gated). The streaming
sink and the Rust `repark_iceberg::write::append` API are below the SQL router. The ANSI door
cannot create such a column. A catalog failure while the gate loads the target is returned,
not skipped (`a_target_that_fails_to_load_refuses_instead_of_writing_positionally` went red
on the first draft, which skipped it).

**Controls.** A nested `timestamptz_ns` leaf and a nested microsecond leaf are not matched
(`a_nested_zoned_or_microsecond_leaf_is_not_guarded`); R-008 and R-011 stay as recorded.

**The change from main, adopted.** A nested `timestamp_ns` write that stored on main now
refuses: the session wall through `INSERT … SELECT`, `append` and field assignment, the UTC
wall through `INSERT OVERWRITE`, MERGE and the overwrite writers.

**Draft sentence for the v1.5.4 release note.** *Writing a value into a column that holds a
`timestamp_ns` field inside a struct, an array or a map is refused with
`[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] … A nested timestamp_ns leaf is not writable
yet`; earlier releases stored the session wall through some statements and the UTC wall
through others. Rows already stored are read, carried and compacted unchanged; a statement
that omits the column or gives it `NULL` still runs; a top-level `timestamp_ns` column now
stores the session wall through every statement.*

### 11.2 The unfiltered `UPDATE` no longer stores a cut value (C-043)

`UPDATE t SET v = CASE … ELSE NULL END`, `if(…, c, NULL)` and `array(c, NULL)[0]` with no
`WHERE` raised a raw Arrow error on main. The unit's first fix made that door answer, and
since the expression is typed microseconds upstream (R-017) it stored a value cut to
microseconds, and for a DST-gap wall an hour off: 25 cells of the re-verify. The INSERT hook
now refuses an `UPDATE` plan whose `SET` value holds a cast from a nanosecond timestamp to a
coarser one (`refuse_narrowed_update`; a typed NULL is not a narrowing, a `CASE` condition is
not searched):

```text
[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incompatible data for the table
`ice.ns.u`: Cannot safely cast `v` "TIMESTAMP" to "TIMESTAMP_NS". The value was narrowed from
nanoseconds to microseconds before the store; give the NULL beside it the type timestamp_ns.
SQLSTATE: KD000
```

**Not widened.** The filtered `UPDATE` and every other door are untouched: they store the cut
value on main (R-017) and a refusal there would also refuse a written
`CAST(ns AS TIMESTAMP)` and `date_trunc('second', ns)`, which store on main. On this one door
main answered nothing for these spellings, so nothing that stored is refused, with one
exception: `array(c, NULL)[0]` of a `timestamp_ns` source stored a cut value at the UTC wall
on main and is now refused (one cell per zone of `gq.py trunc`).

### 11.3 Encoded sources, and pins read from the file (C-044, C-045)

A run-end-encoded instant through `INSERT OVERWRITE`, `INSERT … BY NAME`,
`overwritePartitions` and `insertInto(overwrite=True)` stored its UTC wall, on main and at
`c6d947a3` (32 cells of the re-verify): `wall_kernel_reads` did not see through the encoding,
so the plain cast kept the UTC reading. It now sees through `RunEndEncoded` and `Dictionary`
for a nanosecond wall target, and `zone_store_frame` casts the column to its values' type
(`wall_kernel_input`) before the kernel. The kernel is unchanged and still refuses an encoded
argument, so the doors that refused such a source on main refuse it still.

Pins: `test_an_encoded_instant_source_stores_the_session_wall` (both encodings, the four
doors, America/New_York, non-zero digits below the microsecond, the value read from the
Parquet file with `pyarrow.parquet`), and
`test_every_door_writes_the_digits_below_the_microsecond_into_the_parquet_file` (all nineteen
doors, three zones, `timestamp_ns` and `timestamptz_ns` sources, read from the table's live
data files, not through SELECT).

### 11.4 The verifier's scripts re-run on the split (C-046)

`nx.py`, `nx2.py`, `lx.py`, `leak.py` and `updtrunc.py` unchanged; `gq.py` with one setup line
changed (it seeded its nested table by an INSERT that is now refused; the seed is the same
SELECT as a CTAS). Head, America/New_York and Asia/Kolkata, against the verifier's own base
outputs for `ca5a062a`.

**Nested `timestamp_ns` cells, 16,070 in the two zones:**

| Script | Cells | Named refusal | Stored | Other |
|---|---|---|---|---|
| `nx.py` (28 shapes, 19 doors, 5 sources) | 4,750 | 4,750 | 0 | 0 |
| `nx2.py` (46 pairing shapes) | 8,740 | 8,740 | 0 | 0 |
| `lx.py` (19 nested Arrow layouts) | 2,280 | 2,148 | 0 | 132 |
| `leak.py` (42 placements of a narrowed value) | 84 | 84 | 0 | 0 |
| `gq.py guard` (nested writes, America/New_York) | 46 | 46 | 0 | 0 |

The 132 are not door outcomes: 120 are layouts the probe cannot build through pandas, on
both builds, and 12 are the probe's own setup step writing a fixed-size list into a
`timestamptz_ns` or `TIMESTAMP` sibling column, the raw Arrow error both builds raise. No
older gate of main answers first on a gated column: the count of `CANNOT_FIND_DATA`,
`EXTRA_STRUCT_FIELDS`, store-assignment and planner texts on these cells is 0, where the
re-verify counted 11,950 over five zones.

**Controls:** 570 nested control cells of `nx.py` (`timestamptz_ns`, `TIMESTAMP_NTZ` and
`TIMESTAMP` leaves) are identical to base in value and in error text; 504 SELECT statements
and 18 unrelated writes of `gq.py guard` are identical. **Controls moved: 0.**

**Top-level `timestamp_ns` cells:** the 240 `top_dict` and `top_ree` cells of `lx.py`: every
answering cell equals the rule; the 16 run-end-encoded cells of §11.3 are corrected and no
other cell differs from `c6d947a3`. `gq.py guard`: the two top-level writes whose wall the
unit corrects, as before. `gq.py trunc` (312 cells): 52 plain cells keep every digit; 52
`nvl` cells are refused on both builds; of the 208 cells of a value beside an untyped NULL,
192 store a cut value exactly as at `c6d947a3` (R-017) and 16 are refused: 4 are
`coalesce` on the unfiltered `UPDATE`, a planner fault on both builds; 10 are the S1 cells of
§11.2; 2 are the array element of §11.2. `updtrunc.py`: the `CASE` and `if` forms are
refused, the plain column stores `…123456789`.

**R-017, measured count kept as the re-verify stated it:** 510 of 520 answering cells store a
value cut to microseconds at `c6d947a3` (485 on main); the unit corrected the wall in 172 of
them and cut no digit main kept. After the split the 25 cells of §11.2 are refused and not
cut. The remaining cut cells are main's and stay a recorded row.

### 11.5 Mutants of the split (C-047)

Each applied to the committed tree, run against the named suites, and restored.

| Mutant | Killed by |
|---|---|
| S1 the gate skips a struct | the gate's unit pin; four door pins |
| S2 the gate skips a map key | the unit pin; the door pin |
| S3 the gate skips a map value | the unit pin; the door pin |
| S4 the gate skips a list | the unit pin; the door pin |
| S5 the gate skips the large, view and fixed-size lists | the unit pin only: an Iceberg table's Arrow type never has those layouts |
| S6 the gate matches a zoned leaf too | the unit pin; the control door pin |
| S7 a bare NULL is refused | the allowed-statement door pin |
| S8 a bare NULL beside a value passes | the unit pin |
| S9 a field assignment is not a supply | two unit pins; the field-assignment door pin |
| S10 MERGE is not gated | two door pins |
| S11 UPDATE is not gated | two door pins |
| S12 a MERGE star is read as absent | the unit pin; the door pin |
| S13 a source the gate cannot read is read as absent | two unit pins |
| S14 `REPLACE WHERE` is not intercepted | the door pin |
| S15 `BY NAME` is read by position only | the allowed-statement door pin |
| S16 a failed load of the target is skipped | `a_target_that_fails_to_load_refuses_instead_of_writing_positionally` |
| S17 the router does not call the gate | four door pins |
| S19 the unfiltered `UPDATE` refusal is off | the `UPDATE` door pin |
| S20 a typed NULL counts as narrowing | the `UPDATE` door pin |
| S21 the store does not see through `RunEndEncoded` | the `ntz_store` unit pin |
| S22 the store names no plain type for an encoded source | the `ntz_store` unit pin |
| S23 the wall kernel's field is not nullable for an instant | the kernel pin; the overflow door pin |
| S24 the unfiltered `UPDATE` searches a `CASE` condition | the `UPDATE` door pin |
| S25 the store hands the kernel the encoded column | the facade pin `test_an_encoded_instant_source_stores_the_session_wall` |

S20 and S24 survived the first run; a pin was added for each and they were run again.

### 11.6 Gates of the split (C-048)

Run 2026-10-09 on `e44c2610` (the code) and on the tree of the records commit (the document
gates), one cargo command at a time under the build lock on cores 32-47.

| Command | Exit | Result |
|---|---|---|
| `cargo fmt --all -- --check` | 0 | |
| `make rust-clippy` | 0 | |
| `make rust-panic-ban` | 0 | |
| `cargo test --locked -p repark-functions --lib` | 0 | 903 passed, 1 ignored |
| `cargo test --locked -p repark-iceberg --lib` | 0 | 973 passed |
| `cargo test --locked -p repark-spark --lib` | 0 | 2634 passed, 5 ignored |
| `cargo test --locked -p repark-sql --lib` | 0 | 393 passed |
| `cargo test --locked -p repark-spark --test timestamp_ns_wall_doors --test timestamp_ns_nested_shapes` | 0 | 7 + 6 passed |
| `make develop` | 0 | |
| `pytest python/repark/tests/test_ice_tsns_merge_wall_1.py -q -n 8` | 0 | 351 passed |
| `pytest python/repark/tests -q -n 8 -k "iceberg or v3 or merge or timestamp or nested or struct"` | 0 | 3575 passed, 140 skipped, 11 xfailed |
| `ruff check .` and `ruff format --check .` (0.15.22) | 0 | |
| `python3 scripts/sync_map_md.py --check` | 0 | 374 maps clean |
| `bash scripts/check_map_md.sh --base origin/main`, and against `ca5a062a` | 0 | |
| `make check-ledger-grammar`, `make check-ledgers` | 0 | |
| `make check-docs-links` | 0 | 7523 links |
| `make spell-check` | 0 | |
| `make check-rust-file-size`, `make check-lib-rs` | 0 | `router.rs` at 1000 of 1000 |
| `make check-docs-compaction` | 0 | |

The pins were seen red first: on `65bb2042`, which holds the pins over the fold's code, five of
the six Rust nested-shape tests and 116 of the 351 facade tests fail (112 nested refusals and
the four run-end-encoded doors).

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
- **Q8 (C-037, OWNER).** Should each door pair struct fields as Spark's does? *Premise:* on
  main and at head, for every leaf type, the INSERT family pairs by name where Spark pairs by
  position (a swapped struct stores different fields on the two engines, and a renamed field
  stored NULL on main), and `INSERT … BY NAME` and `overwritePartitions` pair by Arrow's rule
  where Spark pairs by name (§10.1, 80 Spark cells). Fold 2 made the nanosecond conform follow
  each door and refuse where a leaf would be left NULL; it changed no door. *Lean:* yes, as its
  own unit with the Spark cells as the oracle; it changes what stored data means for swapped
  or renamed structs, so not in a bug release.
- **Q9 (C-038, RULING).** Should a nanosecond value beside an untyped NULL keep its type?
  *Premise:* `coalesce(ns, NULL)`, `nvl`, `array(ns, NULL)`, `CASE … ELSE NULL END` and
  `if(…, ns, NULL)` are typed microseconds and floor the value, on main and at head; a
  top-level `timestamp_ns` column stores the cut value through every door, and a nested leaf
  is now refused (§10.4). *Lean:* yes, and before the nanosecond types are called supported:
  the cast of an untyped NULL that type coercion inserts should not be read as the SQL
  `TIMESTAMP`. It is the session's timestamp rule, shared by every query, so a unit of its
  own with the read-side pins.
- **Q10 (C-039, RULING).** Should the `VALUES` spellings that fail before any store be fixed
  or refused by name? *Premise:* a `TIMESTAMP` literal inside `array`, `named_struct` or `map`
  in a `VALUES` list fails with a raw Arrow type error with no table involved; a multi-row
  `VALUES` of maps mixing a `TIMESTAMP_NTZ` literal and `to_timestamp_ntz` panics in Arrow; a
  struct literal of the wrong arity fails in the planner. These are the cells of the nested
  matrices that are neither a stored wall nor a named refusal (§10.3). *Lean:* yes, a separate
  unit; the first is the same stale `VALUES` schema the INSERT conform already repairs for a
  nanosecond target.
- **Q11 (C-029, RULING).** Is a named refusal the right answer where main stored a NULL leaf?
  *Premise:* through the by-name doors a struct with a renamed, recased or missing field
  stored NULL in the `timestamp_ns` leaf and kept the row, on main and at `59462d6a`; fold 2
  refuses those statements by name (§10.1), reading the ruling's "no third outcome" as
  excluding a NULL the source did not hold. Spark stores the value on the INSERT doors (by
  position) and refuses on the by-name doors. *Lean:* keep the refusal until Q8 is decided.
- **Q12 (C-049, OWNER).** When is the nested store scheduled? *Premise:* a nested
  `timestamp_ns` write that stored on main now refuses by name (§11.1), so a user of such a
  column cannot write it in v1.5.4 except by CTAS; the card holds the evidence and the four
  things the unit must settle first (pairing, layouts, the typing beside a NULL, the leak
  routes). *Lean:* its own unit after R-016 and R-017 are ruled on, since both decide what
  the stored value is.
- **Also seen, no question:** `TIMESTAMP_NTZ '<wall>'` refuses `expects an Int64 wall` when
  the wall is within about 36 minutes of the epoch, in a plain `SELECT` too (O-4). It is not a
  nanosecond or Iceberg defect.
