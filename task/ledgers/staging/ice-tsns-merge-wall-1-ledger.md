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
| C-005 | At head no cell main answers correctly has moved: the 1605 control cells and the 268 `timestamp_ns` cells main answered right or refused by the ratified gate are identical, and every carry (DELETE, sibling UPDATE and MERGE, three maintenance rewrites, both modes) reads back the seeded ticks. | The facade matrix against main's recorded cells. | **PROVEN** | §5: 1926 cells, 47 moved, all 47 on the `timestamp_ns` target; `test_control_targets_answer_what_main_answered` (285), `test_untouched_rows_carry_their_ticks` (108). |
| C-006 | `UPDATE t SET v = <column or literal>` with no `WHERE` into a `timestamp_ns` column answers by the rule from a `TIMESTAMP`, `TIMESTAMP_NTZ` or `timestamptz_ns` source, with every nanosecond digit; main raised a raw Arrow error (9 matrix cells). | The `update_column` cells; the Rust door's `update` door; the literal and NULL edge pins. | **PROVEN** | §4.2, §5; `test_update_with_no_where_stores_a_literal`. |
| C-007 | No door drops a digit below the microsecond from a true nanosecond input (`timestamp_ns` column, `timestamptz_ns` column, nine-digit casts, an Arrow nanosecond array), INSERT included. | The matrix moments all carry such digits; `test_true_nanosecond_inputs_keep_every_digit`. | **PROVEN** | §3, §5: no truncating cell on main or at head for a `timestamp_ns` target. |
| C-008 | One conversion, one place: a nanosecond wall target reaches INSERT's own kernel `__repark_cast_timestamp_ns__` from every site, through one function (`ntz_store::wall_cast_udf_name`); no kernel was added and `timestamp_ntz_cast.rs` is unchanged. | The name pin, the per-unit SQL pin, the diff. | **PROVEN** | §4.1; `ns_wall_udf_name_matches_the_registered_udf`, `a_wall_target_casts_through_the_kernel_of_its_unit`. |
| C-009 | The native ANSI door resolves the kernel the shared MERGE and UPDATE sites now emit. | An ANSI-door UPDATE, MERGE update and MERGE insert into `timestamp_ns`. | **PROVEN** | §4.3; `repark-sql` `update_and_merge_into_timestamp_ns_store_the_wall`; mutant M4. |
| C-010 | Edge inputs answer as INSERT answers on every door: an instant past 2262 is `[CAST_OVERFLOW]` under ANSI and NULL without it; a `DATE`, a NULL and a refused integer are unchanged from main. | The edge probe on main and at head (§6) and its pins. | **PROVEN** | §6: 56 probe cells, 40 identical, 16 moved from a raw Arrow error; `test_an_instant_past_2262_*`. |
| C-011 | Five hand mutants are each killed by a named pin. | §7. | **PROVEN** | §7: M1..M5 red, sources restored. |
| C-012 | The unit's gates are green with real exit codes. | §8. | **PROVEN** | §8. |
| C-013 | A `timestamptz_ns` target stores one instant for one wall on every door. | Out of this unit by the brief's control rule; measured (O-1, O-6). | **OPEN** | Q1: 38 cells read a wall as UTC and 6 raise a raw Arrow error on main and at head. Parity row ICE-TSNS-SQL-1-R-008. |
| C-014 | A nanosecond source narrows into a microsecond column the same way on every door. | Out of this unit by the brief's control rule; measured (O-2, O-3). | **OPEN** | Q2: 72 cells truncate toward zero or read a wall as UTC. Parity row ICE-TSNS-SQL-1-R-009. |
| C-015 | `UPDATE` and `DELETE` lower the nanosecond cast spellings. | Out of this unit: a statement-lowering gap, not a stored value (O-5). | **OPEN** | Q3: 6 `timestamp_ns` cells stay refused. Parity row ICE-TSNS-SQL-1-R-010. |

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

Unchanged: a `DATE` stores midnight of its day on every door; an integer refuses with the
same store-assignment text; `v + INTERVAL 1 DAY`; a NULL; the `days(v)` partition values.

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
- **Also seen, no question:** `TIMESTAMP_NTZ '<wall>'` refuses `expects an Int64 wall` when
  the wall is within about 36 minutes of the epoch, in a plain `SELECT` too (O-4). It is not a
  nanosecond or Iceberg defect.
