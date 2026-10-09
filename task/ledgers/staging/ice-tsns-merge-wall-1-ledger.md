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
| C-004 | At head every door stores the rule's value in a `timestamp_ns` column: the 38 wrong cells are right. | The facade matrix pins against `expected_wall`; the Rust-door pins. | **OPEN** | Step 0 only measures; the fix is the next commit. |
| C-005 | At head no cell main answers correctly has moved, on any target, door, source or zone, and every carry reads back unchanged. | The facade matrix pins against main's recorded answers. | **OPEN** | The next commit. |

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
