# Card ICE-NTZ-NESTED-WALL-1: a nested microsecond `TIMESTAMP_NTZ` field stores one wall on every door

**Date:** 2026-10-10. **Filed by:** the v1.5.4 docs lane (Claude Haiku 5.5), from the parity row R-011 measured on 2026-10-09.

**Status:** open. Target v1.5.5. One unit of its own.

**Sibling:** card ICE-TSNS-NESTED-1, which refuses a nested `timestamp_ns` leaf by name:
[ice-tsns-nested-1-card-2026-10-09.md](ice-tsns-nested-1-card-2026-10-09.md).
Parity row: ICE-TSNS-SQL-1-R-011 in [docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md).

## Why

In a session whose zone is not UTC, a `TIMESTAMP` or `timestamptz_ns` instant written into a
field of `struct<v: TIMESTAMP_NTZ, n: INT>` stores two different walls, depending on the door:

- Field assignment stores the **session** wall: `MERGE … UPDATE SET t.st.v = …` and
  `UPDATE … SET st.v = …`, 28 measured cells.
- Every other door stores the **UTC** wall: nested `INSERT … VALUES`, `SELECT`, `BY NAME` and
  `OVERWRITE`, `REPLACE WHERE`, MERGE INSERT, whole-struct assignment through MERGE or UPDATE,
  and the DataFrame writers, 106 measured cells.

Example from the parity row: `TIMESTAMP '2026-01-02 03:04:05.123456+00:00'` in America/New_York
stores `1767305045123456` through `SET t.st.v` and `1767323045123456` through `INSERT`. The
two agree only when the session is UTC or the source is a wall (379 cells).

The split is identical on main `40fc916f` and after ICE-TSNS-MERGE-WALL-1 (#1018). That unit
left the microsecond control alone by ruling.

## The ask

- One wall on every nested door. The parity row's rule is the session wall, which field
  assignment already stores; the 106 cells on the other doors change to it.
- Measure the rule against Spark 4.1.2 live before the pin is written. The parity row records
  that the nested doors were not measured on Spark by that unit; the top-level rule is the
  `TIMESTAMP` to `TIMESTAMP_NTZ` cast, and the nested form should follow the same cast.
- The fix rewrites what the 106 cells store. Every cell in the 28 field-assignment cells keeps
  its value.
- Pin each door with a microsecond value and read the stored value from the Parquet file, in
  three or more session zones.
- Controls do not move: a `timestamp_ns` leaf and every `timestamptz` leaf stay as ICE-TSNS-NESTED-1
  and ICE-TSNS-MERGE-WALL-1 left them.

## Release note (required)

The release that ships this fix must say:

> Nested `TIMESTAMP_NTZ` values written by `INSERT` before the fix hold the UTC wall.

The note also names the doors that changed, with the value each stored before and after.

## Gates

- Every door in the table above, three or more session zones, values read from the Parquet files.
- No pin of a nested `timestamp_ns` leaf moves: those stay refused under ICE-TSNS-NESTED-1.
- The pinned test `a_nested_microsecond_ntz_field_keeps_the_split_main_has` in
  `crates/repark-spark/tests/timestamp_ns_wall_doors.rs` is rewritten in the same change, as
  the parity row says.
- An Opus verifier on the product pull request.
