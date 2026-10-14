# v1.5.6 release notes (draft)

This draft collects one line per stored-value change as each unit of the 1.5.6 cycle lands, for
the release PR to edit into the notes.

## Stored values that change

- **Nanosecond values into a microsecond `TIMESTAMP` column read a naive wall in the session zone
  on the overwrite doors (ICE-TSNS-FLOOR-DOORS-1, R-009).** `INSERT OVERWRITE` (plain, with a
  column list, `BY NAME`, with a static `PARTITION`), `INSERT … BY NAME`, the MERGE insert arm
  (every spelling), `overwritePartitions` and `insertInto(overwrite=True)` now read a
  `timestamp_ns` wall in the session zone, as INSERT does. Before this release they read it as
  UTC, so in a session whose zone is not UTC every naive nanosecond value written through them
  moved by the zone offset, not only values before 1970: in America/New_York
  `2026-03-08 02:30:00.000000001` stored `1772937000000000` where INSERT stores
  `1772955000000000`.
- **A nanosecond value before 1970 with digits below the microsecond now floors on every door,
  `TIMESTAMP_NTZ` included.** Narrowed into a microsecond `TIMESTAMP` or `TIMESTAMP_NTZ`
  column, `-1 ns` stores `-1 µs` on every write door, INSERT into `TIMESTAMP_NTZ` among them;
  before, the overwrite doors above and every door into `TIMESTAMP_NTZ` truncated toward zero
  and stored `0`.
- **The read `CAST(ns AS TIMESTAMP_NTZ)` floors the same way.** `CAST` of a `timestamp_ns` or
  `timestamptz_ns` value to `TIMESTAMP_NTZ` in a query answers `-1 µs` for `-1 ns`, where it
  answered `0`, as `CAST(… AS TIMESTAMP)` already did.
- **A struct value pairs its fields by position on `INSERT`, `REPLACE WHERE` and `insertInto`, as
  Spark does (ICE-STRUCT-PAIRING-1, R-016).** `INSERT INTO t SELECT 1, named_struct('b', 2, 'a', 1)`
  into `st STRUCT<a: INT, b: INT>` now stores `a = 2, b = 1`: the first source field feeds the
  first table field, whatever its name. Before this release these doors paired by name, so the
  same statement stored `a = 1, b = 2`, and a renamed field stored NULL with no error. The same
  holds for `INSERT … VALUES`, `INSERT OVERWRITE` (without a `PARTITION` clause),
  `INSERT INTO … REPLACE WHERE` and `insertInto` with and without `overwrite=True`, at every
  depth and inside arrays and maps. A struct with more or fewer fields than the column now
  refuses (`EXTRA_STRUCT_FIELDS`, `STRUCT_MISSING_FIELDS`).
- **`writeTo().append()` and `saveAsTable` in append mode refuse a struct they cannot pair by
  name, where they stored NULL.** A source struct missing one of the column's fields, or
  carrying one it lacks, now refuses `CANNOT_FIND_DATA` or `EXTRA_STRUCT_FIELDS` and writes
  nothing; before, the missing field was stored NULL and the extra one dropped, `merge-schema`
  included. A struct whose fields differ only in order stores as before; one whose fields
  differ only in letter case now stores by name, where it refused. `overwritePartitions()`
  pairs the same way.
- **`writeTo().overwrite(condition)` stores a struct field spelled in another letter case.**
  Under the default `spark.sql.caseSensitive=false`, a source field `A` now feeds the table's
  `a`, as Spark does; before, that field was stored NULL.
- **The Rust append API and the streaming sink refuse a struct whose fields are not the table's,
  in order.** They do not pair struct fields; a batch whose struct column names its fields
  differently or in another order now refuses `CANNOT_SAFELY_CAST` with a hint to write the
  table's field names in the table's order, where the batch cast paired it by a rule of its
  own. `INSERT OVERWRITE … PARTITION (…)` refuses such a struct the same way, where it stored
  by name (Spark stores it by position).

Tables already written keep the values they hold; this release ships no migration.
