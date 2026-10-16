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
- **A struct field Spark's store assignment refuses now refuses, where it was stored.** After
  a struct pairs, each of its fields is held to the same rule a plain column is:
  `INSERT INTO t SELECT 1, named_struct('a', '5', 'b', 'y')` into
  `st STRUCT<a: BIGINT, b: STRING>` now refuses `CANNOT_SAFELY_CAST` (``Cannot safely cast
  `st`.`a` "STRING" to "BIGINT"``) and writes nothing; before it stored `a = 5`. The same
  holds for a BOOLEAN into an INT field, a STRING into a DATE field, a field that meets a
  struct where the table has a plain value (or the reverse), at every depth and inside arrays
  and maps, on every door, `INSERT OVERWRITE … PARTITION (…)` included. A struct with more or
  fewer fields than the column refuses there with `EXTRA_STRUCT_FIELDS` or
  `STRUCT_MISSING_FIELDS`, as on the other doors. Widening (`INT` into `BIGINT`) and any value
  into a STRING field still store.
- **`INSERT INTO t (st, id) VALUES (…)` with a column list pairs a struct by position.** The
  struct written to `st` feeds `st`'s fields in order, as every other `INSERT` door does:
  `INSERT INTO t (st, id) VALUES (named_struct('b', 2, 'a', 1), 1)` stores `a = 2, b = 1`, and a
  renamed field stores its value; before, this spelling paired by name and stored NULL for a
  renamed field.
- **By-name struct fields match letter case as Spark does, beyond ASCII.** Under
  `spark.sql.caseSensitive=false` a source field `É` feeds the table's `é` and `ẞ` feeds `ß`
  on `INSERT … BY NAME`, `writeTo().append()`, `saveAsTable`, `overwritePartitions()`, every
  MERGE arm and `UPDATE`; before, MERGE and `UPDATE` refused and the DataFrame writers stored
  NULL or paired the wrong field. `ß` and `SS` still do not match, as on Spark.
- **`UPDATE t SET st = (SELECT st FROM src) WHERE id = 0` stores the subquery's struct.** With a
  `WHERE` that compares a column to a value, on the Spark SQL door, an uncorrelated
  `(SELECT column FROM table …)` assigned in `SET` now stores its value, its struct fields
  paired by name; before, the statement succeeded and left the row unchanged.
- **`UPDATE … SET col = (SELECT …)` with `WHERE true`, an `IN` list or no `WHERE` now refuses,
  for every column type, where it reported success and wrote nothing.** The refusal reads
  `UPDATE with SET column = (SELECT ...) is not supported on this statement: RePark cannot
  write the subquery value here, so it refuses before any write.` and no row changes. On
  `repark.sql()` every `UPDATE` whose `SET` value is a subquery refuses this way.
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

## Behaviour changes

- **The local-file write policy checks writes below the root (PA-LOCAL-FS-POLICY-1, audit
  PA-02).** With `repark.sql.allowLocalFilesystemDDL=false` (the default), `EXPLAIN ANALYZE
  COPY … TO '<outside>'` now refuses with the policy error and writes nothing, as the bare
  `COPY` already did; plain `EXPLAIN` still answers.
- **Containment follows symlinks through missing parents (audit PA-03).** A destination such
  as `<warehouse>/link/new/sub/export`, where `link` points outside and the middle
  directories are absent, now refuses; a `..` after a missing directory and a dangling
  symlink refuse as indeterminate.
- **The guard reads a location the way the write reads it (owner ruling 103).** A symlink
  followed by `..` (`<warehouse>/deep/../../out`), a percent-encoded dot segment in a `file:`
  URL (`%2e%2e`, `%2E%2E`, `.%2e`) and a relative `file:` URL (read as `/x`, not against the
  working directory) no longer write outside the warehouse. A `..` that steps back over a
  symlink inside the warehouse now refuses even when the write would land inside it
  (`<warehouse>/deep/../x` with `deep` an in-warehouse symlink wrote `<warehouse>/x` before).
- **Not yet covered:** the policy does not yet cover a Spark-door `CREATE TABLE … LOCATION` to a
  local path outside the warehouse. Card: LOCAL-FS-CREATE-TABLE-LOCATION-1.
- **Non-ASCII locations classify instead of panicking (audit PA-04).** A location whose
  multibyte character straddles byte 5, 7 or 9 (`aaaaé`, `aaaaaaé`, `file://aaaaaaaaé`) now
  gets the policy refusal (or writes, under the opt-in) instead of an internal error.
- **`repark.sql()` applies the same policy (owner ruling 2026-10-15).** A local `COPY` or
  `CREATE EXTERNAL TABLE` outside every registered warehouse now refuses on that entry,
  wrapped or not; it has no opt-in. Before, it wrote anywhere the process could.
- **Dated loss (2026-10-15, owner ruling 104): `repark.sql()` no longer runs any local-file
  DDL.** That entry registers no warehouse, so every `COPY … TO` a local path (absolute,
  relative, `file:`, `file://`, `file://localhost`), every `EXPLAIN ANALYZE COPY` to one, and
  every `CREATE EXTERNAL TABLE` with a local location, the read-only registration of an existing
  local file or directory included, now refuses with "repark.sql() does not allow local-file
  DDL". The entry has no opt-in: `SET repark.sql.allowLocalFilesystemDDL = true` fails there,
  and no session config reaches it. Before this release all of these ran. Card:
  REPARK-SQL-LOCAL-DDL-1.
- **The ANSI door's opt-in opens.** A session whose `repark.sql` extension sets
  `allowLocalFilesystemDDL=true` now writes outside on the ANSI door; before, the door
  refused regardless.

## Known issues

- **REPARK-SQL-UPDATE-SUBQUERY-WHERE-1 (known issue):** through `repark.sql()`, `UPDATE … WHERE col
  IN (SELECT …)` or `= (SELECT …)` updates every row; use the Spark SQL session until it is fixed.
