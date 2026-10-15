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
- **Non-ASCII locations classify instead of panicking (audit PA-04).** A location whose
  multibyte character straddles byte 5, 7 or 9 (`aaaaé`, `aaaaaaé`, `file://aaaaaaaaé`) now
  gets the policy refusal (or writes, under the opt-in) instead of an internal error.
- **`repark.sql()` applies the same policy (owner ruling 2026-10-15).** A local `COPY` or
  `CREATE EXTERNAL TABLE` outside every registered warehouse now refuses on that entry,
  wrapped or not; it has no opt-in. Before, it wrote anywhere the process could.
- **The ANSI door's opt-in opens.** A session whose `repark.sql` extension sets
  `allowLocalFilesystemDDL=true` now writes outside on the ANSI door; before, the door
  refused regardless.
