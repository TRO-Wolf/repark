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
- **A NULL beside a nanosecond value no longer cuts it to microseconds
  (ICE-TSNS-TYPE-PLANNER-1, R-017).** `coalesce(ns, NULL)`, `nvl`, `array(ns, NULL)`,
  `CASE … ELSE NULL`, `if(…, ns, NULL)` and the other positions that join two values keep
  all nine fraction digits and the nanosecond type, so `INSERT … SELECT coalesce(ns, NULL)`
  into a `timestamp_ns` column stores `…123456789` where it stored `…123456000`.
- **A microsecond and a nanosecond timestamp joined in one position take nanoseconds.**
  `coalesce(ns, TIMESTAMP '…')`, `array(ns, ts)`, `CASE`, `UNION`, comparisons and
  `aggregate(array(ns), CAST(NULL AS TIMESTAMP), …)` answer `timestamp[ns]` with the
  nanosecond digits kept, where they answered microseconds.
- **A written `CAST(ns AS TIMESTAMP)` floors to microseconds on every door.** `UPDATE T SET
  v = CAST(c AS TIMESTAMP)` with no `WHERE` into a `timestamp_ns` column stores `-1000` for
  `-1 ns`, where it kept `-1`.
- **`UPDATE` and `DELETE` with a `CAST('…' AS timestamp_ns)` or `timestamptz_ns` literal
  now store (R-010).** They raised `Unsupported SQL type timestamp_ns`.
- **`sequence` over a nanosecond column answers nanoseconds.** It raised `ListArray
  expected data type Timestamp(ns)…`.
- **The Rust ANSI door (`repark_sql::AnsiDialect`) types `TIMESTAMP` as the naive
  microsecond wall and `TIMESTAMPTZ` as the microsecond instant.** `CAST('…789' AS
  TIMESTAMP)` answers `timestamp[us]` without the last three digits, where it answered
  `timestamp[ns]`.
- **The Python `repark.sql()` callable types `TIMESTAMP` as the microsecond instant.**
  `CAST('2024-01-02 03:04:05.123456789' AS TIMESTAMP)` answers `timestamp[us, tz=UTC]`
  `…123456`, where it answered `timestamp[ns]` `…123456789`; the routing fix of PA-02 moves
  it to the naive wall of ruling 94.

Tables already written keep the values they hold; this release ships no migration.
