# Card PARQUET-INT96-AS-NTZ-1: a Spark-written INT96 timestamp reads as `timestamp_ntz`

**Date:** 2026-11-22. **Filed by:** the orchestrator (`claude-opus-5-5`). **Source:** an independent
workload measurement against Spark 4.1.2 found a calendar date that moved in a New York session; re-measured here on
main `c5370be6` (the released 1.5.6 wheel's source). **Severity:** S1 (owner, 2026-11-22): silent, it changes
which rows a date filter returns, and INT96 is Spark's default timestamp encoding.

**Status:** FIXED 2026-11-22 (PR #1411), built by the builder (`claude-opus-5-5`) on `fix/parquet-int96-as-ntz-1`;
[registry entry](../../../docs/spark-sql-iceberg-parity.md#parquet-int96-as-ntz-1--a-spark-written-int96-timestamp-reads-as-an-instant-in-the-session-zone--fixed-2026-11-22-pr-1411).

## The defect

Spark 4.1.2 writes a `TIMESTAMP` column to Parquet as INT96 by default (`spark.sql.parquet.outputTimestampType`
answers `INT96`). Spark reads that column back as `timestamp`, an instant shown in the session zone. RePark reads it
as `timestamp_ntz` holding the UTC wall time. In a UTC session the two agree; in any other session zone every
rendered value, and every date or field derived from it, differs, with no error.

## Measured (Spark 4.1.2 writer, UTC session; both readers in each zone)

| Reader zone | Expression | Spark | RePark `c5370be6` |
|---|---|---|---|
| any | the column's type | `timestamp` | `timestamp_ntz` |
| UTC | `CAST(ts AS STRING)`, `to_date(ts)` | `2025-01-15 01:00:00`, `2025-01-15` | equal |
| America/New_York | `CAST(ts AS STRING)` | `2025-01-14 20:00:00` | `2025-01-15 01:00:00` |
| America/New_York | `to_date(ts)` | `2025-01-14` | `2025-01-15` |
| America/New_York | `CAST(ts AS STRING)` for `2025-07-01 12:00:00` UTC | `2025-07-01 08:00:00` | `2025-07-01 12:00:00` |
| both | `unix_timestamp(ts)` | `1736902800` | equal |
| America/New_York | `hour(ts)` (second measurement) | `20` | `1` |
| America/New_York | rows where `CAST(ts AS DATE) = DATE'2025-01-14'` (second measurement) | 1 | 0 |

The file's footer holds `required int96 ts`. The independent measurement also found that a file written by Spark
with `TIMESTAMP_MICROS`, or by Polars or pyarrow as INT64 microseconds adjusted to UTC, reads the same on both
engines in both zones; only INT96 diverges.

## Reach

Any plain Parquet file a Spark job wrote with its defaults, read by RePark in a session whose zone is not UTC. A job
that reads such a file and writes the result stores the shifted dates and the changed type.

## Related

- **Home: the Spark plan's item P01-05** (honor per-file INT96 timestamp conversion). The fix belongs there, not in
  a new design: P01-05's design (not built) decodes INT96 leaves per file before filters and aggregation; reading
  Spark's own INT96 as an instant is the base case it builds on.
- Audit family NEW-settings-5 (S2): INT96 from writers other than Spark's own ignores
  `spark.sql.parquet.int96TimestampConversion`, a second rule on the same leaves, also under P01-05.

## Measured before the fix (main `5caea2a5`, 2026-11-22)

Synthetic fixtures (`python/repark/tests/fixtures/parquet_int96_as_ntz_1/`): seven ids with a winter and a summer
2025 instant, a NULL, `1960-03-01 05:30:00.25`, `2999-12-31 22:00:00.123456` and two instants either side of a New
York and a Kolkata date boundary, written by Spark 4.1.2 as `INT96` (top level and inside a struct, an array and a
map, beside an `INT64` `TIMESTAMP_NTZ`), as `TIMESTAMP_MICROS`, as one directory of both, and by pyarrow (Polars files were measured in
the scratch domain table, not committed).
288 compared cells (three zones, both ANSI modes, footers aside); main differed from Spark on 184, head only on the 12
cells of a route RePark does not have (below). Beyond the card's first table, main also:

| Case | Spark | main `5caea2a5` |
|---|---|---|
| `2999-12-31 22:00:00.123456` UTC, any zone | that instant | `1830-11-22 22:50:52.704352768` (nanoseconds wrapped past 2262), so `max`, `min`, `unix_timestamp`, `to_date` and `GROUP BY` date are wrong in UTC too |
| a join of the `INT96` column to an `INT64` instant or wall time | 6 rows (New York, naive: 1) | ANSI on: `CAST_OVERFLOW`; ANSI off: the 2999 row lost in UTC, one wrong pair (`7, 1`) in New York |
| `ts = ntz` in one file | `true` in UTC, `false` elsewhere | ANSI on: `CAST_OVERFLOW`; off: `true` in every zone, NULL for 2999 |
| the nested leaves `s.t`, `a[1]`, `m['k']` | `timestamp` in the session zone | `timestamp_ntz`, UTC wall time |
| one directory of an `INT96` and a `TIMESTAMP_MICROS` file | `timestamp` | refused: `Fail to merge schema field 'ts'` |
| `write.parquet` after the read, read back cold | `timestamp`, the instant | stores `INT64 TIMESTAMP(NANOS,false)`: RePark reads the UTC wall time as `timestamp_ntz` in every zone, and Spark refuses the file (`PARQUET_TYPE_ILLEGAL`) |
| `CREATE TABLE … USING iceberg AS SELECT` the column | `timestamp` (timestamptz) | refused: `timestamp_ns is not supported until v3` |

Equal on main and left so: an `INT64` leaf from Spark (micros, millis, `TIMESTAMP_NTZ`), pyarrow or Polars, adjusted
or not; a self-join; and pyarrow's `INT96`, whose embedded Arrow schema already names UTC. Row-group pruning: Spark
writes no `INT96` statistics, pyarrow none either, and Arrow's statistics converter reads none, so the leaf is never
pruned, on main or after the fix.

## Fix

`spark_nullable::with_int96_as_instants` sets DataFusion 54.1's `datafusion.execution.parquet.coerce_int96` to `us`
and `coerce_int96_tz` to `UTC` on every RePark session config (`session/df_guards.rs`
`context_with_df_54_1_rule_guards`), after the explicit `datafusion.*` keys, which keep their values. DataFusion
then infers and decodes an `INT96` leaf without an embedded Arrow schema (every Spark-written file) nested ones included, as `Timestamp(Microsecond, "UTC")`, Spark's
`timestamp`, at schema inference and in the opener, on `read.parquet`, `format("parquet").load`, temp views over
them, and `CREATE EXTERNAL TABLE … STORED AS PARQUET`. Microseconds match Spark's precision and reach past 2262.
`INT64` leaves keep their footer's `isAdjustedToUTC`. Iceberg scans read through iceberg-rust's own reader and do not
use this option; an Iceberg table whose data file holds `INT96` (Spark's `add_files`) reads every timestamp as NULL
on main and head ([ICEBERG-INT96-NULL-1](iceberg-int96-null-1-card-2026-11-22.md), not this unit).

After the fix every one of the 276 cells on the routes RePark has equals Spark; a write after the read stores `INT64
TIMESTAMP(MICROS,true)` (Spark writes `INT96`): both engines read it back as the same `timestamp` instants, and the
Iceberg table is `timestamptz`.

**Not this unit:** SQL over ``parquet.`<path>` `` is not a RePark route (both builds: table not found; a strict xfail);
`repark.sql` refuses a local Parquet path on the native door (`CREATE EXTERNAL TABLE to local filesystem path`, both
builds), so its reading arm is pinned through the core session's `CREATE EXTERNAL TABLE` in Rust.

## NEW-settings-5 (not taken)

`spark.sql.parquet.int96TimestampConversion=true` makes Spark shift `INT96` from writers other than parquet-mr by the
session zone (a pyarrow file reads `2025-01-15 01:00:00` in New York and Kolkata). RePark stores the key and ignores
it on both builds (it reads the instant). The rule needs the per-file `created_by` and a decode-time shift, not a
session option, so it stays with P01-05.

## Pins

- `python/repark/tests/io/test_parquet_int96_as_ntz_1.py` against `parquet_int96_as_ntz_1_oracle.json`, recorded by
  `_record_parquet_int96_as_ntz_1.py` on PySpark 4.1.2 (`--fixtures` writes the fixtures, `--rewrite` the oracle, no
  flag re-verifies): 56 tests, both ANSI modes, every zone in each. Main `5caea2a5`: 44 red (three routes'
  `timestamp_ntz` schema, the zone shift, the 2999 wrap, the refused directory and Iceberg write, the NTZ file
  written), 12 controls green; head: 56 green; the SQL-path route is a strict xfail on both.
- Rust `repark-core` `spark_nullable::int96_tests`: a low-level `INT96` file read by `read_parquet` and by
  `CREATE EXTERNAL TABLE` as `Timestamp(Microsecond, "UTC")` with the exact microseconds (2999 included), an `INT64`
  leaf kept zone-less, and the config keeping an explicit setting.
- Mutant (the `with_int96_as_instants` call in `df_guards.rs` removed): the same 44 facade pins turn red as on main and
  the 12 controls stay green; in Rust the two read tests fail (the leaf arrives as nanoseconds) and the two config
  tests, which call the function directly, pass.

## Delta round 1 (2026-11-22, PR #1411's verifiers)

- **Path append (fixed):** a local `write.mode("append").parquet(dst)` onto a Spark `INT96` directory refused on the
  first head (`type mismatch source=timestamp[us, tz=UTC] vs path=timestamp[ns]`): `_validate_path_append_schema`
  read the destination with `pyarrow.parquet.read_schema`, which knows nothing of the option. Main appended a frame
  read from the directory (A1) but stored zone-less nanoseconds, and refused a `TIMESTAMP` literal (A2). Now
  `writer_layout._session_parquet_schema` changes only the destination's `INT96` leaves read as `timestamp[ns]` to
  `timestamp[us, tz=UTC]`, the default session mapping, keeping pyarrow's field structure, names and map layout (delta
  round 2: delta 1 swapped whole fields for the session's own read). A1 and A2 read back cold, on RePark and
  on Spark, as Spark's rows in UTC and New York; the directory then holds the `INT96` file and RePark's `INT64
  TIMESTAMP(MICROS,true)` file.
- **SQL `RESET` (recorded, not fixed):** `RESET datafusion.execution.parquet.coerce_int96` puts the key back to
  DataFusion's unset value, so `ns` and `timestamp_ntz` return for the rest of the session. `conf.unset` of the key
  leaves the live value (`us`, or a value the user set) in place (measured in delta round 2's re-check). Restoring
  `us`/`UTC` on `RESET` needs a hook on both SQL doors, or moving the default from the session config to read time in
  both Parquet seats: not small. An explicit `ns` stays the user's choice and wraps values past 2262; an explicit `""` fails at the next read.
- **Still unlike Spark (recorded):** an `INT96` leaf whose embedded Arrow schema is zone-less (pyarrow or pandas with
  `use_deprecated_int96_timestamps` over a naive `timestamp[us]`) reads `timestamp_ntz` with the UTC wall time on main
  and head (Spark: `timestamp`; a strict xfail); a file written with `int96RebaseModeInWrite=LEGACY` (Spark 2.x style)
  is not rebased on read (`1500-06-01` reads `1500-06-11`, main the same; the verifier's measurement).
- **Ballista:** not measured on executors. DataFusion 54.1's protobuf `ParquetOptions` carries `coerce_int96` and
  `coerce_int96_tz`, so a serialized Parquet scan should keep them.
- **Pins added:** `append/<case>/<zone>` (A1, A2; UTC and New York; both ANSI modes) red on main and on the first
  head, green after; the zone-less-hint strict xfail; the recorded `add_files` gap (Spark's rows, RePark's NULLs, the
  card). Fixtures `spark_int96_flat/` and `pyarrow_int96_naive/`.

## Delta round 2 (2026-11-22, PR #1411's re-check)

- **Append, map layout (fixed):** delta 1's helper swapped a whole `INT96`-bearing top-level field for DataFusion's
  read, so a map's `key_value` group became `entries`: an append of a map holding a timestamp then passed the check,
  wrote an `entries` file beside Spark's `key_value` file, and RePark could no longer read the directory. The helper
  now walks the Arrow type alongside the Parquet leaves (leaf counts per struct, list and map) and changes only the
  `INT96` leaf types; a field whose `INT96` leaf sits under a map keeps pyarrow's type, so such an append refuses
  cleanly, as on main, and the directory still reads back as Spark's rows.
- **Append, dotted names (fixed):** leaves map to top-level fields by position, not by splitting the dotted path, so
  a column named `ts.utc` appends and reads back as Spark's rows (it refused on delta 1's head; main appended it as
  zone-less nanoseconds).
- **Cost:** no session read in the check. 500 Spark `INT96` part files, read-then-append check, median of 5: main
  0.027 s, delta 1 0.421 s, now 0.038 s.
- **Not covered:** the helper assumes the default mapping (`us`, `UTC`); a session whose user set `coerce_int96`
  to another unit compares against that default.
- **Pins added:** `append/dotted_read_then_append/<zone>` and `refusal/map_read_then_append|map_literal_append/<zone>`
  (UTC and New York, both ANSI modes; a refusal must leave the directory reading cold as Spark's rows); fixture
  `spark_int96_dotted/`.

## Delta round 3 (2026-11-22, the second re-check)

- **Append, embedded Arrow hint (fixed):** delta 2 read the destination through `ParquetFile.schema_arrow`, which
  does not apply a zone-less `ARROW:schema` hint to an `INT96` leaf, and turned it into `timestamp[us, tz=UTC]`. A
  `TIMESTAMP` literal then appended a UTC file beside the zone-less one and RePark could not read the directory back,
  while a read-then-append or a `TIMESTAMP_NTZ` append refused. The helper now decodes the file's `ARROW:schema` hint
  and applies the session reader's rule per `INT96` leaf: the hint's timestamp type where there is one, and
  `timestamp[us, tz=UTC]` for a hint or a physical type of `timestamp[ns]` with no zone (DataFusion's coercion).
- **Append, fixed-size lists (fixed):** `fixed_size_list` counts and rebuilds beside `list` and `large_list`, so a
  `fixed_size_list<struct<…>>` column before an `INT96` column no longer shifts the leaf offset.
- **Pins added:** four `layout/<case>/<zone>` cases (UTC and New York, both modes): a `TIMESTAMP` literal onto the
  zone-less-hint file refuses and the directory still reads; a read-then-append and a `TIMESTAMP_NTZ` literal onto it
  append and read back; a read-then-append onto a pyarrow file with a `fixed_size_list<struct>` before the `INT96`
  column appends and reads back. All eight red on `3c6b9c41`, green after.

## Delta round 4 (2026-11-23, the Opus re-check of round 3; orchestrator ruling)

- **Ruling: the append check no longer follows a hinted file's own rules.** This unit's scope is Spark's own
  `INT96`. An `INT96` leaf of a file without an `ARROW:schema` hint (every Spark-written file, pyarrow with
  `store_schema=False`) compares as `timestamp[us, tz=UTC]`; a file with a hint compares exactly as on main
  (pyarrow's physical read, no hint decoding), so every hinted append refuses cleanly, as main's did. This closes
  R3-F1: a read-then-append onto an `ns`/UTC- or `s`-hinted file passed delta 3's check and wrote a file Spark then
  refused. Delta 3's decoder is gone; the `fixed_size_list` leaf count stays.
- **Reader, corrected (R3-F2):** a leaf whose embedded Arrow schema names a timestamp keeps that hint's own unit and zone (`ns`/UTC reads
  `Timestamp(ns, "UTC")`, `us`/New York `Timestamp(µs, "America/New_York")`), and a zone-less `us`, `ms` or `s` hint reads NTZ (a zone-less `ns` hint, or one naming a non-timestamp type, reads `Timestamp(µs, "UTC")`). Earlier records said a zoned hint reads as `Timestamp(Microsecond, "UTC")`.
- **Unreadable footer (R3-F3):** a malformed `ARROW:schema` made `ParquetFile` raise a raw `ArrowInvalid`; the check
  now raises its `AnalysisException` and leaves the directory unchanged.
- **Pins:** the zone-less-hint cases (a `TIMESTAMP` literal, a read-then-append, a `TIMESTAMP_NTZ` literal), a
  read-then-append onto an `ns`/UTC-hinted and an `s`-hinted file, and a malformed hint all refuse with the
  directory unchanged and readable; Spark reads the untouched `ns`/UTC and `s` directories as instants. A
  `fixed_size_list<struct>` file without a hint appends and reads back. UTC and New York, both modes.

## Fix direction (P01-05, as filed)

Map INT96 leaves to Spark's `timestamp` at schema inference, decoding the stored instant, and keep NTZ for INT64
leaves whose footer says they are not adjusted to UTC. Pins: a Spark-written INT96 file read in UTC,
America/New_York and Asia/Kolkata; `to_date`, `hour`, a filter on a date boundary, and a read-then-write that stores
the result; a file mixing INT96 and INT64 columns.
