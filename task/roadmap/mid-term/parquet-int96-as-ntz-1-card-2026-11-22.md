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
then infers and decodes an `INT96` leaf without an embedded Arrow schema, or whose embedded Arrow schema names a zone, nested ones included, as `Timestamp(Microsecond, "UTC")`, Spark's
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
  `writer_layout._session_parquet_schema` takes a part file's `INT96`-bearing top-level fields from the session's own
  read of that file, as the S3 append already does (`path_write/append.rs`). A1 and A2 read back cold, on RePark and
  on Spark, as Spark's rows in UTC and New York; the directory then holds the `INT96` file and RePark's `INT64
  TIMESTAMP(MICROS,true)` file.
- **`RESET` and `conf.unset` (recorded, not fixed):** they put `coerce_int96` back to DataFusion's unset value, so
  `ns` and `timestamp_ntz` return for the rest of the session. Restoring `us`/`UTC` needs a hook on `RESET` on both SQL
  doors and on `conf.unset`, or moving the default from the session config to read time in both Parquet seats: not
  small. An explicit `ns` stays the user's choice and wraps values past 2262; an explicit `""` fails at the next read.
- **Still unlike Spark (recorded):** an `INT96` leaf whose embedded Arrow schema is zone-less (pyarrow or pandas with
  `use_deprecated_int96_timestamps` over a naive `timestamp[us]`) reads `timestamp_ntz` with the UTC wall time on main
  and head (Spark: `timestamp`; a strict xfail); a file written with `int96RebaseModeInWrite=LEGACY` (Spark 2.x style)
  is not rebased on read (`1500-06-01` reads `1500-06-11`, main the same; the verifier's measurement).
- **Ballista:** not measured on executors. DataFusion 54.1's protobuf `ParquetOptions` carries `coerce_int96` and
  `coerce_int96_tz`, so a serialized Parquet scan should keep them.
- **Pins added:** `append/<case>/<zone>` (A1, A2; UTC and New York; both ANSI modes) red on main and on the first
  head, green after; the zone-less-hint strict xfail; the recorded `add_files` gap (Spark's rows, RePark's NULLs, the
  card). Fixtures `spark_int96_flat/` and `pyarrow_int96_naive/`.

## Fix direction (P01-05, as filed)

Map INT96 leaves to Spark's `timestamp` at schema inference, decoding the stored instant, and keep NTZ for INT64
leaves whose footer says they are not adjusted to UTC. Pins: a Spark-written INT96 file read in UTC,
America/New_York and Asia/Kolkata; `to_date`, `hour`, a filter on a date boundary, and a read-then-write that stores
the result; a file mixing INT96 and INT64 columns.
