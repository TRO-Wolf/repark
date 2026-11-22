# Card PARQUET-INT96-AS-NTZ-1: a Spark-written INT96 timestamp reads as `timestamp_ntz`

**Date:** 2026-11-22. **Filed by:** the orchestrator (`claude-opus-5-5`), not dispatched. **Source:** an independent
workload measurement against Spark 4.1.2 found a calendar date that moved in a New York session; re-measured here on
main `c5370be6` (the released 1.5.6 wheel's source). **Severity:** S1 (owner, 2026-11-22): silent, it changes
which rows a date filter returns, and INT96 is Spark's default timestamp encoding.

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

## Fix direction (P01-05)

Map INT96 leaves to Spark's `timestamp` at schema inference, decoding the stored instant, and keep NTZ for INT64
leaves whose footer says they are not adjusted to UTC. Pins: a Spark-written INT96 file read in UTC,
America/New_York and Asia/Kolkata; `to_date`, `hour`, a filter on a date boundary, and a read-then-write that stores
the result; a file mixing INT96 and INT64 columns.
