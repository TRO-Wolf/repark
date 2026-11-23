# Card ICEBERG-INT96-NULL-1: an Iceberg data file holding `INT96` reads every timestamp as NULL

**Date:** 2026-11-22. **Filed by:** the builder (`claude-opus-5-5`) in delta round 1 of PARQUET-INT96-AS-NTZ-1 (PR
#1411), from that PR's verifier. **Severity:** S1 (silent NULLs; owner, 2026-11-23). **Not that unit:** the
Iceberg scan reads through iceberg-rust's own Parquet reader, which the PARQUET-INT96-AS-NTZ-1 option does not reach.

## The defect

Spark's `add_files` procedure (the documented way to bring an existing Spark Parquet table into Iceberg, as
`migrate` and `snapshot` do) registers a Spark-written file as is, so an Iceberg v2 table can hold `INT96` timestamp
leaves; Spark reads them back as instants. RePark reads the table's schema as `timestamp` and every `ts` as NULL.

## Measured (PySpark 4.1.2 + iceberg-spark-runtime 1.11.0; RePark main `5caea2a5` and PR #1411's head)

Reproduction: a Spark-default (`INT96`) Parquet directory of `(id INT, ts TIMESTAMP)`, then in Spark with a hadoop
catalog `lc`:

```sql
CREATE TABLE lc.db.added (id INT, ts TIMESTAMP) USING iceberg;
CALL lc.system.add_files(table => 'db.added', source_table => '`parquet`.`<the directory>`');
SELECT id, CAST(ts AS STRING) FROM lc.db.added ORDER BY id;
```

then in RePark `spark.read.format("iceberg").load("<warehouse>/db/added")` (or its `metadata.json`).

| Reader (America/New_York) | schema | rows |
|---|---|---|
| Spark 4.1.2 | `id int, ts timestamp` | `1, 2025-01-14 20:00:00`; `2, 2025-07-01 08:00:00` |
| RePark main and head | `id int, ts timestamp` | `1, NULL`; `2, NULL` |

The verifier's larger reproduction (15 ids, years 1 to 9999) gave NULL for every row on both builds. Spark's
answers are recorded as `iceberg_add_files/America/New_York` in
`python/repark/tests/io/parquet_int96_as_ntz_1_oracle.json`; the gap is a recorded-gap pin there
(`test_a_recorded_gap_keeps_its_card_and_its_spark_answer`), since a committed Iceberg fixture would carry absolute
file paths.

## Reach

Any Iceberg table migrated from Spark Parquet with `add_files`, `migrate` or `snapshot` whose files kept Spark's
default `INT96`, read by RePark: the timestamps come back NULL with no error, so filters on them drop every row.

## Fix direction

Decode an `INT96` leaf in the Iceberg scan as the instant its Julian day and nanoseconds name (Iceberg's own
readers do this for imported files), at microseconds; pin with a Spark `add_files` table in UTC and a non-UTC zone.
