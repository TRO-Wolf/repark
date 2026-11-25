# Card PARTITIONBY-NULL-WRITE-1: path `partitionBy` writes store a NULL partition value as the type's default

**Date:** 2026-11-25. **Filed by:** the orchestrator (`claude-opus-5-5`), from finding F-1 of the final verifier of
PARQUET-PARTITION-DISCOVERY-1 (PR #1430), re-measured here on main. **Severity:** S1 (owner, 2026-11-25). It is
a silent wrong write: a NULL becomes `0` or `1970-01-01` on disk, and Spark reads that value back. **Status:** built
on branch `fix/partitionby-null-write-1` (builder `claude-opus-5-5`); PR open for review, not merged. PR #1430 is
parked on it.

## Measured

Measured on live Spark 4.1.2 and on a build of main `14d2404a6` (#1429's tested head), in UTC.
`createDataFrame(rows, schema).write.partitionBy("p").<format>(dir)`; the cells list the partition directories written.

| Partition column | Rows | Spark 4.1.2 | Main |
|---|---|---|---|
| `p int` | `(1, NULL), (2, 5)` | `p=5`, `p=__HIVE_DEFAULT_PARTITION__` | **`p=0`**, `p=5` |
| `p date` | `(1, NULL)` | `p=__HIVE_DEFAULT_PARTITION__` | **`p=1970-01-01`** |
| `p string` | `(1, NULL), (2, 'a')` | `p=__HIVE_DEFAULT_PARTITION__`, `p=a` | `p=`, `p=a` |
| `p string` | `(1, ''), (2, 'a')` | `p=__HIVE_DEFAULT_PARTITION__`, `p=a` | `p=`, `p=a` |

- **The same for Parquet, CSV and JSON.** ORC path writes are not implemented on main, so they refuse.
- **Read back by live Spark,** the Parquet `p int` copy gives `(1, 0), (2, 5)`; Spark's own copy gives `(1, NULL), (2, 5)`.
- **String partitions** land in `p=/`, which Spark's reader and PARQUET-PARTITION-DISCOVERY-1's reader refuse, so
  that case is loud.
- **Iceberg is not affected:** an Iceberg table partitioned by `p` or `days(d)`, written by `INSERT … VALUES` and
  `writeTo().append()`, keeps the NULL rows in the NULL partition, and Spark reads them back as NULL.

## Seat

The path writer hands partitioning to DataFusion's `COPY … PARTITIONED BY`, which writes a NULL as the type's default.
The facade builds the clause in `python/repark/src/repark/spark/dataframe/writer_layout.py:541`
(`partitioned_by_sql_clause`), and the session adds it at `crates/repark-core/src/session/path_write.rs:169`. The
text writer already names the NULL directory as Spark does (`crates/repark-core/src/text_partition.rs:530-532`).

## First step

Measure every partition type (integers, decimals, floats with NaN, booleans, dates, timestamps in two zones, strings
with `/`, `=`, `%` and non-ASCII), two partition levels, `mode("overwrite")` with
`partitionOverwriteMode=dynamic`, and a read-then-write copy of a discovered directory. Then write NULL and empty values
to `__HIVE_DEFAULT_PARTITION__`, and escape directory names as Spark does.

## Result (branch `fix/partitionby-null-write-1`)

**Step 0** (live Spark 4.1.2, main's package built from `57c92b411`, the branch head; both ANSI modes; 115 cells per
mode; the table and scripts are in
[task/wo/partitionby-null-write-1/evidence/](../../../wo/partitionby-null-write-1/evidence/domain-table.md)):
cells equal to Spark move from 50 to 124 of 230. 74 cells (37 per mode) move from DIFF to EQUAL, and 38
more (19 per mode) move toward Spark but stay DIFF: CSV cells whose directories now equal Spark's (the header line
keeps them DIFF), and float cells whose NULL now lands in `__HIVE_DEFAULT_PARTITION__`. **No cell moves off
Spark.** Every cell that equalled Spark on main, or refused on main, answers the same. Data-file rows and RePark's
read-backs are unchanged in every cell, except that the CSV header line is now repeated in the NULL row's new part.
Both ANSI modes agree cell for cell.

**The fix.** Parquet, CSV and JSON path writes, on local paths and S3, now read the COPY from a frame whose partition
columns hold the demux's own text, with a NULL or empty value as `__HIVE_DEFAULT_PARTITION__`
(`crates/repark-core/src/session/path_write/hive_dirs.rs`). The written directories, or the S3 keys this write
added, are then renamed with Spark's `escapePathName` set, which the text writer's `escape_partition_value` already
holds. The facade's seats are `writer_readwriter.py` `_apply_path_write` and `writer_layout.py`
(`_resolved_partition_columns`, `run_through_temp_view(hive_partitions=…)`, `_spell_partition_dirs`).

**Still unlike Spark** (measured at head, both ANSI modes):
- **Floats.** Float and double partition values keep the demux's Rust text: `p=1`, `p=-0`, `p=inf`, `p=-inf`,
  `p=100000000000000000000`, where Spark writes `p=1.0`, `p=-0.0`, `p=Infinity`, `p=-Infinity`, `p=1.0E20`. This holds
  for Parquet, CSV, JSON and the text writer. A NULL float now lands in `__HIVE_DEFAULT_PARTITION__`.
- **Refused partition types.** Decimal, timestamp, `timestamp_ntz` (in both zones) and binary partition columns refuse
  on Parquet, CSV and JSON ("not yet supported to write to hive partitions"), where Spark writes `p=1.50`,
  `p=2024-03-01 10%3A30%3A00` and `p=ab`. The text writer accepts decimal and timestamp, as Spark does.
- **CSV header.** A partitioned CSV write puts a header line in each part; Spark writes none.
- **Dynamic overwrite.** `mode("overwrite")` with `partitionOverwriteMode=dynamic` refuses
  (PLAIN-DYNAMIC-OVERWRITE-GUARD-1), where Spark replaces only the partitions the rows hold.
- **Read-then-write copy.** A copy of a Spark-written partitioned directory refuses, because RePark's path reader
  drops the partition column (`partitionBy column 'p' is not in the DataFrame columns ['id']`). That is
  PARQUET-PARTITION-DISCOVERY-1's scope. RePark's own read-back of any partitioned directory has no partition
  column, on main and at head alike.
- **SQL doors.** `INSERT OVERWRITE DIRECTORY '<dir>' USING <fmt> SELECT …` refuses with a parse error, where Spark
  writes it (Spark refuses the `PARTITIONED BY` form, and so does RePark).
  `CREATE TABLE … USING parquet PARTITIONED BY (p) LOCATION … AS SELECT` writes an Iceberg table (`data/p=null`)
  where Spark writes a Hive layout, and `saveAsTable` with `format("parquet")` refuses.

**PR #1430.** PARQUET-PARTITION-DISCOVERY-1 is parked on this card. Its verifier's F-1 is this defect, seen through a
`partitionBy` copy of a discovered directory. After this unit merges, #1430 takes main and gets a narrow re-check of
F-1. Its discovered string partition columns may be dictionary-typed; `hive_dirs.rs` names
`Dictionary(_, Utf8)` columns by the same rule.

Up: [map.md](map.md).
