# Card CSV-JSON-PARTITION-DISCOVERY-1: CSV and JSON reads of a partitioned directory drop the partition columns

**Date:** 2026-11-24. **Filed by:** the orchestrator (`claude-opus-5-5`), from the Step 0 of
PARQUET-PARTITION-DISCOVERY-1 (PR #1430), which measured these readers as out of its seat. **Severity:** S1 (owner,
2026-11-25). It is the same shape as PARQUET-PARTITION-DISCOVERY-1: a read-then-write copy stores the table without its
partition columns, silently. **Status:** fixed on `fix/csv-json-partition-discovery-1` (2026-11-26), awaiting gates and
review. It follows #1430, whose Parquet seat these readers now share.

## Measured

Measured on live Spark 4.1.2 and on main `514281bd4`. #1430 does not change these cells. The fixtures are a Spark
`partitionBy("p")` write of four rows over `p=1/` and `p=2/`, in both ANSI modes. Each probe is in
[the PARQUET-PARTITION-DISCOVERY-1 evidence](../../../wo/parquet-partition-discovery-1/evidence/summary.md).

| Read | Query | Spark 4.1.2 | Main |
|---|---|---|---|
| `spark.read.option("header", "true").csv(dir)` | schema and collect | `id, s, p int`, 4 rows | **`id, s`**, 4 rows |
| the same | `where("p = 2")` | 2 rows | refuses (`AnalysisException`) |
| the same | read, `write.parquet`, read back | `id, s, p`, 4 rows | **`id, s`**, 4 rows, silently |
| `spark.read.json(dir)` | schema and collect | `id bigint, s, p int`, 4 rows | **`id bigint, s`**, 4 rows |
| the same | `where("p = 2")` | 2 rows | refuses (`AnalysisException`) |
| the same | read, `write.parquet`, read back | `id, s, p`, 4 rows | **`id, s`**, 4 rows, silently |

- **Agreement with Spark:** CSV with a header 5 of 100 cells, CSV without a header 5 of 99, and JSON 5 of 100.
- **What does match:** `count()`, because it does not need the partition column.

## Seat

- **The two readers:**
  - CSV reads through `read_csv_path` and `_apply_reader_schema_semantics`;
  - JSON reads through `context.read_json`.
- **What they share:** neither shares #1430's Parquet seat (`crates/repark-core/src/spark_nullable/partition.rs`).
  ORC and text reads already use `partition_discovery::discover_partitions`.
- **The narrow fix:** each reader discovers partitions the way #1430's Parquet seat does: the hidden-path rule, Spark's
  type inference and its setting, overlap, conflicts, and partition-only file pruning. Unpartitioned reads stay on
  main's path.

## Fixed (2026-11-26)

- **Seat:** the CSV and JSON readers call #1430's discovery, now `spark_nullable/partition.rs::read_discovered`
  parameterised by the file format; when it answers `None`, the read is main's listing read, so an unpartitioned read
  is unchanged.
- **Measured:** [the evidence summary](../../../wo/csv-json-partition-discovery-1/evidence/summary.md) has the step 0
  scorecard, the #1430 corpora replay and the pin counts.

## Still unlike Spark

- **A stray file in a partition:** a file without the reader's suffix (`.csv`, `.json`, or the compressed suffix) is
  not read; Spark reads every non-hidden file. Main's listing suffix is kept, as on a flat directory; a follow-up in
  the shape of FILE-EXTENSION-FILTER-1.
- **CSV with a user schema** (`spark.read.schema(...).csv(dir)`) and **CSV with `nullValue`:** main's read, so the
  partition columns are missing. The positional text path and the facade's `nullValue` mapping do not yet know the
  partition columns.
- **CSV whose header names the partition column** (`p` in the file and in `p=10/`): main's read, the file's text.
- **Mixed-case directories** (`p=1/`, `P=2/`, case-insensitive): one column, named after the first directory in path
  order (`p`); Spark names it `P`. Shared with the Parquet seat.
- **`where("p = 2")` on a string or date partition:** the pruning cast raises an Arrow cast error in both ANSI modes,
  where Spark raises `CAST_INVALID_INPUT` (ANSI on), answers no rows (ANSI off), or raises
  `DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES` (date). Shared with the Parquet seat.
- **A JSON user schema that types a partition column** casts the inferred value, not the directory text, so `p=01`
  read as `p STRING` would give `1` where Spark gives `01` (not measured).
- **CSV `inferSchema=true` in a case-sensitive session:** partition names compare case-insensitively.
- **Refused as on the Parquet seat:** `basePath` and `recursiveFileLookup`. **Main's unpartitioned path:** several
  paths in one `load()`, and globs.

Up: [map.md](map.md).
