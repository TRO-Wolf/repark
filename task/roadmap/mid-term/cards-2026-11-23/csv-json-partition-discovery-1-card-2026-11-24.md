# Card CSV-JSON-PARTITION-DISCOVERY-1: CSV and JSON reads of a partitioned directory drop the partition columns

**Date:** 2026-11-24. **Filed by:** the orchestrator (`claude-opus-5-5`), from the Step 0 of
PARQUET-PARTITION-DISCOVERY-1 (PR #1430), which measured these readers as out of its seat. **Severity:** S1 (owner,
2026-11-25). It is the same shape as PARQUET-PARTITION-DISCOVERY-1: a read-then-write copy stores the table without its
partition columns, silently. **Status:** filed, not dispatched. It follows #1430, which builds the Parquet seat these
readers should share.

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

Up: [map.md](map.md).
