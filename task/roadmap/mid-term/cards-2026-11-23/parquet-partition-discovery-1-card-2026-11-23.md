# Card PARQUET-PARTITION-DISCOVERY-1: `read.parquet` of a partitioned directory drops the partition columns

**Date:** 2026-11-23. **Filed by:** the orchestrator (`claude-opus-5-5`), from both verifiers of PR #1411 (each saw
the column missing on main and on the branch) and a re-measurement here. **Severity:** S1 proposed (owner to rate):
a read-then-write copy stores the table without its partition columns, silently. **Status:** filed, not dispatched.

## Measured

A Spark 4.1.2 `partitionBy("p")` write of `(id BIGINT, p INT, s STRING)`, four rows over `p=1/` and `p=2/`, local
paths, session zone `America/New_York`; RePark main `5caea2a5` (the 1.5.7 product tree).

| Read | Spark 4.1.2 | RePark main |
|---|---|---|
| `read.parquet(root)` schema | `struct<id:bigint,s:string,p:int>` | `struct<id:bigint,s:string>` |
| `read.parquet(root).collect()` | four rows with `p` | four rows without `p` |
| `read.parquet(root).where("p = 2")` | the two `p = 2` rows | `AnalysisException: Schema error: No field named p` |
| `read.format("parquet").load(root)` schema | with `p` | without `p` |
| `read.parquet(root + "/p=1")` (a leaf) | `struct<id:bigint,s:string>` | the same |
| `option("basePath", root).parquet(root + "/p=1")` | with `p` | refuses: `reader option 'basePath' is not supported by repark yet` |
| `read.parquet(root).write.parquet(copy)`, read back | the copy keeps `p` | the copy has no `p` |

A query that names the column fails loudly. A `SELECT *`, a `collect()` or a read-then-write loses the column without
a message. RePark's own `partitionBy("p")` Parquet write of `(id BIGINT, p INT)` lays out `p=1/` and `p=2/` and
reads back as `struct<id:bigint>` (measured on the same tree), so its own output does not read back whole either.

## Not measured

S3 paths (the Rust path-read seat may differ from the local facade read), CSV, JSON and ORC partitioned reads, SQL
over a temporary view of the read, and type inference of the directory values. Text reads already discover the
keys ([IO-TEXT-PARTDISC-1](../../../../docs/spark-sql-iceberg-parity.md#io-text-partdisc-1--text-reads-of-partitioned-dirs-discover-the-keys));
`crates/repark-core/src/partition_discovery.rs` holds that discovery, with Spark's inference order and the
`CONFLICTING_PARTITION_COLUMN_NAMES` refusal.

## First step

Step 0 measures every plain-path reader (Parquet, CSV, JSON, ORC, text) over local and S3 partitioned layouts against
Spark, then reuses the text reader's discovery for the Parquet seat. Iceberg tables are unaffected: their partition
values live in the table metadata.

Up: [map.md](map.md).
