# map — task/wo/native-dataframe-write-silent-1/evidence

## Purpose

The records behind [card NATIVE-DATAFRAME-WRITE-SILENT-1](../../../roadmap/mid-term/native-dataframe-write-silent-1-card-2026-11-13.md):
step 0 on main, and the cause.

## Contents

- [step0/](step0/map.md) — main `1d9d2808`: 612 native cells and 312 Spark-door cells (four frame
  shapes each), the diagnostic run with the dropped frame collected, the partitioned reach probe
  and the workarounds.

## Step 0 in one table (main, native door; all four frames agree in every row)

| Verb | Iceberg table that exists (`ice`, `hd`) | In-memory table (`ice.ms.*`, `datafusion.public.*`) | New path | Existing path |
|---|---|---|---|---|
| `saveAsTable` append, `format("iceberg").save(name)` append | **SILENT** | refused (`USING` parse, unknown catalog) | — | — |
| `insertInto`, `overwrite=False` and `True` | **SILENT** | **SILENT** | — | — |
| `writeTo().append()`, `overwritePartitions()` | **SILENT** | refused (not found, unknown catalog) | — | — |
| `.parquet` / `.csv` / `.json`, `mode("append")` | — | — | **SILENT**, one empty file | **SILENT**, one empty file added |
| `.parquet` / `.csv` / `.json`, `mode("overwrite")` | — | — | **SILENT**, one empty file | **S1: the existing rows are deleted**, one empty file left |
| `saveAsTable` overwrite / error / errorifexists, `writeTo` create / createOrReplace / replace | refused (`USING` parse, or already exists) | refused | — | — |
| `writeTo().overwrite(cond)` | refused (`REPLACE` parse) | refused | — | — |
| `saveAsTable` ignore | stores (a no-op on an existing table) | refused | — | — |
| `.text`, append and overwrite | — | — | stores | stores |

- **A new table name, a bare name, a view or a temp view:** every verb refuses (not found,
  `USING` parse, `unknown catalog 'spark_catalog'`, or `Writing into a view`).
- **`option("path", …).save()` and `format("iceberg").save(<path>)`:** refused.
- **No cell refuses after an effect.** The only cells that remove rows are the 12 S1
  existing-path overwrites.
- **A partitioned Iceberg table is not reachable on the native door.** Native DDL refuses
  `PARTITIONED BY` (`Hive distribution not supported`), and the native `hd` catalog does not see
  a table that the Spark door created in the same warehouse (`table 'hd.ns.p0' not found`). The
  cause below does not depend on partitioning.
- **Glue, S3 Tables and REST** were not measured: they do not run locally. The cause sits above
  any catalog, in the writer's handling of the planned frame.
- **The Spark door** stores the model's rows in every cell it does not refuse. A `saveAsTable`
  onto a temp-view name writes `spark_catalog.default.<name>`, which the view shadows. Its
  `writeTo().overwrite(p = 1)` over a file holding both partitions refuses with `Cannot delete
  file where some, but not all, rows match` and leaves one more data file on disk. That is
  outside this unit.

## The cause

1. Every writer verb runs its SQL through `run_through_temp_view`, and that function discards
   the frame `_native.session_sql_with_write_options` returns
   (`python/repark/src/repark/spark/dataframe/writer_layout.py:112`).
2. On the native door that frame has not executed. `LocalFsGuardedDialect` wraps
   `DataFusionDialect`, whose default `execute_with_write_options` calls `execute`
   (`crates/repark-core/src/dialect.rs:111`). `execute` ends at `belt.execute(plan)`
   (`dialect.rs:156`), which is `ctx.execute_logical_plan(plan)`
   (`crates/repark-core/src/pre_execute.rs:57`). That call returns a lazy frame for a
   `LogicalPlan::Dml` insert or a `LogicalPlan::Copy`. The pyfunction wraps the lazy frame and
   returns it (`crates/repark-python/src/session_write_options.rs:65`), so nothing ever runs it.
3. The Spark door's dialect runs the write inside its router
   (`crates/repark-spark/src/dialect.rs:39`), so its frame is not pending.
4. The path S1 follows from the same lazy frame. The `COPY` into the staging directory never
   runs. `_apply_path_write` then creates the empty staging directory
   (`python/repark/src/repark/spark/dataframe/writer_readwriter.py:429`), writes one empty file
   into it (`:430`), deletes the existing destination (`:457`) and renames staging into place
   (`:466`).
5. **Proof:** with the frame collected inside a wrapper (the diagnostic run), every native SILENT
   and S1 cell stores the model's rows. The two exceptions are the in-memory
   `insertInto(overwrite=True)` cells, which then refuse loudly. No Spark-door cell changes.

## Pointers

- Up: [order map](../map.md)
