# map — task/wo/native-dataframe-write-silent-1/evidence

## Purpose

The records behind [card NATIVE-DATAFRAME-WRITE-SILENT-1](../../../roadmap/mid-term/native-dataframe-write-silent-1-card-2026-11-13.md):
step 0 on main, the cause, and the head run with the pins red on main and green on the head.

## Contents

- [step0/](step0/map.md) — main `1d9d2808`: 1568 native cells and 868 Spark-door cells, over
  seven frame shapes each. Also the diagnostic run with the dropped frame collected, the
  partitioned reach probe and the workarounds. The second round adds the inventory critic's
  spellings.
- [after/](after/map.md) — the head: the same cells, the Rust and facade pins red on main and
  green on the head.

## Step 0 in one table (main, native door; all seven frames agree in every row)

The frames are `repark.sql` itself, `filter`, `select`, `union`, `cache()`, `localCheckpoint()`
and a self-join.

| Verb | Existing Iceberg table (`ice`, `hd`) | In-memory table (`ice.ms.*`, `datafusion.public.*`) | New path | Existing path |
|---|---|---|---|---|
| `saveAsTable` append; `format("iceberg").save(name)` append and **overwrite** | **SILENT** | refused | — | — |
| `insertInto`, `overwrite=False`/`True`; `partitionBy(c).insertInto` | **SILENT** | **SILENT** | — | — |
| `writeTo().append()` (plain, `tableProperty`, `partitionedBy`), `overwritePartitions()` | **SILENT** | refused | — | — |
| `.parquet`/`.csv`/`.json` append | — | — | **SILENT**, one empty file | **SILENT**, one empty file added |
| `.parquet`/`.csv`/`.json` overwrite, plain or `partitionBy(c)`; `format("parquet").save(p)`, `save(p, format=, mode=)`, `format("parquet").option("path", p).save()` | — | — | **SILENT**, one empty file | **S1: the existing rows are deleted**, one empty file left |
| `.parquet`/`.csv`/`.json` with the default mode, `error`, `errorifexists`, `ignore`; `partitionBy(c)` append | — | — | **SILENT** | refused (`PATH_ALREADY_EXISTS` or the schema check); `ignore` is a no-op |
| `saveAsTable` overwrite/error/errorifexists; `writeTo` create/createOrReplace/replace/`overwrite(cond)` | refused (parse, or already exists) | refused | — | — |
| `mergeInto(...).merge()` (control: it does not reach the seat) | refused | refused | — | — |
| `saveAsTable` ignore | stores (a no-op) | refused | — | — |
| `.text` append and overwrite | — | — | stores | stores |

- **Counts:** 308 SILENT, 63 S1, and 28 `partitionBy(c).insertInto` cells that return
  unchanged where Spark refuses. That is 399 guarded cells. 49 cells store, and 1120 refuse
  before any effect.
- **A new table name, a bare name, a view or a temp view:** every verb refuses.
  `format("iceberg").option("path", …).save()` refuses on write options;
  `format("iceberg").save(<path>)` refuses as not found. With a path format, `option("path", p)`
  is a path write: SILENT or S1 above.
- **A partitioned Iceberg table is not reachable on the native door.** Native DDL refuses
  `PARTITIONED BY`, and the native `hd` catalog does not see a table the Spark door created in
  the same warehouse. Glue, S3 Tables, REST and a Postgres-mounted catalog were not measured. The
  cause sits above any catalog.
- **The Spark door** stores the model's rows wherever it does not refuse, with two exceptions
  outside this unit:
  - It stores `partitionBy(c).insertInto` rows where Spark refuses (14 cells).
  - Its `writeTo().overwrite(p = 1)` over a file holding both partitions refuses with `Cannot
    delete file where some, but not all, rows match` and leaves one more data file.

## The cause

1. **The dropped frame.** Every table verb and every Parquet/CSV/JSON path write runs its SQL
   through `run_through_temp_view`. That function discards the frame
   `_native.session_sql_with_write_options` returns
   (`python/repark/src/repark/spark/dataframe/writer_layout.py:112`).
2. **That frame is pending on the native door.** `LocalFsGuardedDialect` wraps
   `DataFusionDialect`, whose default `execute_with_write_options` calls `execute`
   (`crates/repark-core/src/dialect.rs:111`). `execute` ends at `ctx.execute_logical_plan(plan)`
   (`dialect.rs:156`, `crates/repark-core/src/pre_execute.rs:57`), which is lazy for a
   `LogicalPlan::Dml` insert and for a `LogicalPlan::Copy`.
3. **The Spark door is eager.** Its router collects `Dml`/`Copy` statements and returns their
   batches (`crates/repark-spark/src/spark_ast.rs:271` and `:295-301`).
4. **The path S1.** The staging `COPY` never runs. `_apply_path_write` makes the staging
   directory and one empty file (`python/repark/src/repark/spark/dataframe/writer_readwriter.py:429-430`),
   deletes the destination (`:457`) and renames staging into place (`:466`).
5. **`mergeInto` takes another route,** `MergeIntoWriter.merge` → `sql_built`
   (`python/repark/src/repark/spark/merge.py:162`). The native door refuses `MERGE` at plan
   time, so the facade pin keeps a control there.
6. **Proof:** with the frame collected (the diagnostic run), every SILENT and S1 cell stores the
   model's rows. The in-memory `insertInto(overwrite=True)` cells refuse loudly instead, and no
   Spark-door cell changes.

## Observations outside this unit (for the records batch)

- `note_local_write_root` (`writer_readwriter.py:417`) runs before the seat. After a refusal,
  the never-created staging path stays on the native session's local-write allow list.
- An S3 path overwrite deletes the destination before it copies (`path_write.rs:403`).
- A native path containing `'` is refused by the local-file policy before the seat, with a
  message that names the staging path.
- The Spark-door rows above: `partitionBy(c).insertInto` stores, and the orphan file left by
  `overwrite(cond)`.

## Pointers

- Up: [order map](../map.md)
