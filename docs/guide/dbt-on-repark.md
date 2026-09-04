# dbt on RePark

`dbt-repark` runs dbt's compiled SQL **in process** through `repark.sql()`. There is no server,
no Thrift endpoint and no JVM: dbt and the engine share one Python process and one
`ReparkSession`.

Filed by unit DBT-1 (2026-09-04). It is **not published** — no wheel and no PyPI release — so it
is installed from a checkout. Everything below was measured against the package's own suite,
`python/dbt-repark/tests`.

## What works today

One materialization: `table`, with `file_format='iceberg'`. That is what the cutover pipeline's
gold stage uses — two full-rebuild models and ten generic test blocks — and it is what this
adapter was built to run.

| dbt feature | On RePark |
|---|---|
| `materialized='table'` | yes, `create or replace table … using iceberg tblproperties (…) as …`, one Iceberg snapshot |
| `tblproperties`, `partition_by` | yes — the `dbt-spark` clause macros, unchanged |
| generic tests (`unique`, `not_null`, `accepted_values`, `relationships`) | yes |
| `source()`, `ref()`, `--select`, `--full-refresh` | yes |
| `threads` | yes — one shared session, one cursor per statement |
| `dbt docs generate` | yes — columns come from the frame schema, not `DESCRIBE` |
| a namespace that does not exist yet | created before the model builds |
| `persist_docs: {relation: true}` | **refused** — [DBT-RELCOMMENT-1](../spark-sql-iceberg-parity.md) |
| `materialized='view'` | **refused** — [DBT-VIEW-1](../spark-sql-iceberg-parity.md) |
| `materialized='incremental'` | **refused** — [DBT-TEMPVIEW-1](../spark-sql-iceberg-parity.md) |
| snapshots | **refused** — [DBT-TEMPVIEW-1](../spark-sql-iceberg-parity.md) |
| `persist_docs: {columns: true}` | **refused** — [DBT-COLCOMMENT-1](../spark-sql-iceberg-parity.md) |
| `location_root`, `options`, `clustered_by` / `buckets` | **refused** — [DBT-CTASCLAUSE-1](../spark-sql-iceberg-parity.md) |

Every refusal happens at compile time with a message naming its registry row, so a model never
half-builds. The differences from Apache Spark are owned by
[the divergence registry](../spark-sql-iceberg-parity.md) §2.5 and §7 — this page does not
restate them.

## Install

```
pip install dbt-core==1.9.*
pip install -e python/dbt-repark
```

From a checkout of this repository, `make py-test-dbt` provisions the same pins and runs the
package's own suite against the built native module.

`dbt-spark` arrives as a dependency. It brings no driver: its `pyhive`, `thrift`, `pyodbc` and
`pyspark` transports are all optional extras, and `dbt-repark` uses none of them. What it does
bring is the macro package whose `create_table_as` already emits the statement RePark serves.

## Profile

```yaml
gold:
  target: prod
  outputs:
    prod:
      type: repark
      catalog: glue_catalog
      schema: my_namespace
      threads: 4
      catalog_properties:
        spark.sql.catalog.glue_catalog: org.apache.iceberg.spark.SparkCatalog
        spark.sql.catalog.glue_catalog.catalog-impl: org.apache.iceberg.aws.glue.GlueCatalog
        spark.sql.catalog.glue_catalog.warehouse: s3://your-warehouse/
        spark.sql.catalog.glue_catalog.io-impl: org.apache.iceberg.aws.s3.S3FileIO
```

| Field | Meaning |
|---|---|
| `catalog` | the RePark catalog name. dbt calls this `database`; either spelling works. |
| `schema` | the RePark namespace. |
| `catalog_properties` | a `spark.sql.catalog.<name>.*` block, carried verbatim to the session builder. This is the Glue / S3 Tables path. |
| `warehouse` | a directory, for a **memory** catalog. This is the test and single-process path — see the warning below. |
| `session_properties` | any other builder key, applied before the session is built. |

`catalog_properties` and `warehouse` are alternatives; give exactly one.

Every relation renders three parts, `catalog.namespace.table`, because RePark's `DESCRIBE` and
`ALTER TABLE` require the catalog part
([DBT-QUALIFY-1](../spark-sql-iceberg-parity.md)). You do not write this — the adapter does.

**A memory catalog lives in the session that registered it.** Tables another process created are
not visible, whatever the `warehouse` path says. Use `warehouse` only when the same process both
creates and reads the tables; use `catalog_properties` for anything durable.

## Session configuration, not `SET`

`SET spark.sql.…` is not a statement RePark accepts, so dbt's `server_side_parameters` has no
equivalent. Configuration goes on the builder through `session_properties`, before the session
exists. There is nothing to set afterwards.

## Threads

`threads` behaves as dbt expects. One `ReparkSession` is shared by the whole run — RePark's
`getOrCreate` answers one session per process — and each statement gets its own cursor on the
calling thread. Concurrent statements on that shared session are pinned. Concurrent writes to
*one* table are an Iceberg commit conflict, exactly as on Spark, and dbt does not schedule
them.

A second profile with a different catalog or warehouse in the same process is **refused**, not
silently attached to the live session.

## Transactions

There are none. Every Iceberg commit is its own snapshot, so `dbt run` is not atomic across
models. To undo a run, roll a table back:

```sql
CALL glue_catalog.system.rollback_to_snapshot(table => 'my_namespace.gold_fct', snapshot_id => …)
```

## See also

- [../../python/dbt-repark/map.md](../../python/dbt-repark/map.md) — the route, and why a Thrift
  endpoint was rejected on measurement.
- [../cutover/inventory.md](../cutover/inventory.md) — the cutover this adapter unblocks (step C6).
- [../spark-sql-iceberg-parity.md](../spark-sql-iceberg-parity.md) — every difference from Spark.
- [sql-doors.md](sql-doors.md) — the SQL surface underneath.
