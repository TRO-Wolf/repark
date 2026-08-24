# Iceberg tables

Iceberg is not a bolt-on in repark: the engine owns an `iceberg-rust` fork
([ADR-0001](../adr/0001-own-iceberg-fork.md)) and the commit machinery — snapshots, MERGE, schema
evolution — sits underneath both SQL doors. This guide is about reaching it from Python.

**Which door.** Everything below runs through the **Spark door** — `ReparkSession` and
`spark.sql(...)`. The native `repark.sql` callable is a stock DataFusion session today with no
catalog configuration surface, so it is not the door for Iceberg work; see
[sql-doors.md](sql-doors.md) "Honest scope, today" and [STATUS.md](../../STATUS.md) for where that
stands.

## Nothing to configure, to start with

A bare session already has an Iceberg catalog: `spark_catalog`, in-memory, over a temporary
warehouse that is removed when the session stops.

```python
from repark import ReparkSession

spark = ReparkSession.builder.appName("iceberg").getOrCreate()
spark.range(3).write.saveAsTable("t")
spark.table("t").orderBy("id").show()
```

```text
+----+
| id |
+----+
| 0  |
| 1  |
| 2  |
+----+
```

Opt out with `repark.sql.autoMemoryCatalog=false` on the builder if you want an empty session.

## Catalogs

Four catalog kinds are recognized. Two are for AWS, one is for local work, one is not Iceberg at
all:

| `type` | What it is |
|---|---|
| `glue` | AWS Glue Data Catalog — the **primary** production catalog |
| `s3tables` | Amazon S3 Tables — the **secondary** one |
| `memory` | repark's own in-process catalog over a warehouse directory: local development and tests, no AWS |
| `postgres` / `postgresql` / `jdbc` | a **non-Iceberg** JDBC read catalog; the config parses, engine-core registration is not implemented |

Anything else — `rest`, `hadoop`, `hive`, a misspelling — refuses at `getOrCreate` naming the key,
the value it did not recognize, and the set it accepts:

```python
(
    ReparkSession.builder.appName("bad")
    .config("spark.sql.catalog.warehouse_only.type", "hive")
    .config("spark.sql.catalog.warehouse_only.warehouse", "/srv/warehouse")
    .getOrCreate()
)
```

```text
IllegalArgumentException: repark config error: spark.sql.catalog.warehouse_only.type /
repark.sql.catalog.warehouse_only.type has an unrecognized value 'hive' (expected 'glue',
's3tables', 'memory', 'postgres', or 'jdbc')
```

### The conf keys

Catalogs are declared on the builder under `spark.sql.catalog.<name>.*`. The synonym
`repark.sql.catalog.<name>.*` is the same keyspace — mix the two spellings freely, but giving the
same property *different* values under both fails loud naming both keys rather than picking one.

| Key | Meaning |
|---|---|
| `spark.sql.catalog.<name>.type` | `glue` / `s3tables` / `memory` / `postgres` |
| `spark.sql.catalog.<name>.catalog-impl` | the Java class-name spelling, if that is what your existing config carries; the suffix decides the kind |
| `spark.sql.catalog.<name>.warehouse` | warehouse location — **required** for `memory` and for `glue` |
| `spark.sql.catalog.<name>.table_bucket_arn` | S3 Tables' table-bucket ARN. If it is absent and `warehouse` holds an ARN, the ARN is carried across |
| `spark.sql.catalog.<name>.catalog_id` | Glue: the AWS account that owns the catalog (this is the key, not `glue.id`) |
| `spark.sql.catalog.<name>.uri` | Glue: an endpoint override |
| `spark.sql.catalog.<name>.io-impl` | accepted and **dropped** — the fork's `FileIO` is not pluggable by Java class name |
| anything else under `<name>.` | passed through verbatim to the storage layer (region, `s3.*` credentials, …). Secret-shaped keys are redacted in diagnostics |

A Glue catalog is therefore two required lines on the builder — a type and a warehouse — plus
whatever your storage layer needs, which passes through untouched:

```python
spark = (
    ReparkSession.builder.appName("etl")
    .config("spark.sql.catalog.prod.type", "glue")
    .config("spark.sql.catalog.prod.warehouse", "s3://example-warehouse/prod/")
    .getOrCreate()
)
```

An S3 Tables catalog is the same shape with `type` set to `s3tables` and `table_bucket_arn` set to
your table bucket's ARN.

### Warehouse locations

Four location forms are accepted: `s3://`, `s3a://`, `file://`, and a bare **absolute**
filesystem path. `s3://` and `s3a://` are both real — repark does not normalize one into the
other, so paths round-trip under whichever scheme you configured.

Anything else refuses loudly rather than being silently treated as a local path:

```python
spark.register_memory_catalog("bad", "gs://example-warehouse/wh")
```

```text
AnalysisException: Error during planning: unsupported storage scheme `gs://` in location
`gs://example-warehouse/wh`: RePark selects a `FileIO` backend by scheme and supports `s3://`,
`s3a://`, `file://`, or a bare absolute filesystem path
```

…including the single-slash typo, which gets its own diagnosis:

```python
spark.register_memory_catalog("bad2", "s3:/example-warehouse/wh")
```

```text
AnalysisException: Error during planning: malformed storage location `s3:/example-warehouse/wh`:
a `:` before the first `/` looks like a mistyped URI scheme — did you mean `scheme://…`? RePark
supports `s3://`, `s3a://`, `file://`, or a bare absolute filesystem path
```

A relative path, or the empty string, refuses the same way.

## A local catalog you can actually run

Everything in the rest of this guide was executed against a local catalog with no AWS anywhere.
`register_memory_catalog(name, warehouse)` is the Python door for it; the `spark.sql.catalog.*`
config block above does the same thing declaratively.

```python
spark.register_memory_catalog("local", "/tmp/repark-warehouse")
spark.sql("CREATE NAMESPACE local.sales")
spark.sql(
    "CREATE TABLE local.sales.orders USING iceberg "
    "PARTITIONED BY (region) "
    "TBLPROPERTIES ('format-version' = '2') AS "
    "SELECT * FROM (VALUES (1, 'eu', 10.0), (2, 'us', 20.0)) AS t(id, region, amount)"
)
spark.sql("SELECT * FROM local.sales.orders ORDER BY id").show()
```

A namespace created without `LOCATION` still writes under that warehouse
(`…/repark_ctas/local/sales/<table>`). Pass `LOCATION` when the table should own a
subdirectory of its own.

```text
+----+--------+--------+
| id | region | amount |
+----+--------+--------+
| 1  | eu     | 10.0   |
| 2  | us     | 20.0   |
+----+--------+--------+
```

## Reading

Two spellings, and the difference between them matters:

```python
spark.table("local.sales.orders")                              # temp views win on a bare name
spark.read.format("iceberg").load("local.sales.orders")        # catalog only, never a temp view
```

Both return the same rows here. `spark.table` prefers a temp view when the name is unqualified,
which is what PySpark does; `format("iceberg").load(...)` is catalog-only, so it cannot be shadowed
by a same-named view — reach for it when a script creates views and reads tables in the same
namespace.

## Writing

SQL DML works as you would expect, and `MERGE INTO` is repark's own executor rather than a
rewrite into something weaker:

```python
spark.sql("INSERT INTO local.sales.orders SELECT 3 AS id, 'eu' AS region, 30.0 AS amount")
spark.sql("UPDATE local.sales.orders SET amount = 11.0 WHERE id = 1")
spark.sql("DELETE FROM local.sales.orders WHERE id = 2")
spark.sql(
    "MERGE INTO local.sales.orders AS tgt "
    "USING (SELECT * FROM (VALUES (3, 'eu', 33.0), (4, 'us', 44.0)) AS s(id, region, amount)) AS src "
    "ON tgt.id = src.id "
    "WHEN MATCHED THEN UPDATE SET tgt.amount = src.amount "
    "WHEN NOT MATCHED THEN INSERT *"
)
spark.sql("SELECT * FROM local.sales.orders ORDER BY id").show()
```

```text
+----+--------+--------+
| id | region | amount |
+----+--------+--------+
| 1  | eu     | 11.0   |
| 3  | eu     | 33.0   |
| 4  | us     | 44.0   |
+----+--------+--------+
```

The DataFrame writers are there too. `df.write.format("iceberg").mode(...).saveAsTable(...)` takes
the familiar modes (`append` / `overwrite` / `error` / `errorifexists` / `ignore`), and the v2
`writeTo` builder carries `create()`, `createOrReplace()`, `replace()` and `append()`:

```python
new = spark.createDataFrame([(5, "us", 55.0)], ["id", "region", "amount"])
new.writeTo("local.sales.orders").append()
spark.sql("SELECT count(*) AS n FROM local.sales.orders").show()
```

```text
+---+
| n |
+---+
| 4 |
+---+
```

Writes into an existing table resolve columns **by name**, not by position.

### The write forms that refuse

Three, and each refuses because the alternative would be silent data loss:

- **`writeTo(...).overwritePartitions()`** — Spark's dynamic partition overwrite:

  ```text
  UnsupportedOperationException: overwritePartitions: Spark's dynamic partition overwrite
  (partition-scoped replace) is not supported by the repark engine yet — a static INSERT OVERWRITE
  would silently replace ALL rows, not just the source's partitions. Use createOrReplace() for a
  deliberate full rebuild, or append(). (Engine path: iceberg-rust fork ReplacePartitions, not yet
  wired.)
  ```

  `writeTo(...).overwrite(condition)` refuses for the same reason.

- **`INSERT OVERWRITE … PARTITION (…)`** — registry row
  [DML-1](../spark-sql-iceberg-parity.md#dml-1--insert-overwrite--partition-):

  ```text
  UnsupportedOperationException: This feature is not implemented: INSERT OVERWRITE … PARTITION (…)
  is not supported yet (static and dynamic partition overwrite). Empty sources must not full-table
  wipe sibling partitions; non-empty sources must not silently whole-table replace. Use static
  whole-table INSERT OVERWRITE, or DELETE with a partition predicate + INSERT INTO …
  ```

- **`TRUNCATE TABLE`** — registry row
  [DML-2](../spark-sql-iceberg-parity.md#dml-2--truncate-table), because Iceberg has no truncate
  primitive and the two things it could mean commit differently:

  ```text
  UnsupportedOperationException: This feature is not implemented: TRUNCATE TABLE is not supported
  yet — use INSERT OVERWRITE … SELECT … WHERE false (empty overwrite wipe) or DELETE FROM <table>
  without a predicate …
  ```

Whole-table `INSERT OVERWRITE` (no `PARTITION` clause) works.

## Time travel

Both the bare Spark spelling and the `FOR SYSTEM_*` spelling parse, over a snapshot id or a
branch / tag name:

```python
snaps = spark.sql(
    "SELECT snapshot_id FROM local.sales.orders.snapshots ORDER BY committed_at"
).collect()
first = snaps[0]["snapshot_id"]

spark.sql(f"SELECT * FROM local.sales.orders VERSION AS OF {first} ORDER BY id").show()
spark.sql(f"SELECT * FROM local.sales.orders FOR SYSTEM_VERSION AS OF {first} ORDER BY id").show()
```

```text
+----+--------+--------+
| id | region | amount |
+----+--------+--------+
| 1  | eu     | 10.0   |
| 2  | us     | 20.0   |
+----+--------+--------+
```

(Both statements print that; snapshot ids are per-commit, so yours will differ.)
`TIMESTAMP AS OF '<ts>'` / `FOR SYSTEM_TIME AS OF <ts>` are the time-keyed equivalents. Time travel
composes with `WHERE`, projections, CTEs, subqueries and joins, and a travelled relation can be the
source of a CTAS, a `MERGE … USING`, or an `INSERT … SELECT`.

The reader has the option form, honoured only on `format("iceberg")`:

```python
spark.read.format("iceberg").option("snapshot-id", str(first)).load(
    "local.sales.orders"
).orderBy("id").show()
```

Four options exist — `snapshot-id`, `as-of-timestamp`, `branch`, `tag` — and they are **mutually
exclusive**:

```text
AnalysisException: Iceberg time-travel reader options are mutually exclusive; got as-of-timestamp
and snapshot-id
```

`start-snapshot-id` / `end-snapshot-id` (incremental read) are not implemented. On the **write**
side there is no branch or tag targeting: writes go to the current snapshot, and
`writeTo(...).option("branch", ...)` refuses — registry row
[REF-1](../spark-sql-iceberg-parity.md#ref-1--writing-to-a-branch-or-tag) explains the supported
path (read a ref, then re-pin it with `CREATE OR REPLACE BRANCH`).

## Metadata tables

Address them as `catalog.namespace.table.<suffix>`. Sixteen exist at pin `5e7b2e4`:

`snapshots` · `manifests` · `all_manifests` · `files` · `data_files` · `delete_files` ·
`all_files` · `all_data_files` · `all_delete_files` · `entries` · `all_entries` · `history` ·
`refs` · `metadata_log_entries` · `partitions` · `position_deletes` (schema only; scan
refuses until the fork ports `PositionDeletesBatchScan`)

```python
spark.sql(
    "SELECT snapshot_id, operation FROM local.sales.orders.snapshots ORDER BY committed_at"
).show()
```

```text
+---------------------+-----------+
| snapshot_id         | operation |
+---------------------+-----------+
| 5257164831836018923 | append    |
| 8267454960064748677 | append    |
| 5851461005167077229 | overwrite |
| 1955206084829184770 | delete    |
| 8414361259379819430 | overwrite |
| 6325637909470701385 | append    |
+---------------------+-----------+
```

Three things to know:

- **They are read-only.** Every write form — `INSERT` / `UPDATE` / `DELETE` / `MERGE` / CTAS /
  `TRUNCATE` / `CREATE VIEW` / `DROP` / `ALTER` — gets one diagnostic, registry row
  [MT-2](../spark-sql-iceberg-parity.md#mt-2--the-read-only-diagnostic-on-a-write-to-a-metadata-table):

  ```text
  AnalysisException: Error during planning: Iceberg metadata table `local.sales.orders.snapshots`
  is read-only — INSERT/UPDATE/DELETE/MERGE/CTAS/TRUNCATE/CREATE VIEW/DROP/ALTER targeting a
  metadata table is not supported
  ```

- **A real table wins.** If a table genuinely occupies the full path, it is read as a table, not
  reinterpreted as a metadata suffix.
- **Time travel does not compose with them** — registry row
  [MT-1](../spark-sql-iceberg-parity.md#mt-1--time-travel-composed-with-a-metadata-table). The base
  table `AS OF` a snapshot works; so does the metadata table on its own; the combination is out of
  scope and refuses.

They are also hidden from `SHOW TABLES` and `information_schema` while staying addressable by name
— a deliberate convergence with Spark and Trino, decided in
[ADR-0006](../adr/0006-hide-iceberg-metadata-tables-from-enumeration.md).

## Maintenance

Each procedure is documented on its own below. [The maintenance
runbook](#the-maintenance-runbook) is the order to run them in, and how often.

Seven procedures run through `CALL`. Six of them are maintenance and return Spark's full column
list; the seventh is adoption:

```python
spark.sql(
    "CALL local.system.register_table("
    "table => 'sales.sparkv3', "
    "metadata_file => '/tmp/repark-v3-1-spark-mor/metadata/v8.metadata.json')"
).show()
```

```text
+---------------------+--------------------+-----------------------+
| current_snapshot_id | total_records_count| total_data_files_count|
+---------------------+--------------------+-----------------------+
| 4803484336433650168 | 40                 | 4                     |
+---------------------+--------------------+-----------------------+
```

Those three numbers are the Spark-written format-v3 fixture this engine ships for CI
(`crates/repark-spark/src/tests/fixtures/v3-spark-mor/`). Live `SELECT` after the three
Puffin vectors apply is 37 rows.

That is how a table another engine already wrote — including a format-v3 table with Puffin
deletion vectors — becomes visible here. The engine still cannot *create* a v3 table. Hadoop
catalog pointers named `vN.metadata.json` register and read; a later write names that
convention rather than only the filename (registry `V3-ADOPT-1`). S3 Tables refuses
registration in the fork; Glue implements it.

Six maintenance procedures return Spark's full column list:

```python
spark.sql("CALL local.system.rewrite_data_files(table => 'sales.orders')").show()
```

```text
+----------------------------+------------------------+-----------------------+-------------------------+----------------------------+
| rewritten_data_files_count | added_data_files_count | rewritten_bytes_count | failed_data_files_count | removed_delete_files_count |
+----------------------------+------------------------+-----------------------+-------------------------+----------------------------+
| 0                          | 0                      | 0                     | 0                       | 0                          |
+----------------------------+------------------------+-----------------------+-------------------------+----------------------------+
```

On a **format-v3** table this one refuses rather than running:

```text
CALL rewrite_data_files will not compact `sales.orders`: it is a V3 table, and V3 mandates row
lineage (`_row_id`, `_last_updated_sequence_number`) which this engine's rewrite does not carry
through...
```

RePark creates tables at format v2, so this only reaches a v3 table that was already in your
catalog. The rewrite would return the right rows and give every one of them a new `_row_id`,
which tells anything reading the table incrementally that all of them changed. Spark carries
lineage through the same compaction unchanged, so compact v3 tables there for now. Registry row
`V3-LINEAGE-1`.

### Compacting position deletes

On a merge-on-read table every `MERGE`, `UPDATE` and `DELETE` leaves a position-delete file
behind, and scans get slower as they pile up. Ten sequential MERGEs of the same 200 ids into
a 1,000-row table leave ten delete files; this CALL folds them (measured, STATUS MW
scorecard). `rewrite_position_delete_files` merges them:

```python
spark.sql(
    "CALL local.system.rewrite_position_delete_files(table => 'sales.orders')"
).show()
```

```text
+------------------------------+--------------------------+-----------------------+-------------------+
| rewritten_delete_files_count | added_delete_files_count | rewritten_bytes_count | added_bytes_count |
+------------------------------+--------------------------+-----------------------+-------------------+
| 8                            | 1                        | 12104                 | 2110              |
+------------------------------+--------------------------+-----------------------+-------------------+
```

The row set is unchanged — compaction rewrites which files mask the deleted rows, never which
rows are masked. When there is nothing to compact you get four zeros rather than an error.

On a **format-v3** table this refuses instead of running. Those tables carry Puffin deletion
vectors rather than Parquet position deletes, and a deletion vector is file-scoped, so there is
nothing to bin-pack. The refusal names how many it found; it does not return zeros and leave you
thinking the table was already clean. repark writes no v3 delete files itself — it creates tables
at format v2 and refuses merge-on-read writes on v3 — so this only comes up on a table another
engine wrote.

Two differences from Spark are worth knowing before you port a maintenance job, and neither
changes what a query returns:

- repark and Spark both wait for 5 delete files in a group before compacting
  (RP-1 / fork F-1 retired [MOR-1](../spark-sql-iceberg-parity.md#mor-1--rewrite_position_delete_files-compacts-below-sparks-min-input-files-floor)).
- repark writes one delete file per partition where Spark's default writes one per data file
  ([MOR-2](../spark-sql-iceberg-parity.md#mor-2--merge-on-read-delete-files-are-partition-granularity-where-sparks-default-is-per-file)).

### Compacting manifests

Every commit writes a manifest, so a table that is appended to or merged into often ends up with
hundreds of small ones, and every scan reads the whole manifest list first. `rewrite_manifests`
re-groups the entries into fewer manifests without touching a single data file:

```python
spark.sql("CALL local.system.rewrite_manifests(table => 'sales.orders')").show()
```

```text
+---------------------------+-----------------------+
| rewritten_manifests_count | added_manifests_count |
+---------------------------+-----------------------+
| 5                         | 1                     |
+---------------------------+-----------------------+
```

The live file set is identical before and after — a re-grouped entry keeps its original snapshot
id and sequence numbers, which is what keeps merge-on-read deletes and incremental scans correct.
When there is nothing to re-group you get two zeros and no new snapshot, exactly as Spark does.

Four things to know before you port a maintenance job:

- **Only the current partition spec is rewritten.** Manifests written under an older spec are kept
  as they are. That is Spark's default too.
- **`spec_id` refuses**, because this engine always rewrites the current spec and will not accept
  an argument it would ignore. `use_caching` is accepted and does nothing — it tunes Spark's own
  DataFrame cache — but it takes a boolean literal here, where Spark also accepts a quoted
  `'true'`
  ([MANIFEST-2](../spark-sql-iceberg-parity.md#manifest-2--rewrite_manifests-refuses-spec_id-use_caching-is-accepted-and-does-nothing)).
- **Delete manifests are not rewritten.** Spark compacts them in a second leg of the same
  procedure; this engine reports the data leg only and leaves them in place
  ([MANIFEST-1](../spark-sql-iceberg-parity.md#manifest-1--rewrite_manifests-rewrites-data-manifests-only-spark-rewrites-delete-manifests-too)).
  If that leaves nothing for the data leg to do, the call refuses rather than returning two zeros
  that read as "already clean". Compacting the delete FILES first with
  `rewrite_position_delete_files` is what reduces them.
- **On a table whose manifests are individually larger than
  `commit.manifest.target-size-bytes`, `added_manifests_count` will not match Spark's**, because
  the two engines split the entries over a different number of manifests
  ([MANIFEST-3](../spark-sql-iceberg-parity.md#manifest-3--above-the-manifest-target-size-rewrite_manifests-writes-a-different-number-of-manifests)).
  `rewritten_manifests_count` matches, and so does the row set. At the 8 MB default both engines
  write one manifest and the counts are identical.

`expire_snapshots` and `rollback_to_snapshot` are the other two, and `expire_snapshots` returns
Spark's full six-column result:

```python
spark.sql(
    "CALL local.system.expire_snapshots(table => 'sales.orders', retain_last => 5)"
).show()
```

```text
+--------------------------+-------------------------------------+--------------------------------------+
| deleted_data_files_count | deleted_position_delete_files_count | deleted_equality_delete_files_count  |
+--------------------------+-------------------------------------+--------------------------------------+
| 4                        | 2                                   | 0                                    |
+--------------------------+-------------------------------------+--------------------------------------+
```

(plus `deleted_manifest_files_count`, `deleted_manifest_lists_count` and
`deleted_statistics_files_count`.) On a merge-on-read table the three-way split matters: the
position-delete files are counted separately rather than folded into the data-file total.

### Removing orphan files

Files that no snapshot references — an aborted write, a crashed job — are invisible to Iceberg and
cost storage forever. `remove_orphan_files` finds them.

**It is the only procedure here that destroys data, so its defaults are not Spark's.**

```python
spark.sql(
    "CALL local.system.remove_orphan_files("
    "table => 'sales.orders', older_than => TIMESTAMP '2026-08-18 00:00:00')"
).show()
```

```text
+-----------------------------------------------+
| orphan_file_location                          |
+-----------------------------------------------+
| s3://bucket/sales/orders/data/00003-abc.parquet|
+-----------------------------------------------+
```

That call **listed** those files. It did not delete them. Two differences from Spark, both
deliberate, both registry rows:

- **`older_than` is required**
  ([ORPHAN-1](../spark-sql-iceberg-parity.md#orphan-1--remove_orphan_files-requires-older_than-spark-defaults-it)).
  Spark defaults it to `now - 3 days`. Deleted files do not come back, so the cutoff is not
  something to leave to a default.
- **`dry_run` defaults to true**
  ([ORPHAN-2](../spark-sql-iceberg-parity.md#orphan-2--remove_orphan_files-defaults-to-a-dry-run-spark-defaults-to-deleting)).
  Spark's default deletes. Read the listing first, then arm it:

```python
spark.sql(
    "CALL local.system.remove_orphan_files("
    "table => 'sales.orders', older_than => TIMESTAMP '2026-08-18 00:00:00', "
    "dry_run => false)"
).show()
```

`dry_run` takes a boolean literal. A quoted `'false'` refuses rather than being read as false, so
a typo cannot arm the deletion.

One rule is **not** a repark invention: an `older_than` less than 24 hours in the past refuses,
because a short interval can delete files an in-flight commit has written but not yet referenced.
Apache Spark enforces the same floor for the same reason.

If some files cannot be deleted, the call fails and says how many — it never reports a partial
delete as a success.

### Maintenance on Glue and S3 Tables

These procedures run against every catalog, including Glue and S3 Tables.

**On S3 Tables, expect an occasional commit conflict and retry it.** The service runs its own
compaction and snapshot expiry, which commits alongside yours. When the service rewrites a file
that one of your in-flight position deletes refers to, Iceberg's validation catches it and the
commit fails. That is the concurrency control working. Your table is not damaged, and re-running
the procedure is the correct response.

Anything else refuses and lists what is supported, rather than pretending:

```text
UnsupportedOperationException: This feature is not implemented: CALL
system.migrate is not supported. Supported procedures: expire_snapshots, register_table,
rewrite_data_files, rewrite_manifests, remove_orphan_files, rewrite_position_delete_files,
rollback_to_snapshot.
```

### The maintenance runbook

Table management is one scheduled cycle, not six separate procedures. Run the whole cycle behind
your merge workload, in this order:

1. Your merge workload runs: `MERGE`, `UPDATE`, `DELETE`.
2. `rewrite_position_delete_files` folds the position deletes to one file per partition.
3. `rewrite_data_files` compacts the data files those merges fanned out.
4. `rewrite_manifests` re-groups the manifests the first two steps churned.
5. `expire_snapshots` drops the snapshots that now hold only replaced files.
6. `remove_orphan_files` lists what no snapshot references. This is the dry-run default.
7. `remove_orphan_files` with `dry_run => false` deletes that listing, once you have read it.

Each step is one task in an Airflow DAG. The block below is **steps 2 to 6**. Step 1 is your
merge workload. Step 7 is the same orphan call with `dry_run => false` added.

```python
from datetime import UTC, datetime, timedelta

CATALOG = "local"
TABLE = "sales.orders"
now = datetime.now(UTC)
# `expire_snapshots` deletes every snapshot older than this cutoff. The cutoff IS your
# time-travel window. Set it to the oldest instant you must still be able to read.
EXPIRE_CUTOFF = (now - timedelta(days=7)).strftime("TIMESTAMP '%Y-%m-%d %H:%M:%S'")
# `remove_orphan_files` refuses a cutoff under 24 hours old. Three days clears that floor.
ORPHAN_CUTOFF = (now - timedelta(days=3)).strftime("TIMESTAMP '%Y-%m-%d %H:%M:%S'")

MAINTENANCE_CYCLE = [
    f"CALL {CATALOG}.system.rewrite_position_delete_files(table => '{TABLE}')",
    f"CALL {CATALOG}.system.rewrite_data_files(table => '{TABLE}')",
    f"CALL {CATALOG}.system.rewrite_manifests(table => '{TABLE}')",
    f"CALL {CATALOG}.system.expire_snapshots(table => '{TABLE}', "
    f"older_than => {EXPIRE_CUTOFF}, retain_last => 1)",
    f"CALL {CATALOG}.system.remove_orphan_files(table => '{TABLE}', "
    f"older_than => {ORPHAN_CUTOFF})",
]

for statement in MAINTENANCE_CYCLE:
    spark.sql(statement).show()
```

**Always pass `older_than` to `expire_snapshots`.** Without it the engine falls back to the
table's `history.expire.max-snapshot-age-ms`. That property defaults to **5 days**. It is a
time-travel default, not a maintenance one. The cycle then keeps five days of every file it
replaced. On a table younger than five days it reclaims nothing at all. Measured on a 6,000-row
merge-on-read table: three cycles with no cutoff each answered six zeros. The warehouse grew
**544 kB → 3,266 kB (6.0×)**
([MW-8 §3](../../task/ledgers/completed/mw-8-maintenance-runbook-ledger.md#3-measurements-the-expire-cutoff-and-the-idle-cycle-2026-08-24)).
The runbook itself then produces the growth the `expire_snapshots` rule below warns about.

**The cutoff is your time-travel window, so pick it before you schedule the cycle.** A cycle
reclaims only what is older than the cutoff. `retain_last => 1` is a floor, not a cap. It keeps
the current snapshot and nothing else, so it never softens the cutoff. The block computes `now`
once, at the top. So a cycle reclaims what earlier cycles left, never what it just wrote.
Measured through this block on the same table. A seven-day window reclaimed nothing. It left
[time travel](#time-travel) intact. A zero window took the table from 12 snapshots to 8. Time
travel to the snapshot the CTAS wrote then failed with `unknown Iceberg snapshot id`. A cutoff
one day in the future reclaimed 48 data files, 12 delete files, 39 manifests and 11 manifest
lists. It left one snapshot
([MW-8 §3](../../task/ledgers/completed/mw-8-maintenance-runbook-ledger.md#3-measurements-the-expire-cutoff-and-the-idle-cycle-2026-08-24)).

**The order is load-bearing.** Fold the delete files before you compact the data. At 50 merges
of debt, step 2 read 400 delete files and left 8, so step 3 read 8. Reverse the two and the
expensive step reads 50 times the delete files
([MW-7 §6.2](../../task/ledgers/completed/mw-7-scale-measurement-ledger.md#6-what-the-numbers-set-as-mw-8s-runbook-defaults)).

**Run the cycle every 10 merges. Treat 20 merges as the ceiling.** Scans cross 2× the compacted
control at 19.6 merges, and merge 20 already measures 2.05×. At 10 merges every probe still sits
at or below the control. The ceiling tolerates about 2× degradation. It does not hold you under
it
([MW-7 §6.1](../../task/ledgers/completed/mw-7-scale-measurement-ledger.md#6-what-the-numbers-set-as-mw-8s-runbook-defaults)).

**Trigger on the delete-file count where your platform reports it.** A scan opens delete files,
so the file count is the closer proxy for what the debt costs. The same 2× crossing sits at
about 157 delete files. This engine writes one delete file per partition per commit
([MOR-2](../spark-sql-iceberg-parity.md#mor-2--merge-on-read-delete-files-are-partition-granularity-where-sparks-default-is-per-file)).
The measured table had 8 partitions. A merge count stops meaning anything once the merge size
changes. The file count keeps its meaning.

**Never skip `expire_snapshots`.** Until it runs, every file the rewrites replaced stays
reachable from the snapshot that wrote it. One measured copy-on-write warehouse held 14,782 MB
for a 342 MB table. That is **43×**. The measured cycle took it back to 342 MB. It used a cutoff
one day in the future with `retain_last => 1`. That expires every snapshot but the head, and it
takes all time travel with it
([MW-7 §6.3](../../task/ledgers/completed/mw-7-scale-measurement-ledger.md#6-what-the-numbers-set-as-mw-8s-runbook-defaults),
[§4.4](../../task/ledgers/completed/mw-7-scale-measurement-ledger.md#44-the-maintenance-sequence-at-50-merges-of-debt)).
A production window is a trade against that. A window of zero is not a trade.

**`rewrite_manifests` and the orphan dry run are cheap, so run both every cycle.** They cost
0.4 s and 0.1 s at 50 merges of debt. `rewrite_manifests` cut the manifest list from 25,665 to
3,659 bytes, and every reader opens that file first
([MW-7 §6.4](../../task/ledgers/completed/mw-7-scale-measurement-ledger.md#6-what-the-numbers-set-as-mw-8s-runbook-defaults)).

**The orphan step is a net that lags by a day.** `older_than` must be at least 24 hours in the
past, which is Apache Spark's floor too. So a cycle never sees the orphans that the same cycle's
`expire_snapshots` just created. Step 6 catches yesterday's cycle, not today's. A dry run that
lists nothing on a young warehouse is not a clean bill of health
([ORPHAN-1](../spark-sql-iceberg-parity.md#orphan-1--remove_orphan_files-requires-older_than-spark-defaults-it)).

**Budget the cycle at about 2.5 minutes** for a 10-million-row merge-on-read table carrying 50
merges of debt
([MW-7 §6.5](../../task/ledgers/completed/mw-7-scale-measurement-ledger.md#6-what-the-numbers-set-as-mw-8s-runbook-defaults)).

#### What the cycle cannot reclaim

**A cycle does not return the table to baseline.** `rewrite_data_files` never selects a data file
that is correctly sized, however many of its rows are deleted. Those dead rows stay, and the
position-delete file covering them stays too. Apache Spark reclaims both. Registry row
[RDF-1](../spark-sql-iceberg-parity.md#rdf-1--rewrite_data_files-never-selects-a-delete-laden-file-so-its-dead-rows-are-retained-forever).
The fix is fork ask F-16.

Here is what you see after a cycle, so you do not go looking for a fault. A merge-on-read table
still reads at **2.02×** the compacted copy-on-write control on a point predicate. On a partition
predicate it reads at **2.45×**. It still holds **1.90×** the control's live bytes
([MW-7 §4.3](../../task/ledgers/completed/mw-7-scale-measurement-ledger.md#43-mor-against-the-cow-control--what-merge-on-read-costs-on-read),
[§4.4](../../task/ledgers/completed/mw-7-scale-measurement-ledger.md#44-the-maintenance-sequence-at-50-merges-of-debt)).
Delete files survive with their records. Every answer is correct at every point, and nothing
fails. Cadence bounds how far a scan degrades between cycles. It does not bound the dead rows,
which grow until the fork carries Java's delete-ratio clause.

#### Retrying a step

Retry the step that failed. Do not restart the cycle. Two failures here are expected rather than
exceptional.

**On S3 Tables, a step can fail on a commit conflict.** "Maintenance on Glue and S3 Tables" above
says why that is the concurrency control working rather than damage. Retry that step.

**Step 4 refuses on an idle cycle.** When steps 2 and 3 rewrote nothing, `rewrite_manifests` has
nothing to do on the data manifests. It then raises `UnsupportedOperationException` rather than
answering two zeros while delete manifests stay uncompacted
([MANIFEST-1](../spark-sql-iceberg-parity.md#manifest-1--rewrite_manifests-rewrites-data-manifests-only-spark-rewrites-delete-manifests-too)).
Apache Spark answers `0, 0` there. The refusal points at step 2. Step 2 cannot help once the
delete files are the permanent `RDF-1` residue, because those delete manifests never go away.
Catch the exception, or guard step 4 on steps 2 and 3 having rewritten something. You reach this
by running the cycle twice with no merges in between. A retry after a successful cycle does
exactly that.

#### Porting a Spark maintenance DAG

Six edits, and none of them changes what a query returns:

- `expire_snapshots` here has no default cutoff that suits a maintenance cycle. Pass `older_than`
  explicitly, as the block above does.
- `remove_orphan_files` needs an explicit `older_than` here. Spark defaults it
  ([ORPHAN-1](../spark-sql-iceberg-parity.md#orphan-1--remove_orphan_files-requires-older_than-spark-defaults-it)).
- `remove_orphan_files` is a dry run here by default. Spark deletes. Step 7 needs
  `dry_run => false`, as a boolean literal
  ([ORPHAN-2](../spark-sql-iceberg-parity.md#orphan-2--remove_orphan_files-defaults-to-a-dry-run-spark-defaults-to-deleting)).
- `rewrite_manifests` refuses `spec_id`, and takes a boolean literal for `use_caching` where
  Spark also casts a quoted `'true'`
  ([MANIFEST-2](../spark-sql-iceberg-parity.md#manifest-2--rewrite_manifests-refuses-spec_id-use_caching-is-accepted-and-does-nothing)).
- `rewrite_manifests` reports the data leg only, and **raises** where Spark answers `0, 0`, when
  the data leg is idle and delete manifests remain
  ([MANIFEST-1](../spark-sql-iceberg-parity.md#manifest-1--rewrite_manifests-rewrites-data-manifests-only-spark-rewrites-delete-manifests-too)).
  A ported DAG needs a guard or a caught exception on that task.
- `added_manifests_count` diverges from Spark's above `commit.manifest.target-size-bytes`
  ([MANIFEST-3](../spark-sql-iceberg-parity.md#manifest-3--above-the-manifest-target-size-rewrite_manifests-writes-a-different-number-of-manifests)).
  The rewritten count matches, and so does the row set.
## Listing what is there

`SHOW NAMESPACES` needs an explicit catalog — repark has no "current catalog" concept, so there is
nothing for a bare form to resolve against (registry row
[NS-1](../spark-sql-iceberg-parity.md#ns-1--show-namespaces-without-in--from-requires-an-explicit-catalog)):

```python
spark.sql("SHOW NAMESPACES IN local").show()
```

```text
+-----------+
| namespace |
+-----------+
| sales     |
+-----------+
```

```python
spark.sql("SHOW NAMESPACES")
```

```text
AnalysisException: Error during planning: SHOW NAMESPACES requires an explicit catalog —
`SHOW NAMESPACES IN <catalog>` (RePark has no current-catalog concept, so there is no default to
resolve against)
```

`SHOW TABLES IN …` is not implemented (registry row
[ST-1](../spark-sql-iceberg-parity.md#st-1--show-tables-in--is-unimplemented)); the facade catalog
API is the working route:

```python
print([t.name for t in spark.catalog.listTables("local.sales")])
```

```text
['orders']
```

Nested `SHOW NAMESPACES IN catalog.namespace` also refuses — registry row
[NS-2](../spark-sql-iceberg-parity.md#ns-2--nested-show-namespaces-in-catalognamespace-is-refused).

## The registry sections that govern Iceberg

Everything above that refuses has a row. These are the sections to read before promising a
migration:

| Section | Covers |
|---|---|
| [§2.1 Iceberg metadata tables](../spark-sql-iceberg-parity.md#21-iceberg-metadata-tables) | MT-1, MT-2, F-V4-1 |
| [§2.2 Snapshot-ref DDL (`BRANCH` / `TAG`)](../spark-sql-iceberg-parity.md#22-snapshot-ref-ddl-branch--tag) | REF-1, REF-2 |
| [§2.3 DML statement forms](../spark-sql-iceberg-parity.md#23-dml-statement-forms) | DML-1 … DML-5 (including [DML-5](../spark-sql-iceberg-parity.md#dml-5--serializable-merge-conflict-detection-breadth), the over-broad serializable MERGE conflict check and its `write.merge.isolation-level` relief valve) |
| [§2.4 Namespace and table listing statements](../spark-sql-iceberg-parity.md#24-namespace-and-table-listing-statements) | NS-1, NS-2, ST-1 |

The registry is authoritative. Where this guide and a row disagree, the row wins.

## See also

- [sql-doors.md](sql-doors.md) — which `sql()` reaches a catalog, and why there are two.
- [session-and-conf.md](session-and-conf.md) — how builder config differs from a live `conf.set`.
- [ADR-0001](../adr/0001-own-iceberg-fork.md) — why repark owns an `iceberg-rust` fork.
- [ADR-0006](../adr/0006-hide-iceberg-metadata-tables-from-enumeration.md) — why `$`-suffixed
  metadata tables are hidden from enumeration.
- [../design/sql-doors.md](../design/sql-doors.md) — the settled design of the native door's
  Iceberg DDL vocabulary, for what is coming rather than what is here.
- [../spark-sql-iceberg-parity.md](../spark-sql-iceberg-parity.md) — the divergence registry.
