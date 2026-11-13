# Card NATIVE-CTAS-ICEBERG-REFUSE-1 — `repark.sql()` CTAS into an Iceberg table stored zero rows and reported success

**Date:** 2026-11-12 · **Filed by:** Claude Opus 5.5 (`claude-opus-5-5`), the second builder ·
**Source:** owner ruling 2026-11-12, verbatim "Okay go with the default" (item 3, option (b)) ·
**Severity:** S1, silent data loss on a shipped door · **Status:** FIXED as a refusal (v1.5.7,
second-builder slot). The real native CTAS is deferred to the owner's routing decision (option
(c), the `repark-sql` router); this card does not build it.

## Why

`repark.sql()` is DataFusion's own SQL handling behind one RePark guard, not the `repark-sql`
router. The path, read in the code and measured:

- `repark.sql()` (`python/repark/src/repark/__init__.py:72-98`) calls
  `PyReparkSession.native().sql()` (`crates/repark-python/src/session.rs:150-188`). The native
  session sets no SQL dialect and turns on the local-file guard (`with_local_fs_guard`, line 161).
- `sql`, `sql_built` (line 201) and `session_sql_with_write_options`
  (`crates/repark-python/src/session_write_options.rs:16`) all reach
  `ReparkSession::sql_with_write_options_inner` (`crates/repark-core/src/session/write_options.rs:99-128`),
  which runs the session dialect.
- That dialect is `LocalFsGuardedDialect` over `DataFusionDialect`
  (`crates/repark-core/src/local_fs_policy.rs:103-113`, built at `crates/repark-core/src/session.rs:318`).
  `DataFusionDialect::execute` (`crates/repark-core/src/dialect.rs:145-157`) plans with
  `SessionState::create_logical_plan` and executes the plan.
- DataFusion plans CTAS as `DdlStatement::CreateMemoryTable` (datafusion-sql 54.1.0
  `statement.rs:577-610`). `SessionContext::create_memory_table` (datafusion 54.1.0
  `execution/context/mod.rs:869-928`) collects the query into a partitioned `MemTable` and calls
  `register_table` on the target schema. For `OR REPLACE` over an existing table it calls
  `deregister_table` first (line 892), then collects (line 896).
- On an Iceberg catalog that schema is the fork's `IcebergSchemaProvider`
  (`crates/integrations/datafusion/src/schema.rs`). Its `register_table` (line 271) is a create
  with no data: `ensure_table_is_empty` (line 437) refuses a table with rows, but it scans
  **partition 0 only** (line 445). A one-partition result is refused with `register_table does
  not support tables with data`. A result whose rows are not in partition 0 passes, and the
  table is created empty: `UNION`, `SELECT DISTINCT`, `INTERSECT`, `EXCEPT` and an aggregate.
  Its `deregister_table` (line 337) drops the Iceberg table.

So the record in [NATIVE-DOOR-REPLACE-KILL-1](native-door-replace-kill-1-card-2026-10-27.md)
("`repark.sql()` runs CTAS and RTAS through `crates/repark-sql/src/create_table.rs`") is wrong,
and DOOR-PROTECTION-INVENTORY-1's measurement (the router ships on no door) is right.

## Measured on main (`a3f8c7d9`, release build)

Step 0 is in [the evidence](../../wo/native-ctas-iceberg-refuse-1/evidence/map.md): 380 native
cells, 66 Spark-door cells and 66 Spark 4.1.2 cells. On both Iceberg catalogs of the native
session (the memory catalog and a `type=hadoop` catalog):

| CTAS query shape | Native door on main |
|---|---|
| `UNION`, `SELECT DISTINCT`, `INTERSECT`, `EXCEPT`, `GROUP BY` aggregate | **creates the table, stores zero rows, answers success** |
| a literal, a plain scan, `UNION ALL`, `VALUES`, a join | refuses: `Unexpected => register_table does not support tables with data.` (`PySparkException`), no table |

The same holds for every spelling: lowercase, mixed case, `IF NOT EXISTS`, `OR REPLACE`, a
column list, a mixed-case target and an unqualified target under
`SET datafusion.catalog.default_catalog`. `EXPLAIN` answers a plan; `EXPLAIN ANALYZE` and
`EXECUTE` of a `PREPARE` fail with `Unsupported logical plan: CreateMemoryTable`; `REPLACE TABLE`
fails to parse; `CREATE TEMPORARY TABLE` and a multi-statement string are refused by DataFusion.

**The existing-table case (stale-write verifier 2, F4):** `CREATE OR REPLACE TABLE t AS …` over
an existing Iceberg `t` drops `t` before the query runs. Whether the query then fails
(`10 / (id - id)`) or succeeds, the statement errors and `t` is gone: on the hadoop catalog its
metadata directory is empty; on the memory catalog it no longer reads. Spark keeps `t` on a
failed replace and replaces it on a good one.

Native `CREATE TABLE t (<columns>)` then `INSERT INTO t SELECT …` stores Spark 4.1.2's rows for
the ten step-0 shapes on both catalogs, and the Spark door's CTAS of the same statement stores
Spark's rows for them. Two measured exceptions (the verifiers, 2026-11-13, the same on main):
a native `INSERT` whose query unions an `INT` column with a bare integer literal (`SELECT id
FROM src UNION SELECT 9`) fails with `cannot convert text Int64 into byte Int32`, and
`CAST(9 AS INT)` fixes it (`SELECT 1 UNION SELECT 9`, literals only, stores its rows);
and a CTE-shaped CTAS fails on the Spark door with `table 'spark_catalog.default.<cte>' not
found` (the CTE-TEMP-VIEW-PRECEDENCE-1 family), where the native workaround stores its rows.

## The fix

`native_ctas::refuse_iceberg_ctas` (`crates/repark-core/src/local_fs_policy/native_ctas.rs`),
called from `refuse_native_statement` (`crates/repark-core/src/local_fs_policy.rs:89-93`), the
native door's guard. It reads the plan that guard already builds, so it adds no parse. It walks
the plan (so `EXPLAIN`, `EXPLAIN ANALYZE` and `PREPARE` are covered) for a `CreateMemoryTable`
that carries a query (its input is not the empty relation a plain `CREATE TABLE` plans),
resolves the target against the session's default catalog and schema, and refuses when the
target catalog is a registered Iceberg catalog and the target schema is not DataFusion's in-memory
`MemorySchemaProvider`. A native `CREATE SCHEMA <iceberg catalog>.<name>` registers such an
in-memory schema (datafusion 54.1.0 `execution/context/mod.rs:1023-1024`, stored as given by
`ReparkCatalogProvider::register_schema`, `crates/repark-iceberg/src/catalog/provider.rs:114`); its
tables never reach Iceberg, and CTAS there stores the right rows on main, so it stays answered. Every Iceberg catalog kind the native session
can register (memory, `type=hadoop`, Glue, S3 Tables) is entered by
`register_iceberg_catalog_with_policy` (`crates/repark-core/src/session.rs:458`), so one
registry lookup covers them. The target test is the one predicate `refused_schema`; refusing
every native CTAS is a one-line change to it, if the owner rules that.

The refusal is `UnsupportedOperationException`, raised at planning before any write. The
source tables are read to plan the query, but nothing is created, replaced or dropped:

> repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t:
> create the table with its columns, CREATE TABLE ice.ns.t (<columns>), then INSERT INTO ice.ns.t
> SELECT ...; or run the same statement through spark.sql (NATIVE-CTAS-ICEBERG-REFUSE-1)

The target is printed quoted where it needs quotes (`ice.ns."MixQ"`), so the workaround, followed
literally, reaches the same table (fixed 2026-11-13 after verifier 1's F1).

**F4 is closed on the native door** for Iceberg targets: the replace refuses before
`deregister_table`, and the existing table keeps its rows, snapshots and metadata file.

Unchanged, and pinned: native CTAS into the session's in-memory catalog (bare and
`datafusion.public.`), a plain `CREATE TABLE`, `CREATE VIEW`, `INSERT … SELECT`, every Spark-door
statement, and the statements DataFusion refuses first (`REPLACE TABLE`, `CREATE TEMPORARY
TABLE`, a multi-statement string). `CREATE EXTERNAL TABLE` into an Iceberg schema is not a CTAS
and still fails with the fork's `register_table` text, creating nothing.

## Pins

- `python/repark/tests/sql/test_native_ctas_iceberg_refuse_1.py`: every inventory row on both
  Iceberg catalogs refuses with the class and text and leaves no table; an existing table's rows,
  snapshots and metadata files are unchanged; the controls with their rows.
- `crates/repark-core/src/local_fs_policy/native_ctas/tests.rs`: the seat, through a real guarded
  session.

## Relation

- **NATIVE-CTAS-FAILURE-CLEANUP-1** is named in
  [CTAS-QUERY-FAILURE-CLEANUP-1's order](../../wo/ctas-query-failure-cleanup-1/order.md) (scope
  table and the cards list) but was never filed: no card, map row or registry row carries it.
  This card covers its native-door Iceberg half, because the native door no longer creates an
  Iceberg table from a query at all.
- [NATIVE-DOOR-REPLACE-KILL-1](native-door-replace-kill-1-card-2026-10-27.md): its premise does
  not hold (see Why). Native CTAS and RTAS into an Iceberg table now refuse, but a native
  `CREATE OR REPLACE TABLE t (<columns>)` with no query is not refused: it still drops and
  re-creates `t` (a fresh `v1` with a new table uuid and no snapshots; the old files are
  orphaned), the same on main (verifier 1, 2026-11-13). That replace is what remains to pin.
- The registry row is NATIVE-CTAS-ICEBERG-REFUSE-1 in
  [the parity doc](../../../docs/spark-sql-iceberg-parity.md).
- Found, not carded: a native `CREATE SCHEMA` under an Iceberg catalog answers success, ignores
  `WITH (location = …)` and makes an in-memory schema, so its tables are not durable and write no
  file (measured on main; `test_write_body_carriers_1.py` and the ICE-STRUCT-PAIRING-1 exception
  recorder seed their native tables this way).
- Not ruled: whether native CTAS into the in-memory catalog also refuses. Measured on main and
  pinned here, it stores the right rows for every shape.
