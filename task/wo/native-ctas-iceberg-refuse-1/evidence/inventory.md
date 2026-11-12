# NATIVE-CTAS-ICEBERG-REFUSE-1 — every way a CTAS reaches the native door's create

Read on main `a3f8c7d9`; each row names its measured cell in [step0/table.md](step0/table.md)
(main) and [after/table.md](after/table.md) (head). DataFusion lines are datafusion 54.1.0 and
datafusion-sql 54.1.0; fork lines are iceberg-rust `66901f02`.

## Entry points

All three native SQL entries reach one seat, `refuse_native_statement`
(`crates/repark-core/src/local_fs_policy.rs:89-93`), through
`ReparkSession::sql_with_write_options_inner` (`crates/repark-core/src/session/write_options.rs:99-128`)
and `LocalFsGuardedDialect::execute` / `execute_with_write_options` (`local_fs_policy.rs:52-67`).

| Entry | Code | Cell |
|---|---|---|
| `repark.sql(query)` | `python/repark/src/repark/__init__.py:72-98` → `PyReparkSession.sql` | every native cell |
| `PyReparkSession.native().sql(query)` | `crates/repark-python/src/session.rs:150-188` (`native` sets `with_local_fs_guard`, line 161, and no SQL dialect, so `session_dialect` wraps `DataFusionDialect`, `local_fs_policy.rs:103-113`, built at `session.rs:318`) | every native cell (the facade calls it) |
| `PyReparkSession.native().sql_built(query)` | `crates/repark-python/src/session.rs:201` → `ReparkSession::sql_built` (`write_options.rs:66-77`) | same seat; not separately measured |
| `session_sql_with_write_options(native, …)` | `crates/repark-python/src/session_write_options.rs:16` → `sql_with_write_options` (`write_options.rs:46-63`) | same seat; not separately measured |

No other native entry plans SQL text: the only `create_logical_plan` call outside tests is
`PreExecute::plan` (`crates/repark-core/src/pre_execute.rs:35-37`), which both the guard and
`DataFusionDialect::execute` (`crates/repark-core/src/dialect.rs:145-157`) use.

## Statement forms

| Form | How DataFusion plans it | Main (both Iceberg catalogs) | Head |
|---|---|---|---|
| `CREATE TABLE t AS SELECT …` | `CreateMemoryTable` (`statement.rs:577-610`) | zero rows + success for `UNION`, `DISTINCT`, `INTERSECT`, `EXCEPT`, aggregate; `register_table does not support tables with data` for the rest | guard refusal |
| `CREATE TABLE IF NOT EXISTS … AS` | same, `if_not_exists` | as above; over an existing table: no-op success | guard refusal |
| `CREATE OR REPLACE TABLE … AS` | same, `or_replace`; `create_memory_table` deregisters (drops) an existing table first (`context/mod.rs:890-905`) | as above; over an existing table the table is dropped and the statement errors (F4) | guard refusal; the existing table is untouched |
| `REPLACE TABLE … AS` | the Generic parser rejects it | `ParseException`, nothing created | unchanged |
| `CREATE TABLE t (cols) AS …` | `CreateMemoryTable` over a cast projection | as plain CTAS | guard refusal |
| `CREATE TABLE … AS VALUES …` | `CreateMemoryTable` over `Values` | `register_table` refusal | guard refusal |
| `CREATE TEMPORARY TABLE … AS` | refused by the SQL planner (`statement.rs:355`) | `Temporary tables not supported` | unchanged |
| `CREATE EXTERNAL TABLE t … LOCATION` | `CreateExternalTable`, not a CTAS | the fork's `register_table` refusal, nothing created | unchanged |
| a CTAS in a multi-statement string | refused before planning | `only supports a single SQL statement` | unchanged |
| `EXPLAIN CREATE TABLE … AS` | `Explain` over `CreateMemoryTable` | answers a plan, creates nothing | guard refusal |
| `EXPLAIN ANALYZE CREATE TABLE … AS` | `Analyze` over `CreateMemoryTable` | `Unsupported logical plan: CreateMemoryTable` | guard refusal |
| `PREPARE p AS CREATE TABLE … AS`, then `EXECUTE p` | `Prepare` over `CreateMemoryTable` | `PREPARE` answers; `EXECUTE` fails `Unsupported logical plan` | `PREPARE` refuses; `EXECUTE` finds no statement |
| lowercase, mixed-case keywords | the same plans | as plain CTAS | guard refusal |

## Targets

| Target | Resolution | Main | Head |
|---|---|---|---|
| in-memory catalog, bare `m` | `datafusion.public.m` | stores Spark's rows, every shape | unchanged |
| in-memory catalog, `datafusion.public.m` | itself | stores Spark's rows, every shape | unchanged |
| the memory catalog `ice` (`register_memory_catalog`, `crates/repark-core/src/session/memory_catalog.rs:30-33`) | Iceberg | the table above | refusal |
| a `type=hadoop` catalog `hd` (build config or `register_late_catalogs`) | Iceberg, `CatalogKind::Memory` with Hadoop naming (`crates/repark-core/src/catalog_kind.rs:24-31`, `session.rs:515`) | the table above | refusal |
| a mixed-case target `ICE.Ns.t` | normalized to `ice.ns.t` | as `ice` | refusal naming `ice.ns.t` |
| a bare target under `SET datafusion.catalog.default_catalog = 'ice'` | resolved against the session default | as `ice` | refusal |
| Glue (`session.rs:531`), S3 Tables (`session.rs:540`) | Iceberg; registered by the same `register_iceberg_catalog_with_policy` (`session.rs:458`) | read only (no AWS calls) | the same registry lookup refuses |
| an in-memory schema under an Iceberg catalog (native `CREATE SCHEMA ice.s`, a `MemorySchemaProvider`) | not Iceberg: no file is written | stores Spark's rows (measured on main) | unchanged |
| Postgres | not an Iceberg catalog (registration refused on this core, `session.rs:555-561`) | — | not touched |
