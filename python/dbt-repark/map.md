# map — python/dbt-repark

## Purpose

`dbt-repark`: the dbt adapter that runs dbt's compiled SQL **in process** through
`repark.sql()`. It exists so cutover step C6 can move the gold stage off Spark/Glue
([../../docs/cutover/inventory.md](../../docs/cutover/inventory.md) ruling 2). The unit that
built it is DBT-1; the design, the measured refusals and the acceptance result live in
[../../task/ledgers/staging/dbt-1-adapter-ledger.md](../../task/ledgers/staging/dbt-1-adapter-ledger.md).
pins: dbt-1-adapter/C-001

**Not published in this unit.** There is no wheel, no PyPI name reserved, no entry in the root
`pyproject.toml` uv workspace, and no row in `uv.lock`. Adding a workspace member would pull
`dbt-core` into the repo lock for a package nothing in `make ci` imports. The tests put
`src/` on `sys.path` (see [tests/map.md](tests/map.md)); `dbt` finds the adapter because
`dbt.adapters` and `dbt.include` are namespace packages.

## The route, and why the other one was not taken

Two routes could give RePark a dbt path:

1. an **in-process adapter** whose connection runs compiled SQL through `repark.sql()`;
2. a **Spark-Thrift-compatible endpoint** so unmodified `dbt-spark` connects over the wire.

Route 2 was rejected on measurement, not on cost. The refusals DBT-1 measured are in the
**statement surface**, not the transport: `show table extended`, `show tables in`, `show
databases`, `show tblproperties`, two-part `describe extended`, `create or replace view`,
`create or replace temporary view` and `alter column … comment` are all refused by
`repark.sql()`. A Thrift endpoint would deliver those statements faithfully and collect exactly
the same errors, so it buys a wire protocol and no working model. The measured table is
[tests/test_statement_surface.py](tests/test_statement_surface.py), which is the pin, not prose.

Inside route 1 the adapter **subclasses `dbt-spark`'s `SparkAdapter`** and declares
`dependencies=["spark"]`, rather than re-authoring from `SQLAdapter`. The reason is the
production dbt project: gold's models are configured with `file_format='iceberg'`,
`tblproperties`, `partition_by` and `location_root`, and `spark__create_table_as` already emits
`create or replace table … using iceberg tblproperties (…) as …` — the one statement the SQL
door serves. Re-authoring those clause macros would risk silent config drift in a project the
cutover must not edit. What `dbt-spark` gets wrong for RePark is overridden and nothing else.

## Contents

- `pyproject.toml` — package metadata and dependencies (`dbt-core` 1.9.x, `dbt-spark` 1.9.x,
  `repark`). `dbt-spark`'s heavy transports (`pyhive`, `thrift`, `pyodbc`, `pyspark`) are all
  behind *extras*, so the dependency adds no driver.
- `src/dbt/adapters/repark/` — the adapter: credentials, connection manager, session cursor,
  relation, adapter class. Arrives with the build commit.
- `src/dbt/include/repark/` — the macro package: the overrides and the two refusals. Arrives with
  the build commit.
- `tests/` — the statement-surface pins; the gold acceptance and the deferred Glue leg arrive
  with their own commits. See [tests/map.md](tests/map.md).

## I want to...

| ...do this | go to |
|---|---|
| See what dbt emits and what RePark answers | [tests/test_statement_surface.py](tests/test_statement_surface.py) |
| Run the two gold models end to end | `.venv/bin/python -m pytest python/dbt-repark/tests -q` |
| Read the divergence registry this unit files rows into | [../../docs/spark-sql-iceberg-parity.md](../../docs/spark-sql-iceberg-parity.md) |

## Pointers

- Up: [../map.md](../map.md)
- The SQL door this adapter drives: [../repark/src/repark/map.md](../repark/src/repark/map.md)
- The cutover step that consumes it: [../../docs/cutover/inventory.md](../../docs/cutover/inventory.md) §6 C6

## Debug

| Symptom | First check |
|---|---|
| `Could not find adapter type repark!` | `src/` is not on `sys.path`; `tests/conftest.py` is what puts it there |
| `Cannot set database in spark!` | a relation was built from `SparkRelation`; the adapter's own `ReparkRelation` is the three-part one |
| `table 'datafusion.<ns>.<t>' not found` | a two-part name reached the SQL door; every relation must render `catalog.namespace.table` |
| `incremental` / `snapshot` model refuses | deliberate — RePark has no temporary views, so dbt's merge staging cannot run |
| a `view` model refuses | deliberate — `DBT-VIEW-1` in the registry |
