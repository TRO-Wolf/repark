# map — docs/examples/catalog/

## Purpose

Worked examples for the `Catalog` metadata remainder over the default session:
current catalog/database setters, table-existence probes, session UDF
registration, and per-database table listings. Examples construct the session
as `repark = ReparkSession.builder…` and read the surface through
`catalog = repark.catalog`; see [../map.md](../map.md). The memory catalog,
temp views, and the local filesystem only — no cloud catalog, no JVM.

## Contents

- [set_current_names.py](set_current_names.py) — `setCurrentCatalog` /
  `set_current_catalog` and `setCurrentDatabase` / `set_current_database`:
  register a memory catalog (currentCatalog flips to it), set both spellings
  back and forth, read each value back, and a namespace made via
  `create_namespace`.
- [table_exists.py](table_exists.py) — `tableExists` / `table_exists`: a temp
  view answers True, a missing name answers False, both spellings.
- [register_function.py](register_function.py) — `registerFunction` /
  `register_function`: register a scalar UDF through the catalog, probe it with
  `functionExists`, answer with it inside SQL. An omitted return type declares
  string, so the example's UDFs return strings. The return-value arm diverges —
  §7 `EX-SES-1`.
- [list_tables.py](list_tables.py) — `list_tables`: the exact `MANAGED` row for
  a memory-catalog Iceberg table, the `TEMPORARY` view row, the bare arm, and
  an exact-pattern arm.

`Catalog.list_databases` stays on the backlog: it is the same function object
as the divergent `listDatabases` (§7 `EX-CAT-2`), so covering it would paper
over that row.

## Pointers

- Up: [../map.md](../map.md)
- Pins: [../../../python/repark/tests/test_examples_window_catalog.py](../../../python/repark/tests/test_examples_window_catalog.py)
- Ledger: [../../../task/ledgers/staging/ex-21-catalog-session-ledger.md](../../../task/ledgers/staging/ex-21-catalog-session-ledger.md)
