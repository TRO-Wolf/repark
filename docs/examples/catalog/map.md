# map — docs/examples/catalog/

## Purpose

Worked examples for the `Catalog` metadata surface over the default session:
current catalog/database names, namespace probes, temp-view drops, listings,
UDF probes, and cache teardown. Examples construct the session as `repark =
ReparkSession.builder…` and read the surface through `catalog =
repark.catalog`; see [../map.md](../map.md). Temp views only — no cloud
catalog, no JVM.

## Contents

- [current_names.py](current_names.py) — `currentCatalog` / `current_catalog`
  and `currentDatabase` / `current_database` on the untouched default
  session.
- [views_and_exists.py](views_and_exists.py) — `databaseExists` /
  `database_exists` (default namespace True, missing name False) and
  `dropTempView` / `drop_temp_view` (True on the first drop, False after).
- [list_names.py](list_names.py) — `listTables` (the exact `Table` row for one
  temp view, plus a pattern arm) and `listCatalogs` / `list_catalogs` (the
  `spark_catalog` `CatalogMetadata` row).
- [udf_probe.py](udf_probe.py) — `functionExists` / `function_exists` against
  a session-registered temp UDF (True) and an unknown name (False).
- [clear_cache.py](clear_cache.py) — `clearCache` / `clear_cache` return None
  and the cached frame keeps answering.

Every snake_case spelling is a repark extension (`hasattr` measured False on
live PySpark 4.1.2) covered beside its camelCase twin, measured Spark-equal.
Three roster names stay on the backlog as measured divergences:
`getDatabase` / `get_database` (§7 EX-CAT-1), `listDatabases` (§7 EX-CAT-2,
cross-referencing FA-2), and the `functionExists(name, dbName)` arm (§7
EX-CAT-3) — all pinned in `python/repark/tests/test_examples_window_catalog.py`.

## Pointers

- Up: [../map.md](../map.md)
- Pins: [../../../python/repark/tests/test_examples_window_catalog.py](../../../python/repark/tests/test_examples_window_catalog.py)
- Ledger: [../../../task/ledgers/staging/ex-20-window-catalog-ledger.md](../../../task/ledgers/staging/ex-20-window-catalog-ledger.md)
