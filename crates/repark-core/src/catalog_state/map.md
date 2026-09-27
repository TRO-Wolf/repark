# map — repark-core/src/catalog_state

## Purpose

Child modules of `../catalog_state.rs` (the `CatalogRegistry`). New in CATALOG-1
(2026-09-26): the session's current catalog lives here, so every clone of the registry (the
per-statement snapshots) shares one current catalog. See [../map.md](../map.md).

## Contents

- `session_catalog.rs` — **CATALOG-1 (2026-09-26):** `CurrentCatalog` (catalog, namespace,
  pinned). `CatalogRegistry::with_session_catalogs` (build: an unpinned current catalog from
  `spark.sql.defaultCatalog`), `current_defaults`, `set_defaults` (`USE` and the planner-default
  mirror; pins the current catalog), `apply_default_catalog` (the runtime conf; a no-op once
  pinned, `None` means `spark_catalog`), `default_namespace_for` (`default` for `spark_catalog`,
  `""` otherwise, as Spark's Iceberg catalogs answer), `configured_default_catalog`, and
  `current_catalog_error` (Spark's `CATALOG_NOT_FOUND` text for
  an unpinned name that is neither `spark_catalog` nor registered; `USE` validates what it pins).
  The registry also holds the refused-kind placeholders (`insert_refusal` / `refusal` /
  `has_refusals` on `../catalog_state.rs`; a real `insert` clears the refusal; a refusal wins
  over `CATALOG_NOT_FOUND` in `current_catalog_error`). pins: catalog-1/C-001, C-003, C-004,
  C-006, C-007, C-008

## Pointers

- Up: [../map.md](../map.md). Tests: [../session/tests/map.md](../session/tests/map.md)
  (`session_catalog.rs`).
