# map — repark-core/src/catalog_config

## Purpose

File-backed test modules of `../catalog_config.rs`. That file sits at its exact size baseline,
so new catalog-config pins live here.

## Contents

- `hadoop_naming_tests.rs` — **PR-B hadoop naming (2026-09-24):** `type=hadoop` (and
  ` Hadoop `, and the `repark.sql.catalog.` spelling) puts `metadata-naming=hadoop` into the spec
  props. `type=memory` and `catalog-impl=org.apache.iceberg.inmemory.InMemoryCatalog` add no
  `metadata-naming`. An explicit `metadata-naming` wins over `type=hadoop` and passes through
  verbatim. The near misses `type=hadoopx` and `type=` still refuse, naming the key and the
  value. Mutation: make the `type` arm record `false` and the three `hadoop` pins go red.
  `bare_hadoop_catalog_value_adds_no_metadata_naming`: a bare `spark.sql.catalog.<name> =
  hadoop` resolves to `Memory` and adds no `metadata-naming` (class sweep, 2026-09-24).

## Pointers

- Up: [../map.md](../map.md). Session-level pins: [../session/tests/map.md](../session/tests/map.md).
