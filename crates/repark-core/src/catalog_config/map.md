# map — repark-core/src/catalog_config

## Purpose

File-backed modules of `../catalog_config.rs`: `refusal.rs` (production) and the test modules.
New catalog-config pins live here so the parent file stays under its size baseline.

## Contents

- `refusal.rs` — **CATALOG-1 (2026-09-26):** `CatalogRefusal` (Spark's refusal text plus its
  error class, `UnsupportedOperation` / `IllegalArgument`), the
  `repark.sql.catalogExtensions` opt-in read (any case of the key, only the value `true`),
  and the `Block` refusal decision (a kind from `type=memory` alone, or from both
  `type` and `catalog-impl`, with raw values echoed). pins: catalog-1/C-006, C-007, C-008
- `refusal_tests.rs` — **CATALOG-1 (2026-09-26):** the parse-level refusal pins (the
  `type=memory` shapes incl. case variants, the both-keys shapes, the long form and
  `type=hadoop` staying catalogs, the opt-in keeping the memory type and the agreeing
  pair, the opt-in reading only a `true` value). pins: catalog-1/C-006, C-007, C-008
- `hadoop_naming_tests.rs` — **PR-B hadoop naming (2026-09-24):** `type=hadoop` (and
  ` Hadoop `, and the `repark.sql.catalog.` spelling) puts `metadata-naming=hadoop` into the spec
  props. `type=memory` and `catalog-impl=org.apache.iceberg.inmemory.InMemoryCatalog` add no
  `metadata-naming`. An explicit `metadata-naming` wins over `type=hadoop` and passes through
  verbatim. The near misses `type=hadoopx` and `type=` still refuse, naming the key and the
  value. Mutation: make the `type` arm record `false` and the three `hadoop` pins go red.
  `bare_hadoop_catalog_value_adds_no_metadata_naming`: a bare `spark.sql.catalog.<name> =
  hadoop` resolves to `Memory` and adds no `metadata-naming` (class sweep, 2026-09-24).
  **CATALOG-1 (2026-09-26):** the two `type=memory` tests run under the opt-in.

## Pointers

- Up: [../map.md](../map.md). Session-level pins: [../session/tests/map.md](../session/tests/map.md).
