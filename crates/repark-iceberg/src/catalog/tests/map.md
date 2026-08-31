# map — repark-iceberg/src/catalog/tests

CC-3 (2026-08-30): comments condensed to one line; banners removed; truncated comments rewritten as complete sentences (D-001).

## Purpose

Catalog adapter tests. `catalog/mod.rs` declares `#[cfg(test)] mod tests;`.

## Contents

- `mod.rs` — thin index.
- `catalog.rs` — AWS-free unit battery: CTAS reality, builder validation, live-list staleness,
  O(1) invalidation, scheme selection, span secret-hygiene, fork-patch proof, T6 residual pins.
- `namespace_scoped.rs` — G17 wrapper pins for `NamespaceScopedCatalog`.
  pins: rp-1-fork-repin/C-003
  pins: rp-4-fork-repin/C-002
- `lineage_columns.rs` — **V3-4 critic:** stored `_row_id` wins over `first_row_id +` pos;
  `WHERE id = lit` keeps matching lineage rows; `try_new_with_snapshot` is absent.
  pins: v3-4-serve-lineage-columns/C-017, C-019, C-020

## Pointers

- Up: [../map.md](../map.md)
