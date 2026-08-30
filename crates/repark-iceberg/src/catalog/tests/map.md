# map — repark-iceberg/src/catalog/tests

## Purpose

Catalog adapter tests. `catalog/mod.rs` declares `#[cfg(test)] mod tests;`.

## Contents

- `mod.rs` — thin index.
- `catalog.rs` — AWS-free unit battery: CTAS reality, builder validation, live-list staleness,
  O(1) invalidation, scheme selection, span secret-hygiene, fork-patch proof, T6 residual pins.
- `namespace_scoped.rs` — G17 wrapper pins for `NamespaceScopedCatalog`.
  pins: rp-1-fork-repin/C-003

## Pointers

- Up: [../map.md](../map.md)
