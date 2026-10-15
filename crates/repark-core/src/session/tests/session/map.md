# map — repark-core/src/session/tests/session

## Purpose

Submodules of the session test battery (`../session.rs`).

## Contents

- `catalog_registration.rs` — atomic same-name provider/handle publication, distinct-name
  build overlap, exact duplicate errors, and provider-build failure isolation.
  `pins: rust-catalog-registration/C-001`
  `pins: rust-catalog-registration/C-002`
  `pins: rust-catalog-registration/C-003`
  `pins: rust-catalog-registration/C-004`
  **ENC-1-RESIDUE-1 item 5 (2026-10-15):** the race pin no longer asserts `Arc::ptr_eq`
  on the published handle — registration always wraps now — and instead asserts the
  published handle serves the winner's warehouse (winner namespace exists, loser
  namespace does not) with the winner's provider schemas.

## Pointers

- Up: [../map.md](../map.md)
