# map — repark-spark/src/insert_overwrite

## Purpose

`INSERT OVERWRITE` support. The parent `insert_overwrite.rs` holds probing and
stage-then-swap execution; this directory holds the store-overflow source
conformance it calls before staging.

## Contents

- `store.rs` — **CAST-OVERFLOW-INSERT-1 (2026-09-29):** `conform_types` resolves the
  positional targets and runs `wrap_store_outputs` over the analyzed source plan, so an
  out-of-range fractional store refuses `CAST_OVERFLOW_IN_TABLE_INSERT` with the column
  named. Split from the parent (file-size gate).

## Pointers

- Up: [../map.md](../map.md)
