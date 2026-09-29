# map — repark-spark/src/insert_overwrite

## Purpose

`INSERT OVERWRITE` support. The parent `insert_overwrite.rs` holds probing and
stage-then-swap execution; this directory holds the store-overflow source
conformance it calls before staging.

## Contents

- `store.rs` — **CAST-OVERFLOW-INSERT-1 (2026-09-29):** `conform_types` resolves the
  positional targets and runs `wrap_store_outputs` over the analyzed source plan, so an
  out-of-range fractional store refuses `CAST_OVERFLOW_IN_TABLE_INSERT` with the column
  named. Split from the parent (file-size gate). Re-verify VO3-1 (2026-09-29): passes
  `Some("INSERT OVERWRITE")` so the store gate judges before the overflow check.
- `assignment_types.rs` — **merge origin/main v1.5.1 (2026-09-29):** pure move of the
  empty-OW assignment type matrix from the parent (file-size gate): identity, UTF-8
  aliasing, safe widenings and NULL/total-text casts are total, everything else fails
  closed, with the two unit pins moved alongside.

## Pointers

- Up: [../map.md](../map.md)
