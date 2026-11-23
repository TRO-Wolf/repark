# map — repark-spark/src/insert_overwrite

## Purpose

`INSERT OVERWRITE` support. The parent `insert_overwrite.rs` holds probing and
stage-then-swap execution; this directory holds the store-overflow source
conformance it calls before staging.

## Contents

- `store.rs` — **ICE-STRUCT-PAIRING-1 step 3 (2026-10-14), parity row R-016:** `conform_types` takes the door's `StructPairing` and table label
  and passes them to the conform stage (`conform_stage.rs`; formerly `wrap_store_outputs`).
- `store.rs` — **WRITE-SPINE-1 step 2 fix round 1 (2026-10-25):** `gate_op` is set only for the InsertOverwrite site, so the PARTITION path's own refusal text answers (WS2-28).
- `store.rs` — **CAST-OVERFLOW-INSERT-1 (2026-09-29):** `conform_types` resolves the
  positional targets (`table_targets`) and runs the conform stage (`conform_stage.rs`; formerly `wrap_store_outputs`) over the analyzed source plan, so an
  out-of-range fractional store refuses `CAST_OVERFLOW_IN_TABLE_INSERT` with the column
  named. Split from the parent (file-size gate). Re-verify VO3-1 (2026-09-29): passes
  `Some("INSERT OVERWRITE")` so the store gate judges before the overflow check, with
  targets from the presented schema (UUID presents as text) so the verdict matches the
  per-batch gate, which stages presented batches too.
- `assignment_types.rs` — **merge origin/main v1.5.1 (2026-09-29):** pure move of the
  empty-OW assignment type matrix from the parent (file-size gate): identity, UTF-8
  aliasing, safe widenings and NULL/total-text casts are total, everything else fails
  closed, with the two unit pins moved alongside.
  **STRING-CAST-LENIENT-1 (2026-11-22):** the parent's `expr_has_unsafe_cast` judges the analyzer's
  `__repark_string_to_{tinyint,smallint,int,bigint}__(x)` (`repark_functions::cast_map::string_integral_cast_parts`)
  as the `CAST(x AS T)` it replaced, so a laundered empty overwrite still refuses (pins: the `*_cast_launder_*` tests).

## Pointers

- Up: [../map.md](../map.md)
