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
  **STRING-CAST-LENIENT-1 (2026-11-22) + ANSI-OFF-NARROWING-OVERFLOW-1 (2026-11-23):** `expr_has_unsafe_cast` judges
  `__repark_string_to_*__` (`string_integral_cast_parts`) and `__repark_narrow_to_*__` (`narrow_cast_parts`) as the
  `CAST(x AS T)` they replaced, so a laundered empty overwrite still refuses.

- `const_cast.rs` — **INSERT-OVERWRITE-CONST-CAST-1, narrow fix (2026-11-25):** `refuse_constant_cast`, run once by the
  overwrite doors before they wipe, replace or commit (the empty whole-table arm after the W03 check, the partition path,
  `replace_where.rs` for `writeTo().overwrite(...)` and `ctas.rs` for a `saveAsTable` overwrite). Under ANSI, when the
  source reads no named relation (CTEs and the door's own `_repark_writer_` view aside) and plans to projections, filters
  and aliases over a `FROM`-less `SELECT`, each output column that is a cast of a string literal is judged by the decision
  table (`spark_cast_raises`: ASCII-letter strings to the integral and microsecond timestamp types, and to `FLOAT` /
  `DOUBLE` minus `nan`, `inf`, `infinity` and their `d` / `f` forms, measured on Spark 4.1.2). A listed value runs through
  RePark's own cast on one row, and the guard refuses only with that cast's `[CAST_INVALID_INPUT]` error. A cast's
  operand may sit in the session's `__repark_decimal_cast_nullable__` identity marker. Its unit tests pin the table,
  the scope test and the relation check; `tests/insert_overwrite_const_cast.rs` plans through a full session. Card:
  [insert-overwrite-const-cast-1](../../../../task/roadmap/mid-term/cards-2026-11-23/insert-overwrite-const-cast-1-card-2026-11-24.md).

## Pointers

- Up: [../map.md](../map.md)
