# map — repark-spark/src/void_type

## Purpose

WO LTZ-STORE-INT-1 support. The parent `void_type.rs` holds the Spark-door `VOID`
handling (`CAST(NULL AS VOID)` rewrite, the non-NULL VALUES refusal into
`unknown` columns); this directory holds the VALUES store-assignment gate for
`TIMESTAMP` (LTZ) targets it calls first.

## Contents

- `ltz_values_store.rs` — `refuse_unassignable_ltz_values` judges each VALUES
  cell mapped to a microsecond `Timestamp` column through the shared
  `incompatible_update_message` gate — bare literals by their Spark kind, anything
  else by a `SELECT <cell>` probe — so `VALUES (0, 1)` refuses with Spark's
  recorded `CANNOT_SAFELY_CAST` text while NULL, DATE, TIMESTAMP, TIMESTAMP_NTZ
  and explicit `CAST`s still store. Skips TIMESTAMP_NTZ targets, partitioned
  inserts, and anything it cannot map. Reached from
  `refuse_insert_void_values`. **Fold 2026-09-28 (critic V-001):** null-valued
  cells pass; numerics refuse through a DDL-name fallback even when the shared
  gate cannot name them; `1L` reads `BIGINT`; DECIMAL casts and typed literals
  read by declared type. **Fold 2026-09-28 (verifier VL-1..VL-6):** the V-001 null
  rule narrows to bare NULL — `CAST(NULL AS T)` is judged by `T`; fractionals
  read `DECIMAL(p,s)` and out-of-`INT`-range integers read `BIGINT`.
  **Fold 2026-09-28 (re-verify RL-1..RL-3):** the VL-1 function deferral narrows
  to two-argument `nvl`/`ifnull`, probed as `coalesce(a, b)`; every other
  function is judged by its probed type; leading-zero fractions count precision
  from significant digits (six classifier tests).
  **Fold 2026-09-28 (LTZ-STACKED-SIGN-1):** the classifier peels any number of
  unary Plus/Minus signs and parentheses, and the probe parenthesizes a leading
  stacked-minus chain so its rendering cannot form a `--` comment; `- -1BD`
  (an `Identifier` to this parser) refuses through the repaired probe exactly
  as `-1BD` does (nine classifier tests). NTZ stacked cells still write: that
  probe lives in `void_type.rs`, outside this fold.
  **Fold 2026-09-28 (LTZ-STACKED-SIGN-1 verifier fold, VG-1/VG-2/VG-8):** the
  local parenthesizer is deleted in favor of the shared recursive one in
  `void_type.rs`, used at all three probe sites; `probe_source_type` propagates
  the analyzer error instead of passing the row when the probe cannot parse;
  `number_text_type` is `pub(crate)` for `update_cast.rs` (ten classifier
  tests, one asserting no `--` survives rendering).
  **Fold 2026-09-28 (LTZ-STACKED-SIGN-1 re-verify fold, RN2-2/RN2-3):** a
  `DEFAULT` cell is skipped like a bare NULL (`is_null_or_default_cell` in the
  parent), and both `nvl`/`ifnull` operands render through the shared
  parenthesizer. `ntz_probe_that_cannot_parse_refuses_instead_of_passing` is the
  NTZ door's twin of the fail-closed pin: it drives the parent's `check_ntz_row`
  with an unparsable cell and expects the `ParserError` back (eleven tests).
  **Fold 2026-09-29 (LTZ-STACKED-SIGN-1 second re-verify fold, RN3-1):** the
  probe renders through the parent's shared `probe_text`, which dollar-quotes
  a string literal holding a quote with the same value, so a backslash before
  a quote no longer refuses the row on this door either (eighteen tests).
  **Fold 2026-09-29 (re-verify 3, RN4-1):** the dollar-quote tag also grows
  past a closer the value runs into (`'it''s$p'`), so a quoted value ending in
  `$` plus the tag no longer closes the probe early and refuses (nineteen
  tests).
  pins: ltz-store-int-1/C-001

## Pointers

- Up: [../map.md](../map.md)
