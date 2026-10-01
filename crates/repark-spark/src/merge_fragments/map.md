# map — repark-spark/src/merge_fragments

## Purpose

Case handling for MERGE fragment SQL (`ON` / predicate / value seats),
dispatched per `NameRule` by the parent
[`merge_fragments.rs`](../merge_fragments.rs).

## Contents

- `exact.rs` — **WO CASESENS-1 slice 2 (2026-09-27):** the
  `caseSensitive=true` fragment check. `check_fragment_exact` parses with
  `DatabricksDialect` and walks depth-0 identifiers the way
  `column_resolution`'s `FragmentRepair` does; an exact hit is backtick-quoted
  as written, a case-only hit refuses `UNRESOLVED_COLUMN.WITH_SUGGESTION`
  naming the written path with every scope field listed target scope first
  then source (R6, byte-exact on `p1/cs_merge_on`), no hit is left alone.
  `check_identity_exact` shares the walker for the single-scope identity-DML
  selection with bare candidates. Both compare names only through
  `repark_common::names::NameRule`.
  pins: casesens-1/C-008, C-017
  **STRING-LITERAL-ESCAPE-1 verifier fold (2026-09-30):** the re-render goes
  through `repark_iceberg::write::sql_text::render_for_reparse` so string
  values re-parse exactly. pins: string-literal-escape-1/C-010

## Pointers

- Up: [../map.md](../map.md)

## Debug

First checks: `cargo test -p repark-spark --lib -- casesens merge`. Escalate to: [../map.md#debug](../map.md).
