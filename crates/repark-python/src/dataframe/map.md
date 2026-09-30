# map — repark-python/src/dataframe

## Purpose

`PyDataFrame` is the Python-facing immutable plan plus its shared runtime: lazy
transforms, terminal actions, schema introspection, and Arrow C Stream export.

## Modules

- [`mod.rs`](mod.rs) — the binding: plan access, terminals, builders, schema, and
  stream export.
  **DEEP-FILTER-CHAIN-CRASH-1 verifier fold (2026-09-29):** the inline test
  module moved here to `tests.rs` untouched (the file would otherwise pass its
  ceiling); terminals and `analyzed_arrow_schema_native` drive through
  `frame_drive_segment` plus `block_on_grown_sized`, and the column-taking
  builders (`select`, `filter`, `with_column`, `sort`, `join_on_condition`,
  `aggregate`) refuse past the expression cap and grow past the nesting depth
  (or on a small calling stack) through `drive_columns`. The relocation shed
  every doc comment per the comment ban, so the `PyResult`-returning fns carry
  per-item `#[allow(clippy::missing_errors_doc)]` (repo precedent:
  `repark-distributed`).
  pins: deep-filter-chain-crash-1/C-001
- [`tests.rs`](tests.rs) — **DEEP-FILTER-CHAIN-CRASH-1 verifier fold
  (2026-09-29):** the `dataframe` unit tests, moved verbatim from the inline
  module (Arrow export values, types, laziness, errors, schema caching).
