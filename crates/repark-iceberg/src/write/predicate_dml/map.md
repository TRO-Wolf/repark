# map — repark-iceberg/src/write/predicate_dml

CC-3 (2026-08-30): comments condensed to one line; banners removed; truncated comments rewritten as complete sentences (D-001).

## Purpose

Children of [`predicate_dml.rs`](../predicate_dml.rs) (`execute_predicate_dml` — identity
DELETE / UPDATE with a subquery `WHERE`): the test batteries and, since V3-8, the projection
helpers. Both are split out because the parent sits near its size ceiling.

Created by **LRS-5 (2026-08-20)**: both had been included from `write/` with `#[path = "…"]`.
Source comments retain predicate and cleanup contracts; implementation narration is omitted.
AGENTS.md allows a test-fixture exception only where the canonical layout cannot work — here it
works, so the attribute is gone rather than documented.

## Contents

- `lineage.rs` — **V3-8 (2026-09-02):** the write-column list, the survivor/new-value SELECT
  lists and the update value schema, with the format-v3 lineage pair appended when the table
  carries it (`_last_updated_sequence_number` projected NULL for a changed row, as V3-7's MERGE
  writer does). Extracted so `predicate_dml.rs` stays under its exact size baseline.
  **V3-9 (2026-09-02):** also `push_identity_pair`, which reuses the previous `Arc<str>` when
  the matched row's data-file path is unchanged — one allocation per distinct path instead of
  one per row (600k rows on one file: 31.0 → 11.5 ms, 66,000,000 → 110 B retained). It lives
  here rather than in the parent because `predicate_dml.rs` sits at an exact 1164-line ceiling.
  pins: v3-8-subquery-where-lineage/C-002; v3-9-mor-predicate-dml-dv/C-009
- [tests/](tests/map.md) — DELETE and identity UPDATE batteries.

## Pointers

- Up: [`../map.md`](../map.md)
