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
  here rather than in the parent because `predicate_dml.rs` sits at an exact line ceiling (1142 since RP-7). **RP-7 (2026-09-02):** `push_pairs_from_batch` moved here for the same reason; both identity collectors now stream and call it once per arriving batch.
  pins: v3-8-subquery-where-lineage/C-002; v3-9-mor-predicate-dml-dv/C-009
- `residual.rs` — **RP-7 (2026-09-02):** `identity_scan_residual`, the key-bounds residual the
  identity DML scratch scan carries. Re-parses `selection_sql` (the spec carries SQL, not an AST)
  and matches only a POSITIVE uncorrelated `IN` or a positive `EXISTS` whose correlation is one
  bare equality; everything else, and `repark.merge.scan-pruning=false`, leaves the scan
  unfiltered. Bounds come from `scan_prune::residual_bounds_predicate`, the same helper PERF-04
  gave MERGE. Extracted so `predicate_dml.rs` stays under its exact size baseline, which
  ratcheted 1164 → 1142 in the same change.
  pins: rp-7-f18-repin/C-005
- [tests/](tests/map.md) — DELETE and identity UPDATE batteries.

## Pointers

- Up: [`../map.md`](../map.md)
