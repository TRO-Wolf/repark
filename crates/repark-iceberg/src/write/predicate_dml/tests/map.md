# map — repark-iceberg/src/write/predicate_dml/tests

CC-3 (2026-08-30): comments condensed to one line; banners removed; truncated comments rewritten as complete sentences (D-001).

## Purpose

Identity DELETE/UPDATE tests. `predicate_dml.rs` declares `#[cfg(test)] mod tests;`.

## Contents

- `mod.rs` — thin index.
- `plain.rs` — **RP-9 r2:** `try_allowed_plain_identity` accepts `DELETE … WHERE id = 0` on a
  three-part name and refuses a subquery `WHERE`, a literal `IN` list, an `UPDATE`, and a
  four-part branch selector (those stay on the IN/EXISTS allow-list or the fork delete exec).
  pins: rp-9-repin-f23/C-005
- `predicate_dml.rs` — DELETE: `IN` / `NOT IN (SELECT …)` including the NULL 3VL trap,
  `[NOT] EXISTS` with and without correlation, correlated `IN`, isolation-level pins
  (M19 / A10).
- `update.rs` — identity `UPDATE … SET <scalar> WHERE col IN`. Unknown
  `write.delete.granularity` refuses before any parquet write (MW-9). **V3-9:**
  `identity_pairs_share_one_arc_per_data_file_path` counts `Arc` identities over 600,003 pairs
  on two paths and requires exactly two allocations.
  pins: v3-9-mor-predicate-dml-dv/C-009

## Pointers

- Up: [../map.md](../map.md)
