# map — repark-spark/src/router/nested_ns

## Purpose

The unit tests of the parent `nested_ns.rs`, the router's half of the nested naive-timestamp
gates (R-015, R-011 and ZONE-SEAM-1's ZS-9 refusal). They moved here from an inline `mod tests`
on 2026-10-23 (ZONE-SEAM-1 fix round 3), when item A's leaf-by-leaf exemption read took the
parent past the 1000-line ceiling; the parent's prose stays in [../map.md](../map.md).

## Contents

- `tests.rs` — the fail-closed read of a column that resolves in no input, the two fail-closed
  arms of the lateral walk (an outer reference no enclosing join resolves, an unmodelled node
  that refers out), and the pure UTC alias set (`only_a_pure_utc_alias_counts_as_utc`), moved
  unchanged. pins: ice-ntz-nested-refuse-1/C-001, C-042, zone-seam-1/C-024
  **GATE-RETIRE-1 (2026-10-25, ledger C-003, restated):** with the arm's body forced to `false`, the
  two discriminating walker tests are `a_column_that_resolves_in_no_input_reads_every_input_whole` and
  `an_unmodelled_node_that_refers_out_reads_the_other_side_whole`; the third,
  `an_outer_reference_no_enclosing_join_resolves_fails_closed`, passes with any predicate and pins
  nothing about the arm.

## Pointers

- Up: [../map.md](../map.md)
