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
  from the parent file. Since GATE-RETIRE-1 (2026-10-25) the three lineage-walker tests take a
  predicate local to this file that restates the retired R-011 container-cast arm (a `CAST` or
  `TRY_CAST` whose target holds a naive timestamp and whose input holds a zoned one, or does not
  resolve). No assertion or expected value changed. pins: ice-ntz-nested-refuse-1/C-001, C-042,
  zone-seam-1/C-024
  pins: gate-retire-1/C-003

## Pointers

- Up: [../map.md](../map.md)
