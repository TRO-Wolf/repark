# map — repark-core/src/dynamic_flatten/tests

CC-2 closing-critic remediation: review-round label narration swept from prose; safety and
accuracy contracts restored in condensed form (see the unit ledger's findings dispositions).

## Purpose

Submodules of `../tests.rs`. Split out so the parent stays under the
Rust file-size default ceiling. Dirty-list-under-null-parent CASE pin
`null_parent_dirty_list_child_is_null_not_exploded` lives in `../tests.rs`
(Pin 1 companion), not here.

## Contents

- `octo.rs` — octo C2/C3 kernel pins: `LargeList` / `FixedSizeList` explode,
  `ListView` / `LargeListView` refuse (default-depth top-level plus R-S1-003
  nested `max_depth=1` and top-level `max_depth=0`), max-depth remaining-schema
  truncation (`max_depth_remaining_schema_is_truncated` kills unbounded output
  — token / "truncated" / len — not the join-then-truncate allocation path).
- `preserve_nulls.rs` — DFP-1 plan-shape and value/type matrix for preserve-null Unnest across
  List, LargeList, FixedSizeList, and Dictionary<List>; keeps sequential Cartesian expansion.
  pins: dfp-1-preserve-null-unnest/C-001, C-002, C-003, C-004, C-005, C-006

## Pointers

- Up: [../map.md](../map.md)

## Debug

| Symptom | First check |
|---|---|
| LargeList / FixedSizeList stay nested | Pins `large_list_explodes`, `fixed_size_list_explodes`. |
| Null lists lose their row or empty lists survive false mode | Pins in `preserve_nulls.rs`. |
| ListView leave-nested | Pins `list_view_refuses_loud`, `large_list_view_refuses_loud` (default depth, top-level). Nested struct wrap + `max_depth=1`: `nested_list_view_max_depth_one_refuses_loud`, `nested_large_list_view_max_depth_one_refuses_loud`. Top-level `max_depth=0`: `top_level_list_view_max_depth_zero_refuses_loud`. |
| Null parent list explodes 99/100 | Pin lives in `../tests.rs`: `null_parent_dirty_list_child_is_null_not_exploded`. |

First checks: `cargo test -p repark-core dynamic_flatten`. Escalate to: [../map.md](../map.md).
