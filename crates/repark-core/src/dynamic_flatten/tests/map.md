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
  **PERF-DYNFLATTEN-1** (stats are `#[cfg(test)]`; the product path never counts):

  | pin | holds |
  |---|---|
  | `flatten_stats_depth_three_struct_counts_repeated_schema_walks` | 3 expansions, 10 walks, 4 passes, 20 fields |
  | `flatten_stats_two_sibling_lists_are_sequential_unnests` | 2 Unnests, 4-row Cartesian |
  | `product_dynamic_flatten_does_no_plan_walk` | `dynamic_flatten` leaves `PLAN_WALKS` at 0; the stats entry raises it to 1 |

  Mutations: drop the `has_struct_columns` walk → walks 10 → 6, depth-three pin reds (1 of 2);
  route `dynamic_flatten` back through the stats entry → the no-plan-walk pin reds (1 of 1).
  pins: perf-dynflatten-1-measure/C-002
- `preserve_nulls.rs` — DFP-1 plan-shape and value/type matrix for preserve-null Unnest across
  List, LargeList, FixedSizeList, and Dictionary<List>; keeps sequential Cartesian expansion.
  pins: dfp-1-preserve-null-unnest/C-001, C-002, C-003, C-004, C-005, C-006

  **PERF-DYNFLATTEN-2** joins it because the plan-shape assertion it already owns —
  how many `CASE WHEN` projections a rewrite emits — is the assertion the extractor moves.
  These two pins land here rather than in a new module because `../tests.rs` holds an exact
  1442-line baseline and a `mod` line would raise it.

  | pin | holds |
  |---|---|
  | `struct_expansion_uses_the_null_mask_extractor_not_a_case` | a plain-struct expansion emits `repark_null_mask_field` once and `CASE WHEN` zero times |
  | `dictionary_struct_expansion_keeps_the_case_projection` | a `Dictionary(_, Struct)` parent still emits the CASE and never the extractor |

  Mutation: route plain structs back through `null_safe_field` (`null_mask_extractable` → `false`)
  → the extractor pin reds and the dictionary pin stays green, **1 red of 2**.
  pins: perf-dynflatten-2-null-mask/C-003

## Pointers

- Up: [../map.md](../map.md)

## Debug

| Symptom | First check |
|---|---|
| LargeList / FixedSizeList stay nested | Pins `large_list_explodes`, `fixed_size_list_explodes`. |
| Null lists lose their row or empty lists survive false mode | Pins in `preserve_nulls.rs`. |
| ListView leave-nested | Pins `list_view_refuses_loud`, `large_list_view_refuses_loud` (default depth, top-level). Nested struct wrap + `max_depth=1`: `nested_list_view_max_depth_one_refuses_loud`, `nested_large_list_view_max_depth_one_refuses_loud`. Top-level `max_depth=0`: `top_level_list_view_max_depth_zero_refuses_loud`. |
| Null parent list explodes 99/100 | Pin lives in `../tests.rs`: `null_parent_dirty_list_child_is_null_not_exploded`. |
| A struct expansion emits `CASE WHEN` again | `null_mask_extractable` refused a plain `Struct` parent — pin `struct_expansion_uses_the_null_mask_extractor_not_a_case`. |

First checks: `cargo test -p repark-core dynamic_flatten`. Escalate to: [../map.md](../map.md).
