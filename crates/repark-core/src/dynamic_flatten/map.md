# map — repark-core/src/dynamic_flatten

CC-4 (2026-08-30): remaining banner files condensed to the one-line rule
(pins: cc-3-comment-condensation/C-009).

## Purpose

File-backed tests for the `dynamic_flatten` plan-rewrite kernel
(`../dynamic_flatten.rs`): structs first, then lists one-at-a-time, matching
the Python `dynamicFlatten` state machine. Schema-only walks; no physical
operator.

## Contents

- `tests.rs` — Arrow value **and** type pins. The first four are the design
  mutation pins (null-parent CASE with dirty children, in-place column order,
  list-of-struct then unnest, prefixed-name collision) plus Pin-1 list-child
  companion `null_parent_dirty_list_child_is_null_not_exploded` (dirty valid
  List under a null parent). Remaining engine cases plus octo C1 pins:
  ScanForbidden plan-build spy, dotted-name Unnest bind, Dictionary-of-List
  unwrap, dict-struct type + utf8 dict-struct, list-of-map refuse, dirty-child
  mid-struct nulls, ReparkSession wrapper harness, DEFECT-2 flatten-then-project.
- `tests/` — octo C2/C3 pins split out of `tests.rs` (file-size ceiling). See
  [tests/map.md](tests/map.md).

## Pointers

- Up: [../map.md](../map.md)
- Facade contract: `python/repark/tests/test_dynamic_flatten.py`
- Ledger: [../../../../task/df1-rust-flatten-ledger.md](../../../../task/ledgers/archive/2026-08/2026-08-20-df1-rust-flatten-ledger.md)

## Debug

| Symptom | First check |
|---|---|
| Null parent struct yields 0/""/false | The CASE `parent IS NULL THEN <typed null>` was dropped — pin `null_parent_struct_fields_are_null_not_zero` (dirty children at the parent-null slot). |
| Null parent list explodes 99/100 | CASE skipped for List children — pin `null_parent_dirty_list_child_is_null_not_exploded` (dirty valid `[99, 100]` under a null parent). |
| Null mid-struct yields 0 | Dirty children at the mid-null and outer-null slots — pin `null_mid_struct_fields_are_null_not_zero`. |
| Column order is survivors-first | Expansion is not in schema field order — pin `unnest_preserves_interleaved_column_order`. |
| `From<&str>` / `col(name)` on a `s.f` / `wrap.nums` column | Every schema field must bind through `Column::new_unqualified` — pin `dotted_list_column_unnest_uses_unqualified_bind`. |
| Empty lists survive `empty_as_null=false` | Unnest drops zero-length arrays regardless of `preserve_nulls` (that flag only keeps NULL lists). `empty_as_null=true` rewrites EMPTY to a singleton-null list first — pin `null_and_empty_array_values`. |
| `array<map>` explodes | List-of-map must refuse `[DYNAMIC_FLATTEN_UNSUPPORTED_ELEMENT]` — pin `list_of_map_refuses_loud`. |
| ListView leave-nested | ListView / LargeListView refuse `[DYNAMIC_FLATTEN_UNSUPPORTED_ELEMENT]` — pins `list_view_refuses_loud`, `large_list_view_refuses_loud` (default depth, top-level); nested wrap + `max_depth=1`: `nested_list_view_max_depth_one_refuses_loud`, `nested_large_list_view_max_depth_one_refuses_loud`; top-level `max_depth=0`: `top_level_list_view_max_depth_zero_refuses_loud`. |
| LargeList / FixedSizeList stay nested | Both arms of `list_element_type` explode — pins `large_list_explodes`, `fixed_size_list_explodes`. |
| Dictionary list Unnest rejects | Cast Dictionary<_, List> to List before Unnest — pin `dictionary_list_is_unwrapped_one_level`. |

First checks: `cargo test -p repark-core dynamic_flatten`. Escalate to: [../map.md](../map.md).
