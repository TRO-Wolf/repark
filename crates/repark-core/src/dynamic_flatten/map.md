# map — repark-core/src/dynamic_flatten

CC-4 (2026-08-30): remaining banner files condensed to the one-line rule
(pins: cc-3-comment-condensation/C-009).

## Purpose

The `dynamic_flatten` plan-rewrite kernel's one non-test module plus its
file-backed tests (`../dynamic_flatten.rs`): structs first, then lists
one-at-a-time, matching the Python `dynamicFlatten` state machine. Schema-only
walks; the null-mask extractor is the only physical work this directory owns.

## Contents

- `null_mask.rs` — **PERF-DYNFLATTEN-2 (2026-09-04):** the struct-field
  extractor that replaced the per-leaf `CASE WHEN parent IS NULL` on plain
  `Struct` parents. One scalar UDF, `repark_null_mask_field(parent, 'field')`:
  it takes the child array as `get_field` would and unions the parent's
  validity into it with `arrow::compute::nullif`, so the null-parent slots
  cost one bit-and over the validity buffer instead of two full
  `filter_record_batch` copies and a `zip` per leaf per level. Contract:

  | rule | why |
  |---|---|
  | plain `Struct` parents only (`null_mask_extractable`) | a `Dictionary(_, Struct)` parent keeps `null_safe_field`: `get_field` there returns `Dictionary(K, child)` while the typed null is `child`, and the CASE's coercion between them is the shipped output type. The extractor would have to reproduce that coercion to stay byte-identical, and the path is unmeasured. |
  | `placement` stays at the trait default (`KeepInPlace`) | **Measured, not chosen.** Two variants were built and both red `nested_struct_in_struct` — a shape that collects on `main` — with `Optimizer rule 'push_down_leaf_projections' failed … AmbiguousReference { name: "id" }`, which is `DYNFLATTEN-QUALNAME-1` reaching a case it did not reach before: (1) declaring `MoveTowardsLeafNodes` the way `GetFieldFunc` does, and (2) re-expressing the leaf as `mask(get_field(parent, 'x'), parent)` so `get_field` survives in the plan. A `get_field` sitting at the top of a projection expression takes a different route through that rule than the same call buried in a CASE branch, which the rule will not hoist out of a conditional. The CASE was hiding `get_field` from the rule, and the extractor has to hide it too. |
  | the extractor replaces `get_field`, it does not wrap it | consequence of the row above, and the one cost this unit pays: a struct leaf is no longer visible to `push_down_leaf_projections`, so `read.parquet(…).dynamicFlatten().select(one_leaf)` reads the whole parent struct where it could have read one leaf. `dynamicFlatten` expands **every** leaf, so nothing is pruned in the un-projected case, which is what the bed measures; the projected case is unmeasured. Pin `multi_pass_flatten_then_project_survives_leaf_pushdown` (in `../tests.rs`) still passes. |
  | output field is the child field `with_nullable(true)` | a null parent yields a null child, which is what the CASE's null literal branch typed. |
  | the parent's `null_count() == 0` short-circuits to the child | no buffer is allocated when nothing is masked (`struct_*_nonull`). |

  pins: perf-dynflatten-2-null-mask/C-002
- `tests.rs` — Arrow value **and** type pins. The first four are the design
  mutation pins (null-parent CASE with dirty children, in-place column order,
  list-of-struct then unnest, prefixed-name collision) plus Pin-1 list-child
  companion `null_parent_dirty_list_child_is_null_not_exploded` (dirty valid
  List under a null parent). Remaining engine cases plus octo C1 pins:
  ScanForbidden plan-build spy, dotted-name Unnest bind, Dictionary-of-List
  unwrap, dict-struct type + utf8 dict-struct, list-of-map refuse, dirty-child
  mid-struct nulls, ReparkSession wrapper harness, DEFECT-2 flatten-then-project.
- `tests/` — octo C2/C3 pins split out of `tests.rs` (file-size ceiling). See
  [tests/map.md](tests/map.md), including DFP-1 preserve-null plan and value/type pins.

## Pointers

- Up: [../map.md](../map.md)
- Facade contract: `python/repark/tests/test_dynamic_flatten.py`
- Ledger: [../../../../task/df1-rust-flatten-ledger.md](../../../../task/ledgers/archive/2026-08/2026-08-20-df1-rust-flatten-ledger.md)
- DFP-1: [../../../../task/ledgers/completed/dfp-1-preserve-null-unnest-ledger.md](../../../../task/ledgers/archive/2026-08/2026-08-31-dfp-1-preserve-null-unnest-ledger.md)
  pins: dfp-1-preserve-null-unnest/C-007, C-008, C-009, C-010, C-011, C-012
- PERF-DYNFLATTEN-1: `dynamic_flatten_with_stats` counts rewrite passes, schema
  walks, struct expansions, list explodes, and plan-node kinds. Pins in
  [tests/map.md](tests/map.md).
  pins: perf-dynflatten-1-measure/C-002

## Debug

| Symptom | First check |
|---|---|
| Null parent struct yields 0/""/false | The parent's validity is not reaching the leaf — `null_mask.rs` on a plain struct, the CASE `parent IS NULL THEN <typed null>` on a dictionary struct. Pin `null_parent_struct_fields_are_null_not_zero` (dirty children at the parent-null slot). |
| Null parent list explodes 99/100 | The mask was skipped for List children — pin `null_parent_dirty_list_child_is_null_not_exploded` (dirty valid `[99, 100]` under a null parent). |
| Null mid-struct yields 0 | Dirty children at the mid-null and outer-null slots — pin `null_mid_struct_fields_are_null_not_zero`. |
| Column order is survivors-first | Expansion is not in schema field order — pin `unnest_preserves_interleaved_column_order`. |
| `From<&str>` / `col(name)` on a `s.f` / `wrap.nums` column | Every schema field must bind through `Column::new_unqualified` — pin `dotted_list_column_unnest_uses_unqualified_bind`. |
| Empty lists survive `empty_as_null=false` | Unnest drops zero-length arrays regardless of `preserve_nulls` (that flag only keeps NULL lists). `empty_as_null=true` rewrites EMPTY to a singleton-null list first — pin `null_and_empty_array_values`. |
| Null lists trigger a CASE projection | Preserve-null Unnest carries them without rewriting; pins in `tests/preserve_nulls.rs`. |
| `array<map>` explodes | List-of-map must refuse `[DYNAMIC_FLATTEN_UNSUPPORTED_ELEMENT]` — pin `list_of_map_refuses_loud`. |
| ListView leave-nested | ListView / LargeListView refuse `[DYNAMIC_FLATTEN_UNSUPPORTED_ELEMENT]` — pins `list_view_refuses_loud`, `large_list_view_refuses_loud` (default depth, top-level); nested wrap + `max_depth=1`: `nested_list_view_max_depth_one_refuses_loud`, `nested_large_list_view_max_depth_one_refuses_loud`; top-level `max_depth=0`: `top_level_list_view_max_depth_zero_refuses_loud`. |
| LargeList / FixedSizeList stay nested | Both arms of `list_element_type` explode — pins `large_list_explodes`, `fixed_size_list_explodes`. |
| Dictionary list Unnest rejects | Cast Dictionary<_, List> to List before Unnest — pin `dictionary_list_is_unwrapped_one_level`. |

First checks: `cargo test -p repark-core dynamic_flatten`. Escalate to: [../map.md](../map.md).
