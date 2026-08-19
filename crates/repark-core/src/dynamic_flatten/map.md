# map — repark-core/src/dynamic_flatten

## Purpose

File-backed tests for the `dynamic_flatten` plan-rewrite kernel
(`../dynamic_flatten.rs`): structs first, then lists one-at-a-time, matching
the Python `dynamicFlatten` state machine. Schema-only walks; no physical
operator.

## Contents

- `tests.rs` — Arrow value **and** type pins. The first four are the design
  mutation pins (null-parent CASE with dirty children, in-place column order,
  list-of-struct then unnest, prefixed-name collision). Remaining engine cases
  plus octo C1 pins: ScanForbidden plan-build spy, dotted-name Unnest bind,
  Dictionary-of-List unwrap, list-of-map refuse, ReparkSession wrapper harness,
  DEFECT-2 flatten-then-project.

## Pointers

- Up: [../map.md](../map.md)
- Facade contract: `python/repark/tests/test_dynamic_flatten.py`
- Ledger: [../../../../task/df1-rust-flatten-ledger.md](../../../../task/df1-rust-flatten-ledger.md)

## Debug

| Symptom | First check |
|---|---|
| Null parent struct yields 0/""/false | The CASE `parent IS NULL THEN <typed null>` was dropped — pin `null_parent_struct_fields_are_null_not_zero` (dirty children at the parent-null slot). |
| Column order is survivors-first | Expansion is not in schema field order — pin `unnest_preserves_interleaved_column_order`. |
| `From<&str>` / `col(name)` on a `s.f` / `wrap.nums` column | Every schema field must bind through `Column::new_unqualified` — pin `dotted_list_column_unnest_uses_unqualified_bind`. |
| Empty lists survive `empty_as_null=false` | `UnnestOptions { preserve_nulls: false }` did not drop them — add the `array_length` filter fallback. |
| `array<map>` explodes | List-of-map must refuse `[DYNAMIC_FLATTEN_UNSUPPORTED_ELEMENT]` — pin `list_of_map_refuses_loud`. |
| Dictionary list Unnest rejects | Cast Dictionary<_, List> to List before Unnest — pin `dictionary_list_is_unwrapped_one_level`. |

First checks: `cargo test -p repark-core dynamic_flatten`. Escalate to: [../map.md](../map.md).
