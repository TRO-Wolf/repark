# Unit ledger — DF1 native `dynamic_flatten`

**Unit:** DF1 · **Date:** 2026-08-19 · **Lane:** grok/df1-rust-flatten ·
**Executor:** Grok (Actor) · **Worktree:** `/tmp/grok-df1` ·
**Branch:** `grok/df1-rust-flatten` ·
**Base:** origin/main @ `0f6aa2c`

**Charter:** port r24 DF1 `DataFrame.dynamicFlatten` from the Python Spark facade
into a Rust plan-rewrite kernel. Same observable contract as
`python/repark/tests/test_dynamic_flatten.py`. No new physical operator. No
RePark DataFrame newtype.

---

## 1. Proposition ledger

| ID | Proposition | Verdict |
|---|---|---|
| C-001 | Kernel lives in `crates/repark-core/src/dynamic_flatten.rs` + file-backed tests + `dynamic_flatten/map.md`. | PROVEN |
| C-002 | `lib.rs` re-exports `dynamic_flatten` and `DynamicFlattenOptions`; crate root stays under 150. | PROVEN — measured 101 |
| C-003 | Algorithm: structs first (always), then lists one-at-a-time in schema order; list-of-struct becomes same-name struct. | PROVEN — pins 2 and 3 |
| C-004 | Struct expand is Project + null-safe CASE + `get_field`; not DF struct `unnest_columns`. | PROVEN — pin 1 reds if CASE is removed |
| C-005 | List explode uses `UnnestOptions { preserve_nulls: false }` via `unnest_columns_with_options` + `Column::new_unqualified`. Empty lists drop under that option (no `array_length` filter needed). | PROVEN — `null_and_empty_array_values` |
| C-006 | Every schema field binds through `Column::new_unqualified`. | PROVEN — kernel; `s.f` pin |
| C-007 | Errors are `Error::Analysis` with tokens `[DYNAMIC_FLATTEN_NAME_COLLISION]`, `[DYNAMIC_FLATTEN_MAX_DEPTH]`, `[DYNAMIC_FLATTEN_EMPTY_STRUCT]`, `[DYNAMIC_FLATTEN_UNSUPPORTED_ELEMENT]`. | PROVEN |
| C-008 | Facade is type-gates + `_plan().dynamic_flatten(...)` + `_spawn`. | PROVEN |
| C-009 | Python tests remain the facade contract; octo C1 added native-kernel docstring / list-of-map / dotted-Unnest pins. | PROVEN |
| C-010 | `empty_as_null` is on `DynamicFlattenOptions` (omitted from the API sketch; required by the facade contract). | PROVEN |
| C-011 | `core.py` ceiling ratcheted DOWN (7350 → 7225; measured 7191). | PROVEN |
| C-012 | `dataframe.rs` stays under 1500 (measured 1457). | PROVEN |

---

## 2. Pins

First four (design mutation pins):

| Pin | Claim |
|---|---|
| `null_parent_struct_fields_are_null_not_zero` | Null parent → NULL leaves, not 0/""/false |
| `unnest_preserves_interleaved_column_order` | `z, a_x, a_y, m` — not survivors-first |
| `list_of_struct_explodes_then_unnests` | `legs` → `legs_leg_id`, `legs_side` |
| `prefixed_name_collision_with_top_level_refuses` | `a_x` + `a.x` refuses LOUD |

Remaining engine cases live in `crates/repark-core/src/dynamic_flatten/tests.rs`.
Facade contract: `python/repark/tests/test_dynamic_flatten.py` (unchanged assertions).

---

## 3. Fence (diff names)

- `crates/repark-core/src/dynamic_flatten.rs`
- `crates/repark-core/src/dynamic_flatten/tests.rs`
- `crates/repark-core/src/dynamic_flatten/map.md`
- `crates/repark-core/src/lib.rs`
- `crates/repark-core/src/map.md`
- `crates/repark-core/map.md`
- `crates/repark-python/src/dataframe.rs`
- `crates/repark-python/src/map.md`
- `crates/repark-python/map.md`
- `python/repark/src/repark/spark/dataframe/core.py`
- `python/repark/src/repark/spark/dataframe/plan_collapse.py`
- `python/repark/src/repark/spark/dataframe/map.md`
- `python/repark/src/repark/spark/map.md`
- `python/repark/tests/test_dynamic_flatten.py`
- `python/repark/tests/map.md`
- `scripts/check_lib_py.py` (ceiling DOWN)
- `scripts/map.md`
- `crates/repark-core/src/session/df_guards.rs`
- `crates/repark-core/src/session/map.md`
- `task/df1-rust-flatten-ledger.md` (this file)
- `task/map.md`

---

## 4. Gates

| Gate | Exit |
|---|---|
| `cargo test -p repark-core dynamic_flatten` | 0 (32 passed; octo C1) |
| `make verify` | 0 (octo C1) |
| `make develop` + pytest `python/repark/tests/test_dynamic_flatten.py` | 0 (40 passed; octo C1) |

---

## 5. Notes

- Kernel harness uses `ReparkSession` (leaf pushdown ON, wrapped by
  `UnnestSafeLeafProjectionPushdown`). Pin: `kernel_harness_installs_unnest_safe_leaf_pushdown`.
  Disabling the flag instead of installing the wrapper is the blanket skip the
  session pins forbid.
- `preserve_nulls: false` **does** drop empty lists (pin decides: no extra
  `array_length` filter).
- Maps are not unnested. List-of-map refuses LOUD
  (`[DYNAMIC_FLATTEN_UNSUPPORTED_ELEMENT]`). Dictionary of Struct/List unwraps
  one level so the walk does not skip Parquet dict-structs; dict-lists are
  **cast** to List before Unnest.
- `UnnestSafeLeafProjectionPushdown` now owns the inner rule's TopDown walk
  (`apply_order = None`). Per-node `rewrite` missed `Unnest` child reconstruction
  errors on native Unnest+`get_field` plans. Existing DEFECT-2 pins stay green.

---

## 6. Octo cycle 1 (Half B)

| ID | Disposition | Pin / evidence |
|---|---|---|
| C1-Q-001 | REMEDIATED | `test_dynamic_flatten_docstring_describes_native_kernel` |
| C1-Q-002 | REMEDIATED | `list_of_map_refuses_loud` + `test_dynamic_flatten_map_element_still_refuses_loud` (`dynamicFlatten`) |
| C1-Q-003 | REMEDIATED | `plan_build_does_not_execute` (`ScanForbidden`) |
| C1-SEC-001 | REMEDIATED | `dotted_list_column_unnest_uses_unqualified_bind` + `test_custom_separator_list_column_unnest` |
| C1-L-001 | REMEDIATED | `null_parent_struct_fields_are_null_not_zero` (dirty children) |
| C1-L-002 | REMEDIATED | GA4 docstring no longer claims the guard; kernel `multi_pass_flatten_then_project_survives_leaf_pushdown` |
| C1-L-003 | REMEDIATED | `kernel_harness_installs_unnest_safe_leaf_pushdown` |
| C1-L-004 | REMEDIATED | `dictionary_list_is_unwrapped_one_level` |
| C1-CL-001 | REMEDIATED | ledger link `../../../../task/…` |
| C1-CL-002 | REMEDIATED | fence lists `scripts/map.md`, `df_guards.rs`, `session/map.md` |
| C1-CL-003 | REMEDIATED | spark `map.md` region text |
| C1-CL-004 | REMEDIATED | `Authored-By: Grok (grok-4.6)` on this commit |
| C1-CL-005 | DEFERRED | skip (historical c25 ledger) |
| C1-CL-006 | REMEDIATED | `task/map.md` Live unit ledgers row |
