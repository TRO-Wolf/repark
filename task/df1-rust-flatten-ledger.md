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
| C-007 | Errors are `Error::Analysis` with tokens `[DYNAMIC_FLATTEN_NAME_COLLISION]`, `[DYNAMIC_FLATTEN_MAX_DEPTH]`, `[DYNAMIC_FLATTEN_EMPTY_STRUCT]`. | PROVEN |
| C-008 | Facade is type-gates + `_plan().dynamic_flatten(...)` + `_spawn`. | PROVEN |
| C-009 | Python tests unchanged except a native-planner comment. | PROVEN |
| C-010 | `empty_as_null` is on `DynamicFlattenOptions` (omitted from the API sketch; required by the facade contract). | PROVEN |
| C-011 | `core.py` ceiling ratcheted DOWN (7350 → 7225; measured 7191). | PROVEN |
| C-012 | `dataframe.rs` stays under 1500 (measured 1456). | PROVEN |

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
- `python/repark/tests/test_dynamic_flatten.py` (comment only)
- `python/repark/tests/map.md`
- `scripts/check_lib_py.py` (ceiling DOWN)
- `task/df1-rust-flatten-ledger.md` (this file)
- `task/map.md`

---

## 4. Gates

| Gate | Exit |
|---|---|
| `cargo test -p repark-core dynamic_flatten` | 0 (27 passed) |
| `make verify` | 0 |
| `make develop` + pytest `python/repark/tests/test_dynamic_flatten.py` | 0 (38 passed) |

---

## 5. Notes

- Test harness uses `SessionContext` with
  `enable_leaf_expression_pushdown = false`. Stock `SessionContext::new()` leaves
  DF-54.1 `push_down_leaf_projections` on, which miscompiles Unnest+`get_field`
  (DEFECT-2). `ReparkSession` wraps that rule; this kernel harness is the
  DataFusion DataFrame API.
- `preserve_nulls: false` **does** drop empty lists (pin decides: no extra
  `array_length` filter).
- Maps are not unnested. Dictionary of Struct/List unwraps one level so the
  walk does not skip Parquet dict-structs.
- `UnnestSafeLeafProjectionPushdown` now owns the inner rule's TopDown walk
  (`apply_order = None`). Per-node `rewrite` missed `Unnest` child reconstruction
  errors on native Unnest+`get_field` plans. Existing DEFECT-2 pins stay green.
