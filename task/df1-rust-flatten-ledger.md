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
| C-002 | `lib.rs` re-exports `dynamic_flatten` and `DynamicFlattenOptions`; crate root stays under 150. | PROVEN — measured 101 (`splitlines` / `wc -l`; C2-CL-002 auditor 102 was off-by-one) |
| C-003 | Algorithm: structs first (always), then lists one-at-a-time in schema order; list-of-struct becomes same-name struct. | PROVEN — pins 2 and 3 |
| C-004 | Struct expand is Project + null-safe CASE + `get_field`; not DF struct `unnest_columns`. | PROVEN — pin 1 reds if CASE is removed |
| C-005 | List explode uses `UnnestOptions { preserve_nulls: false }` via `unnest_columns_with_options` + `Column::new_unqualified`. Empty arrays drop because Unnest emits no rows for zero-length lists (empty ≠ null); `preserve_nulls` only keeps NULL lists. `empty_as_null=true` rewrites EMPTY to a singleton-null list so the row survives. | PROVEN — `null_and_empty_array_values` |
| C-006 | Every schema field binds through `Column::new_unqualified`. | PROVEN — kernel; `s.f` pin |
| C-007 | Errors are `Error::Analysis` with tokens `[DYNAMIC_FLATTEN_NAME_COLLISION]`, `[DYNAMIC_FLATTEN_MAX_DEPTH]`, `[DYNAMIC_FLATTEN_EMPTY_STRUCT]`, `[DYNAMIC_FLATTEN_UNSUPPORTED_ELEMENT]`. | PROVEN |
| C-008 | Facade is type-gates + `_plan().dynamic_flatten(...)` + two-path spawn (`_spawn_preserving_identity` iff ordered engine field names unchanged, else `_spawn`). | PROVEN |
| C-009 | Python tests remain the facade contract; octo C1/C2 added native-kernel docstring, list-of-map, dotted-Unnest, empty-struct, and tightened token-regex pins. | PROVEN |
| C-010 | `empty_as_null` is on `DynamicFlattenOptions` (omitted from the API sketch; required by the facade contract). | PROVEN |
| C-011 | `core.py` ceiling ratcheted DOWN (7350 → 7225; measured 7192 via `splitlines()` / `wc -l`; C3-CL-001 auditor 7191 was off-by-one vs last line). | PROVEN |
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
Facade contract: `python/repark/tests/test_dynamic_flatten.py` (C1/C2 added pins;
token regexes require the bracketed `[DYNAMIC_FLATTEN_*]` tokens).

H1 two-path spawn (filed finding: already-flat `dynamicFlatten` dropped identity maps):

| Pin | Claim |
|---|---|
| `test_already_flat_h1_join_preserves_display_overlay` | Already-flat H1 join keeps Spark-legal duplicate display names, `to_arrow` overlay, and origin-qualified `select(left["b"])`. Reverting the preserve branch reds this. |
| `test_expanding_h1_flatten_drops_stale_overlay` | One-field struct expand (`payload{x}` → `payload_x`, count-stable) does **not** restuck parent display names. Always-preserve reds this. |

---

## 3. Fence (diff names)

- `crates/repark-core/src/dynamic_flatten.rs`
- `crates/repark-core/src/dynamic_flatten/tests.rs`
- `crates/repark-core/src/dynamic_flatten/tests/octo.rs`
- `crates/repark-core/src/dynamic_flatten/tests/map.md`
- `crates/repark-core/src/dynamic_flatten/map.md`
- `crates/repark-core/src/lib.rs`
- `crates/repark-core/src/map.md`
- `crates/repark-core/map.md`
- `crates/repark-python/src/dataframe.rs`
- `crates/repark-python/src/map.md`
- `crates/repark-python/map.md`
- `python/repark/src/repark/spark/dataframe/core.py`
- `python/repark/src/repark/spark/dataframe/map.md`
- `python/repark/src/repark/spark/dataframe/plan_collapse.py`
- `python/repark/src/repark/spark/dataframe/map.md`
- `python/repark/src/repark/spark/map.md`
- `python/repark/src/repark/spark/functions_expr.py` (`_explode_keep_null` constructor deleted)
- `python/repark/tests/test_dynamic_flatten.py`
- `python/repark/tests/map.md`
- `scripts/check_lib_py.py` (ceiling DOWN)
- `scripts/map.md`
- `crates/repark-core/src/session/df_guards.rs`
- `crates/repark-core/src/session/df_guard_tests.rs`
- `crates/repark-core/src/session/map.md`
- `task/df1-rust-flatten-ledger.md` (this file)
- `task/map.md`

---

## 4. Gates

| Gate | Exit |
|---|---|
| `cargo test -p repark-core dynamic_flatten` | 0 (41 passed; R-S1-003) |
| `make verify` | 0 (R-S3-H1) |
| pytest `python/repark/tests/test_dynamic_flatten.py` | 0 (43 passed; R-S3 two-path spawn pins) |

---

## 5. Notes

- Kernel harness uses `ReparkSession` (leaf pushdown ON, wrapped by
  `UnnestSafeLeafProjectionPushdown`). Pin: `kernel_harness_installs_unnest_safe_leaf_pushdown`.
  Disabling the flag instead of installing the wrapper is the blanket skip the
  session pins forbid.
- Empty arrays drop because Unnest emits no rows for zero-length lists
  (independent of `preserve_nulls`, which only keeps NULL lists). Pin decides:
  no extra `array_length` filter. `empty_as_null=true` rewrites EMPTY first.
- Maps are not unnested. List-of-map refuses LOUD
  (`[DYNAMIC_FLATTEN_UNSUPPORTED_ELEMENT]`). ListView / LargeListView refuse
  the same token. Dictionary of Struct/List unwraps one level so the walk does
  not skip Parquet dict-structs; dict-lists are **cast** to List before Unnest.
- `UnnestSafeLeafProjectionPushdown` `apply_order` is `None` (owns the walk so
  a `Projection` under `Unnest` cannot bypass decline). Swallow is the Unnest
  *path* (this node / ancestor / subtree), not whole-plan. Mixed-plan
  non-`Unnest` sibling inner Err stays loud
  (`mixed_plan_non_unnest_inner_error_stays_loud`).
- `explode_lists_in_schema_order` explodes **every** list column in one pass
  (list-of-struct becomes a same-name struct while sibling lists still explode).
  Matches the Python state machine (core.py algorithm step 3 / kernel docstring).
- `max_depth` bounds rewrite passes, not row cartesian / schema width.
- `_explode_keep_null` deleted: unused leftover after the native kernel.

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

---

## 7. Octo cycle 2 (Half B)

| ID | Disposition | Pin / evidence |
|---|---|---|
| C2-Q-001 | REMEDIATED | `apply_order=None` owns the walk; swallow is the Unnest *path* (node / ancestor / subtree), not whole-plan. Pin `mixed_plan_non_unnest_inner_error_stays_loud`. |
| C2-Q-002 | REMEDIATED | `test_empty_struct_only_schema_refuses_loud`; collision/max_depth match `\[DYNAMIC_FLATTEN_*\]`. |
| C2-Q-003 | REMEDIATED | `large_list_explodes` + `fixed_size_list_explodes`. |
| C2-L-001 | REMEDIATED | `null_mid_struct_fields_are_null_not_zero` dirty children (`x=Some(0)` under null outer / null mid). |
| C2-Q-004 | REMEDIATED | Deleted unused `_explode_keep_null` (no callers after native kernel). |
| C2-SEC-001 | WITHDRAWN | Pre-existing `col()` dotted-name parse (`dataframe.rs` `distinct_on` / `PyColumn::column`); not a flatten kernel defect. Kernel Unnest already `Column::new_unqualified`. Facade collect after dotted flatten already pinned (`test_custom_separator`, `test_custom_separator_list_column_unnest`). |
| C2-SAF-001 | WITHDRAWN | `max_depth` is a rewrite-pass counter, not a row-cartesian / schema-width limiter. Out of charter to add a memory limiter. Docstring sentence added on `DynamicFlattenOptions.max_depth` and the facade method. |
| C2-SAF-002 | REMEDIATED | `format_fields` truncates at 240 chars; pin `max_depth_remaining_schema_is_truncated`. |
| C2-L-002 | REMEDIATED | dict-struct asserts exact `Int64`; `dictionary_utf8_struct_is_unwrapped_one_level`. |
| C2-L-003 | REMEDIATED | Ledger C-005 + map.md: Unnest drops empty arrays regardless of `preserve_nulls`. |
| C2-L-004 | REMEDIATED | ListView / LargeListView refuse `[DYNAMIC_FLATTEN_UNSUPPORTED_ELEMENT]`; pin `list_view_refuses_loud`. |
| C2-L-005 | WITHDRAWN | Contract: `explode_lists_in_schema_order` explodes every list in one pass; list-of-struct becomes a same-name struct while sibling lists still explode. Quoted from kernel docstring + facade `core.py` algorithm step 3 (Python loop / now-Rust). |
| C2-CL-001 | REMEDIATED | C-005 cause rewritten (empty ≠ null). |
| C2-CL-002 | REMEDIATED | Re-measured `lib.rs` 101 (`splitlines` / `wc -l`); auditor 102 was off-by-one. |
| C2-CL-003 | REMEDIATED | Pins section no longer says Python tests "(unchanged assertions)". |

---

## 8. Octo cycle 3 (Half B)

| ID | Disposition | Pin / evidence |
|---|---|---|
| C3-Q-001 | REMEDIATED | `large_list_view_refuses_loud` — `LargeListViewArray` + `[DYNAMIC_FLATTEN_UNSUPPORTED_ELEMENT]` (same token as `list_view_refuses_loud`). |
| C3-L-001 | REMEDIATED | Same pin as C3-Q-001. |
| C3-SEC-001 | WITHDRAWN | Flatten-emitted dotted names reparsed by `distinct_on` `col()` / filter SQL are pre-existing ident parsers; kernel Unnest bind is already `Column::new_unqualified`. Do not rewrite `distinct_on`. |
| C3-SAF-001 | REMEDIATED | `format_fields` streams Debug into a 240-char writer (code-path). Pin `max_depth_remaining_schema_is_truncated` kills unbounded output (C2-SAF-002: token / `"truncated"` / len) — it does **not** kill the allocation path (join-then-truncate of a full dump would still pass). |
| C3-L-002 | WITHDRAWN | `mixed_plan` pin uses `BoomOnFilter`, not a real leaf-pushdown success-wrong rewrite. Residual oracle; DF-54.1 Unnest `with_new_exprs` still Err so the clone is kept. |
| C3-L-003 | REMEDIATED | `null_mid_struct_fields_are_null_not_zero` asserts exact `[None, Some(9), None]`. |
| C3-CL-001 | REMEDIATED | Re-measured `core.py` 7192 (`splitlines()` / `wc -l`); ledger C-011 + `check_lib_py.py` comment. |
| C3-CL-002 | REMEDIATED | `test_dynamic_flatten_plan_build_does_not_force_collect` docstring cites C1-Q-003 (not C2-Q-003). |
| C3-CL-003 | REMEDIATED | Fence lists `crates/repark-core/src/session/df_guard_tests.rs`. |
| C3-CL-004 | REMEDIATED | `crates/repark-core/src/map.md` cites `dynamic_flatten/tests/octo.rs`. |

---

## 9. S1 review (comment honesty)

| ID | Disposition | Pin / evidence |
|---|---|---|
| R-S1-001 | REMEDIATED | Facade `test_null_parent` / `test_null_mid` docstrings pin createDataFrame-door NULL leaves (Python `None`, clean children). They do not claim C1-L-001 CASE-drop; dirty-child CASE-drop stays the engine pins. |
| R-S1-002 | REMEDIATED | Pin comment + this ledger's C3-SAF-001: `max_depth_remaining_schema_is_truncated` kills unbounded output (C2-SAF-002: token / `"truncated"` / len), not the allocation path. |
| R-S1-003 | REMEDIATED | Nested struct-wrap `ListView` / `LargeListView` + `max_depth=1` refuse `[DYNAMIC_FLATTEN_UNSUPPORTED_ELEMENT]` (`nested_list_view_max_depth_one_refuses_loud`, `nested_large_list_view_max_depth_one_refuses_loud`). Top-level `ListView` + `max_depth=0`: `top_level_list_view_max_depth_zero_refuses_loud`. Existing `list_view_refuses_loud` stays default-depth top-level. |
| G3b | REMEDIATED | `test_dynamic_flatten_array_of_struct_inside_array_element_struct`: flatten is native Unnest; `_sql_array_of` postfix mutant only kills `explode_outer`. |

---

## 10. S2 review

| ID | Disposition | Pin / evidence |
|---|---|---|
| R-S2-fence | REMEDIATED | Fence lists `python/repark/src/repark/spark/functions_expr.py` (`_explode_keep_null` constructor deleted, C2-Q-004). |

---

## 11. S3 review (H1 already-flat spawn)

Filed finding: already-flat `dynamicFlatten` always `_spawn`'d and dropped H1
`_display_names` / `_engine_names` / `_origin_map`. Two-path spawn matches
`filter` / `limit` / `sort` / `distinct`: preserve iff ordered engine field names
are unchanged; expanding rewrites keep `_spawn` so a copied overlay cannot zip
stale parent names onto prefixed leaves (one-field struct count-stable case).

| ID | Disposition | Pin / evidence |
|---|---|---|
| R-S3-H1-flat | REMEDIATED | `test_already_flat_h1_join_preserves_display_overlay` |
| R-S3-H1-expand | REMEDIATED | `test_expanding_h1_flatten_drops_stale_overlay` |
| C-008 | REMEDIATED | Proposition restated to the two-path spawn. |
