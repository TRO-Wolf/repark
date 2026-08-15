# Unit ledger — FN-E collections / higher-order aliases

**Unit:** FN-E · conductor-13 Track T1 · **Date:** 2026-08-15 ·
**Lane:** `/tmp/grok-fnc` · **Executor:** Grok (grok-4.6) ·
**Worktree:** `/tmp/grok-fnc` · **Branch:** `grok/fn-e-collections` ·
**Base (FROZEN):** `cd0db4f459e62994b45f8aadd1d5b58f040d90a5`
(independent of FN-C #115 and FN-D #119).

**Charter:** `FN-MANIFEST.md` FN-E GO-14 + conductor-13 A7/A8.
**SEPMO:** octo + C4. Floor S1. Sequential hat-switch.

Registry / STATUS / lockfiles / `.github` / board / `crates/` closed.

## GO / deferred

| Name | Class | Disposition |
|---|---|---|
| cardinality | ALIAS size | SHIPPED (`_scalar("cardinality")` — already a `call_scalar` alias of `size`) |
| array_agg | ALIAS collect_list | SHIPPED (PySpark synonym; NULL excluded like `collect_list`) |
| array_size | ALIAS size | SHIPPED |
| named_struct | THIN-WIRE | SHIPPED as SHIM via `PyColumn.make_struct` (same DF `named_struct` kernel as `F.struct`). `call_scalar` has no arm. |
| map_from_entries | THIN-WIRE | **DEFERRED** — SQL `map_from_entries` works; `call_scalar` has no arm; no Column helper |
| shuffle | THIN-WIRE | **DEFERRED** — SQL `shuffle` works; `call_scalar` has no arm (A8) |
| create_map | SHIM | **DEFERRED** — `map_from_arrays` Column builder is UOE; no map constructor on `call_scalar` |
| map_contains_key | SHIM | SHIPPED `array_contains(map_keys(m), k)` |
| array_append | SHIM | SHIPPED `flatten(array(arr, array(x)))` + `when(isnull(arr), NULL)`. `F.concat` is string-only (Utf8). |
| array_prepend | SHIM | SHIPPED `flatten(array(array(x), arr))` + NULL-array wrap |
| array_compact | SHIM | **DEFERRED** — `array_except(arr, [NULL])` de-duplicates (not Spark compact); `array_remove(NULL)` returns NULL; `filter` is ENGINE-WORK |
| arrays_overlap | SHIM | SHIPPED `size(array_except(array_intersect(a,b), [NULL])) > 0` (null-only intersection is not overlap) |
| element_at | SEMANTIC-HAZARD | **DEFERRED** — engine `element_at` is SQL-registered (1-based, 0 raises `INVALID_INDEX_OF_ZERO`); `call_scalar` has no arm; 0-based `array_element`/`getitem` cannot honest-shim 0-raise + negative-from-end |
| get | SEMANTIC-HAZARD | SHIPPED `_scalar("getitem")` (0-based array / map-by-key). Pins vs SQL `element_at`. |

Charter ENGINE-WORK (named, not implemented): `map_concat` / `map_filter` / `map_zip_with`,
`transform` / `transform_keys` / `transform_values`, `filter` / `exists` / `forall` /
`aggregate` / `reduce` / `zip_with`, `array_insert`, `inline` / `inline_outer` / `stack`,
`from_json` / `to_json` / `get_json_object` / `json_array_length` / `json_object_keys`.

`_PRE_SPLIT_ALL` pin move: **253 → 262** (9 shipped names). Declared in the PR body.

## Gates

- `make verify` — exit 0 (fmt, clippy, panic-ban, crate-dag, lib-rs, rust-file-size,
  lib-py, manifest, rust tests).
- `make preflight` — exit 0. Facade pytest: **3129 passed, 71 skipped**
  (`test_functions_e.py` + split-identity included). `make audit` +
  `make workflows-lint` green.
- `functions_collections.py` **173 / 2500**. `functions.py` **1816 / 2500**.
  `functions_expr.py` untouched at **1873 / 2500**. No new EXCEPTIONS.

## ACC

- Risk tier: standard. Sequential hat-switch. Floor S1. octo + C4.
- Cycle 1 Critic-1: `F.concat` for array_append would stringify (`[1, 2][4]`). REMEDIATED
  — flatten+array glue + NULL-array `when(isnull)`.
- Cycle 1 Critic-1: `size(array_intersect)` counts `[NULL]` as overlap. REMEDIATED —
  `array_except(..., array(NULL))` before `size > 0`.
- Cycle 1 Critic-1: `array_compact` via `array_except` drops duplicates. HALTED
  (ENGINE-WORK).
- Cycle 1 Critic-1: `element_at` via `n-1` maps index 0 to last / NULL, not
  `INVALID_INDEX_OF_ZERO`. HALTED (`call_scalar` miss; A8).
- Critic-2: CLEAN (no injection; named_struct names escaped for `sql_expr`;
  foldable-name gate).
- C4: 9/9 shipped in `__all__`; deferred absent; no `crates/` / lockfile edits;
  no FN-C/D imports; no new EXCEPTIONS.
- Label: `OCTO-CONVERGED`.

## Files

- `python/repark/src/repark/spark/functions_collections.py` — defs (NEW sibling, A7)
- `python/repark/src/repark/spark/functions.py` — late import + `__all__` only
- `python/repark/tests/test_functions_e.py` — Arrow value+type pins
- `python/repark/tests/test_functions_split_identity.py` — pin move 253→262
- `python/repark/src/repark/spark/map.md`, `python/repark/tests/map.md`, `task/map.md`

## Mutation-proof pins (name the test that reds if the def is dropped)

| Behavior | Test |
|---|---|
| `cardinality` / `array_size` value+type ≡ `size` | `test_cardinality_and_array_size_alias_of_size` |
| `array_agg` ≡ `collect_list` (NULL excluded) | `test_array_agg_alias_of_collect_list` |
| `named_struct` fields + Arrow struct | `test_named_struct_fields_value_and_type` |
| `named_struct` odd-length refuse | `test_named_struct_rejects_odd_length` |
| `map_contains_key` hit/miss | `test_map_contains_key` |
| `array_append` / `array_prepend` values | `test_array_append_and_prepend` |
| NULL array / NULL element | `test_array_append_null_array_and_null_element` |
| `arrays_overlap` + nulls-only False | `test_arrays_overlap` |
| `get` 0-based vs SQL `element_at` 1-based / 0 raises | `test_get_is_zero_based_vs_sql_element_at` |
| `get` map-by-key | `test_get_map_by_key` |
