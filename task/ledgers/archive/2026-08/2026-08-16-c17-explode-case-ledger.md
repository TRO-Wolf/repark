# Unit ledger — U-DF-1 string-form explode case-loss

**Unit:** U-DF-1 · **Date:** 2026-08-16 · **Lane:** conductor-17 ·
**Executor:** Grok (grok-4.6) · **Worktree:** `/tmp/grok-c17` ·
**Branch:** `grok/c17-explode-case` ·
**Base (FROZEN):** origin/main at #152 (v0.3.1 release truth-up)

**Charter:** string-form `F.explode` / `F.explode_outer` on a createDataFrame
field whose name is not all-lowercase (`Legs`) failed at generator mid-project
(`Schema error: No field named legs`). Same defect is the `dynamicFlatten`
list pass (`explode(list_field.name)`). Column-form `F.explode(df['Legs'])`
and all-lowercase names already worked.

This unit does **not** edit crates/, lockfiles, the divergence registry, or
SQL-door surfaces.

---

## 1. Proposition ledger

| ID | Proposition | Verdict |
|---|---|---|
| C-001 | The failing native expr is `generator._inner` at `_select_with_generator` mid-project (phase-2 unnest SQL is already quoted). | PROVEN — swap is only that expression |
| C-002 | The generator already carries a single-ident source as `sql_expr` (`quote_column_sql_expr` of the ColumnOrName / `F.col` name). | PROVEN — `explode` / `explode_outer` set `sql_expr=array_column.sql_expr_without_alias()`; alias/cast keep it |
| C-003 | Recovering that ident via `_sql_ident_bare_name` and rebinding with `_bind_schema_column` quotes the canonical engine field. | PROVEN — helper `_bound_generator_array` |
| C-004 | Compound `sql_expr` (`_sql_ident_bare_name` is None) keeps `generator._inner`. | PROVEN — helper guard; existing coalesce/compound pins stay green |
| C-005 | Unresolved names swallow `AnalysisException` and keep `generator._inner` (engine-shaped missing-field error). | PROVEN — helper `except`; hostile `make_array(1,2,3)` pin stays |
| C-006 | `core.py` stays 8199 lines (ceiling 8200, ratchet-down-only). | PROVEN — `wc -l` after the import-line extend + 3272 swap |
| C-007 | No `Column.__slots__` attr; no `functions_expr` body change; crates/ closed. | PROVEN — diff names |
| C-008 | Tests + maps + this ledger land in the same commit. | PROVEN — this commit |

---

## 2. Pins (facade, Arrow value **and** type)

| Pin | File | Claim |
|---|---|---|
| `test_explode_str_capitalized_list_column` | `test_explode_rewrite.py` | `F.explode('Legs')` on createDataFrame |
| `test_explode_outer_str_capitalized_list_column` | same | `explode_outer('Legs')` keeps null/empty |
| `test_explode_str_case_insensitive_capitalized_list` | same | `F.explode('LEGS')` finds `Legs` |
| `test_explode_getitem_capitalized_list_column` | same | `F.explode(df['Legs'])` regression |
| `test_explode_col_capitalized_list_column` | same | `F.explode(F.col('Legs'))` |
| `test_explode_str_absent_column_names_missing` | same | absent name still loud; message names it |
| `test_list_of_struct_capitalized_legs_and_sibling_struct` | `test_dynamic_flatten.py` | `Legs` list-of-struct + sibling `Meta` → `Legs_leg_id` / `Meta_account` |

---

## 3. Fence (diff names)

- `python/repark/src/repark/spark/column.py` — `_bound_generator_array`
- `python/repark/src/repark/spark/dataframe/core.py` — import + mid-project call (net-zero)
- `python/repark/tests/test_explode_rewrite.py`
- `python/repark/tests/test_dynamic_flatten.py`
- `python/repark/tests/map.md`
- `python/repark/src/repark/spark/map.md`
- `python/repark/src/repark/spark/dataframe/map.md`
- `task/c17-explode-case-ledger.md` (this file)
- `task/map.md`

Not touched: `functions_expr.py`, `crates/**`, `Cargo.lock`, `uv.lock`,
divergence-registry docs.

---

## 4. Gates

Recorded after `make verify && make preflight` (chained, logged to a file,
`echo $?` — never piped through tail).

| Gate | Exit |
|---|---|
| `make verify` | 0 |
| `make preflight` | 0 |
| facade pytest (inside preflight) | 3278 passed, 70 skipped |

---

## 5. Provocation

Reverting `_bound_generator_array` to `return generator._inner` must red
`test_explode_str_capitalized_list_column` and
`test_list_of_struct_capitalized_legs_and_sibling_struct`.
