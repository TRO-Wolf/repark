# Unit ledger — FN-C aggregates / window aliases

**Unit:** FN-C · conductor-13 Track T1 · **Date:** 2026-08-15 ·
**Lane:** `/tmp/grok-fnc` · **Executor:** Grok (grok-4.6) ·
**Worktree:** `/tmp/grok-fnc` · **Branch:** `grok/fn-c-aggregates` ·
**Base (FROZEN):** `cd0db4f459e62994b45f8aadd1d5b58f040d90a5`.

**Charter:** `FN-MANIFEST.md` FN-C + conductor-13 Addendum A1–A12 (A7 themed
sibling; A8 window names are ENGINE-WORK). **SEPMO:** octo + C4. Floor S1.
`max_cycles=2`.

Registry / STATUS / lockfiles / `.github` / board / `crates/` closed.

## GO / deferred

| Name | Class | Disposition |
|---|---|---|
| first_value | ALIAS of `first` | SHIPPED |
| last_value | ALIAS of `last` | SHIPPED |
| std | ALIAS of `stddev` | SHIPPED |
| count_if | SHIM `count(when(cond, lit(1)))` | SHIPPED |
| bool_and | SHIM `min` on boolean | SHIPPED — `PyColumn.aggregate` has no `bool_and` arm |
| every | ALIAS of `bool_and` | SHIPPED (landed together) |
| bool_or | SHIM `max` on boolean | SHIPPED — no `bool_or` arm |
| some | ALIAS of `bool_or` | SHIPPED (landed together) |
| sum_distinct / sumDistinct | SHIM | **DEFERRED** — `count_aggregate(..., distinct=True)` is count-only; `aggregate("sum")` has no distinct flag; `PyColumn.sql` cannot bind columns |
| approx_count_distinct / approxCountDistinct | THIN-WIRE | **DEFERRED** — no `approx_distinct` on `PyColumn.aggregate` / `call_scalar`; aliasing `count_distinct` would be a lie |
| lag, lead, nth_value, percent_rank, cume_dist | THIN-WIRE in manifest | **DEFERRED** — A8: PyColumn exposes only `row_number`/`rank`/`dense_rank`/`ntile`; `crates/repark-python` CLOSED |
| any_value, max_by, min_by, product | ENGINE-WORK | **DEFERRED** (charter) |
| grouping, grouping_id, percentile | ENGINE-WORK | **DEFERRED** (charter) |
| window, window_time, session_window | ENGINE-WORK | **DEFERRED** (charter) |

`_PRE_SPLIT_ALL` pin move: 253 → 261 (8 shipped names). Declared in the PR body.

## OCTO

- Risk tier: window/aggregate semantic blast. `max_cycles=2`. `severity_floor=S1`.
  octo + C4.

### Cycle 1

- **Critic-1 quality Q-001 S1:** empty-group behaviour unpinned (`count_if` → 0,
  `bool_and` → NULL). REMEDIATED:
  `test_count_if_empty_group_is_zero`, `test_bool_and_empty_group_is_null`.
- **Critic-1 Q-002 S2:** SHIM display names stay `count(CASE…)` / `min` / `max`
  / `first` / `last`, not Spark `count_if` / `bool_and` / `first_value`.
  ACCEPTED_FLAGGED (below floor; value+type pins are the charter surface; FN-A
  same flag).
- **Critic-2 security:** CLEAN — no `PyColumn.sql` interpolation; shims reuse
  existing `count`/`when`/`min`/`max`/`first`/`last`/`stddev` builders.
- **Critic-3 logic:** CLEAN — `when(cond, lit(1))` makes False/NULL not count;
  boolean `min`/`max` are NULL-skipping AND/OR; first/last ignore-nulls pin is
  the unique-non-null order-independent case.
- **Critic-4 claims C4-001 S1:** deferred names could be silently stubbed.
  REMEDIATED: `test_fn_c_deferred_names_are_absent`. Shipped 8/8 in `__all__`;
  no `crates/` / lockfile / STATUS edits.

### Cycle 2

- Critic-1/2/3/4: CLEAN. Early-stop.

**Label:** `OCTO-CONVERGED`.

## Gates

- `make verify` — exit 0
- `make preflight` — exit 0 (facade pytest **3132 passed, 71 skipped**;
  audit + workflows-lint green). Green-only coverage batch: ruff I001
  (`last_value` before `last_day`; `functions_agg` import order) was
  fixed before the first official verify/preflight.

## Files

- `python/repark/src/repark/spark/functions_agg.py` — defs (new sibling)
- `python/repark/src/repark/spark/functions.py` — late import + `__all__` only
- `python/repark/tests/test_functions_c.py` — Arrow value+type pins
- `python/repark/tests/test_functions_split_identity.py` — pin move
- `python/repark/src/repark/spark/map.md`, `python/repark/tests/map.md`, `task/map.md`

## Mutation-proof pins (name the test that reds if the def is dropped)

| Behavior | Test |
|---|---|
| `first_value` ≡ `first` (ignore-nulls unique non-null) | `test_first_value_alias_of_first` |
| `last_value` ≡ `last` (ignore-nulls unique non-null) | `test_last_value_alias_of_last` |
| `std` ≡ `stddev` (sample stdev of {1,2,3} is 1.0) | `test_std_alias_of_stddev` |
| `count_if` counts True only (False/NULL skipped) | `test_count_if_counts_true_only` |
| `count_if` predicate Column | `test_count_if_accepts_a_predicate_column` |
| `count_if` grouped + select global-agg | `test_count_if_grouped_and_select_path` |
| `bool_and` / `every` ≡ `min` on bool | `test_every_alias_of_bool_and` |
| `bool_and` any-False → False | `test_bool_and_is_false_when_any_false` |
| `bool_or` / `some` ≡ `max` on bool | `test_some_alias_of_bool_or` |
| `bool_or` any-True → True | `test_bool_or_is_true_when_any_true` |
| `count_if` empty group is 0 | `test_count_if_empty_group_is_zero` |
| `bool_and` empty group is NULL | `test_bool_and_empty_group_is_null` |
| deferred names absent (no stubs) | `test_fn_c_deferred_names_are_absent` |
| `__all__` pin 261 | `test_functions_all_matches_pre_split_inventory` |
