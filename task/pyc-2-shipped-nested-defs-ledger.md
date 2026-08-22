# PYC-2 — remaining shipped nested defs

**Unit:** PYC-2
**Branch:** `feat/pyc-2-shipped-nested-defs`
**Base:** `origin/main` @ `97d93a8` (#206)
**Date:** 2026-08-22
**Path:** LIGHT-leaning STANDARD (pure refactor, LRS, ten shipped modules)
**Approval:** user "sync to remote main, then review briefs/next-sequence.md and let's get started"

The ordered queue is [briefs/next-sequence.md](../briefs/next-sequence.md). PYC-2 is the
remaining shipped nested-`def` burn-down. Campaign invariant: **no call that worked
before returns a different value.**

---

## Domain (gate-counted 14, plus one ancestor-set extra)

| File | Site | Disposition |
|---|---|---|
| `dataframe/joins_columns.py` | `_grouped_agg_func`, `_arrow_grouped_func` | lift + `functools.partial` |
| `session/session_core.py` | `_as_int`, `_clause_end_after` | lift to `_coerce.py` (file is on the 2500-line default ceiling) |
| `session/session_core.py` | `probe` under `if prefer_temp_view` | lift (`functools.partial(_temp_view_home_ref, inner)`). Gate-invisible (parent is `If`); PYC-1 emptied the ancestor set too |
| `session/_funcs.py` | `int_size_to_ok` | inline (`bit_width <= 64`) |
| `session/_funcs.py` | `_drop_view` | lift; `weakref.finalize(frame, func, session, name)` extra-args |
| `udtf.py` | `_map_batches` | lift + `functools.partial` |
| `udtf.py` | `_build` | **pragma** (decorator factory closes over `returnType`). Redundant `_decorator` wrapper deleted (`return _build`) |
| `types.py` | `verifier` | **pragma** (the closure is the function's product) |
| `functions.py` | `kind` | lift as `_lit_list_item_kind` |
| `polars.py` | `quote_ident` | lift as `_quote_join_ident` |
| `row.py` | `convert` | lift as `_convert_nested_row_value` (accumulator / recursive) |
| `ml/ext/_arrow_util.py` | `_drop` | lift; finalize extra-args |
| `ml/feature/_transformers.py` | `rec` | lift as `_polynomial_expansion_monomials` (depth bound 3) |

EXCEPTIONS rows for all ten files **deleted**, not zeroed. Remaining nested-def rows: **9**
(parity harness + `scripts/`). Dataclass rows unchanged at 23.

## Pins

- `python/repark/tests/test_pyc_2_nested_defs.py` — ancestor-walk emptiness on the eight
  lifted modules; pragma identity on `types.verifier` and `udtf._build`; EXCEPTIONS keys
  gone; CDF/ext finalize extra-args form.
- Behaviour stays on existing tests: `test_applyinpandas`, `test_pandas_udf`, `test_row`,
  `test_session_range`, `test_udtf`, `test_polars_core`, `test_f1_errorclass`,
  `test_f2_fail_value`, `test_e2_readwriter`, `test_create_dataframe_materialize`,
  `test_ml_feature_oracle` polynomial.

## Bait

Empty `# nested-def:` reason on `types.py` `verifier` →
`check_python_conventions.py` exit 1 (`defines 1 nested function(s) (ceiling 0)`).
Restored; gate green.

## Gate

`make verify` exit 0. One unrelated iceberg timing pin
(`listing_cost_list_tables_cheaper_than_provider_rebuild`) flaked on the first
run (10.5 ms vs 2.5 ms) and passed on retry; not this unit.

`make preflight` exit 0 immediately before the PR: **3659 passed, 70 skipped,
0 failed** facade (`python/repark/tests`). python-conventions: 162 files,
9 nested-def rows, 23 dataclass rows. lib-py clean. audit + workflows-lint
clean. The wrapper `test_compat_smoke_suite_in_subprocess` skipped (no
`record` extra in preflight — expected).

## Ceilings

`session_core.py` 2500 → 2482 (helpers live in `_coerce.py`). `_funcs.py` 8396 → 8390
(`int_size_to_ok` inlined). `_transformers.py` 2734 → 2763 (under 2800).
