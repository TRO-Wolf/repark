# Unit ledger — FN-W window functions

**Unit:** FN-W · conductor-14 Track T2 · **Date:** 2026-08-15 ·
**Lane:** `/tmp/grok-fnw` · **Executor:** Grok (grok-4.6) ·
**Worktree:** `/tmp/grok-fnw` · **Branch:** `grok/fn-w-window-fns` ·
**Base (FROZEN):** `a5d2d98a449815891016923594c8b1dcd4ae3b43`
(origin/main #130; workspace version 0.2.0).

**Charter:** conductor-13 A8 ENGINE-WORK window names + conductor-14 addendum
A4 (Rust grant = `crates/repark-python/src/column.rs` only). **SEPMO:**
single-agent. Floor S1.

Registry / STATUS / lockfiles / `.github` / board / `Cargo.toml [patch]`
closed. Rest of `crates/repark-python` closed.

## GO / deferred

| Name | Class | Disposition |
|---|---|---|
| lag | THIN-WIRE DF `lag_udwf` | **SHIPPED** — no IntegerType cast; Python `lag(col, offset=1, default=None)` |
| lead | THIN-WIRE DF `lead_udwf` | **SHIPPED** — no IntegerType cast; Python `lead(col, offset=1, default=None)` |
| nth_value | THIN-WIRE DF `nth_value_udwf` | **SHIPPED** — Spark 1-based; no IntegerType cast |
| percent_rank | THIN-WIRE DF `percent_rank_udwf` | **SHIPPED** — already Float64; no i32 cast |
| cume_dist | THIN-WIRE DF `cume_dist_udwf` | **SHIPPED** — already Float64; no i32 cast |
| ignoreNulls=True | PySpark kwarg | **HONEST CUT BY NAME** — DF UDWFs have an `ignore_nulls` evaluator flag, but `WindowFunction` NullTreatment is not wired here; do not claim the kwarg |
| remaining A8 residuals | — | stay with FN-C (`sum_distinct` / `approx_count_distinct` / ENGINE-WORK aggs) |

`_PRE_SPLIT_ALL` pin move: 291 → 296 (5 shipped names). Declared in the PR body.

`column.rs` measured after rustfmt: **2200 / 2200** (ceiling). All five names
fit; no EXCEPTIONS raise; no multi-file pymethods split.

## Files

- `crates/repark-python/src/column.rs` — `window_udwf` + five `#[staticmethod]` arms
- `crates/repark-python/src/map.md` — FN-W row
- `python/repark/src/repark/spark/functions_window.py` — Python defs
- `python/repark/src/repark/spark/functions.py` — late import + `__all__` only
- `python/repark/tests/test_functions_w.py` — Arrow value+type pins
- `python/repark/tests/test_functions_split_identity.py` — pin move
- `python/repark/tests/test_functions_c.py` — one-line note; five names leave the absence list
- `python/repark/src/repark/spark/map.md`, `python/repark/tests/map.md`, `task/map.md`

## Mutation-proof pins

| If this is dropped… | this test reds |
|---|---|
| `F.lag` / first-row NULL + type preserve | `test_lag_default_offset_first_row_is_null` |
| `F.lead` / last-row NULL | `test_lead_default_offset_last_row_is_null` |
| explicit default | `test_lag_and_lead_explicit_default` |
| NULL source row | `test_lag_lead_null_source_row_is_returned` |
| string type preserve (no i32 cast) | `test_lag_preserves_string_input_type` |
| `nth_value` 1-based | `test_nth_value_is_one_based` |
| `percent_rank` Float64 | `test_percent_rank_is_float64` |
| `cume_dist` Float64 | `test_cume_dist_is_float64` |
| `ignoreNulls` silently accepted | `test_ignore_nulls_is_not_a_parameter` |
| `__all__` pin 296 | `test_functions_all_matches_pre_split_inventory` |

## Gates

- `make verify` — exit 0
- `make develop` — exit 0 (fresh maturin develop into project `.venv`)
- `make py-test-facade` — exit 0 (**3224 passed, 71 skipped**)
- `make preflight` — exit 0 (verify + facade 3224/71 + cargo-deny/pip-audit +
  workflows-parse + zizmor)
