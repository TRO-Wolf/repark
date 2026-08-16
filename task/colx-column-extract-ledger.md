# Unit ledger — COLX column helper extract

**Unit:** COLX · conductor-15 Track T3 · **Date:** 2026-08-15 ·
**Lane:** `/tmp/grok-colx` · **Executor:** Grok (grok-4.6) ·
**Worktree:** `/tmp/grok-colx` · **Branch:** `grok/colx-column-extract` ·
**Base (FROZEN):** `8cbde88bb076cbf09976fa0bfbc702472f267fca`

**Charter:** conductor-15 T3 + A8/A12. Convert
`crates/repark-python/src/column.rs` (2200/2200) into `column/mod.rs` +
`column/window.rs` + `column/expr_build.rs`. `#[pymethods]` stays in
`mod.rs`. PyO3 `multiple-pymethods` stays **off**. Zero behavior change.
Ratchet `check_rust_file_size.py` EXCEPTIONS DOWN (never up).

Registry / STATUS / lockfiles / `.github` / board / `Cargo.toml [patch]`
closed.

## GO / deferred

| Name | Class | Disposition |
|---|---|---|
| `window_udwf` / `window_udwf_i32` | inherent helpers | **MOVED** → `column/window.rs` as `pub(super)` |
| `spark_window_frame` + offset/bound scalars | free fns | **MOVED** → `column/window.rs` (`spark_window_frame` `pub(super)`) |
| `reciprocal_trig_or_inf` | free fn | **MOVED** → `column/expr_build.rs` `pub(super)` |
| `strip_outer_alias` / `collapse_identity_alias_chain` / `extract_projection_expr` | free fns | **MOVED** → `column/expr_build.rs` `pub(super)` |
| `parse_data_type` / `parse_decimal_type` / `TIMESTAMP_UNIT` | free fns + const | **MOVED** → `column/expr_build.rs` |
| `collect_aggregate` / `count_distinct_argument` | inherent, post-pymethods impl | **MOVED** → `column/expr_build.rs` `pub(super)` (not inside `#[pymethods]`; no public-surface change) |
| helper unit tests | `#[cfg(test)]` | **MOVED** with the helpers they pin (`expr_build.rs::tests`) |
| `expr_tests` | pymethods handoff pins | **STAYED** in `mod.rs` (tests `sql` / `call_scalar`, not extracted helpers) |
| unmovable helper | — | **none** (§6 empty) |

## Measured sizes (splitlines, post-`cargo fmt`)

| Path | Lines | Ceiling |
|---|---|---|
| `column/mod.rs` | 1779 | 1850 (EXCEPTIONS; was `column.rs` 2200) |
| `column/window.rs` | 101 | default 1500 |
| `column/expr_build.rs` | 359 | default 1500 |

Ratchet: deleted `crates/repark-python/src/column.rs` key; added
`crates/repark-python/src/column/mod.rs` at 1850 (measured 1779). Never
raised. Helpers under the default ceiling — no new EXCEPTIONS rows.

## Files

- `crates/repark-python/src/column.rs` — **deleted** (module directory replaces it)
- `crates/repark-python/src/column/mod.rs` — `#[pyclass]` + single `#[pymethods]`
- `crates/repark-python/src/column/window.rs` — window-UDF + frame helpers
- `crates/repark-python/src/column/expr_build.rs` — expr-construction helpers + tests
- `crates/repark-python/src/column/map.md` — new
- `crates/repark-python/src/map.md` — FN-W / `column.rs` path sentences
- `crates/repark-python/map.md` — parent path `column.rs` → `column/`
- `scripts/check_rust_file_size.py` — EXCEPTIONS key move + ratchet DOWN
- `scripts/map.md` — lockstep note for the EXCEPTIONS key move
- `task/map.md` — this ledger row

`lib.rs` already has `mod column;` — no extra `mod` line.

## Mutation-proof pins (untouched; entire existing suite is the gate)

| If this is dropped… | this test reds |
|---|---|
| facade cast vocab | `parse_data_type_maps_facade_primitive_cast_vocabulary` |
| unknown cast refuse | `parse_data_type_rejects_unknown_and_malformed` |
| alias peel | `collapse_identity_alias_chain_peels_same_name_stack` |
| qualifier + metadata | `collapse_identity_alias_chain_preserves_qualifier_and_metadata` |
| `F.expr` / `substr` pos-0 | `expr_sql_substr_zero_matches_spark` |
| `call_scalar` substr pos-0 | `call_scalar_substr_zero_matches_spark` |
| `5/2` Float64 handoff | `expr_sql_integer_division_hands_off_float64` |

## §6 Unmoved helpers

None. `collect_aggregate` / `count_distinct_argument` sat in a second
inherent `impl PyColumn` *after* `#[pymethods]` and moved without a
public-surface change.

## Gates

- `make format` — exit 0
- `make verify` — exit 0 (inside `make preflight`; rust-file-size 216 files
  clean / 13 exceptions; `cargo test --locked --workspace` all `test result: ok`)
- `make develop` — exit 0 (fresh maturin 1.14.1 into project `.venv`)
- `make py-test-facade` — exit 0 (**3230 passed, 71 skipped**)
- `make preflight` — exit 0 (verify + facade 3230/71 + cargo-deny/pip-audit +
  workflows-parse + zizmor)
