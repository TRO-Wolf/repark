# Unit ledger — DISPLAY-POLARS-1 step 1 · default display style flips to polars

**Retires:** this ledger moves to `../completed/` in the unit's last commit.
This file closes when DISPLAY-POLARS-1 merges, or when the owner closes the slate row.

**Unit:** DISPLAY-POLARS-1 step 1 · **Date:** 2026-09-09 · **Executor:** Muse Spark (muse-spark-1.3-contributor), Actor ·
**Branch:** `feat/display-polars-1` · **Card facts on:** `f00ed9ea`
**Model:** Muse Spark (muse-spark-1.3-contributor)
**risk_tier:** standard.

Step 1 only: D-1 + D-2 under orchestrator rulings D-9 (`default_display_style()` lives in
`session_configuration.py`, re-exported through `_funcs.py`) and D-10 (no parity conftest;
the parity suite runs green instead). Renderer, `repr`, count behaviour, and the four config
keys are steps 2–5 and untouched here.

## Proposition ledger

| ID | Clause | Evidence | Verdict |
|---|---|---|---|
| C-001 | D-1: `_DEFAULT_DISPLAY_STYLE` is `"polars"`, resolved through new `default_display_style()` which reads `REPARK_DISPLAY_STYLE` first (validated by `normalize_display_style`, invalid refuses loud naming the three styles) and then the constant; builder `.config("repark.display.style", …)` and `session.display_style` keep outranking both. | `test_default_style_polars_clean_env` + `test_env_override_spark_restores_grid` green; red run below; `Builder._resolve_display_style` falls back to `default_display_style()` while an explicit builder key still validates through `normalize_display_style` (existing builder/reuse pins green). `conf.unset` / unset-`conf.get` also fall back to `default_display_style()` (env-aware default, not the raw constant) — this repaired the one full-suite red, `test_conf_unset_display_style_resets_to_spark`, with the expectation untouched. | **PROVEN** |
| C-002 | D-2: `python/repark/tests/conftest.py` pins `spark` via `os.environ.setdefault("REPARK_DISPLAY_STYLE", "spark")` at import; the old default-grid test is split into `test_env_override_spark_restores_grid` (env `spark` → byte-identical grid, expectation unchanged) and `test_default_style_polars_clean_env` (clean env → fresh session `display_style` is `polars`). `make py-test-facade` green proves R-6. | Conftest diff + both pins green + full facade suite green; red run below. | **PROVEN** |
| C-003 | D-5: the styled renderer fetches `limit(max_rows + 1)` first and, when fewer than `max_rows + 1` rows return, renders the frame whole with no `count()` and no tail fetch; only a larger frame pays the count. | Step 2. | OPEN |
| C-004 | D-4: `__repr__` renders data with a count under `polars` and `duckdb` regardless of `spark.sql.repl.eagerEval.enabled`, `spark` keeps the existing eagerEval behaviour, and `_repr_html_` returns `None` under the two styled modes. | Step 3. | OPEN |
| C-005 | D-3, D-6, D-7, D-8: the four `repark.display.*` keys, the renderer's measured fidelity against polars itself, `show(truncate=…)` mapping onto `str_len`, and any row polars' output cannot reproduce from Arrow alone filed as a disclosed residue. | Step 4 (tier I). | OPEN |
| C-006 | D-1's documentation: `docs/guide/session-and-conf.md` states the polars default, the four keys, the environment override, and the narrowed count note. | Step 5. | OPEN |

## Red first

Both pins written before the source change and run on the base tree (conftest `setdefault`
already in place):

- `test_default_style_polars_clean_env` → `FAILED` with `AssertionError` at
  `test_display_styles.py:218` (fresh session reported `spark`, not `polars`).
- `test_env_override_spark_restores_grid` → `FAILED` with
  `ImportError: cannot import name 'default_display_style' from 'repark.spark.session'`
  at `test_display_styles.py:262`.

Short summary: `2 failed in 0.24s`. The committed pins keep both failing atoms (the literal
`polars` assertion and the new-name import, moved top-level); only import placement changed
after the red run.

## Decisions

| ID | Decision | Basis |
|---|---|---|
| D-9 | `default_display_style()` defined beside `_DEFAULT_DISPLAY_STYLE` / `normalize_display_style` in `session_configuration.py`, re-exported through the existing `_funcs.py` path; no public signature moved. | Orchestrator ruling 2026-09-09; verified on this tree. |
| D-10 | No parity conftest created: the only `conftest.py` files are `python/repark/tests/conftest.py` and `python/dbt-repark/tests/conftest.py`, and the only `display_style` occurrence under `python/repark-parity/` is a file path in `test_cap_1_source_file_line_cap.py`. Obligation replaced by a green parity-suite run under the flipped default. | Orchestrator ruling 2026-09-09; parity run below. |

## Gates

| Command | Result |
|---|---|
| `VIRTUAL_ENV=$PWD/.venv uv run --no-project python -m pytest python/repark/tests/test_display_styles.py -q` | green: `44 passed` |
| `make py-test-facade` | green: `5820 passed, 368 skipped` (one interim red, `test_conf_unset_display_style_resets_to_spark`, repaired via the C-001 unset fix; no expectation touched) |
| `PYTHONPATH=python/repark-parity/src VIRTUAL_ENV=$PWD/.venv uv run --no-project python -m pytest python/repark-parity/tests -q` | green: `624 passed` |
| targeted re-run at final content (`test_display_styles.py` + `test_production_file_size.py` + `test_t3_ux_polish.py`) | green: `69 passed` |

## Pins

| Pin | Location |
|---|---|
| `test_default_style_polars_clean_env` | `python/repark/tests/test_display_styles.py` |
| `test_env_override_spark_restores_grid` | `python/repark/tests/test_display_styles.py` |

## Notes for the orchestrator

| Item | Note |
|---|---|
| No `COVERAGE_ATTESTATION` block | Per the brief, the orchestrator writes it at the departure edit. |
| `test_production_file_size.py` | Lockstep pin updates in this step: `_DEFAULT_DISPLAY_STYLE` body hash re-measured, `default_display_style` rows added to the hash/owner/runtime-name tables. |
| Size gates | `test_display_styles.py` kept at exactly 1175 lines; `session_core.py` at exactly 2411 lines. |
| Out of scope observed | `python/repark/src/repark/spark/dataframe/display.py::_resolve_display_style` docstring still says "default spark"; `docs/guide/session-and-conf.md` still documents the `spark` default. Both belong to step 5 (docs). |

## Unit state (orchestrator, 2026-09-09)

Step 1 of five landed. C-001 and C-002 are PROVEN and gated; C-003 to C-006 carry the four
remaining steps and stay OPEN, so this ledger stays in `staging/` and files no coverage
attestation yet — the unit is in flight, not delivered.

Two orchestrator rulings were needed because the card's Home named the wrong module:

- **D-9:** `default_display_style()` lives in `session_configuration.py`, beside
  `_DEFAULT_DISPLAY_STYLE` (line 232) and `normalize_display_style` (line 235), re-exported
  through the existing `_funcs.py` path. The card said `session_core.py`, which only receives
  these names through a star-import. Verified on the tree before ruling.
- **D-10:** the parity half of D-2 is dropped — no `conftest.py` exists under
  `python/repark-parity/`, and that suite's only `display_style` occurrence is a file path inside
  a baseline table, not a rendering call. The drop was required to be *measured*, not assumed:
  the parity suite runs green under the flipped default (624 passed), which is the evidence that
  no parity expectation depended on the old default.

- **D-11 (orchestrator, 2026-09-09):** flipping the default broke two *doc examples*, which
  neither `make preflight` nor the facade suite can catch — `check_example_coverage.py` skips
  execution when the native module is not importable, and examples run outside pytest, so D-2's
  conftest pin does not reach them. CI's `build + import smoke` leg caught both.
  `docs/examples/session/display_style.py` asserted the default was `spark`; it now documents the
  polars default truthfully and switches to `spark` instead of away from it.
  `docs/examples/dataframe/show_sort.py` asserts the spark grid's line shapes while demonstrating
  `sort`, so it now pins `repark.display.style=spark` on its builder — the example is about
  sorting, and its assertions are about that grid. An example that renders the *polars* look
  belongs to step 5 with the guide rewrite, not here, because step 4 changes the renderer again.
  Verified locally under a clean environment: both examples exit 0 and all 209 examples execute
  green (`check_example_coverage.py --require-execute`).
