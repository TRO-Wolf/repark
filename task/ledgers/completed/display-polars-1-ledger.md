# Unit ledger — DISPLAY-POLARS-1 step 1 · default display style flips to polars

**Retires:** this ledger moves to `../completed/` in the unit's last commit.
This file closes when DISPLAY-POLARS-1 merges, or when the owner closes the slate row.

**Unit:** DISPLAY-POLARS-1 step 1 · **Date:** 2026-09-09 · **Executor:** Muse Spark (muse-spark-1.3-contributor), Actor ·
**Branch:** `feat/display-polars-1` · **Card facts on:** `f00ed9ea`
**Model:** Muse Spark (muse-spark-1.3-contributor) step 1 · GLM 5.3 Flash (zai/glm-5.3-flash) step 2 · GLM 5.3 Flash (zai/glm-5.3-flash) step 3 · Muse Spark (muse-spark-1.3) step 4 · Muse Spark (muse-spark-1.3-contributor) step 5
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
| C-003 | D-5: the styled renderer fetches `limit(max_rows + 1)` first and, when fewer than `max_rows + 1` rows return, renders the frame whole with no `count()` and no tail fetch; only a larger frame pays the count. | Step 2 implemented 2026-09-09 (polars branch; duckdb branch untouched — see step-2 record for scope). `test_small_frame_renders_without_count` red-first then green (`assert [1] == []` at `test_display_polars_default.py:70` on base); `test_large_frame_counts_once` green (base-green by design: the old flow already counted exactly once). The protected-pin red (`test_styled_show_does_not_full_collect`, `assert 11 <= 5`, `test_display_styles.py:855`) was resolved by **R-11**, not by a code change: the pin's polars section re-pinned to `max_rows_per_export = 2 * edge + 1 = 11` (one token; the duckdb call keeps `=2`; the `assert all(row_count < 12 …)` full-collect tooth is byte-identical), so the pin still forbids collect-then-slice on the 12-row frame. Green under R-11: `46 passed in 1.33s` (test_display_styles.py + test_display_polars_default.py) and `5830 passed, 369 skipped, 45 warnings in 705.21s (0:11:45)` (`make py-test-facade`). | **PROVEN** |
| C-004 | D-4: `__repr__` renders data with a count under `polars` and `duckdb` regardless of `spark.sql.repl.eagerEval.enabled`, `spark` keeps the existing eagerEval behaviour, and `_repr_html_` returns `None` under the two styled modes. | Step 3 implemented 2026-09-09 (`display.py` `_repr` / `_repr_html` bodies; `core.py` wrapper docstrings only). `_repr` resolves the display style first: under `polars`/`duckdb` it returns `_render_styled_show(frame, style, n=20, truncate_at=20)` — the same call show() makes with its own defaults (`n=20`, `truncate=True` → cap 20) — regardless of eagerEval; `_repr_html` returns `None` for the two styled modes before any eagerEval read. The spark branch (schema form, eager-eval grid/HTML, both bridge-peek branches) is untouched. Red first: `4 failed, 3 passed in 0.29s` — `test_polars_repr_renders_table_without_eager_eval` (`assert 'DataFrame[id: int]' == 'shape: (7, 1)...'`, line 139), `test_duckdb_repr_renders_table` (same schema-form atom, line 147), `test_styled_repr_html_is_none` (`assert "<table border='1'>…" is None` with eager-eval on under polars, line 171), `test_small_frame_repr_does_not_count` (`'DataFrame[id: int]'.startswith('shape: (7, 1)')` false, line 194); `test_spark_repr_unchanged` green on base by design (keep-green guard, no red atom exists for unchanged behaviour). Gates: `51 passed in 1.37s` (test_display_styles.py + test_display_polars_default.py) and `5835 passed, 369 skipped, 45 warnings in 700.13s (0:11:40)` (`make py-test-facade`). | **PROVEN** |
| C-005 | D-3, D-6, D-7, D-8: the four `repark.display.*` keys, the renderer's measured fidelity against polars itself, `show(truncate=…)` mapping onto `str_len`, and any row polars' output cannot reproduce from Arrow alone filed as a disclosed residue. | Step 4 implemented 2026-09-09: three polars-oracle pins byte-equal against live polars 1.43.2 (`repr(frame) == str(pl.from_arrow(frame.to_arrow()))`), `test_str_len_cuts_with_ellipsis` and `test_display_keys_conf_get_set` green; red run `5 failed, 7 passed` on base; residues R-001…R-006 filed below; follow-up (same day) moves the code under the ceilings with zero behavior change (`plan_collapse.py` 1168→1057 via new `polars_cells.py`, `session_core.py` 2411→2410, both ratchet DOWN, all 56 pins byte-identical); gates in the step-4 record. | **PROVEN** |
| C-006 | D-1's documentation: `docs/guide/session-and-conf.md` states the polars default, the four keys, the environment override, and the narrowed count note. | Step 5 implemented 2026-09-09: the `repark.display.style` section rewritten — polars default with an executed fresh-session transcript (`display_style` `polars`, all four `conf.get` values, the 2-row polars table), the duckdb/spark switch transcript (both grids executed verbatim), the four-key table (style `polars`, max_rows `10` with 5 + 5 edges, max_cols `8` with 4 + 4, str_len `30`, D-12, 32 nowhere), the precedence chain (builder `.config`/`session.display_style` > `REPARK_DISPLAY_STYLE` > built-in `polars`, invalid refuses loud naming the three styles — each leg measured), the narrowed count note (probe `limit(max_rows + 1)` = 11, 7-row `show()` zero `count()` calls, 12-row exactly one), the `truncate` mapping (`True` → session `str_len`, `False` → no cut, int → that width, with executed cut transcripts), and the step-3 sentences (`repr(df)` byte-identical to `show()` under polars/duckdb regardless of eagerEval, `_repr_html_` `None` there, spark untouched). Every python/text block in the section is a transcript of a snippet run in this clone with `.venv/bin/python`, pasted verbatim. Gates: `make check-docs-links` clean (700 files, 4518 links), `make check-docs-compaction` clean, `sync_map_md.py --check` clean (227 maps), display files `56 passed in 1.71s`. | **PROVEN** |

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
| R-11 | D-5's 11-row probe is right. The protected pin `test_styled_show_does_not_full_collect` re-pins its **polars section only** to `max_rows_per_export = 2 * edge + 1 = 11`; the duckdb section keeps `max_rows_per_export=2` untouched; the `assert all(row_count < 12 …)` line stays exactly as it is, so the pin still forbids collect-then-slice on the 12-row frame. Step 2 is polars-only; the duckdb style keeps its current fetch pattern until step 4. | Owner ruling R-11 (binding, 2026-09-09); resolves DISPLAY-POLARS-1-S2-Q-001, lean (a). |
| D-4a | `__repr__` under `polars` and `duckdb` returns the styled table as text regardless of `spark.sql.repl.eagerEval.enabled`; under `spark` the existing eager-eval behaviour stays byte-identical, including the `DataFrame[a: bigint, …]` schema form when eager-eval is off. Implemented as a style-first branch in `display._repr`; the spark branch below it is unchanged. | Step-3 brief, binding orchestrator implementation ruling. |
| D-4b | The styled repr is exactly what `show()` prints with its own defaults (`n=20`, `truncate=True` → cap 20), produced by the same `_render_styled_show` call and returned instead of printed; no second rendering path, no duplicated formatting, no separate count line (the count is the shape line the renderer already emits). | Step-3 brief, binding orchestrator implementation ruling. |
| D-4c | `_repr_html_` returns `None` under `polars` and `duckdb` (Jupyter falls back to the text repr) with eager-eval both on and off; under `spark` its behaviour — escaped HTML table when eager-eval is on, `None` otherwise — is untouched. An HTML table for the styled modes is DISPLAY-POLARS-2, not this card. | Step-3 brief, binding orchestrator implementation ruling. |
| D-4d | The bridge-peek path (`_use_bridge_peek`) keeps its current behaviour for `spark`. For the styled modes the styled repr does not consult the peek path — routing styled repr through `_render_styled_show` (D-4b's mechanism mandate, "no second rendering path") leaves the peek path unengaged, so it changes no rendering. Measured on the base tree, 12-row uncached bridged frame under `polars`: `show()` takes the peek before style resolution (spark grid, `count()` 0 calls, UDF 1 run) while `_render_styled_show(frame, "polars", n=20, truncate_at=20)` renders the polars table (`count()` 1 call, UDF 2 runs). Consequence, measured and disclosed: an uncached mapInArrow frame's repr under a styled mode renders the styled table and pays the D-5 count for frames past the probe, while its `show()` prints the peek grid — a pre-existing show-side peek-vs-style hijack (untouched, out of step-3 scope; `show()` was not modified). Filed as DISPLAY-POLARS-1-S3-Q-001 for an orchestrator ruling if a later step should reconcile the two doors for bridged frames. | Step-3 brief, binding orchestrator implementation ruling; measurements in the step-3 record. |
| D-4e | Nothing in the styled repr calls `count()` on a frame the styled renderer would not have counted: repr reuses the same `_render_styled_show` call with the same default args as `show()`, so D-5's discipline rides along (7-row repr: zero counts — pinned by `test_small_frame_repr_does_not_count`). | Step-3 brief, binding orchestrator implementation ruling; the bridged-frame residue is D-4d's disclosure, not a new fetch path. |
| D-12 | `repark.display.str_len` defaults to **30**, not the card's 32: polars 1.43.2 with a clean environment cuts strings longer than 30 to 30 characters plus `…` (measured over lengths 28–34 and a 60-char cell; `pl.Config(fmt_str_lengths=30)` renders byte-identical to the default while `=32` renders 32+`…`; polars' own `set_fmt_str_lengths` docstring example shows a 30-char cut). The card's 32 would break byte-identity under defaults and its own oracle pins. | Settled by orchestrator ruling 2026-09-09 (follow-up round): measurement wins per D-6, keep 30. Step 5 quotes 30, no open question. |

## Gates

| Command | Result |
|---|---|
| `VIRTUAL_ENV=$PWD/.venv uv run --no-project python -m pytest python/repark/tests/test_display_styles.py -q` | green: `44 passed` |
| `make py-test-facade` | green: `5820 passed, 368 skipped` (one interim red, `test_conf_unset_display_style_resets_to_spark`, repaired via the C-001 unset fix; no expectation touched) |
| `PYTHONPATH=python/repark-parity/src VIRTUAL_ENV=$PWD/.venv uv run --no-project python -m pytest python/repark-parity/tests -q` | green: `624 passed` |
| targeted re-run at final content (`test_display_styles.py` + `test_production_file_size.py` + `test_t3_ux_polish.py`) | green: `69 passed` |
| step-4 red run: new pins on base tree | `5 failed, 7 passed` (all five red-first atoms fail as required) |
| step-4: `test_display_polars_default.py` + `test_display_styles.py` | green: `56 passed` |
| step-4 neighbors (`test_dfcore_6_eager_preview`, `test_dfcore_4b_show_goldens`, `test_t3_ux_polish`, `test_dfcore_1_exports`, `test_dfcore_4b_exports`, `test_production_file_size`, `test_session_config_knobs`, `test_session`, `test_builder_config_map`, `test_getorcreate_catalogs`) | green (one interim red, `test_show_styled_vertical_warning_attributes_to_caller`, repaired as stale-cache, not product: `107 passed` + `103 passed`) |
| `make py-test-facade` | green: `5842 passed, 369 skipped, 7 xfailed` (all 7 xfails are base-tree `test_df_eager_1.py` red-first pins, verified zero xfail markers in the display files) |
| parity suite (`python/repark-parity/tests`) | green: `639 passed` |
| `make py-test-parity-cap` | green: `23 passed` (dual-table ratchet holds both sides) |
| `make ci` | green, exit 0 (lib-py ratchets, ledger grammar, example coverage with the True-cap change, manifest, spell-check) |
| `make verify` rust-test (`cargo test --locked --workspace`) | RED, pre-existing and unrelated: 2 `repark-iceberg` `dv_close` tests fail on a poisoned part-dv fixture lock under workspace-parallel runs, pass as `-p repark-iceberg --lib` (429/429) on both trees; the step-4 diff holds zero `.rs` files so the Rust binary is unchanged — same failure with or without this commit |
| follow-up: display files at final content | green: `56 passed`, byte-identical to round 1 |
| follow-up: `make py-test-facade` | green: `5842 passed, 369 skipped, 7 xfailed` (identical counts to round 1; xfails unchanged base-tree eager pins) |
| follow-up: `make py-lint` | green (`ruff check .`, all checks passed) |
| follow-up: `python3 scripts/check_lib_py.py` | green: `598 files clean` (new module counted, no-stub held, DOWN ratchets exact) |
| follow-up: `make py-test-parity-cap` | green: `23 passed` |
| follow-up neighbors (session/conf, eager preview, show goldens, ux polish, production size: 8 files) | green: `143 passed` |

## Pins

| Pin | Location |
|---|---|
| `test_default_style_polars_clean_env` | `python/repark/tests/test_display_styles.py` |
| `test_env_override_spark_restores_grid` | `python/repark/tests/test_display_styles.py` |
| `test_small_frame_renders_without_count` | `python/repark/tests/test_display_polars_default.py` (step 2) |
| `test_large_frame_counts_once` | `python/repark/tests/test_display_polars_default.py` (step 2) |
| `test_polars_repr_renders_table_without_eager_eval` | `python/repark/tests/test_display_polars_default.py` (step 3) |
| `test_duckdb_repr_renders_table` | `python/repark/tests/test_display_polars_default.py` (step 3) |
| `test_spark_repr_unchanged` | `python/repark/tests/test_display_polars_default.py` (step 3) |
| `test_styled_repr_html_is_none` | `python/repark/tests/test_display_polars_default.py` (step 3) |
| `test_small_frame_repr_does_not_count` | `python/repark/tests/test_display_polars_default.py` (step 3) |
| `test_polars_oracle_ints_strings_nulls` | `python/repark/tests/test_display_polars_default.py` (step 4) |
| `test_polars_oracle_floats_bools_dates` | `python/repark/tests/test_display_polars_default.py` (step 4) |
| `test_polars_oracle_wide_frame_col_ellipsis` | `python/repark/tests/test_display_polars_default.py` (step 4) |
| `test_str_len_cuts_with_ellipsis` | `python/repark/tests/test_display_polars_default.py` (step 4) |
| `test_display_keys_conf_get_set` | `python/repark/tests/test_display_polars_default.py` (step 4) |

## Notes for the orchestrator

| Item | Note |
|---|---|
| No `COVERAGE_ATTESTATION` block | Per the brief, the orchestrator writes it at the departure edit. |
| `test_production_file_size.py` | Lockstep pin updates in this step: `_DEFAULT_DISPLAY_STYLE` body hash re-measured, `default_display_style` rows added to the hash/owner/runtime-name tables. |
| Size gates | `test_display_styles.py` kept at exactly 1175 lines; `session_core.py` at exactly 2411 lines. |
| Out of scope observed | `python/repark/src/repark/spark/dataframe/display.py::_resolve_display_style` docstring still says "default spark"; `docs/guide/session-and-conf.md` still documents the `spark` default. Both belong to step 5 (docs). |

## Step 2 record — D-5 small-frame single fetch (2026-09-09)

**Executor:** GLM 5.3 Flash (zai/glm-5.3-flash), Actor · **Branch:** `feat/display-polars-1-step2`.

**Implementation** (`python/repark/src/repark/spark/dataframe/display.py`, `_render_styled_show`
only; no new module-level names, so the frozen export surface of `test_dfcore_1_exports.py`
holds): the polars branch now probes `limit(2 * edge + 1)` = `limit(11)` once via `to_arrow()`.
Fewer rows returned → the probe IS the frame; it renders whole (`shape` from the probe's exact
row count) with no `count()` and no tail fetch, first `n` rows only when `n` caps the keep-set
(`show(n)` cap semantics preserved: `probe.slice(0, max(n, 0))`). Probe full → one `count()`,
head window sliced off the probe, tail via `_preview_tail_rows` as before. The duckdb branch is
byte-identical to step 1 (counts first, head fetch, tail fetch). Scope: **polars only** — the
card's step-2 pins are polars-measured (a 12-row frame is small under duckdb `show(20)`), and a
both-styles probe would additionally red `test_dfcore_6_eager_preview.py::
test_styled_show_keeps_its_count`. Duckdb scope filed as DISPLAY-POLARS-1-S2-Q-002.

**Red first** (new pins in `python/repark/tests/test_display_polars_default.py`, run on the base
tree before the display.py edit): `test_small_frame_renders_without_count` → `FAILED` with
`assert [1] == []` at `test_display_polars_default.py:70` (base calls `count()` for the 7-row
frame); `1 failed, 1 passed in 0.18s`. `test_large_frame_counts_once` passed on base by design —
the old flow already counted exactly once for 12 rows; it is the keep-green guard against
double-count/dropped-count mutations, not a red atom. Both green after the edit (`2 passed`).

**Sanctioned pin edit** (not the protected test): `test_polars_style_uses_count` asserted
`count == 1` for a 1-row frame — false under D-5 by construction. Frame swapped to
`_ORDERED_12_SQL` (12-row → large path → count once); line-neutral, `test_display_styles.py`
stays exactly 1175 lines (lib-py exact baseline held; diff is one line).

**Protected-pin conflict (the HALT):** `test_styled_show_does_not_full_collect`, polars section,
`assert max(rows_per_call) <= max_rows_per_export` → `assert 11 <= 5` where
`11 = max([11, 5])` at `test_display_styles.py:855`. The D-5 probe is by construction a single
`limit(max_rows + 1)` export; the cap pins the old fetch discipline (head 5 + tail 5). The
brief orders: red → hand back, never edit. Duckdb section of the same test is green (polars-only
scope). All other teeth of the protected test stay honest under the rework: no `collect`, no
`to_polars`, no root facade/native export, exactly one `_preview_tail_rows`, exactly one
`limit_with_skip(7, 5)`, the lws plan is the streamed one.

**Ruling needed (DISPLAY-POLARS-1-S2-Q-001):** which yields — (a) re-pin `max_rows_per_export`
to the probe size (`2 * edge + 1` = 11 for the polars section), (b) fetch the probe via
`to_arrow_batches()` so `rows_per_call` keeps watching only `to_arrow` windows, or (c) another
shape. Lean: (a) — the cap then reads "no export exceeds the probe size", still forbids
full-collect+slice (11 < 12), keeps every tooth, keeps the probe visible to the spy, and is a
one-token amendment the orchestrator sanctions; (b) would blind `rows_per_call` to the probe.

**R-11 resolution (owner, 2026-09-09, this round):** the ruling took lean (a). The pin was
resolved by **ruling, not by a code change**: the only source delta is the one-token re-pin
`max_rows_per_export` 5 → 11 in `test_display_styles.py:874`'s polars call; the duckdb call and
the `< 12` assertion are untouched, and the renderer stands as committed in 16a9346d. The same
round cut `_render_styled_show`'s docstring back to its one-line summary under the comment ban
and moved the probe/count explanation into
`python/repark/src/repark/spark/dataframe/map.md` under the `display.py` entry. This paragraph
supersedes the parked-round close below: the close lands as one commit on
`feat/display-polars-1-step2`.

**Step-2 gates measured:**

| Command | Result |
|---|---|
| pins on base tree (red run) | `1 failed, 1 passed` (small-frame pin red as required) |
| `python/repark/tests/test_display_polars_default.py` after the edit | `2 passed` |
| `python/repark/tests/test_display_styles.py` after the edit | `1 failed, 43 passed` — the one failure is the protected pin above |
| neighbors (`test_dfcore_6_eager_preview`, `test_dfcore_4b_show_goldens`, `test_t3_ux_polish`, `test_dfcore_4b_exports`, `test_dfcore_1_exports`, `test_production_file_size`, new pins) | `53 passed` |
| `make py-test-facade` | see final handback (run in flight at ledger-write time; result recorded in `handback.json`) |
| R-11 round: `VIRTUAL_ENV=$PWD/.venv uv run --no-project python -m pytest python/repark/tests/test_display_styles.py python/repark/tests/test_display_polars_default.py -q` | green: `46 passed in 1.33s` |
| R-11 round: `make py-test-facade` | green: `5830 passed, 369 skipped, 45 warnings in 705.21s (0:11:45)` |

Nothing is committed: the brief's gate list cannot go green without a ruling on Q-001, and a
commit whose tree reds a named gate would embed the red state. The working tree holds the
implementation, both pins, the sanctioned one-line pin edit, and this ledger + map entry for
review.

## Step 3 record — D-4 repr honours the display style (2026-09-09)

**Executor:** GLM 5.3 Flash (zai/glm-5.3-flash), Actor · **Branch:** `feat/display-polars-1-step3`,
cut from `main` at `8a2928c2`.

**Implementation** (`python/repark/src/repark/spark/dataframe/display.py`, `_repr` and
`_repr_html` bodies only; `core.py` wrapper docstrings only; no new module-level names, so the
frozen export surface of `test_dfcore_1_exports.py` / `test_dfcore_4b_exports.py` holds;
`test_display_styles.py` untouched): `_repr` resolves the display style first. Under `polars` /
`duckdb` it returns `_render_styled_show(frame, style, n=20, truncate_at=20)` — byte-for-byte
what `show()` prints with its own defaults, because `show()` reaches the identical call through
`_normalize_show_args(20, True)` — regardless of `spark.sql.repl.eagerEval.enabled`, with no
second rendering path. `_repr_html` returns `None` for the two styled modes before any
eagerEval read, so Jupyter falls back to the text repr. The spark branch keeps the schema form
(eager off), the eager-eval grid/HTML (eager on), and both bridge-peek branches byte-identical.
The multi-line docstrings the two bodies carried were cut to one line under the comment ban;
the eagerEval conf-key contract and the HTML-escape contract stay recorded in
`python/repark/src/repark/spark/dataframe/map.md`.

**Red first** (five pins added to `python/repark/tests/test_display_polars_default.py`, run on
the base tree before the display.py/core.py edits): `4 failed, 3 passed in 0.29s` —

- `test_polars_repr_renders_table_without_eager_eval` → `assert 'DataFrame[id: int]' ==
  'shape: (7, 1)...'` at `test_display_polars_default.py:139` (base repr is the schema form).
- `test_duckdb_repr_renders_table` → same schema-form atom at line 147.
- `test_styled_repr_html_is_none` → `assert "<table border='1'>…" is None` with eager-eval on
  under polars at line 171 (base renders the HTML table).
- `test_small_frame_repr_does_not_count` → `'DataFrame[id: int]'.startswith('shape: (7, 1)')`
  false at line 194 (base repr never reaches the styled renderer).
- `test_spark_repr_unchanged` passed on base by design — it pins behaviour this step must not
  change (schema form eager-off, eager grid, eager HTML; exact strings measured on the base
  tree), the same keep-green-guard role `test_large_frame_counts_once` played in step 2.

All five green after the edit (`7 passed` for the file).

**D-4d bridge-peek measurements** (base tree, 12-row uncached mapInArrow frame under
`repark.display.style=polars`, `DataFrame.count` spy + UDF-run counter): `show()` prints the
spark-grid peek table with `count()` 0 calls and 1 UDF run — `_show` checks
`_use_bridge_peek` before style resolution, so a styled mode never reaches the styled
renderer there. `_render_styled_show(frame, "polars", n=20, truncate_at=20)` on the same
frame prints the polars table with `count()` 1 call and 2 UDF runs. The styled repr therefore
routes through `_render_styled_show` for every frame (D-4b's mechanism mandate) and leaves the
peek path entirely to the spark branch (D-4d's "keeps its current behaviour"): the peek path
changes no rendering in this step. Residue disclosed under D-4d and filed as
DISPLAY-POLARS-1-S3-Q-001; no pin in `test_display_styles.py` covers a bridged frame under a
styled mode, and `show()` was not modified.

**Step-3 gates measured:**

| Command | Result |
|---|---|
| new pins on base tree (red run) | `4 failed, 3 passed in 0.29s` |
| `python/repark/tests/test_display_polars_default.py` after the edit | `7 passed in 0.28s` |
| Gate 1: `VIRTUAL_ENV=$PWD/.venv uv run --no-project python -m pytest python/repark/tests/test_display_styles.py python/repark/tests/test_display_polars_default.py -q` | green: `51 passed in 1.37s` |
| Gate 2: `make py-test-facade` | green: `5835 passed, 369 skipped, 45 warnings in 700.13s (0:11:40)` |

Size gates: `display.py` 324 → 316 lines (stays under the source-size default, no ceiling row);
`test_display_polars_default.py` 91 → 197 lines (under the default ceiling, no ceiling row).
`core.py` 4536 → 4525 lines: the exact baseline in `scripts/check_lib_py.py` ratcheted DOWN
4536 → 4525 in the same commit (one-interim hook red: `lib-py` failed with
`core.py shrank to 4525 lines below exact baseline 4536`; recorded in `scripts/map.md`).
Ruff check + format clean on all three touched Python files.

## Step 4 record — D-3, D-6, D-7, D-8 keys and renderer fidelity (2026-09-09)

**Executor:** Muse Spark (muse-spark-1.3), Actor · **Branch:** `feat/display-polars-1-step4`.

**Keys (D-8).** `_DISPLAY_INT_DEFAULTS` in `session_configuration.py` owns the
three int keys and their defaults (max_rows 10, max_cols 8, str_len 30 under
D-12); `_normalize_display_int` refuses bool, non-digit strings, and values < 1
with `IllegalArgumentException` naming the key. `RuntimeConfig` set/get/unset
mirror the style-key handling (canonical store, alive token, builder sync,
canonical tomb; `get` reads the live token so runtime `conf.set` takes effect
at render time). `ReparkSession.__init__` derives the token fields from the
builder map (invalid refuses inside `getOrCreate`); `_set_config_entry`
canonicalizes the keys case-insensitively; the reuse path validates before the
short-circuit and applies present keys while excluding them from the unapplied
warning exactly like the style key. The new names stay out of `_funcs.py`
(direct sibling imports, the timezone/timestamp precedent), so the
production-file-size hash/owner/runtime tables do not move. `repark.toml`
needs no change: CFG-1's generic profile merge already carries a `display`
table with these snake_case names (`style`, `max_rows` pinned in the Rust
`profile_merge` tests), and the mapping is 1:1 by construction.

**Renderer (D-3, D-6, D-7).** The polars probe is `limit(max_rows + 1)` with
edges `max_rows // 2` and keep-set `min(n, max_rows)` (10/5+5 under defaults —
existing goldens byte-identical). `show(truncate=True)` on a styled frame caps
cells at the session `str_len`; spark keeps its own 20 and the styled repr uses
the session cap so it stays byte-identical to `show()` defaults. Polars cells:
`null`, lowercase bools, `NaN`, mixed-mode floats fitted probe-by-probe
(~75 probes: fixed band, six-decimal rounding, shortest-sci for 7–9-digit
integers, four-decimal-sci beyond, sub-1e-6 split), nested `{1,"x"}` structs
and `["a", 1]` lists with double-quoted strings, `…` cuts at `str_len`
characters. Polars labels: `decimal[p,s]`, unit-aware `datetime[ms|μs|ns]`
(`s` displays as `ms`, measured), `time`, `struct[n]`, `list[inner]`.
`_format_polars_show` hides columns past `max_cols` behind a `…` column
(first `(m+1)//2`, gap, last `m-(m+1)//2`; header and body `…`, dashes and
dtype rows blank). Spark and duckdb spellings, labels, truncation (`...`),
and fetch discipline are byte-untouched. The duckdb door keeps count-first and
its `n`-based keep-set: changing it would red the protected
`test_styled_show_does_not_full_collect` (duckdb section pins
`max_rows_per_export=2`, `skip=(10, 2)`), so unification needs its own card.

**Sanctioned pin edit** (not the protected test): `test_polars_style_truncation`
asserts `"..." in out` for a polars `truncate=10` cut — false by construction
once the polars path cuts with `…`. One token `"..."` → `"…"` at
`test_display_styles.py:360`, line-neutral (file stays exactly 1175 lines).

**Red first** (five pins in `test_display_polars_default.py`, run on the base
tree before the source edits): `5 failed, 7 passed` — oracle ints (struct
`CAST` needed the angle-bracket spelling, then `{1,x}` vs `{1,"x"}` quote
atom), oracle floats (scientific `1.2346e9` width atom), oracle wide (no
column gap on base), str_len (`conf.get` raised `Configuration property
repark.display.str_len is not set`), keys (same `not set` raise). After the
edits: `56 passed` for the two display files.

**Size gates:** the first cut of step 4 raised `plan_collapse.py` to 1357 and
`session_core.py` to 2448. `scripts/check_lib_py.py` is the size SSOT and its
header rules that a baseline INCREASE needs explicit owner approval, so the
orchestrator refused both at audit and sent the round back to absorb them. The
follow-up commit does: the polars cell and type spellers move to the new
`python/repark/src/repark/spark/dataframe/polars_cells.py` (325 lines, no
exception row needed) and the display-key plumbing to
`session/session_configuration.py` beside `default_display_style()`. Final:
`plan_collapse.py` 1168→**1057** and `session_core.py` 2411→**2410**, both
ratchets DOWN needing no approval, both mirrored in the CAP-1 test, with all 56
display pins byte-identical across the move. All other touched files stay under
their ceilings or line-neutral.

**Stale-cache repair (not a product change):** one neighbor,
`test_show_styled_vertical_warning_attributes_to_caller`, failed on the base
tree too — its pytest assertion-rewrite `.pyc` was compiled in the dead clone
`/tmp/oc-describe1` (`co_filename` verified via `marshal`) and revalidates
because the source is unchanged, so the warning attributed to the wrong path.
The file is git-ignored regenerable cache; deleting the two `__pycache__`
trees repairs it (`7 passed`). Recorded here so the facade gate is read
correctly.

**Follow-up (same day, audit findings).** Finding 1 refused the two ceiling
increases, so the step-4 code moved with zero behavior change: the polars
spellers (floats, nested cells, `_cell_text`, `_table_to_cell_rows`, both label
functions, the column gap) moved to the new
`python/repark/src/repark/spark/dataframe/polars_cells.py` (325 lines);
`plan_collapse.py` keeps the show control flow and re-exports the moved names
(`core.py` and `display.py` import sites untouched; the package-gain pin takes
`polars_cells` in its extension set, the file's own new-home pattern).
`plan_collapse.py` 1168→1057, `session_core.py` 2411→2410, both ratchet DOWN
in the script table and the CAP-1 mirror. The key plumbing moved to
`session_configuration.py` as `_canonicalize_display_key` (builder-write
canonicalization with int validation, absorbing the style branch byte for
byte) and `_reuse_display_ints` (validate plus token/snapshot/store/tomb
apply); `session_core.py` keeps one call each. Two deliberate shape changes
versus round 1, both covered by the unchanged pins: int values validate at
`.config()` time (the builder-invalid pin raises inside the same `pytest.raises`
block), and token-miss reads fall back to the validated builder map (build
path needs no token derivation). All 56 display pins plus the exports,
production-size, and session suites green byte-identical. Finding 2 settles
D-12 at 30 (row above); the code is unchanged.

## Step-4 residues (measured, disclosed, not fixed here)

| ID | Residue | Measurement |
|---|---|---|
| R-001 | Timezone-aware timestamps: label matches (`datetime[μs, UTC]`) but the cell does not (`2026-01-02 03:04:05 UTC` vs `str()` `+00:00`). | `pa.timestamp("us", tz="UTC")` single-cell probe; the card names this exact case. The engine coerces SQL and row-built timestamps to `tz=UTC`, so no naive-timestamp oracle was reachable. |
| R-002 | Nanosecond timestamps: label matches (`datetime[ns]`) but sub-microsecond digits cannot survive `to_pylist` (Python datetimes stop at microseconds). | `pa.timestamp("ns")` probe; `s` unit displays as `datetime[ms]` (measured, implemented). |
| R-003 | List cells longer than 4 elements (`[1, 2, … 7]`) rest on one 7-element probe: first two, `…`, last one. Widths 5 and 6 unprobed. | Single probe; the oracle pins cap lists at 3 elements. |
| R-004 | Struct inner strings are double-quoted without escape probing; inner strings are not pre-cut (the composite cut applies once outside). | `{1,"x"}` / `{null,1.5}` / `["a", "bb"]` probed; quote-bearing and >30-char inner strings unprobed. |
| R-005 | Duckdb fetch discipline unchanged (count-first, `n` keep-set); `max_rows` does not cap duckdb. | Forced by the protected pin (duckdb `max_rows_per_export=2`); unification needs its own card. |
| R-006 | Float corners outside the probe table: `float16` columns (quantized like `float32`, unpinned), `%.4e` exponents ≥ 100 (unprobed shape), exact-tie six-decimal rounding (Python and Rust both round the exact binary value correctly, so they agree by construction — unpinned). | Algorithm notes in `dataframe/map.md`; oracle pins cover the fixed, 6-decimal, shortest-sci, and padded-sci bands. |

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

## Residue — DISPLAY-POLARS-1-S3-Q-001 (orchestrator ruling, 2026-09-09)

**Status: FIXED by DISPLAY-BRIDGE-1 (2026-09-09).** `_show` now resolves the display style
before the bridge peek; under `polars` / `duckdb` the peeked table renders through
`_render_styled_show` and the two doors agree for bridged frames — pins in
[../../python/repark/tests/test_display_bridge_1.py](../../python/repark/tests/test_display_bridge_1.py),
record in `task/ledgers/staging/display-bridge-1-ledger.md`. The measurement and ruling below
stand as the history of the residue while it was open.

**Measured, disclosed, not fixed here.** For an uncached `mapInArrow`-bridged frame under a
styled display style, the two doors now disagree: `repr(df)` renders the styled table (paying
D-5's count past the 11-row probe) while `df.show()` prints the Spark-grid peek, because `_show`
checks `_use_bridge_peek` *before* it resolves the style, and step 3 left both `show()` and the
peek path untouched as D-4d requires. The doors already disagreed for these frames before this
step (schema form versus peek grid), so nothing regressed; what changed is that the disagreement
is now visible as two different table renderings.

**Ruling:** accept the divergence for DISPLAY-POLARS-1. The styled repr is the notebook door D-4
targets and its count discipline is exactly D-5's for every frame the styled renderer serves.
Reconciling `show()`'s peek-before-style hijack is a behaviour change to `show()` with its own
pins, outside this card's fence ("do not touch `_render_styled_show`'s fetch logic") and outside
steps 4 and 5. It is carried to the owner as a candidate for its own card, not folded into this
unit; no pin here asserts the two doors agree for bridged frames.

## Step 5 record — the session-and-conf guide rewrite (2026-09-09)

**Executor:** Muse Spark (muse-spark-1.3-contributor), Actor · **Branch:**
`feat/display-polars-1-step5`, cut from `origin/main` at `ad81e9ec` (step 4 merged).

**Docs only.** Zero `.py`/`.rs` files touched: the section now describes the shipped tree
instead of the card. The four stalenesses from the brief are each corrected with a measured
transcript: the default is `polars` (fresh session prints it); the `REPARK_DISPLAY_STYLE`
override with its precedence chain (builder `duckdb` beats env `spark`; env `spark` alone
makes a fresh session report `spark`; `bogus` refuses inside `getOrCreate` with
`IllegalArgumentException` naming `['duckdb', 'polars', 'spark']`); the count note reads
probe-first (7-row `show()` zero `count()` calls, 12-row exactly one, spy-measured); the
key table carries all four keys with `str_len` **30** per D-12. The step-3 sentences
(`repr` byte-identical to `show()`, `_repr_html_` `None`, spark eagerEval untouched) each
rest on a probe from this round. `docs/guide/map.md` carries the section's new scope in
the same commit. The `display.py::_resolve_display_style` "default spark" docstring noted
in step 4's out-of-scope row is still as it was — a code comment, outside this step's
docs-only fence, left for the orchestrator.

## Step-5 gates measured

| Command | Result |
|---|---|
| `make check-docs-links` | clean: `700 files, 4518 links checked — clean` |
| `make check-docs-compaction` | clean |
| `make check-ledgers` | clean: `283 ledgers in bins (218 archived), 795 ledger links resolve, frozen rule clean` |
| `python3 scripts/check_ledger_grammar.py` | clean: `65 live ledgers clean (405 clauses, 1053 pinned clause ids, 2 exception rows)` |
| `python3 scripts/sync_map_md.py --check` | clean: `227 maps clean (strict=off)` |
| `.venv/bin/python -m pytest python/repark/tests/test_display_polars_default.py python/repark/tests/test_display_styles.py -q` | green: `56 passed in 1.71s` |

```yaml
COVERAGE_ATTESTATION:
  pr_unit: display-polars-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every clause walked against behavior, not paraphrase — the polars default and env override (clean-env and spark-env pins), the D-5 probe discipline (small-frame no-count and large-frame counts-once pins), the styled repr and None-HTML doors (five step-3 pins), the keys plus live-polars oracle byte-identity (five step-4 pins), and the guide rewrite whose every transcript was executed in the step-5 probes.
      artifacts: [python/repark/tests/test_display_styles.py, python/repark/tests/test_display_polars_default.py, docs/guide/session-and-conf.md]
    - id: AT-2
      status: ATTACKED
      evidence: Boundaries exercised on both sides — 7 rows under the 11-row probe against 12 rows over it, max_rows 4 giving 2+2 edges, max_cols 4 giving 2+...+2, str_len cuts at 30/10/4, truncate True/False/int, and malformed values refused (0, -1, abc, 10.5, empty, bool True, bogus style via builder and env).
      artifacts: [python/repark/tests/test_display_polars_default.py]
    - id: AT-3
      status: ATTACKED
      evidence: Every refusal path raises IllegalArgumentException naming the key and the valid set — bad style at the builder, at conf.set, and at the env override; bad int keys at conf.set and at the builder — each asserted inside pytest.raises. No retry, timeout, or crash-mid-operation surface exists: rendering is a bounded local fetch plus pure formatting.
      artifacts: [python/repark/tests/test_display_polars_default.py, python/repark/src/repark/spark/session/session_configuration.py]
    - id: AT-4
      status: ATTACKED
      evidence: The token-versus-builder ordering is pinned, not assumed — builder config outranks the env override, runtime conf.set takes effect at render time, conf.unset falls back to the env-aware default_display_style rather than the raw constant (the one full-suite red step 1 repaired). No threads, no shared mutable state beyond the session token.
      artifacts: [python/repark/tests/test_display_styles.py, python/repark/tests/test_display_polars_default.py]
    - id: AT-5
      status: N/A
      justification: Rendering reads local Arrow batches and formats text; no privileged action, no credential, no secret, no deserialization, no network, no path traversal on any display path.
    - id: AT-6
      status: ATTACKED
      evidence: Byte-identity holds both directions — Repark output equals str(pl.from_arrow(...)) under live polars 1.43.2 on the oracle pins, and the spark grid pin is byte-identical to the pre-unit grid. The intentional default flip keeps a compatibility escape (REPARK_DISPLAY_STYLE=spark, fixtures pinned to spark) and every non-reproducible row is a disclosed residue (R-001 through R-006) rather than an absorbed divergence.
      artifacts: [python/repark/tests/test_display_polars_default.py, python/repark/tests/test_display_styles.py]
    - id: AT-7
      status: ATTACKED
      evidence: The system-breaking shape here is a full collect behind a preview, and it is fenced: the styled probe is limit(max_rows + 1) = 11, small frames never count, large frames count exactly once, and the protected test_styled_show_does_not_full_collect still forbids collect-then-slice on the 12-row frame under the R-11 re-pin.
      artifacts: [python/repark/tests/test_display_styles.py, python/repark/tests/test_display_polars_default.py]
    - id: AT-8
      status: ATTACKED
      evidence: No polars import outside tests (D-0 holds by tree inspection), polars>=1.0 stays an optional extra the pins importorskip, the error contract is IllegalArgumentException on every display key, and repark.toml [default.display] carries the same snake_case names (style, max_rows asserted in the Rust profile tests).
      artifacts: [python/repark/src/repark/spark/dataframe/polars_cells.py, crates/repark-core/src/config_file/tests.rs, python/repark/tests/test_display_polars_default.py]
    - id: AT-9
      status: ATTACKED
      evidence: Every failure names the key and the valid values in its message (asserted verbatim in the refusal pins), show() INFO-logs the row count with row data at DEBUG, and each red-first run pasted its failing atom into this ledger — a display failure arrives with the key, the value, and the count.
      artifacts: [python/repark/src/repark/spark/session/session_configuration.py, python/repark/tests/test_display_polars_default.py]
    - id: AT-10
      status: ATTACKED
      evidence: Red-first every round — step 1 (2 failed), step 2 (1 failed, 1 passed), step 3 (4 failed, 3 passed), step 4 (5 failed, 7 passed), each pasted into this ledger; the one protected-pin conflict resolved by owner ruling R-11, never by editing the protected test. Branch liveness: the probe short/long arms, the three style arms, the truncate True/False/int arms, and the token-miss fallback each have a pin whose flip changes the asserted output.
      artifacts: [python/repark/tests/test_display_styles.py, python/repark/tests/test_display_polars_default.py]
  complete: true
```

