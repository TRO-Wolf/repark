# Unit ledger — DISPLAY-POLARS-1 step 1 · default display style flips to polars

**Retires:** this ledger moves to `../completed/` in the unit's last commit.
This file closes when DISPLAY-POLARS-1 merges, or when the owner closes the slate row.

**Unit:** DISPLAY-POLARS-1 step 1 · **Date:** 2026-09-09 · **Executor:** Muse Spark (muse-spark-1.3-contributor), Actor ·
**Branch:** `feat/display-polars-1` · **Card facts on:** `f00ed9ea`
**Model:** Muse Spark (muse-spark-1.3-contributor) step 1 · GLM 5.3 Flash (zai/glm-5.3-flash) step 2 · GLM 5.3 Flash (zai/glm-5.3-flash) step 3
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
| R-11 | D-5's 11-row probe is right. The protected pin `test_styled_show_does_not_full_collect` re-pins its **polars section only** to `max_rows_per_export = 2 * edge + 1 = 11`; the duckdb section keeps `max_rows_per_export=2` untouched; the `assert all(row_count < 12 …)` line stays exactly as it is, so the pin still forbids collect-then-slice on the 12-row frame. Step 2 is polars-only; the duckdb style keeps its current fetch pattern until step 4. | Owner ruling R-11 (binding, 2026-09-09); resolves DISPLAY-POLARS-1-S2-Q-001, lean (a). |
| D-4a | `__repr__` under `polars` and `duckdb` returns the styled table as text regardless of `spark.sql.repl.eagerEval.enabled`; under `spark` the existing eager-eval behaviour stays byte-identical, including the `DataFrame[a: bigint, …]` schema form when eager-eval is off. Implemented as a style-first branch in `display._repr`; the spark branch below it is unchanged. | Step-3 brief, binding orchestrator implementation ruling. |
| D-4b | The styled repr is exactly what `show()` prints with its own defaults (`n=20`, `truncate=True` → cap 20), produced by the same `_render_styled_show` call and returned instead of printed; no second rendering path, no duplicated formatting, no separate count line (the count is the shape line the renderer already emits). | Step-3 brief, binding orchestrator implementation ruling. |
| D-4c | `_repr_html_` returns `None` under `polars` and `duckdb` (Jupyter falls back to the text repr) with eager-eval both on and off; under `spark` its behaviour — escaped HTML table when eager-eval is on, `None` otherwise — is untouched. An HTML table for the styled modes is DISPLAY-POLARS-2, not this card. | Step-3 brief, binding orchestrator implementation ruling. |
| D-4d | The bridge-peek path (`_use_bridge_peek`) keeps its current behaviour for `spark`. For the styled modes the styled repr does not consult the peek path — routing styled repr through `_render_styled_show` (D-4b's mechanism mandate, "no second rendering path") leaves the peek path unengaged, so it changes no rendering. Measured on the base tree, 12-row uncached bridged frame under `polars`: `show()` takes the peek before style resolution (spark grid, `count()` 0 calls, UDF 1 run) while `_render_styled_show(frame, "polars", n=20, truncate_at=20)` renders the polars table (`count()` 1 call, UDF 2 runs). Consequence, measured and disclosed: an uncached mapInArrow frame's repr under a styled mode renders the styled table and pays the D-5 count for frames past the probe, while its `show()` prints the peek grid — a pre-existing show-side peek-vs-style hijack (untouched, out of step-3 scope; `show()` was not modified). Filed as DISPLAY-POLARS-1-S3-Q-001 for an orchestrator ruling if a later step should reconcile the two doors for bridged frames. | Step-3 brief, binding orchestrator implementation ruling; measurements in the step-3 record. |
| D-4e | Nothing in the styled repr calls `count()` on a frame the styled renderer would not have counted: repr reuses the same `_render_styled_show` call with the same default args as `show()`, so D-5's discipline rides along (7-row repr: zero counts — pinned by `test_small_frame_repr_does_not_count`). | Step-3 brief, binding orchestrator implementation ruling; the bridged-frame residue is D-4d's disclosure, not a new fetch path. |

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
| `test_small_frame_renders_without_count` | `python/repark/tests/test_display_polars_default.py` (step 2) |
| `test_large_frame_counts_once` | `python/repark/tests/test_display_polars_default.py` (step 2) |
| `test_polars_repr_renders_table_without_eager_eval` | `python/repark/tests/test_display_polars_default.py` (step 3) |
| `test_duckdb_repr_renders_table` | `python/repark/tests/test_display_polars_default.py` (step 3) |
| `test_spark_repr_unchanged` | `python/repark/tests/test_display_polars_default.py` (step 3) |
| `test_styled_repr_html_is_none` | `python/repark/tests/test_display_polars_default.py` (step 3) |
| `test_small_frame_repr_does_not_count` | `python/repark/tests/test_display_polars_default.py` (step 3) |

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

