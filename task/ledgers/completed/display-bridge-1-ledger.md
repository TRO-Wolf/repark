# Unit ledger — DISPLAY-BRIDGE-1 step 1 · a bridged frame's show() follows the display style (R-13)

**Retires:** this ledger moves to `../completed/` in the unit's last commit.
This file closes when DISPLAY-BRIDGE-1 merges, or when the owner closes the slate row.

**Unit:** DISPLAY-BRIDGE-1 step 1 · **Date:** 2026-09-09 · **Executor:** GLM 5.3 Flash (zai/glm-5.3-flash), Actor ·
**Branch:** `feat/display-bridge-1` · **Card facts on:** `6b814ff0`
**Model:** GLM 5.3 Flash (zai/glm-5.3-flash)
**risk_tier:** standard.

Residue round for DISPLAY-POLARS-1-S3-Q-001: `_show` checked `_use_bridge_peek` before
`_resolve_display_style`, so an uncached `mapInArrow` frame's `show()` printed the Spark-grid
peek while `repr(df)` rendered the styled table. One M round: the branch reorder plus one new
path; `core.py` is untouched (the display bodies live in `dataframe/display.py` since DFCORE-4b).

## Proposition ledger

| ID | Clause | Evidence | Verdict |
|---|---|---|---|
| C-001 | D-1: under the `spark` display style the bridge peek path in `_show` is behaviour-preserving — byte-identical grid output, existing pins green without edits. | Keep-green guard `test_bridged_show_spark_grid_unchanged` (horizontal and vertical goldens reusing the measured `_MIA_SHOW` / `_MIA_SHOW_VERTICAL` bytes) green on the base tree and after the edit; the spark peek branch is verbatim (`_normalize_show_args` → bounded peek → `_format_show_vertical` / `_format_show_table` → print → return). `test_dfcore_4b_eager_goldens.py::test_mapinarrow_backed_display_shapes`, `test_dfcore_4b_show_goldens.py`, and `test_dfcore_6_eager_preview.py::test_raising_action_surfaces_identically` green untouched inside `make py-test-facade` (5851 passed). | **PROVEN** |
| C-002 | D-2: under `polars` / `duckdb` the bridge branch peeks once (`_consume_map_in_arrow_batches(max_output_rows=limit)`) and hands the peeked table to `_render_styled_show` via its new `peeked` argument; a peek that returned fewer rows than asked fixes the shape exactly (no `count()`), a full peek makes the renderer take one `count()` the way `repr` already does; `show()` and `repr` on the same bridged frame render identically. | Red-first pins then green: `test_bridged_show_matches_repr_polars` + `test_bridged_show_matches_repr_duckdb` (12-row doors equal), `test_bridged_small_peek_shape_exact` + `test_bridged_small_peek_shape_exact_duckdb` (3-row exact shape, whole frame, no ellipsis), `test_bridged_show_pays_one_count_when_peek_full` (25-row: spy `== [1]`), `test_bridged_small_peek_never_counts` (12-row peek short: zero counts, exact shape), `test_bridged_styled_show_peeks_once` (exactly one peek per show). Red run `7 failed, 2 passed in 0.26s` on the base tree (below); `65 passed` for the three display files after the edit. Implementation: short peek → `total_rows = peek_table.num_rows`; full peek → one `frame.count()`; polars renders `total_rows <= max_rows` whole (`head = peek.slice(0, n)`, no ellipsis) else the `keep = min(n, max_rows)` split with the tail sliced off the peek when the peek holds the whole frame and `_preview_tail_rows` otherwise; duckdb keeps its `total_rows <= n` whole-frame / `n//2 + n-head_n` split, its tail fetch being reachable only on a full peek. No new module-level names, so the frozen export surfaces hold. | **PROVEN** |
| C-003 | D-3: `vertical=True` under a styled style keeps its existing warning (same message, `UserWarning`, `stacklevel=3`), including on the new styled bridge path; the rendering stays horizontal. | Red-first pin `test_bridged_styled_vertical_warns` (`DID NOT WARN` on base, green after); the single `warnings.warn` site moved above the style dispatch so one block serves the styled non-bridge and styled bridge paths at the same call depth; `test_dfcore_4b_show_goldens.py::test_show_styled_vertical_warning_attributes_to_caller` green untouched. | **PROVEN** |

## Red first

Nine pins in `python/repark/tests/test_display_bridge_1.py` written before the `display.py`
edit and run on the base tree (`6b814ff0`): `7 failed, 2 passed in 0.26s`.

- `test_bridged_show_matches_repr_polars` → `AssertionError: assert False` — the captured
  `show()` output is the Spark-grid peek (`'+----+\n| x  |\n+----+\n| 2  |\n| 4  |…'`) and does
  not start with `'shape: (12, 1)'` (`test_display_bridge_1.py:101`).
- `test_bridged_show_matches_repr_duckdb` → `AssertionError: assert '12 rows' in
  '+----+\n| x  |\n+----+\n| 2  |…'` — same peek grid (`:112`).
- `test_bridged_small_peek_shape_exact` → the 3-row peek grid does not start with
  `'shape: (3, 1)'` (`:121`).
- `test_bridged_small_peek_shape_exact_duckdb` → `assert '3 rows' in
  '+---+\n| x |\n+---+\n| 2 |\n| 4 |\n| 6 |\n+---+'` (`:132`).
- `test_bridged_show_pays_one_count_when_peek_full` → `assert 0 == 1` where
  `0 = <DataFrame.count spy>.call_count` (base peek path never counts) (`:142`).
- `test_bridged_small_peek_never_counts` → the spy atom passes on base (zero counts there
  too); the red atom is the styled shape: `'+----+\n| x  |…'` does not start with
  `'shape: (12, 1)'` (`:154`).
- `test_bridged_styled_vertical_warns` → `Failed: DID NOT WARN. No warnings of type
  (<class 'UserWarning'>,) were emitted.` (`:173`).
- Keep-green on base by design: `test_bridged_show_spark_grid_unchanged` (the D-1 bytes) and
  `test_bridged_styled_show_peeks_once` (base already peeks exactly once) — the guards against
  the fix regressing the spark grid or peeking twice.

Every red atom is the residue's own measurement: under a styled style the bridged `show()`
prints the Spark-grid peek while `repr(df)` renders the styled table.

## Gates

| Command | Result |
|---|---|
| `.venv/bin/python -m pytest python/repark/tests/test_display_bridge_1.py -q` (base tree, red run) | `7 failed, 2 passed in 0.26s` |
| `.venv/bin/python -m pytest python/repark/tests/test_display_bridge_1.py -q` (after the edit) | `9 passed in 0.27s` |
| Gate 1: `.venv/bin/python -m pytest python/repark/tests/test_display_bridge_1.py python/repark/tests/test_display_styles.py python/repark/tests/test_display_polars_default.py -q` | green: `65 passed in 2.01s` |
| Gate 2: `make py-test-facade` | green: `5851 passed, 369 skipped, 7 xfailed` — `703.86s` after the first cut and re-run green at final content (`696.74s`) after the three lint-format fixes; the 7 xfails are the base-tree `test_df_eager_1.py` red-first pins, unchanged |
| Gate 3: `make py-lint` | green: `All checks passed!` (one interim `3 errors` after the first cut — two `E501` wraps and one `SIM108` ternary — fixed before commit) |
| Gate 4: `python3 scripts/check_lib_py.py` | green: `599 files clean (default ceiling 1000; 32 exceptions; facade no-stub held)` |
| Gate 5: `python3 scripts/check_ledger_grammar.py` | green (final run at close) |

## Notes for the orchestrator

| Item | Note |
|---|---|
| Implementation home | `python/repark/src/repark/spark/dataframe/display.py` only (`_show` body, `_render_styled_show` signature + peeked fetch blocks). The card's `core.py` is stale since DFCORE-4b; `core.py` is untouched. |
| Mechanism | `_render_styled_show` gains one keyword-only `peeked: tuple[Any, int] \| None = None` parameter — the peeked table and the row count it asked for. No new module-level names, so the frozen export surfaces of `test_dfcore_1_exports.py` / `test_dfcore_4b_exports.py` hold without edits. |
| Warning hoist | The styled-vertical warning is one site above the style dispatch (same message, `UserWarning`, `stacklevel=3`), so it fires on the styled bridge path at the same call depth; `normalize` still raises before any warning or fetch on bad args. |
| Peek economics | A short peek exhausts the bridge: shape exact, head and tail both slice off the peeked table, no `count()` and no tail fetch — cheaper than the styled repr's probe/count/tail for the same frame. A full peek pays one `count()` (a full bridge run, as `repr` already pays) plus one `_preview_tail_rows`. |
| Size | `display.py` 345 → 393 lines (default ceiling 1000, no exception row); new `test_display_bridge_1.py` 176 lines (no ceiling row). No baseline moved. |
| Residue flip | `DISPLAY-POLARS-1-S3-Q-001` flipped FIXED in `task/ledgers/completed/display-polars-1-ledger.md` (dated status line at the top of the residue section; the original measurement and ruling stand as history). |
| Maps | `python/repark/tests/map.md` (new file entry + pins), `python/repark/src/repark/spark/dataframe/map.md` (display.py entry rewritten where it stated the now-fixed divergence), `task/ledgers/staging/map.md` (ledger entry) — all in the same commit. |

```yaml
COVERAGE_ATTESTATION:
  pr_unit: display-bridge-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every clause walked against behavior on the tree — the spark peek bytes re-pinned horizontal and vertical, the styled door equality pinned per style on both a 12-row and a 3-row bridged frame, the count discipline spied on both peek arms, and the warning pinned by type and message; the card's claims were each reduced to a failing atom before the edit.
      artifacts: [python/repark/tests/test_display_bridge_1.py, python/repark/src/repark/spark/dataframe/display.py]
    - id: AT-2
      status: ATTACKED
      evidence: Boundaries on both sides — 3-row (peek short, frame whole) vs 12-row (peek short, frame past max_rows) vs 25-row (peek full) bridged frames; polars and duckdb splits both; spark style unchanged against byte goldens; vertical on both the spark peek (honored) and the styled paths (warned, horizontal).
      artifacts: [python/repark/tests/test_display_bridge_1.py]
    - id: AT-3
      status: ATTACKED
      evidence: No new failure surface: the styled bridge path reuses `_render_styled_show` and the existing `PySparkException` bridge error contract unchanged (raising-bridge pins green in the facade suite); bad `show` args still raise through `_normalize_show_args` before any peek, pinned by the untouched `NOT_INT`/`NOT_BOOL` diagnostics in the facade suite.
      artifacts: [python/repark/src/repark/spark/dataframe/display.py, python/repark/tests/test_dfcore_6_eager_preview.py]
    - id: AT-4
      status: ATTACKED
      evidence: The style-vs-peek ordering is the pin, not an assumption — resolving the style after the peek (the base tree) fails seven pins; the reorder flips them green with the spark branch byte-identical. No global state added; the peeked tuple is local to one `_show` call.
      artifacts: [python/repark/tests/test_display_bridge_1.py]
    - id: AT-5
      status: N/A
      justification: Rendering reads local Arrow batches and formats text; no privileged action, no credential, no secret, no deserialization, no network on any display path.
    - id: AT-6
      status: ATTACKED
      evidence: Byte-identity held both directions — the spark peek goldens are the pre-change bytes, and the styled bridge output is asserted equal to the styled repr (already oracle-pinned against live polars in DISPLAY-POLARS-1 step 4) rather than to hand-written strings.
      artifacts: [python/repark/tests/test_display_bridge_1.py, python/repark/tests/test_display_polars_default.py]
    - id: AT-7
      status: ATTACKED
      evidence: The system-breaking shape is an unbounded bridge run behind a preview, and it stays fenced — the styled bridge path keeps the bounded peek (max_output_rows=n) as its head fetch, the exact-peek path adds no count at all, and the full-peek path adds exactly one count, spied; the protected partial-collect pins in test_display_styles.py stay green untouched.
      artifacts: [python/repark/tests/test_display_bridge_1.py, python/repark/tests/test_display_styles.py]
    - id: AT-8
      status: ATTACKED
      evidence: No new dependency, no new module, no new public name — the only surface delta is one keyword-only parameter on a private helper; the frozen export pins and the facade no-stub gate are green without edits.
      artifacts: [python/repark/src/repark/spark/dataframe/display.py, python/repark/tests/test_dfcore_4b_exports.py]
    - id: AT-9
      status: ATTACKED
      evidence: Failures keep their taxonomy — bridge errors surface as PySparkException identically on every door (raising-bridge pin green), invalid args raise PySparkTypeError before any fetch, and the styled path's INFO/DEBUG logging contract rides along unchanged.
      artifacts: [python/repark/src/repark/spark/dataframe/display.py]
    - id: AT-10
      status: ATTACKED
      evidence: Red-first — nine pins written and run on the base tree before the display.py edit, 7 red with the failing atoms pasted above; branch liveness: re-ordering the peek before the style resolution, re-counting an exact peek, peeking twice, or dropping the styled-vertical warning each flips a named pin.
      artifacts: [python/repark/tests/test_display_bridge_1.py]
  complete: true
```
