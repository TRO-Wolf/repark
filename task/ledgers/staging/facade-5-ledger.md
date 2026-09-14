# Unit ledger — FACADE-5 · display renderer in Rust — step 0

**Date:** 2026-09-14 · **Branch:** `perf/facade-5-s0` (C-001..C-008) · **Base:** `e147685b`
**Model:** swe-2-high · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** Card FACADE-5 (audit §8): the display renderer — the `display.py`
bodies (514 lines) and the `plan_collapse.py` / `polars_cells.py` formatters —
moves to Rust **byte-identical or not at all**. The audit left its format cost
UNMEASURED (§6): the quoted eager walls were scan effects, never render costs.
Step 0 is measurement and pins only — the fetch/format split baseline per
renderer, the renderer × truncation-rule census, the missing goldens with
mutation proofs, and the step-1 target. No product code under
`python/repark/src/` or `crates/` changes in this step.

**Not in this step:** `STATUS.md`, `python/repark/src/`, `crates/`,
`dataframe/core.py`, `dataframe/eager.py`, `dataframe/cache_handle.py`,
`spark/catalog.py`, `spark/functions_collections.py`, the fenced Rust files the
brief names. No JVM. Steps 1+ are later branches.

**Interpreter.** This clone has no `.venv` and builds nothing. Every Python
command runs `/tmp/f-types4/.venv/bin/python` (release native,
`repark._native.__debug_assertions__` False); `repark` resolves to
`/tmp/f-types4/python/repark/src`, whose product files equal `main`'s:

```
$ git -C /tmp/f-types4 diff origin/main -- python/repark/src crates
$ /tmp/f-types4/.venv/bin/python -c "import repark._native; print(repark._native.__debug_assertions__)"
False
```

Timing processes run under `systemd-run --user --scope -p MemoryMax=8G -p
MemorySwapMax=0` with `OPENBLAS_NUM_THREADS=8 OMP_NUM_THREADS=8`; each cell
waits for an idle box (no cargo/rustc/maturin) and a 1-minute load under 6,
recorded beside the cell.

## PROPOSITION LEDGER — FACADE-5 step 0 — 2026-09-14

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | Provenance: release native (`__debug_assertions__` False), types4 product tree equals `main`, no JVM, memory-capped timing harness. | Header block above; `docs/perf/facade-5-display-baseline-2026-09-14.md` machine header. | **OPEN** | Verified at session start; runner re-asserts `native_is_release()` per run. |
| C-002 | Fetch/format baseline: FETCH (capped rows to Arrow) and FORMAT (formatter over the pre-materialized table) timed separately for ASCII `show`, vertical `show`, `_repr_html_`, duckdb show, polars show, and `repr`, at n ∈ {20, 1000} × truncate on/off × {flat 7-type, 50-column wide, nested struct/array/map}, warmup + 5 reps, medians, idle box + load<6 per cell; each leg's share of the wall stated. | `docs/perf/facade-5-display-baseline-2026-09-14.md` + committed runner. | **OPEN** | Pending measurement. |
| C-003 | Census: every renderer × truncation-rule pair names the §8 pin that binds its bytes today, and every unbound pair is a row of its own; the `test_dfcore_6_eager_preview.py` scan counts recorded. | Census section below. | **OPEN** | Pending. |
| C-004 | Missing goldens only: every unbound pair gets a golden recorded from this base tree under `python/repark/tests/`; record mode refused under CI. | `test_facade_5_display_goldens.py` + committed JSON golden. | **OPEN** | Pending. |
| C-005 | Mutation proof: a one-line mutation turns the new goldens red; restore leaves them green. | Scratch edit, red output, `git checkout` restore, all recorded here. | **OPEN** | Pending. |
| C-006 | The §8 named pins stay green and unedited: `test_dfcore_4b_eager_goldens.py`, `test_dfcore_4b_show_goldens.py`, `test_df_eager_1.py`, `test_dfcore_6_eager_preview.py`, `test_display_styles.py`, `test_display_polars_default.py`. | Those files, same commit, no edits; run counts. | **OPEN** | Pending. |
| C-007 | Step-1 target named from C-002's numbers: (A) a measured format wall and the Rust move that removes it byte-identically, or (B) "no format wall: ship the smallest byte-identical consolidation", naming which formatters move and what stays because it touches `eager.py`. | Step-1 target section. | **OPEN** | Pending. |
| C-008 | Gates: §8 pins green, the new goldens, `make verify`, `test_production_file_size.py` green; staged-diff comment scan empty. | Commands and counts in Evidence. | **OPEN** | Pending. |

## Census — renderer × truncation rule (C-003)

Rule taxonomy (what reaches the formatter, from `_cell_text` /
`_normalize_show_args` / `_eager_eval_limits` / `_repr_html`):

- **T-off** — cap `None`: full cell text (`truncate=False`; int/float ≤ 0).
- **T-ell** — cap ≥ 3: `text[:cap-3] + "..."` (spark + duckdb) or
  `text[:cap] + "…"` (polars, any cap > 0).
- **T-sub** — 0 < cap < 3: `text[:cap]`, no ellipsis (spark + duckdb only).
- **T-hard** — `cell[:cap]`, no ellipsis marker (eager-eval grid + HTML only).
- **T-arg** — door argument normalization: `truncate=True` → cap 20 (spark) /
  `str_len` (styled); int/float/digit-string accepted; bool and non-digit
  refuse `NOT_BOOL`; `n` / `vertical` validation refuses `NOT_INT` / `NOT_BOOL`.
- **T-nest** — nested-cell spelling: polars `{a,b}` / `[x, y, … z]` / quoted
  strings / depth-8 cap; spark + duckdb `str(value)`.
- **T-scal** — scalar spellings: `NULL`/`null`, lowercase booleans, float
  (`str` / `nan` / `NaN` / polars fixed↔scientific switch / f16–f32 scalar
  round-trip), decimal/date/timestamp/binary `str()`, unicode.
- **T-frame** — frame-level rules: spark `+-+` grid + `ljust`, vertical
  `-RECORD i-` + `only showing top`, eager `|rjust|` abutted grid + footer,
  styled head/tail/ellipsis/`shape:`/`N rows (K shown)`, polars `max_cols`
  gap + `---` dashes + type-label row, duckdb centred header + numeric rjust.
- **T-esc** — `html.escape(quote=True)` on names + cells, `<table border='1'>`
  shape, footer (HTML door only).

### Bound today (the §8 pin that binds each pair)

| Renderer | Rule | Bound by |
|---|---|---|
| ASCII `show` | T-off | `test_dfcore_4b_show_goldens.py` `_MIXED_TRUNC_FALSE`; `test_display_styles.py` `_SPARK_NULL_GOLDEN`; substring `test_show_truncate_non_positive_means_no_truncation` (0/−1/−20) |
| ASCII `show` | T-ell | `test_dfcore_4b_show_goldens.py` `_MIXED_DEFAULT` (cap 20), `_MIXED_TRUNC_5` (cap 5); substring `test_default_show_truncate_and_n_unchanged` (cap 10) |
| ASCII `show` | T-arg | `test_show_rejects_bool_n`, `test_show_bad_vertical_and_truncate` (NOT_BOOL ×2 + digit-string smoke), `test_show_truncate_non_positive_means_no_truncation` |
| ASCII `show` | T-scal (NULL/bool/str/unicode) | `_MIXED_*`, `_SPARK_NULL_GOLDEN`, `_UNI_SHOW`, `test_styled_show_boolean_lowercase` |
| ASCII `show` | T-frame | `_MIXED_N0/N1/N5/N6`, `_EMPTY_SHOW`, `_UNI_SHOW`, `_MIA_SHOW`; `test_show_count_tallies` (0 counts) |
| vertical `show` | T-ell (cap 20) | `_MIXED_VERTICAL`, `_UNI_VERTICAL`, `_MIA_SHOW_VERTICAL`, `_SPARK_GRID_MIA_VERTICAL` |
| vertical `show` | T-frame (records + footer) | `_MIXED_VERTICAL_N1`, `_EMPTY_VERTICAL`, `test_vertical_footer_without_golden`, `test_show_count_tallies` |
| `repr` spark lazy | schema form | `_REPR_OFF`, `_SPARK_REPR_SCHEMA`, `test_spark_door_and_str_and_html_unchanged` |
| `repr` eager grid | T-frame (grid + footers) | `_REPR_M1/M2/M20` family (caps 1/2/20, empty/exact/over), `_MIA_REPR`, `test_eager_count_tallies` |
| `_repr_html_` | T-frame (footers, `None` under styled) | `_HTML_M1/M2/M20`, `_HTML_OFF`, `test_styled_repr_html_is_none`, `test_preview_doors_never_count` |
| polars `show` | T-off | `_POLARS_12/EMPTY/ONE_ROW/NULL_NAN_GOLDEN`, `_STYLED_POLARS_12/N3/N0` |
| polars `show` | T-nest | live-polars oracle equality `test_polars_oracle_ints_strings_nulls`, `test_polars_oracle_list_ellipsis_boundary` |
| polars `show` | T-scal | oracle `test_polars_oracle_float_fixed_scientific_switch`, `test_polars_oracle_floats_bools_dates`, `test_styled_show_narrow_arrow_type_labels`, `test_styled_show_boolean_lowercase` |
| polars `show` | T-frame | `_POLARS_*` goldens, `test_polars_max_rows_boundaries_match_live_polars`, `test_lazy_repr_max_cols_elision`, `test_display_keys_conf_get_set`, `test_polars_style_honors_n_keep_set` |
| duckdb `show` | T-off | `_DUCKDB_SMALL/ELLIPSIS/ONE_ROW/EMPTY/NULL_GOLDEN`, `_STYLED_DUCKDB_12/N3/N0` |
| duckdb `show` | T-scal (NULL/nan/bool) | `_DUCKDB_NULL_GOLDEN`, `test_styled_show_boolean_lowercase`, `test_styled_show_narrow_arrow_type_labels` |
| duckdb `show` | T-frame | `_DUCKDB_*`, `test_duckdb_style_show_zero_footer_reports_zero_shown`, `test_duckdb_style_show_one_keeps_first_row`, `test_styled_show_keeps_its_count` |
| styled `repr` | lazy headers | `_POLARS_LAZY_5`, `_DUCKDB_LAZY_5`, `_POLARS_LAZY_12` (`test_display_lazy_1`); `test_lazy_repr_empty_columns` (zero-col substring) |
| styled `repr` | eager = show bytes | `test_polars_repr_renders_table_without_eager_eval`, `test_duckdb_repr_renders_table`, `test_bridged_show_matches_repr_*` |
| cross-door | bridge peek discipline | `_MIA_*`, `test_display_bridge_1` (peek once, ≤ cap rows, ≤ 1 count), `test_mapinarrow_preview_bounds_udf_rows` |
| cross-door | styled vertical warning | `test_show_styled_vertical_warning_attributes_to_caller`, `test_bridged_styled_vertical_warns` |
| cross-door | styled lazy repr under eagerEval | `test_eager_eval_lazy_repr_renders_rows_with_one_plan_run` (one plan run, zero counts) |

### UNBOUND — no golden binds the pair (one golden case each, C-004)

| # | Renderer × rule | Golden case id |
|---|---|---|
| 1 | ASCII × T-sub (cap 2, no ellipsis) | `ascii_trunc2` |
| 2 | ASCII × T-arg digit-string (`truncate="7"` bytes) | `ascii_trunc_str7` |
| 3 | ASCII × T-nest (struct/array/map, cap 20) | `ascii_nested` |
| 4 | ASCII × T-nest × T-off | `ascii_nested_off` |
| 5 | ASCII × T-scal (nan/±inf/decimal/date/timestamp/binary) | `ascii_scalars` |
| 6 | vertical × T-off | `vertical_trunc_off` |
| 7 | vertical × T-ell (cap 5) | `vertical_trunc5` |
| 8 | vertical × T-sub (cap 2) | `vertical_trunc2` |
| 9 | vertical × T-nest + T-scal | `vertical_nested` |
| 10 | eager repr × T-hard (conf truncate 20, long cell) | `eager_trunc_default` |
| 11 | eager repr × T-hard (conf truncate 5) | `eager_trunc_conf5` |
| 12 | eager repr × T-off (conf truncate 0) | `eager_trunc_off` |
| 13 | eager repr × T-nest + T-scal | `eager_nested` |
| 14 | eager repr × 50-column wide (no max_cols rule) | `eager_wide` |
| 15 | HTML × T-esc (names + cells carrying `&<>"'`) | `html_escape` |
| 16 | HTML × T-hard (conf truncate 5) | `html_trunc5` |
| 17 | HTML × T-off (conf truncate 0) | `html_trunc_off` |
| 18 | HTML × T-nest + T-scal | `html_nested` |
| 19 | polars × T-ell `truncate=True` → `str_len` 30 | `polars_trunc_true` |
| 20 | polars × T-ell `truncate=10` | `polars_trunc10` |
| 21 | polars × T-ell `truncate=2` (still `…`) | `polars_trunc2` |
| 22 | polars × unicode cells under cap | `polars_unicode` |
| 23 | duckdb × T-ell `truncate=True` → `str_len` 30 (`...`) | `duckdb_trunc_true` |
| 24 | duckdb × T-ell `truncate=10` | `duckdb_trunc10` |
| 25 | duckdb × T-sub `truncate=2` (no ellipsis) | `duckdb_trunc2` |
| 26 | duckdb × T-nest (`str(dict)`/`str(list)`) | `duckdb_nested` |
| 27 | duckdb × T-scal (floats/dates/decimal/binary) | `duckdb_scalars` |
| 28 | duckdb lazy repr × 12 columns (no max_cols gap rule) | `duckdb_lazy_wide` |
| 29 | styled show × zero columns (polars `shape: (N, 0)` / duckdb `┌┐`) | `styled_zero_cols` |
| 30 | ASCII/eager × zero columns | `spark_zero_cols` |

### Eager-preview scan counts the `test_dfcore_6_eager_preview.py` pins assert

- `test_preview_doors_never_count` — `DataFrame.count` call count **0** on
  `repr`, `_repr_html_`, `show(20, vertical=True)`, `show(20)` for a 25-row
  plain frame at cap 20, and on a cached frame (`repr` byte-identical to the
  plain frame's).
- `test_styled_show_keeps_its_count` — exactly **1** `count()` per polars and
  per duckdb `show(20)` on a 12-row frame.
- `test_mapinarrow_preview_bounds_udf_rows` — the bridge yields **≤ 21**
  (`maxNumRows + 1`) rows on `repr` and `_repr_html_` at cap 20 (row-at-a-time
  bridge over a 100-row frame).
- `test_repr_footer_presence_matches_row_count` /
  `test_html_footer_presence_matches_row_count` — rendered data rows
  `== min(size, cap)`; footer iff `size > cap`; caps {1, 20} × sizes
  {0, cap, cap+1}, plain and bridged.
- `test_max_num_rows_edge_shapes_preserved` — cap `"0"`/`"-3"` render the
  `top 0 rows` footer; `"abc"` falls back to 20; `truncate` conf `3` honoured
  on both repr and HTML.
- `test_raising_action_surfaces_identically` — one bridge `ValueError`
  reaches all four doors with an identical first line (`preview boom`).
- Adjacent count pins in the §8 set: `test_eager_count_tallies` (0 counts on
  repr/HTML plain + bridged), `test_show_count_tallies` (0 counts on both
  show doors), `test_styled_show_does_not_full_collect` (`limit_with_skip`
  `(7, 5)` polars / `(10, 2)` duckdb, every `to_arrow` export < 12 rows, no
  root-plan export on either seam), `test_preview_tail_rows_*` (skip math,
  `total ≤ fetch` short-circuit never calls `limit_with_skip`).

## Evidence

Skeleton commit; sections fill as the step lands.
