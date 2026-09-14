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
| C-001 | Provenance: release native (`__debug_assertions__` False), types4 product tree equals `main`, no JVM, memory-capped timing harness. | Header block above; `docs/perf/facade-5-display-baseline-2026-09-14.md` machine header. | **PROVEN** | Release native re-asserted inside the runner (`native_is_release()`); the run carried `release_proof: true` and a 167,915,168-byte module. |
| C-002 | Fetch/format baseline: FETCH (capped rows to Arrow) and FORMAT (formatter over the pre-materialized table) timed separately for ASCII `show`, vertical `show`, `_repr_html_`, duckdb show, polars show, and `repr`, at n ∈ {20, 1000} × truncate on/off × {flat 7-type, 50-column wide, nested struct/array/map}, warmup + 5 reps, medians, idle box + load<6 per cell; each leg's share of the wall stated. | `docs/perf/facade-5-display-baseline-2026-09-14.md` + committed runner. | **PROVEN** | 96 cells, 96/96 `door_check` byte-equal, loads 4.96–5.52. Every cell is a wall: n=20 format 0.4–3.2 ms at 14–57% share; n=1000 format 17–107 ms at 90–98% share. |
| C-003 | Census: every renderer × truncation-rule pair names the §8 pin that binds its bytes today, and every unbound pair is a row of its own; the `test_dfcore_6_eager_preview.py` scan counts recorded. | Census section below. | **PROVEN** | Census section below: 9-rule taxonomy, bound table, 30 unbound rows, scan-count notes. |
| C-004 | Missing goldens only: every unbound pair gets a golden recorded from this base tree under `python/repark/tests/`; record mode refused under CI. | `test_facade_5_display_goldens.py` + committed JSON golden. | **PROVEN** | 30 cases → `facade_5_display_goldens.json` recorded via `REPARK_FACADE_5_RECORD_GOLDENS=1` on the release interpreter; `test_record_mode_fails_when_ci_is_set` pins the CI refusal. |
| C-005 | Mutation proof: a one-line mutation turns the new goldens red; restore leaves them green. | Scratch edit, red output, `git checkout` restore, all recorded here. | **PROVEN** | 12 one-line mutations M1–M12 below; every case id red ≥ once; `git status` clean on `python/repark/src/` after restore. |
| C-006 | The §8 named pins stay green and unedited: `test_dfcore_4b_eager_goldens.py`, `test_dfcore_4b_show_goldens.py`, `test_df_eager_1.py`, `test_dfcore_6_eager_preview.py`, `test_display_styles.py`, `test_display_polars_default.py`. | Those files, same commit, no edits; run counts. | **PROVEN** | `pytest` on the six files: 99 passed; `git diff` on all six: empty. |
| C-007 | Step-1 target named from C-002's numbers: (A) a measured format wall and the Rust move that removes it byte-identically, or (B) "no format wall: ship the smallest byte-identical consolidation", naming which formatters move and what stays because it touches `eager.py`. | Step-1 target section. | **PROVEN** | Option (A) — measured format wall; see "Step-1 target" in the baseline doc and below. |
| C-008 | Gates: §8 pins green, the new goldens, `make verify`, `test_production_file_size.py` green; staged-diff comment scan empty. | Commands and counts in Evidence. | **PROVEN** | §8 99 passed; new goldens 2 passed; file-size 11 passed; `make verify` exit 0 (ruff check + format clean, ledger-grammar clean); comment scan empty on every commit. |

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
| 30 | spark doors × zero columns — ASCII `show` honest row count, vertical/HTML/eager slice-pad phantoms pinned on 1-row and 0-row frames | `spark_zero_cols`, `spark_zero_cols_empty` |

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

## Mutation proofs (C-005) — each a one-line scratch edit, run, then `git checkout` restored

The timing interpreter reads product code from `/tmp/f-types4`, so mutation
runs put THIS clone's `python/repark/src` first on `PYTHONPATH` and pre-seed
`sys.modules["repark._native"]` from the types4 release `.so` — the clone's
Python bodies are exercised while the release native stays loaded; nothing
under `/tmp/f-types4` is ever written. `changed=` lists the golden case ids
the test reported red (the assert shows at most 12 ids; parenthesized ids are
bitten but sit past that display cap).

| Mut | One-line mutation (restored) | `changed=` case ids reported red |
|---|---|---|
| M1 | `polars_cells.py` `"..."` → `".."` (spark/duckdb ellipsis) | ascii_nested, ascii_scalars, ascii_trunc_str7, duckdb_nested, duckdb_trunc10, duckdb_trunc_true, vertical_nested, vertical_trunc5 |
| M2 | `polars_cells.py` sub-cap `text[:truncate_at]` → `text[:truncate_at + 1]` | ascii_trunc2, duckdb_trunc2, vertical_trunc2 |
| M3 | `polars_cells.py` else-branch `str(value)` → `str(value) + "!"` | ascii_nested, ascii_nested_off, ascii_scalars, ascii_trunc2, ascii_trunc_str7, duckdb_nested, duckdb_scalars, duckdb_trunc10, duckdb_trunc2, duckdb_trunc_true, eager_nested, eager_trunc_conf5 (+vertical_trunc_off, vertical_nested, eager_trunc_default, eager_trunc_off, eager_wide past the display cap) |
| M4 | `polars_cells.py` duckdb `int32` → `int33` type label | duckdb_lazy_wide, duckdb_nested, duckdb_scalars, duckdb_trunc10, duckdb_trunc2, duckdb_trunc_true |
| M5 | `plan_collapse.py` `_eager_eval_grid_row` `rjust` → `ljust` | eager_nested, eager_trunc_conf5, eager_trunc_default, eager_trunc_off, eager_wide |
| M6 | `plan_collapse.py` `_show_grid_row` `" \| ".join` → `"\|".join` | ascii_nested, ascii_nested_off, ascii_scalars, ascii_trunc2, ascii_trunc_str7 |
| M7 | `display.py` `"</table>"` → `"</TABLE>"` | html_escape, html_nested, html_trunc5, html_trunc_off |
| M8 | `polars_cells.py` polars `+ "…"` → `+ "…x"` | polars_trunc10, polars_trunc2, polars_trunc_true |
| M9 | `plan_collapse.py` `_polars_row_line` `"┆".join` → `"\|".join` | polars_trunc10, polars_trunc2, polars_trunc_true, polars_unicode |
| M10 | `plan_collapse.py` zero-col `shape: (N, 0)` → `(0, N)` | styled_zero_cols |
| M11 | `plan_collapse.py` separator `"+-"` → `"x-"` | ascii_nested, ascii_nested_off, ascii_scalars, ascii_trunc2, ascii_trunc_str7, spark_zero_cols |
| M12 | `plan_collapse.py` `"-RECORD"` → `"-REC"` | vertical_nested, vertical_trunc2, vertical_trunc5, vertical_trunc_off |
| M13 | `display.py` vertical `table.slice(0, limit)` → `slice(0, min(limit, table.num_rows))` (slice-pad removed) | spark_zero_cols, spark_zero_cols_empty |
| M14 | `display.py` HTML `table.slice(0, max_rows)` → `slice(0, min(max_rows, table.num_rows))` (slice-pad removed) | spark_zero_cols, spark_zero_cols_empty |

Coverage: all 30 case ids red at least once (weakest links: html_* via M7
only, eager_trunc_default / eager_trunc_off / eager_wide via M5 only,
styled_zero_cols via M10, spark_zero_cols via M11 — each is still a real
door-rendered byte string, not a vacuous constant). Two case fixes during the
proof: `duckdb_trunc_true/10/2` moved from `show(3)` to `show(4)` — at n=3
the duckdb head/tail split never renders the truncatable row, so the case
bound nothing; and infinite floats refuse `createDataFrame`, so `ascii_scalars`
and `duckdb_scalars` take their inf/-inf row through the SQL door.

## Findings — critic-logic round 1 (`/tmp/oc-worker/f-crit5/report.md`, NEEDS_REMEDIATION)

**Rebase note.** The branch was rebased onto `main` at `c9b03c67` (carries
FACADE-4 step 0 #579 and REPLACE-LINEAR-1 #577). `git diff --name-only
30ca2ba1 c9b03c67`:

```
docs/spark-sql-iceberg-parity.md
python/repark-parity/tests/map.md
python/repark-parity/tests/test_cap_1_source_file_line_cap.py
python/repark/src/repark/spark/dataframe/core.py
python/repark/src/repark/spark/dataframe/map.md
python/repark/src/repark/spark/dataframe/replace_expr.py
python/repark/tests/_dfcore_1_expected.py
python/repark/tests/map.md
python/repark/tests/test_dfcore_1_exports.py
python/repark/tests/test_examples_dataframe_c.py
python/repark/tests/test_replace_linear_1.py
scripts/check_lib_py.py
scripts/map.md
task/ledgers/completed/map.md
task/ledgers/completed/replace-linear-1-ledger.md
task/ledgers/staging/map.md
task/ledgers/staging/replace-linear-1-ledger.md
```

`core.py` is the only file the goldens exercise. Its hunks sit at old lines
280/331/371/385 (`_join_qualifiers` slot + `_plan_child`/`_identity_child`
delegating display-name inheritance to `replace_expr._inherit_plan_metadata`,
byte-for-byte the same copies plus the new `_join_qualifiers` carry), 2463
(`replace()` body → `replace_expr._replace`), 2800/2821 (same area), 4076
(import). `__repr__` (2277), `_repr_html_` (2281) and `show` (3645) are outside
every hunk; no display-path body changed. Confirmed empirically: the full
golden suite re-run under this clone's `python/repark/src` (PYTHONPATH shim +
release native) is green byte-identical on `c9b03c67`.

| Critic id | Sev | Substance | Disposition |
|---|---|---|---|
| L-001 | P2 | vertical `show` × zero-column unbound; `Table.slice` pads `n` phantom `-RECORD`s | REMEDIATED — `show_vertical` pinned on 1-row + 0-row frames |
| L-002 | P2 | `_repr_html_` × zero-column unbound; `maxNumRows` phantom `<tr>` rows | REMEDIATED — `html` door pinned on both frames |

```yaml
FINDING:
  id: F-L1
  severity: S2
  category: AT-10
  clause: C-004
  claim: Vertical show and _repr_html_ on zero-column frames were unbound while the live doors emit slice-pad phantom rows.
  evidence: display.py:97-102 / :228-246 — table.slice(0, n) pads a 0-column Arrow table to n rows; 1-row frame prints 5 -RECORDs and 20 body <tr>; 0-row prints the same.
  disposition: REMEDIATED — spark_zero_cols and spark_zero_cols_empty pin show_vertical and html bytes on 1-row and 0-row frames; M13/M14 red both.
```

```yaml
FINDING:
  id: F-L2
  severity: S2
  category: AT-1
  clause: C-004
  claim: The doors disagree on zero-column frames — ASCII show prints the real row count while vertical show, eager repr and _repr_html_ print n / maxNumRows phantom records from the Table.slice pad.
  evidence: spark_zero_cols + spark_zero_cols_empty goldens — show: 1 and 0 rows; show_vertical: 5 RECORDs both; repr_eager: 20 ghost rows both; html: 20 ghost <tr> both.
  disposition: OPEN — owner question for step 1: pin the phantom bytes as the byte-identical contract, or fix the slice pad first (would change bytes; not this step's call).
```

## Step-1 target (C-007) — option (A): measured format wall

C-002's numbers put the format leg at 90–98% of the wall at n=1000 and above
the 20% share bar on nearly every n=20 cell. Step 1 moves the format leg to
Rust byte-identically: `_cell_text` / `_table_to_cell_rows` / the polars
nested + float spellers / `_arrow_pa_type_label` / `_style_type_label` /
`_polars_column_gap` (`polars_cells.py`), then the five grid formatters and
their row/width/rule helpers (`plan_collapse.py`), then the `_repr_html`
body (`display.py`). The fetch leg — `limit`/`to_arrow`/`count`/
`_preview_tail_rows`, bridge peek, eager conf reads, arg normalization, and
anything touching `eager.py` — stays in Python. Full move list in
`docs/perf/facade-5-display-baseline-2026-09-14.md` §Step-1 target.

## Evidence

- `8c4fd8b2` — ledger skeleton + staging map link.
- `db24a7ab` — census (taxonomy, bound table, 30 unbound rows, scan counts).
- `ce8df001` — fetch/format baseline doc + runner + `docs/perf` maps.
- `83a688c9` — 30 unbound-pair goldens + test + tests map.
- `2764511c` — step-1 target: option A, format leg to Rust byte-identical.
- `a877dfab` — gate close-out: full `pins:` citations in the tests map; ruff
  lint/format fixes.
- Baseline runner output `/tmp/facade-5-display-baseline.json` (not
  committed): 96 cells, all `door_check` equal, loads 4.96–5.52, run loads
  8.42→5.18.
- `REPARK_FACADE_5_RECORD_GOLDENS=1` record run: 2 passed, 30 cases written.
- Mutation runs M1–M12: every run `1 failed` with the `changed=` list above;
  `git status --short python/repark/src/` empty after each restore.
- Gates: §8 pins `pytest` 99 passed, all six files unedited; new goldens 2
  passed; `test_production_file_size.py` 11 passed; staged comment scan
  empty on every commit; `make verify` exit 0 — ruff `check .` all checks
  passed, `format --check .` 902 files already formatted, ledger-grammar
  128 live ledgers clean, full rust workspace tests green. First verify
  attempt red on `check-ledger-grammar` (five PROVEN clauses uncited —
  citations must live under `crates/`/`python/`/`scripts/`) and then
  `py-lint` (two E501, B905, B023); both remediated in `a877dfab`.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: facade-5
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every charter clause walked — census enumerates all 6 renderers × 6 truncation rules, each unbound pair maps to exactly one golden case id, and the step-1 target names the formatters the C-002 numbers select.
      artifacts: [task/ledgers/staging/facade-5-ledger.md, python/repark/tests/test_facade_5_display_goldens.py, docs/perf/facade-5-display-baseline-2026-09-14.md]
    - id: AT-2
      status: ATTACKED
      evidence: Boundary inputs exercised in the golden corpus — zero-column frames, inf/nan doubles through the SQL door, nested struct/array/map scalars, truncate=2/5 edges, truncate=False, 50-column wide frames, styled max_rows caps.
      artifacts: [python/repark/tests/facade_5_display_goldens.json, python/repark/tests/test_facade_5_display_goldens.py]
    - id: AT-3
      status: ATTACKED
      evidence: Record mode refused when CI or GITHUB_ACTIONS is set (pinned by test); every mutation run was followed by a git-clean verify on python/repark/src; the runner cross-checks leg-composed bytes against the real door on all 96 cells.
      artifacts: [python/repark/tests/test_facade_5_display_goldens.py, docs/perf/facade-5-display-baseline-2026-09-14/run_facade_5_display.py]
    - id: AT-4
      status: ATTACKED
      evidence: Timing cells gated on idle builders and load<6; stale-.pyc contamination in early mutation lists was detected, the runs repeated under -B, and the changed lists re-verified; the runner is one process with no shared state.
      artifacts: [docs/perf/facade-5-display-baseline-2026-09-14/run_facade_5_display.py, task/ledgers/staging/facade-5-ledger.md]
    - id: AT-5
      status: N/A
      justification: No privileged action, secret, deserialization, or path handling — goldens are committed JSON strings read by pytest.
    - id: AT-6
      status: ATTACKED
      evidence: Goldens recorded from the release base tree and byte-asserted; 96/96 door_check byte-equal; product files under python/repark/src and crates untouched (git status clean throughout, mutation restores verified).
      artifacts: [python/repark/tests/facade_5_display_goldens.json, docs/perf/facade-5-display-baseline-2026-09-14.md]
    - id: AT-7
      status: ATTACKED
      evidence: The baseline is itself the resource measurement — systemd-run MemoryMax=8G MemorySwapMax=0, warmup + 5-rep medians, per-cell idle gating; a 17–107 ms format wall at n=1000 measured and reported, no system-breaking defect.
      artifacts: [docs/perf/facade-5-display-baseline-2026-09-14.md, docs/perf/facade-5-display-baseline-2026-09-14/run_facade_5_display.py]
    - id: AT-8
      status: ATTACKED
      evidence: Release-native provenance asserted per run (native_is_release refuses a debug module); types4 product files diff-verified equal to main before goldens were recorded; the §8 named pins re-run green and unedited; no public interface changed.
      artifacts: [docs/perf/facade-5-display-baseline-2026-09-14/run_facade_5_display.py, task/ledgers/staging/facade-5-ledger.md]
    - id: AT-9
      status: N/A
      justification: No failure path or diagnosis surface added — the runner reports to stdout/JSON and exits nonzero on a door mismatch.
    - id: AT-10
      status: ATTACKED
      evidence: All 30 goldens proven red by twelve one-line mutations (M1–M12) with per-case changed lists; restore leaves the suite green; the §8 pin corpus (99 tests) re-run green; record-mode CI refusal tested.
      artifacts: [python/repark/tests/test_facade_5_display_goldens.py, python/repark/tests/facade_5_display_goldens.json, task/ledgers/staging/facade-5-ledger.md]
  complete: true
```
