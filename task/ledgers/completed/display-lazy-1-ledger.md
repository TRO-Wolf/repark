# Unit ledger — DISPLAY-LAZY-1 step 1 · a lazy frame's `repr` shows the schema, not the data (R-22)

**Unit:** DISPLAY-LAZY-1 step 1 · **Date:** 2026-09-10 · **Executor:** Muse Spark (muse-spark-1.3-contributor), Actor ·
**Branch:** `feat/display-lazy-1` · **Base:** `e4dafea7`
**Model:** Muse Spark (muse-spark-1.3-contributor)
**risk_tier:** standard.

**Retires:** this ledger moves to `../completed/` in the unit's last commit.
This file closes when DISPLAY-LAZY-1 step 1 merges.

Ruling R-22 (2026-09-10) supersedes ruling R-2: a lazy frame renders its SCHEMA.
Existing DISPLAY-POLARS-1, DF-EAGER-1 and DISPLAY-BRIDGE-1 pins that assert a lazy
`repr` renders data are re-pinned to D-1 in this round, never deleted; every
re-pointed pin is named in its clause row. Ruling RF-5 rides this card as C-006.
Step 2 (docs) has its own brief and follows; no doc file is touched here.

Environment note: the clone arrived without the built native module
(`repark._native` absent, contrary to the brief). The 2026-09-10 `repark 1.1.1`
wheel's `_native.abi3.so` from the local uv cache was copied to
`python/repark/src/repark/_native.abi3.so` (gitignored via `*.so`, outside the
commit). Smoke: `SELECT 1 AS a` collects `[Row(a=1)]`. No `maturin develop` ran.

## Proposition ledger

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | D-1: a frame with no stored shape and no materialised cache view renders, under `polars`, the `lazy: N columns, not yet materialized` first line over the data table's own header box (same widths from names and dtypes alone, same `max_cols` elision, names untruncated like the data path), closed directly under the dtype row; `duckdb` uses its own header box the same way; no row count anywhere. | `test_lazy_repr_polars_exact_bytes`, `test_lazy_repr_duckdb_exact_bytes`, `test_lazy_repr_max_cols_elision`, `test_lazy_repr_empty_columns`, `test_lazy_repr_zero_engine_actions` in `python/repark/tests/test_display_lazy_1.py` | **PROVEN** | Red-first `11 failed, 3 passed` on base `e4dafea7` (see Red first); green after the `display._repr` branch: `15 passed` (new file). The polars bytes match the card block exactly; the duckdb/elision transcriptions were reconciled against eager data-table headers rendered in this clone. |
| C-002 | D-2: `_eager_shape` set, a materialised `cache()`/`persist()` view, or an eager-plus-checkpoint frame renders the existing shape-first data table (cached pays one `count()` over the view, eager and checkpointed pay zero); a `cache()`-marked but unmaterialised frame stays lazy; an action on a lazy frame does not flip it to eager. | `test_eager_repr_renders_data_with_zero_counts`, `test_cached_materialised_repr_renders_data_with_one_count`, `test_persist_materialised_repr_renders_data_with_one_count`, `test_checkpointed_repr_renders_data_with_zero_counts`, `test_pending_cache_repr_stays_lazy`, `test_action_does_not_flip_lazy` in `python/repark/tests/test_display_lazy_1.py`; re-pinned `test_repr_of_eager_frame_skips_count_and_matches_lazy_table` in `test_df_eager_1.py` | **PROVEN** | Red-first on base (pending-cache and no-flip pins fail: base renders data); green after the branch. Step-1 disposition CLOSED in step 2: the checkpoint arm now records `_eager_shape` (see C-007), so every checkpoint-materialised frame classifies as materialised. |
| C-003 | D-3: with `spark.sql.repl.eagerEval.enabled` truthy a lazy frame's `repr` renders rows under `polars` and `duckdb` through the same path as `show(eagerEval.maxNumRows)`; on a 7-row frame the plan runs exactly once (one `to_arrow`, zero `count()` calls). | `test_eager_eval_lazy_repr_renders_rows_with_one_plan_run` in `python/repark/tests/test_display_lazy_1.py` | **PROVEN** | Red-first on base (the duckdb leg counts once, `assert [1] == []`); green after the branch: one export, zero counts, rows rendered under both styles. |
| C-004 | D-4: `spark`-style `repr`, `str(df)`, `_repr_html_` (`None` under `polars`/`duckdb`), `show()` and every action are byte-identical; `show() == repr()` still holds for eager bridged frames while a lazy bridged frame renders D-1. | `test_spark_door_and_str_and_html_unchanged`, `test_lazy_bridged_repr_renders_header` in `python/repark/tests/test_display_lazy_1.py`; re-pinned `test_bridged_show_matches_repr_polars`, `test_bridged_show_matches_repr_duckdb`, `test_bridged_small_peek_shape_exact_duckdb` in `test_display_bridge_1.py` | **PROVEN** | Red-first on base (lazy bridged renders data); green after the branch. The spark door never enters the changed code (verified by structure plus the byte pins). |
| C-005 | D-5: `withColumns`, `withColumn`, `select`, `filter`, `groupBy().agg`, `orderBy`, `join` and `union` cost zero UDF-spy calls and zero `count()` calls through `repr`, until an action; a closing action proves the spy UDF is still wired into the plan. | `test_transformations_stay_lazy_until_action` in `python/repark/tests/test_display_lazy_1.py` | **PROVEN** | Red-first on base (50 spy calls after the repr loop). Green after the branch as eight lazy chains: each op holds literal zero through build and repr, each closing `collect()` runs the plan exactly once per output row. Measured during red: the first child plan over a bridge-UDF frame runs the deliberate plan-stable snapshot once (`_prepare_for_plan`), so the chains introduce the spy onto native frames. |
| C-006 | RF-5: the `duckdb` styled branch probes `limit(max_rows + 1)` first and only counts past it, the way R-11 did for `polars`; the protected pin's `duckdb` section re-pins `max_rows_per_export` to 11 with the `all(row_count < 12)` tooth and the `(10, 2)` skip byte-identical. | `test_styled_show_does_not_full_collect` (`duckdb` section) in `python/repark/tests/test_display_styles.py` | **PROVEN** | Red forced by the probe (`assert 11 <= 2`, exports `[11, 2, 2]`, R-11 pattern); green after the one-token re-pin to 11. The duckdb goldens (`_DUCKDB_ELLIPSIS_GOLDEN` et al.) are byte-identical. |
| C-007 | Step-2 close-out of the C-002 disposition (brief asks C-006; taken here as C-007 since C-006 is RF-5): a plain `localCheckpoint()` with no prior `eager()` records the same `_eager_shape` marker the eager path sets, so the frame renders data with zero plan re-runs; a pending checkpoint that later discharges records it too. | `test_plain_checkpoint_repr_renders_data_with_zero_reruns`, `test_pending_checkpoint_discharge_renders_data` in `python/repark/tests/test_display_lazy_1.py` | **PROVEN** | Red-first on the step-1 tree (`assert None == (25, 5)`, `assert None == (12, 1)`); green after the two-line checkpoint-arm change. No new attribute: the shape count runs over the pinned view (checkpoint clears `_map_bridge`, so nothing re-runs); skipped when a shape is already stored, so eager-plus-checkpoint and cache paths keep their exact action profiles. |

## Red first

Base `e4dafea7`, `.venv/bin/python -m pytest python/repark/tests/test_display_lazy_1.py -q`:
`11 failed, 3 passed`. Every D-1/D-3/D-4-lazy/D-5 pin fails because a lazy `repr`
still renders the data table (`assert 'shape: (25, 5)' == 'lazy: 5 colu...'`; the
D-5 loop ends with 50 spy-UDF calls; the duckdb eagerEval leg counts once,
`assert [1] == []`). The 3 green pins hold behavior this card keeps: eager and
persist-materialised data renders, and the spark door. Full FAILED list:
`test_lazy_repr_polars_exact_bytes`, `test_lazy_repr_duckdb_exact_bytes`,
`test_lazy_repr_max_cols_elision`, `test_lazy_repr_zero_engine_actions`,
`test_eager_repr_renders_data_with_zero_counts`,
`test_cached_materialised_repr_renders_data_with_one_count`,
`test_pending_cache_repr_stays_lazy`, `test_action_does_not_flip_lazy`,
`test_eager_eval_lazy_repr_renders_rows_with_one_plan_run`,
`test_lazy_bridged_repr_renders_header`,
`test_transformations_stay_lazy_until_action`.

Green after the `display._repr` branch plus the duckdb probe:
`14 passed` (same command). Two expectations needed transcription fixes against
reviewed output (duckdb labels are `int64`/`double`/`int32`, not the polars
spellings; the `max_cols` gap cell is 3 spaces): each was reconciled against an
eager data-table header rendered in this clone (polars header lines and bottom
byte-identical; duckdb top/names/types byte-identical, bottom narrower by the
dropped footer widening, per D-1). D-5 was rewritten during red as eight lazy
chains after measuring that the first child plan over a bridge-UDF frame runs
the deliberate plan-stable snapshot once (`_prepare_for_plan`, 5 calls on a
5-row frame); the chains introduce the spy onto native frames so every op holds
literal zero, with a per-chain closing action proving the wiring.

## Pins re-pointed (never deleted)

- `test_display_polars_default.py`: `test_polars_repr_renders_table_without_eager_eval`,
  `test_duckdb_repr_renders_table` (lazy asserts the header, row equality moves to eager
  frames), `test_small_frame_repr_does_not_count` (header with zero counts),
  `test_polars_oracle_ints_strings_nulls`, `test_polars_oracle_floats_bools_dates`,
  `test_polars_oracle_wide_frame_col_ellipsis` (oracle equality moves to eager frames),
  `test_str_len_cuts_with_ellipsis`, `test_display_keys_conf_get_set` (row content moves
  to eager frames). `show()` pins untouched.
- `test_df_eager_1.py`: `test_repr_of_eager_frame_skips_count_and_matches_lazy_table`
  (lazy header with zero counts; eager data table over the same header box).
  `test_lazy_checkpoint_styled_repr_discharges_without_count_query` still green, untouched.
- `test_display_bridge_1.py`: `test_bridged_show_matches_repr_polars`,
  `test_bridged_show_matches_repr_duckdb`, `test_bridged_small_peek_shape_exact_duckdb`
  (equality moves to eager bridged frames). Bridge `show()`/count/peek pins untouched.
- `test_display_styles.py`: `test_styled_show_does_not_full_collect` duckdb section,
  one token (`max_rows_per_export` 2 → 11, RF-5/R-11 pattern).

## Gates

- `.venv/bin/python -m pytest python/repark/tests/test_display_lazy_1.py -q`: `15 passed`.
- Six display-adjacent suites (`test_display_lazy_1`, `test_display_styles`,
  `test_display_polars_default`, `test_df_eager_1`, `test_dfcore_6_eager_preview`,
  `test_display_bridge_1`): `103 passed`.
- `make verify`: exit 0 (ci incl. `check-ledgers`, `check-ledger-grammar`,
  `check_lib_py`, plus `cargo test --workspace`).
- Fence: `git diff --cached -- '*.rs' '*.py' '*.toml' '*.sh' '*.yml' | grep -P
  '^\+\s*(//|#(?! noqa))'` prints nothing (one sanctioned `# noqa: N812` rides
  along on the `functions as F` import line).

## Step-2 gates

- C-007 pins red-first on the step-1 tree (`assert None == (25, 5)`,
  `assert None == (12, 1)`), green after the checkpoint-arm change.
- Brief gate command (six display-adjacent suites): `105 passed`; plus
  `test_cache_persist.py` (checkpoint blast radius: all plain-checkpoint
  consumers assert values only): `129 passed`.
- `make verify`: exit 0 after staging. Close-out correction: no baseline increase —
  the `check_lib_py` core.py row ratchets 4487 to 4486, mirrored in the CAP-1 test.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: display-lazy-1-step-1
  complete: true
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Red-first pins for every clause (11 failed on base, pasted above); green after the fix (15 passed new file, 102 passed across the six display-adjacent suites). Step 2: C-007 pins red-first on the step-1 tree (assert None == (25, 5), assert None == (12, 1)), green after the checkpoint-arm change.
      artifacts: [python/repark/tests/test_display_lazy_1.py]
    - id: AT-2
      status: ATTACKED
      evidence: 0/1/5/7/9/12/25-column and 0/3/5/7/12/25-row shapes pinned, including the max_cols gap, str_len cuts, max_rows edges, and the zero-column minimal box; decimal/timestamp/nested labels verified identical between the analyzed schema and executed output.
      artifacts: [python/repark/tests/test_display_lazy_1.py, python/repark/tests/test_display_polars_default.py]
    - id: AT-3
      status: ATTACKED
      evidence: The error path is the red run itself (header-vs-data assertion diffs, the 50-call spy overflow, the forced duckdb count); no raise surface changes and no new error contract.
      artifacts: [python/repark/tests/test_display_lazy_1.py]
    - id: AT-4
      status: N/A
      justification: Single-threaded facade reads of immutable frame marks (_eager_shape, _cache_view); no locks, no shared mutable state, no ordering assumptions beyond the existing materialize order.
    - id: AT-5
      status: ATTACKED
      evidence: No AWS, IAM, secrets, .github, or dependency-file change; one facade module plus five test files, three maps, and the ledger.
      artifacts: [python/repark/src/repark/spark/dataframe/display.py]
    - id: AT-6
      status: ATTACKED
      evidence: Repr is display-only; eager/cached/checkpointed data renders are byte-identical to the shipped tables (re-pinned goldens hold) and the spark door never enters the changed code. Step 2: plain and discharged checkpoints render through the same shape-first path (values asserted identical; cache/persist suites green).
      artifacts: [python/repark/tests/test_df_eager_1.py, python/repark/tests/test_display_bridge_1.py, python/repark/tests/test_cache_persist.py]
    - id: AT-7
      status: ATTACKED
      evidence: The card is a resource fix and the pins fix it: lazy repr performs zero engine actions (UDF/count/export spies all empty); the header is O(columns); materialised paths keep their single-count discipline.
      artifacts: [python/repark/tests/test_display_lazy_1.py]
    - id: AT-8
      status: ATTACKED
      evidence: No public signature change; display.py stays under the default ceiling (check_lib_py clean, no baseline touched); docstring-presence and python-conventions clean. Step 2: two added lines in core.py touch no slots, so dir(DataFrame) and the CAP-1 freeze are unchanged. Close-out round: no baseline increase — the two lines are funded by deleting the three-line cache-pinned early-return rationale in the same seat (fact restated on the dataframe map), so the core.py row ratchets 4487 to 4486 with the debt note unchanged, mirrored in the CAP-1 test.
      artifacts: [scripts/check_lib_py.py, scripts/check_docstring_presence.py, scripts/check_python_conventions.py]
    - id: AT-9
      status: N/A
      justification: No new log or metric surface; show() logging untouched.
    - id: AT-10
      status: ATTACKED
      evidence: Every clause is cited from the tests map, the dataframe map, and the staging map; each new branch (lazy/eager/cached/pending/eagerEval/spark/polars/duckdb/gap/empty) has a named pin whose flip changes the asserted output. Step 2: C-007 is cited from the test file, the tests map, the dataframe map, and the staging map; the guide map covers the docs round.
      artifacts: [task/ledgers/staging/display-lazy-1-ledger.md, python/repark/tests/map.md, python/repark/src/repark/spark/dataframe/map.md, task/ledgers/staging/map.md, docs/guide/map.md]
```
