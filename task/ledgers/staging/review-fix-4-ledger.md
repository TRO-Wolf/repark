# Unit ledger — REVIEW-FIX-4 · the eager frame's checkpoint paths (Q-12, Q-13)

**Retires:** this ledger moves to `../completed/` in the unit's last commit.
This file closes when REVIEW-FIX-4 merges.

**Unit:** REVIEW-FIX-4 · **Date:** 2026-09-10 · **Executor:** Muse Spark (muse-spark-1.3-contributor), Actor ·
**Branch:** `fix/review-fix-4` · **Base:** `0fb84a1d`
**Model:** muse-spark-1.3-contributor
**risk_tier:** standard.

D-1/D-2 close Q-12/Q-13; D-3 closes Q-50. No live-Spark leg: `REPARK_PARITY_LIVE`
was never set, per the card. The oracle is the Spark discharge semantic the card
states: `localCheckpoint(eager=False)` discharges on the next action.

## Proposition ledger

| ID | Clause | Proof obligation | Verdict |
|---|---|---|---|
| C-001 | D-1: `_to_lazy` never interpolates `_cache_view` into SQL; an eager frame goes lazy through `_spawn_preserving_identity(frame._inner)`; a set `_eager_shape` with no `_cache_view` is not a cache-owned frame. | Red on base `0fb84a1d`: `test_checkpointed_eager_lazy_collects_own_rows` fails with `repark.errors.AnalysisException: Error during planning: table 'datafusion.public.none' not found`. Green after the fix: `lazy()` answers a shape-less copy (`_eager_shape is None`, `is_cached is False`) that plans and collects `[1, 2]`. | **PROVEN** |
| C-002 | D-2: `count()` and the styled row count keep the no-count-query ban and still run the pending-checkpoint materialize, so `localCheckpoint(eager=False)` discharges on the next action. | Red on base `0fb84a1d`: `test_lazy_checkpoint_count_discharges_without_action` and `test_lazy_checkpoint_styled_repr_discharges_without_count_query` fail with `assert True is False` on `DataFrame[id: bigint]._checkpoint_lazy` (the spy lists stay empty, so the ban held even while red). Green after the fix: `count()` answers `2` and the styled `repr` answers twelve rows with zero engine actions / zero `count()` calls, `_checkpoint_lazy` is `False`, `_cache_view` is `None`, and the old cache view is gone from `list_temp_view_names()`. | **PROVEN** |
| C-003 | D-3: a temp view named `none` in the session is unreachable from a checkpointed frame's `lazy()`; the frame's own rows come back. | Red on base `0fb84a1d`: `test_checkpointed_eager_lazy_ignores_none_view` fails with `assert [99] == [1, 2]` (`At index 0 diff: 99 != 1`) — the `none` view's row comes back. Green after the fix: the lazy copy collects `[1, 2]`. | **PROVEN** |

## Fix

| Name | Layer |
|---|---|
| `_to_lazy` | `python/repark/src/repark/spark/dataframe/eager.py`; the `session.sql(f"SELECT * FROM {frame._cache_view}")` re-plan is gone, the eager frame goes lazy as `_spawn_preserving_identity(frame._inner)` |
| `_count_rows` | `python/repark/src/repark/spark/dataframe/eager.py`; runs `_materialize_cache_if_needed()` before reading `_eager_shape`, so a pending checkpoint discharges while a known shape still answers with no query |
| `_styled_total_rows` | `python/repark/src/repark/spark/dataframe/display.py`; same materialize-first shape, so a styled preview discharges a pending checkpoint with no `count()` call |

## Pins

`python/repark/tests/test_df_eager_1.py`: `test_checkpointed_eager_lazy_collects_own_rows`,
`test_checkpointed_eager_lazy_ignores_none_view`,
`test_lazy_checkpoint_count_discharges_without_action`,
`test_lazy_checkpoint_styled_repr_discharges_without_count_query`.
Keep-green: the eleven prior tests in the file, plus `test_cache_persist.py`,
`test_dfcore_6_eager_preview.py`, `test_dfcore_4b_eager_goldens.py`,
`test_display_styles.py`, `test_display_polars_default.py`, `test_mapinarrow.py`.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: review-fix-4
  complete: true
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Four red-first pins over the three decisions; red output pasted in the clause rows above; 15/15 green after the fix.
      artifacts: [python/repark/tests/test_df_eager_1.py]
    - id: AT-2
      status: ATTACKED
      evidence: Incidental controls are the eleven prior tests in the file plus the cache, eager-preview, display, and bridge neighbor suites, all green.
      artifacts: [python/repark/tests/test_df_eager_1.py, python/repark/tests/test_cache_persist.py, python/repark/tests/test_display_styles.py, python/repark/tests/test_mapinarrow.py]
    - id: AT-3
      status: ATTACKED
      evidence: The error path is the red run itself (AnalysisException on the None interpolation, stale-flag asserts on the discharge paths); no raise surface changes.
      artifacts: [python/repark/tests/test_df_eager_1.py]
    - id: AT-4
      status: N/A
      justification: Single-handle flag transitions on the calling thread; no shared mutable state and no new concurrency.
    - id: AT-5
      status: ATTACKED
      evidence: No AWS, IAM, secrets, .github, or dependency-file change; only two facade modules and one test file touched.
      artifacts: [python/repark/src/repark/spark/dataframe/eager.py, python/repark/src/repark/spark/dataframe/display.py]
    - id: AT-6
      status: ATTACKED
      evidence: No public signature change; lazy/count/repr return the same shapes as before, over the same rows.
      artifacts: [python/repark/src/repark/spark/dataframe/eager.py, python/repark/src/repark/spark/dataframe/display.py]
    - id: AT-7
      status: N/A
      justification: The card forbids the live-Spark leg (REPARK_PARITY_LIVE never set); the oracle is the card's stated Spark discharge semantic.
    - id: AT-8
      status: ATTACKED
      evidence: eager.py, display.py, and test_df_eager_1.py stay under the default ceiling; check_lib_py and the docstring gate pass.
      artifacts: [scripts/check_lib_py.py, scripts/check_docstring_presence.py]
    - id: AT-9
      status: N/A
      justification: No new log or metric surface.
    - id: AT-10
      status: ATTACKED
      evidence: Pins cited in the tests map and the dataframe map; clause rows carry the red runs.
      artifacts: [task/ledgers/staging/review-fix-4-ledger.md, python/repark/tests/map.md, python/repark/src/repark/spark/dataframe/map.md]
```
