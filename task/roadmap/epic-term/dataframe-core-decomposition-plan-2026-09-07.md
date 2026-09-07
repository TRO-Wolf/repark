# DataFrame core decomposition plan

Opened: 2026-09-07. Class: implementation plan for a unit slate; not itself a unit.
Base: `main` `7a8e94ce` (the squash of PR #412). This document retires to `docs/history/` when
the last DFCORE unit below lands or a dated owner decision closes the slate.

Source: an external audit of `python/repark/src/repark/spark/dataframe/core.py` (2026-09-07,
AST/token inspection plus public-API probes on a scratch clone, no source change), re-measured
by the orchestrator on the same base before this plan was written. Every count in §1 was
re-derived and matched.

## 1. Evidence ledger (re-measured 2026-09-07 on `7a8e94ce`)

| Clause | Checkable finding | Evidence | Verdict |
|---|---|---|---|
| C-001 | `core.py` is 6,302 lines, one class, 168 class-level definitions, 166 distinct method names; nine bodies exceed 100 lines (`select` 242, `_select_with_window_pandas_udfs` 214, `_select_with_pandas_udfs` 200, `_select_with_python_udfs` 146, `join` 118, `_select_with_ordered_window_pandas_udfs` 117, `_select_with_generator` 113, `declare_sorted` 103, `with_columns` 101). | AST inventory. | PROVEN |
| C-002 | Comments are not the size driver: 583 comment-bearing lines, 511 standalone, 996 docstring lines, 588 blank. | Token inventory. | PROVEN |
| C-003 | Import identity constrains extraction: `test_t0_df_regions_import_freeze.py` pins `DataFrame.__module__ == "repark.spark.dataframe.core"` and private exports; `dataframe/__init__.py` copies every non-dunder name from `core` by `dir()`. | The freeze test and the package init. | PROVEN |
| C-004 | `approxQuantile` runs columns × probabilities collects (six for two columns, three probabilities); `percentile_approx` already accepts a probability list (`functions_expr.py`, the `approx_percentile_list` path), and the method's `relativeError` argument is validated and never passed to the engine. | The method body; a 1,000-row probe answered the same values on both shapes. | PROVEN |
| C-005 | Eager `__repr__` / `_repr_html_` fetch `limit(maxNumRows)` then run a full `count()` when the preview is full, only to decide the "only showing top N rows" footer. | `core.py` `__repr__` and `_repr_html_`. | PROVEN |
| C-006 | The CAP-1 ceiling for `core.py` is exact (6,302) in `scripts/check_lib_py.py` and `python/repark-parity/tests/test_cap_1_source_file_line_cap.py`, and the dataframe `map.md` names the exact-row exception; siblings are also exact: `joins_columns.py` 1,239, `plan_collapse.py` 1,168, `writer_readwriter.py` 1,113. | The two ceiling tables. | PROVEN |
| C-007 | `AGENTS.md` and `CLAUDE.md` cited `make check-comment-density` in `make ci`; no such target or script exists, and `test_pr_247_owner_ruling.py` pins its absence. Review holds the comment rule; the file-size ratchets are the mechanical gates. | The Makefile, the PR-247 test, the archived PR-247 revalidation ledger. | PROVEN, corrected in this plan's PR |

Corrections to the audit as received: the three export-error helpers above the class total
65 function lines, not 86; `relativeError` is validated and ignored today, which the audit's
"do not change the contract" sentence must be read as preserving.

## 2. Owner rulings (2026-09-07)

- **R-1** The worker rule "never delete or reword a pre-existing comment" is lifted for the
  DFCORE units only: code that a slice moves arrives comment-free at its new home and the
  rationale lands in that directory's `map.md`; code a slice does not move keeps its comments.
- **R-2** Public-API docstring condensation is out of scope. Docstrings move with their
  functions unchanged.
- **R-3** The stale enforcement sentence is corrected rather than a density gate re-implemented
  (PR #247 dropped that gate deliberately and pins its absence).

## 3. Shape

`DataFrame` stays in `core.py` with its slots, constructor, public signatures, aliases,
properties and identity. Public methods delegate to ordinary private module functions that
take the frame. No dynamic method attachment, no mixin hierarchy. Leaf modules import
`DataFrame` for typing only; runtime construction goes through the existing spawn boundary.
Helpers take the frame and explicit state, never closures: `make check-python-conventions`
refuses nested defs. The three oversized siblings receive nothing.

## 4. Unit slate

| Unit | Moves / changes | Source span | Order and risk |
|---|---|---|---|
| DFCORE-1 | Arrow cell conversion, map pairs and the interval refusal into `rows_export.py`; error-chain inspection, Arrow wrapper-noise filtering and export exception mapping into a new `export_errors.py`; `mapInArrow` schema validation and contiguous group assembly into new `udf_schema.py` / `grouped_udf.py`, imported directly by `joins_columns.py`. | ~80 + 65 + ~177 function lines | First. Leaf helpers, no state. Preserves error classes, chaining and memory advice; empty groups, NaN keys and batch boundaries. |
| DFCORE-2 | The four scalar / classic / window UDF projection rewrites into `udf_projection.py` and `udf_window_projection.py`; `udf_bridge.py` keeps callback execution. | 677 method lines | Second. The largest cohesive extraction; two files each under the default ceiling. |
| DFCORE-3 | Quantiles, corr/cov, crosstab, summary/describe into `statistics.py`; wrappers keep stat/DF API and error ordering. Move-only. | ~241 lines | Third. |
| DFCORE-4a | `sample`, argument normalization, `randomSplit`, `sampleBy` into `sampling.py`. | 282 method lines | Fourth. Seed behaviour, NaN validation, shared randomness and display-name mappings are contracts. |
| DFCORE-4b | repr / HTML, eager limits, `show` validation and rendering into `display.py`, delegating through the existing cache and UDF lifecycle hooks. | 309 method lines | Fifth. |
| DFCORE-5 | `approxQuantile` through the list form of `percentile_approx`: one collect per frame. `relativeError` stays validated and ignored. | perf | After DFCORE-3. Separate behaviour unit, never inside a move. |
| DFCORE-6 | Eager preview fetches `maxNumRows + 1`, renders `maxNumRows`, no `count()`. | perf | After DFCORE-4b. Separate unit. |

Left together for now: constructor / spawn / origin state, cache and checkpoint publication,
the `mapInArrow` bridge (~320 lines over seven private fields), joins and column binding, and
the 242-line `select` until its UDF branches have homes (DFCORE-2). `summary`'s UNION ALL
shape is parked behind an EXPLAIN and a benchmark; no unit until repeated scans are shown.

## 5. Acceptance, every slice

1. `scripts/check_lib_py.py` and the CAP-1 test lower `core.py`'s exact ceiling to the new
   count; a new module gets no row (default ceiling); the dataframe `map.md` exact-row
   sentence stays true. Sibling ceilings never rise.
2. An export snapshot pin: the sorted set of public and private names on
   `repark.spark.dataframe` and `repark.spark.dataframe.core`, taken before the slice, equal
   after it. The package init's `dir()` copy and the silenced F401 rule mean a missed
   re-export is otherwise invisible.
3. The freeze test's identity pins (`__module__`, class identity, slots, no instance
   `__dict__`) unchanged; the overloads and aliases on the class unchanged.
4. Collected test IDs unchanged for pure relocations; semantic proof by public Arrow values,
   types and nullability, not formatted output.
5. Gates: `make verify`, `make preflight`, and the parity harness
   (`.venv/bin/python -m pytest python/repark-parity/tests -q`, which `preflight` does not run
   and which holds CAP-1). A slice that changes Spark-visible behaviour (DFCORE-5, DFCORE-6)
   adds a scoped live-oracle leg.
6. Relevant suites: `test_t0_df_regions_import_freeze.py`, `test_dataframe_actions.py`,
   `test_interchange_parity.py`, `test_cache_persist.py`, `test_mapinarrow.py`,
   `test_pandas_udf.py`, `test_udf.py`, `test_display_styles.py`, `test_join_parity.py`,
   `test_perf_facade_logical_names.py`, `test_perf_facade_collect_rows.py`,
   `test_perf_approxpct_1.py`.

Unit-specific pins: DFCORE-5 covers all-null and empty columns, an empty probability list,
duplicate columns, integer and decimal inputs, and result shape, with a before/after collect
count. DFCORE-6 covers 0, N and N+1 rows on a plain frame and on a `mapInArrow`-backed frame,
`maxNumRows` 1, and the HTML door.

## 6. Process

Each unit is a SEPMO ledger under `task/ledgers/staging/` (`risk_tier: standard`), actor on
Muse Spark 1.3 or Grok 4.6, critic never on Opus, one unit branch per slice merged in the
order above. Do not memoize analyzed schemas or resolved columns across operations: session
configuration, map bridges, origin overlays and the logical/analyzed schema distinction make a
blanket cache unsafe, and the existing perf-facade pins already protect the one-logical-schema
read in column binding.
