# Unit ledger — DF-EXPLAIN-1 · `DataFrame.explain()` prints a plan, not `Row(...)`

**Retires:** this ledger moves to `../completed/` in the unit's last commit.
This file closes when DF-EXPLAIN-1 merges, or when the owner closes the slate row.

**Unit:** DF-EXPLAIN-1 · **Date:** 2026-09-08 · **Executor:** GLM 5.3 Flash (zai/glm-5.3-flash), Actor ·
**Branch:** `feat/df-explain-1` · **Base:** `f00ed9ea`
**Model:** GLM 5.3 Flash (zai/glm-5.3-flash)
**risk_tier:** standard.

Step 1 of the card's Steps table (red-first pins) is done; step 2 (implement D-1..D-3 in
`core.py` + the guide paragraph) is open. Measured facts below were captured on this clone
2026-09-08 through the built facade; nothing is guessed.

## Proposition ledger

| ID | Clause | Proof obligation | Verdict |
|---|---|---|---|
| C-001 | `explain` output renders in Spark's header form: `== Physical Plan ==` with each plan's text verbatim and its real newlines, a blank line between sections, nothing else. | Pins `test_explain_prints_plan_text_without_row_repr`, `test_explain_text_carries_the_physical_plan_header`, `test_explain_text_spans_at_least_three_plan_lines` (measured: the select+filter+withColumn physical plan has six `…Exec` lines). Green on the landed tree; the blank line is added conditionally because the measured `logical_plan` text does not end in a newline while the physical, tree, and metrics texts do. | **PROVEN** |
| C-002 | The D-2 mode map holds: simple → physical only; extended → optimized-logical header then physical; formatted → `EXPLAIN FORMAT TREE` tree rendering; cost/`ANALYZE` → `EXPLAIN ANALYZE`; codegen → physical plus the one trailing not-applicable line; an unknown string raises `PySparkValueError` naming the five modes; the existing `test_df_batch2.py` explain pins stay green. | Pins `test_explain_extended_lists_logical_plan_before_physical`, `test_explain_formatted_carries_datafusion_tree_glyphs`, `test_explain_unknown_mode_raises_naming_the_five_modes` green; measured `EXPLAIN ANALYZE` rows carry `plan_type='Plan with Metrics'` and print under the physical header; `test_df_batch2.py -k explain` 1 passed after the change as before it. | **PROVEN** |
| C-003 | `_explain_text` builds the rendered string and `explain` prints it and returns `None`; tests assert on the string except one smoke test that goes through the print. The pins fix the helper's shape: a method beside `explain` in `core.py` taking the same `(extended, mode)` parameters and returning `str` — the card leaves the parameter list open, so the pins' reading is binding for step 2 (a different shape must come back as a finding against this clause). | The landed `_explain_text` matches the pins' shape; the rendering support (headers, codegen note, `_EXPLAIN_SECTION_PLAN`, `_render_explain_sections`) lives in `explain.py` per the D-5 ruling, with the methods staying on `DataFrame` in `core.py`. Pins `test_explain_text_*`, `test_explain_formatted_*`, `test_explain_prints_plan_text_without_row_repr` green. | **PROVEN** |
| C-004 | `explain` never invokes `DataFrame.collect`; a spy patched onto the class holds it out. | Pin `test_explain_does_not_invoke_collect` green: the landed rendering reads rows through `toLocalIterator()`. Measured base red came from `core.py:3427` `for row in plan.collect():`; the scratch-view registration path does not pass through `DataFrame.collect`. | **PROVEN** |
| C-005 | D-4: `EXPLAIN FORMAT TREE` passes through the Spark door unchanged; the router does not reject the `FORMAT` clause. | Measurement below (2026-09-08): `session.sql("EXPLAIN FORMAT TREE …")` answers one `plan_type='physical_plan'` row whose `plan` is DataFusion's box-drawing tree; no router or bare-name-expansion change is owed, so no hand-back. | **PROVEN** |
| C-006 | Red-first: the seven pins of step 1 run RED on the base tree before any implementation change. | Red run recorded below (7 failed, exit 1, base `f00ed9ea`). | **PROVEN** |

## Base behavior (measured, 2026-09-08, base `f00ed9ea`)

`explain()` on the select+filter+withColumn frame prints two `Row(...)` reprs with literal
`\n` — the bug verbatim (first and last lines abridged for width; the physical row):

```text
Row(plan_type='logical_plan', plan='SubqueryAlias: datafusion.public.__repark_explain_4a6509…\n  Projection: …')
Row(plan_type='physical_plan', plan='ProjectionExec: expr=[id@0 as id, doubled@1 as doubled, doubled@1 * 3 as tripled]\n  ProjectionExec: expr=[…]\n    RepartitionExec: partitioning=RoundRobinBatch(64), input_partitions=1\n      ProjectionExec: expr=[column1@0 as id]\n        FilterExec: …\n          DataSourceExec: partitions=1, partition_sizes=[1]\n')
```

The same frame's plain-`EXPLAIN` physical plan through the door carries six exec lines, so
the ≥3-lines pin has headroom; the six are `ProjectionExec` ×3, `RepartitionExec`,
`FilterExec`, `DataSourceExec`.

## D-4 measurement (2026-09-08)

`session.sql("EXPLAIN FORMAT TREE SELECT id, id * 2 AS doubled FROM (VALUES (1), (2), (3)) t(id)")`
returns one row; the router accepts the `FORMAT` clause unchanged:

```text
Row(plan_type='physical_plan', plan='┌───────────────────────────┐\n│       ProjectionExec      │\n│    --------------------   │\n│          doubled:         │\n│  __repark_spark_int_mul__ │\n│  (CAST(column1 AS Int64)  │\n│            , 2)           │\n│                           │\n│        id: column1        │\n└─────────────┬─────────────┘\n┌─────────────┴─────────────┐\n│       DataSourceExec      │\n│    --------------------   │\n│         bytes: 112        │\n│       format: memory      │\n│          rows: 1          │\n└───────────────────────────┘\n')
```

Glyph set measured in the rendering: `┌ ┐ └ ┘ ┬ ┴ ─ │`. The formatted pin asserts `┌` and `└`.

## Red run (step 1, base `f00ed9ea`)

Command: `VIRTUAL_ENV=$PWD/.venv uv run --no-project python -m pytest python/repark/tests/test_df_explain_1.py -q`

```text
FAILED python/repark/tests/test_df_explain_1.py::test_explain_prints_plan_text_without_row_repr
FAILED python/repark/tests/test_df_explain_1.py::test_explain_text_carries_the_physical_plan_header
FAILED python/repark/tests/test_df_explain_1.py::test_explain_text_spans_at_least_three_plan_lines
FAILED python/repark/tests/test_df_explain_1.py::test_explain_extended_lists_logical_plan_before_physical
FAILED python/repark/tests/test_df_explain_1.py::test_explain_formatted_carries_datafusion_tree_glyphs
FAILED python/repark/tests/test_df_explain_1.py::test_explain_unknown_mode_raises_naming_the_five_modes
FAILED python/repark/tests/test_df_explain_1.py::test_explain_does_not_invoke_collect
7 failed in 0.55s
```

Red shapes, one per failure class:

| Pin | Red reason (verbatim core) |
|---|---|
| `test_explain_prints_plan_text_without_row_repr` | `assert "Row(" not in output` fails — the captured stdout is the two `Row(plan_type=…)` reprs above |
| `test_explain_text_carries_the_physical_plan_header` | `repark.errors.PySparkAttributeError: [ATTRIBUTE_NOT_SUPPORTED] Attribute '_explain_text' is not supported.` (`core.py:2005`) |
| `test_explain_text_spans_at_least_three_plan_lines` | same `_explain_text` red |
| `test_explain_extended_lists_logical_plan_before_physical` | same `_explain_text` red |
| `test_explain_formatted_carries_datafusion_tree_glyphs` | same `_explain_text` red |
| `test_explain_unknown_mode_raises_naming_the_five_modes` | `Failed: DID NOT RAISE PySparkValueError` — `mode="bogus"` currently runs plain `EXPLAIN` |
| `test_explain_does_not_invoke_collect` | spy raise from `core.py:3427` `for row in plan.collect():` → `AssertionError: DataFrame.collect must not run during explain` |

The four `_explain_text` reds surface through the facade's loud unsupported-attribute door
(`PySparkAttributeError`), not a bare `AttributeError`; a step-2 actor seeing exactly that
message is looking at the missing helper, not a broken test.

## Base state of the neighbor pins

`VIRTUAL_ENV=$PWD/.venv uv run --no-project python -m pytest python/repark/tests/test_df_batch2.py -k explain -q`
→ `1 passed, 5 deselected in 0.11s`, exit 0. Recorded so step 2 can prove the D-2 sentence
"existing mode pins stay green" against a captured base, not memory.

## Provisioning

`make py-test-facade` ran once from the clone root before any pytest (uv sync `--locked`,
maturin develop, full facade suite): **5813 passed, 368 skipped in 699.99s**, exit 0. No
second build was needed; this step changes no Rust. Disk before the build: 594G free.

## Notes for step 2

| Item | Note |
|---|---|
| `_explain_text` shape | Method beside `explain` with the same `(extended, mode)` parameters, returning the full rendered string (C-003's binding reading) |
| Verbatim text | The measured plan strings end in a real `\n`; D-1 prints them verbatim, so the simple-mode output ends with the plan's own newline |
| Collected base behavior | `explain()` currently prints the `logical_plan` row too; D-2 drops it for simple mode |
| Out of this step | Implementation, the `docs/guide/` DataFrame paragraph, registry/map prose beyond lockstep — step 2's rows |

## Step 2 landed state (2026-09-08, D-5 ruling applied)

The orchestrator ruled D-5 (2026-09-08): the explain rendering support splits into the new
private module `python/repark/src/repark/spark/dataframe/explain.py`, and
`scripts/check_lib_py.py`'s `core.py` exact baseline ratchets down to the measured post-split
count in the same commit. Applied as ruled:

- `explain.py` holds the four constants (`_LOGICAL_PLAN_HEADER`, `_PHYSICAL_PLAN_HEADER`,
  `_EXPLAIN_CODEGEN_NOTE`, `_EXPLAIN_SECTION_PLAN` — mode → SQL prefix + section keys) and the
  `_render_explain_sections` helper. `explain` and `_explain_text` stay methods on `DataFrame`
  in `core.py` (the card's Home and C-003's binding shape); `_explain_text` keeps mode
  resolution and validation and reads rows through `toLocalIterator()`.
- Baseline ratchet: `core.py` measured **4536** after the split (was 4555 on the unsplit
  working tree against 4539); `check_lib_py.py`'s row moved 4539 → 4536 in this commit. No
  other baseline moved; none raised.
- Measured facts kept verbatim: the `logical_plan` text does not end in a newline while the
  physical, tree, and metrics texts do, so the blank line between sections is added
  conditionally; `EXPLAIN ANALYZE` rows carry `plan_type='Plan with Metrics'` and print under
  the physical header; `formatted` takes every returned row (`EXPLAIN FORMAT TREE` measured
  one `physical_plan` row).
- Gates on the landed tree: `test_df_explain_1.py` **7 passed**; `test_df_batch2.py -k
  explain` **1 passed**; explain consumers green (`test_n2_plan_collapse.py` +
  `test_dataframe_x3_census.py -k "plan or explain"` 18 passed; `test_ta.py` +
  `test_ta_with_indicators.py` 61 passed; `test_types_1.py -k explain` 1 passed);
  `check_lib_py` 591 files clean; ruff, python-conventions, docstring-presence clean. The
  CAP-1 freeze pins in `test_dfcore_1_exports.py` absorbed the split (10 passed):
  `EXPECTED_DATAFRAME_DIR` gains `_explain_text`, the package gains the `explain` submodule,
  and the frozen core/package surfaces gain the two private imports. Full facade suite:
  `make py-test-facade` **5820 passed, 368 skipped**, exit 0.
- Owed at the departure edit (orchestrator, per the D-5 ruling): the ledger's
  `COVERAGE_ATTESTATION` block. All clauses are PROVEN, so `check_ledger_grammar.py` requires
  that block on a staging ledger; the ruling reserved its authorship for the departure edit,
  so this unit hands back with that one dependency recorded instead of filing it here.
  Verbatim finding on the ruled state (`python3 scripts/check_ledger_grammar.py`, exit 1):
  `task/ledgers/staging/df-explain-1-ledger.md: no COVERAGE_ATTESTATION block (ref 05 shape, in a fenced block)`.
  The commit therefore waits for the ruling answer filed in the handback; the working tree at
  parent `512b570e` carries the full diff for review (`git diff`).

## Notes for the orchestrator (step 2)

| Item | Note |
|---|---|
| `explain.py` | New private module; listed in `dataframe/map.md` in the same commit |
| Baseline | `scripts/check_lib_py.py` `core.py` 4539 → 4536 (measured post-split); down only |
| Attestation | Reserved for the departure edit per the D-5 ruling; `check_ledger_grammar.py` reds on an all-PROVEN staging ledger without it, so the commit waits for the ruling answer filed in the handback |
| Guide | `docs/guide/dataframe-guide.md` explain paragraph rewritten (sections, five modes, which execute); `docs/guide/map.md` row updated in the same commit |

## Coverage attestation (orchestrator, 2026-09-08)

```yaml
COVERAGE_ATTESTATION:
  pr_unit: df-explain-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Each of D-1, D-2 and D-3 was checked against behaviour, not paraphrase. The orchestrator re-ran the pins independently of the worker on the implementation tree — test_df_explain_1.py plus test_dfcore_1_exports.py 17 passed, test_df_batch2.py -k explain 1 passed 5 deselected — and confirmed the same seven pins were 7 failed on the base tree at 512b570e before any implementation existed.
      artifacts: [python/repark/tests/test_df_explain_1.py, python/repark/src/repark/spark/dataframe/explain.py, python/repark/src/repark/spark/dataframe/core.py]
    - id: AT-2
      status: ATTACKED
      evidence: The argument domain is the risk surface, because `extended` accepts a bool or a mode string. Exercised: no arguments, True, a mode string passed positionally as `extended`, each of the five mode names, mixed case, a mode containing `analyze`, and an unknown string that must raise PySparkValueError naming all five modes. `extended=False` and `mode=None` both resolve to simple.
      artifacts: [python/repark/tests/test_df_explain_1.py, python/repark/tests/test_df_batch2.py]
    - id: AT-3
      status: ATTACKED
      evidence: The scratch temp view is created and dropped in a try/finally, so the drop runs on the failure path too; that structure is carried over from the base implementation unchanged. Mode validation happens BEFORE the view is created, so an unknown mode cannot leak a view.
      artifacts: [python/repark/src/repark/spark/dataframe/core.py]
    - id: AT-4
      status: N/A
      justification: No shared mutable state, no ordering assumption, no async spawn and no lock. The new module holds only immutable module-level constants and one pure function over its arguments; the scratch view name is uniquified per call by the existing scratch_view_name.
    - id: AT-5
      status: N/A
      justification: No privileged action, no auth surface, no secret, no deserialization and no path handling. The SQL is the existing prefix plus a scratch view name generated by the facade, not user text.
    - id: AT-6
      status: ATTACKED
      evidence: Compatibility is the frozen export surface, and it is pinned rather than asserted. The CAP-1 freeze pins record exactly three declared deltas — EXPECTED_DATAFRAME_DIR gains `_explain_text`, the package gains the `explain` submodule, and the frozen core/package surfaces gain the two private imports — and test_dfcore_1_exports.py is green on exactly those. The observable output of `explain` changes by design; that change is the unit.
      artifacts: [python/repark/tests/test_dfcore_1_exports.py]
    - id: AT-7
      status: N/A
      justification: Not system-breaking. The change removes work rather than adding it: rows arrive through toLocalIterator instead of a full collect, and the plan-printing modes never execute the plan. `mode="cost"` still runs EXPLAIN ANALYZE, which is the documented executing mode and is unchanged from the base.
    - id: AT-8
      status: ATTACKED
      evidence: The upstream contract is DataFusion's EXPLAIN surface, and it was measured rather than presumed. `EXPLAIN FORMAT TREE` passes the Spark door unchanged (C-005, measured at step 1, so D-4 needed no router change); EXPLAIN ANALYZE rows carry plan_type='Plan with Metrics'; and the logical_plan text does not end in a newline while the physical, tree and metrics texts do — which is why the blank line between sections is added conditionally rather than by a blind join.
      artifacts: [python/repark/src/repark/spark/dataframe/explain.py, task/ledgers/staging/df-explain-1-ledger.md]
    - id: AT-9
      status: N/A
      justification: No failure path to diagnose and no logging change. The user-visible diagnosis path is the printed plan itself, which is the thing this unit fixes: it was Row reprs with literal \n and is now the plan text.
    - id: AT-10
      status: ATTACKED
      evidence: The pins were written first and proven red before the implementation existed — 7 failed at 512b570e, re-run by the orchestrator, not taken on the worker's word. Every clause names the pin that discharges it, and the citations are carried in python/repark/tests/map.md because this repository bans comments in code. The file-size gate was held by its sanctioned out, a split with the exact baseline ratcheting DOWN 4539 to 4536 in the same commit; no baseline was raised.
      artifacts: [python/repark/tests/test_df_explain_1.py, python/repark/tests/map.md, scripts/check_lib_py.py]
  complete: true
```
