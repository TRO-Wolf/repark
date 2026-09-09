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
| C-001 | `explain` output renders in Spark's header form: `== Physical Plan ==` with each plan's text verbatim and its real newlines, a blank line between sections, nothing else. | Pins `test_explain_prints_plan_text_without_row_repr`, `test_explain_text_carries_the_physical_plan_header`, `test_explain_text_spans_at_least_three_plan_lines` (measured: the select+filter+withColumn physical plan has six `…Exec` lines). | **OPEN** |
| C-002 | The D-2 mode map holds: simple → physical only; extended → optimized-logical header then physical; formatted → `EXPLAIN FORMAT TREE` tree rendering; cost/`ANALYZE` → `EXPLAIN ANALYZE`; codegen → physical plus the one trailing not-applicable line; an unknown string raises `PySparkValueError` naming the five modes; the existing `test_df_batch2.py` explain pins stay green. | Pins `test_explain_extended_lists_logical_plan_before_physical`, `test_explain_formatted_carries_datafusion_tree_glyphs`, `test_explain_unknown_mode_raises_naming_the_five_modes`; base state of `test_df_batch2.py -k explain` recorded green (1 passed) below. | **OPEN** |
| C-003 | `_explain_text` builds the rendered string and `explain` prints it and returns `None`; tests assert on the string except one smoke test that goes through the print. The pins fix the helper's shape: a method beside `explain` in `core.py` taking the same `(extended, mode)` parameters and returning `str` — the card leaves the parameter list open, so the pins' reading is binding for step 2 (a different shape must come back as a finding against this clause). | Pins `test_explain_text_*`, `test_explain_formatted_*`, `test_explain_prints_plan_text_without_row_repr`. | **OPEN** |
| C-004 | `explain` never invokes `DataFrame.collect`; a spy patched onto the class holds it out. | Pin `test_explain_does_not_invoke_collect` (measured: the base raise comes from `core.py:3427` `for row in plan.collect():`; the scratch-view registration path does not pass through `DataFrame.collect`, so a no-collect rendering path can satisfy the pin). | **OPEN** |
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
