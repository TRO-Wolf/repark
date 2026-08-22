# PYC-1 — the two DataFrame modules (nested-def burn-down)

**Unit:** PYC-1
**Branch:** `feat/pyc-1-dataframe-nested-defs`
**Base:** `origin/main` @ `d3152b1` (V3-1 #203 merged)
**Date:** 2026-08-22
**Path:** HIGH (user asked `/sepmo-octo` then `/critic-overload`; data-path refactor of UDF/show/join SQL)
**Critic engine:** `octo` then `critic-overload`
**Approval:** user "grab the next work group task and proceed" after V3-1 SQM/merge

The ordered queue is [briefs/next-sequence.md](../briefs/next-sequence.md). PYC-1 is the first
conventions unit: lift every nested `def` in `spark/dataframe/core.py` (23) and
`plan_collapse.py` (12). The campaign invariant is the LRS one: **no call that worked before
returns a different value.**

---

## PROPOSITION LEDGER — PYC-1 — 2026-08-22

LOGIC_SCORE: 14/14 (every clause `PROVEN`, zero `OPEN`, zero `REJECTED`)

### C-001 — Finite nested-def domain (enumeration)

**Proposition.** PYC-1's quantified claim ("the two DataFrame modules") ranges over the
AST-measured nested `FunctionDef`/`AsyncFunctionDef` set at `d3152b1`, and that set is:

**`plan_collapse.py` (12):**
`strip_field`, `strip_type`, `_format_show_table.fmt_row`,
`_format_eager_eval_table.fmt_row`, `_format_polars_show.hline`,
`_format_polars_show.row_line`, `_format_duckdb_show.hline`,
`_format_duckdb_show.is_numeric_cell`, `_format_duckdb_show.row_line`,
`_rewrite_qcol_tokens_local._replace`, `_rewrite_join_qcol_sql._side_engine`,
`_rewrite_join_qcol_sql._replace`.

**`core.py` (23 by the gate's immediate-parent walker, plus `_emit_side` under `try:`
which that walker misses):**
`_is_noise`, `_drop_all`, `_input_batches`, `_arrow_func`, `_pdf_iter`,
`_arrow_pandas_udf_func` and its inners (`_arrow_array_to_pandas_series`,
`_nullable_mapper`, `_series_args_for_slot`, `_validate_series_result`,
`_series_to_arrow`, `_run_scalar_on_batch`, `_run_scalar_iter`, `_input_iter`,
`_emit_batch`), `_arrow_python_udf_func` and its inners (`_column_python_values`,
`_run_udf_on_batch`, `_results_to_arrow`), `_ordered_window_func`,
`replace_idents`, `replacer`, `_coerce_seed`. Octo C1 lifted `_emit_side` too.

**Verdict:** `PROVEN` — AST walk at base (`python3 ast.walk`, counts 12 and 23) matches the
seeded `NESTED_DEF_EXCEPTIONS` ceilings.

### C-002 — Zero nested defs in both files; rows deleted not zeroed

**Proposition.** After the unit, `find_nested_definitions` reports 0 in both files, and the
two `NESTED_DEF_EXCEPTIONS` rows are **deleted**, not kept at ceiling 0.

**Verdict:** `PROVEN` — the gate's own contract (`scripts/check_python_conventions.py`) and
the skill's ratchet ("Delete a row rather than zeroing it"). Pin: the guard after the table
edit.

### C-003 — Formatters lift by taking widths as arguments

**Proposition.** `fmt_row` / `hline` / `row_line` / `is_numeric_cell` become module-level
helpers that take the computed widths (or the cell text) as arguments. They do not close over
parent locals.

**Verdict:** `PROVEN` — next-sequence.md PYC-1 "Start with the formatters." Existing goldens:
`python/repark/tests/test_display_styles.py`.

### C-004 — SQL token rewriters take an explicit side map / frame

**Proposition.** `_rewrite_qcol_tokens_local._replace` and `_rewrite_join_qcol_sql`'s
`_side_engine` / `_replace` (including the same-object token counter) become module-level
helpers that take origin map, frames, aliases, and the mutable occurrence counter as
arguments / a small rewriter object. They do not close over parent locals.

**Verdict:** `PROVEN` — next-sequence.md PYC-1 "the SQL token rewriters close over the join
side map and need a context argument." Pins: existing join / filter origin tests.

### C-005 — UDF helpers take an explicit context, not a mechanical lift

**Proposition.** pandas/Arrow/python UDF nested helpers move to module-level functions (a
sibling module if `core.py`'s 7225-line ceiling cannot hold the extra signatures) that take
slots, expected Arrow schema, and iterator-mode flags as arguments. Lifting a closure must
not change what the helper can see: every parent local it read is named on the new
signature.

**Verdict:** `PROVEN` — next-sequence.md + code-quality skill hazard 1. `core.py` is 7221 of
7225 at base, so UDF action callbacks go to `dataframe/udf_bridge.py` (same package, no
DataFrame import at module scope).

### C-006 — LRS invariant

**Proposition.** No public call that worked before this unit returns a different value or
type. This is a refactor of working code. Behaviour pins are the existing facade tests for
the touched surfaces (show styles, mapInArrow/mapInPandas, pandas_udf, classic udf, filter
ident rewrite, sample seed coerce, export-error noise peel, MIA finalize cleanup).

**Verdict:** `PROVEN` — next-sequence.md campaign invariant. New tests only pin layout
contracts the lift itself introduces (pandas import still not at plan time; AST nested-def
count 0).

### C-007 — Out of scope

**Proposition.** This unit does **not**: convert `dataclass` → `BaseModel` (PYC-3); lift
nested defs in any file other than the two named modules and the new sibling they require
(PYC-2); introduce a second Iceberg format version (V3-2); touch IAM/Glue/S3.

**Verdict:** `PROVEN` — next-sequence.md unit boundaries.

### C-008 — Finalize / iterator callbacks that look like "the point"

**Proposition.** `_drop_all` (weakref.finalize) lifts by passing `session` and the names list
as `finalize` extra args (the list object stays shared). `_input_batches` is eliminated:
`func(iter(input_reader))` is the same iterator. Neither becomes a pragma; PYC-1 is a lift
unit, not a pragma unit.

**Verdict:** `PROVEN` — `weakref.finalize(obj, func, *args)` is the documented extra-args
form; the names list is mutated in place both before and after.

### C-009 — Type hints on every new signature; names are verb phrases

**Proposition.** Every lifted helper is annotated (parameters and return). Names describe the
work (`_show_grid_row`, `_duckdb_cell_is_numeric`, `_run_pandas_udf_arrow_batches`), not
`_inner` / `_helper`.

**Verdict:** `PROVEN` — AGENTS.md Python + code-quality rule 4.

### C-010 — File-size ceilings ratchet down only

**Proposition.** `core.py` stays ≤ 7225. `plan_collapse.py` stays ≤ 2500 (default). The new
sibling stays ≤ 2500. No EXCEPTIONS ceiling is raised.

**Verdict:** `PROVEN` — `scripts/check_lib_py.py` SSOT.

### C-011 — pandas import stays action-time

**Proposition.** `DataFrame._select_with_pandas_udfs` does not import pandas. The import lives
inside the action callback (now module-level in `udf_bridge`). The existing source-layout pin
is retargeted to that contract, not to the nested-def spelling.

**Verdict:** `PROVEN` — `test_pandas_udf_bridge_defers_pandas_import` mutation comment is the
contract (plan-time import is the defect).

### C-012 — map.md lockstep

**Proposition.** Every directory this unit writes a `.py` or `.md` in has its `map.md`
updated in the same change: `dataframe/`, `python/repark/tests/`, `scripts/`, `task/`,
`briefs/` if the sequence file moves, root STATUS.

**Verdict:** `PROVEN` — AGENTS.md `map.md` rule.

### C-013 — Q7 import freeze

**Proposition.** Public import paths `repark.spark.dataframe` / `.core` do not grow a required
new name for callers. Internal helpers may move; `core.py` does not have to re-export the new
sibling.

**Verdict:** `PROVEN` — dataframe `map.md` Q7 freeze; `__init__.py` star-binds `core` only.

### C-014 — Tests in the same commit as the lifts

**Proposition.** Layout pins (AST nested-def count, pandas import site) land with the code.
Behaviour is pinned by the existing facade tests named in C-006, which must stay green.

**Verdict:** `PROVEN` — docs/testing.md hard rule 1.

---

## PR carving

One PR unit: all 14 clauses. Splitting formatters from UDF lifts would leave `core.py`'s
exception row half-ratcheted and force a second conventions-table edit.

## Rubric (HIGH / octo)

Blast radius: two shipped DataFrame modules plus a new sibling on the UDF/show/join path —
fails LIGHT 1, 3, 5. STANDARD would be CCC; user asked octo then critic-overload.

## PRE_EXECUTION_REVIEW

Charter frozen as this ledger. Clause-complete single PR. Binding: unit gate `make verify`,
pre-merge `make preflight`. Contingencies additive (revert the branch). **PROCEED.**

---

## Execution (2026-08-22)

Actor lift: nested defs in `core.py` / `plan_collapse.py` → module-level helpers;
UDF action callbacks in `udf_bridge.py`; `_emit_side` (under `try:`, invisible to the
gate walker) lifted as `_emit_join_side_columns`. `NESTED_DEF_EXCEPTIONS` rows for
those two files deleted. `core.py` ceiling 7225 → 6880 (measured 6866).

**Unit gate:** `make verify` exit 0. Facade: pandas_udf / udf / mapInArrow / display
styles / filter rewrite / join (`-k join`) 205 passed; PYC-1 layout pins green.

**Octo** (`cycles=4`, `early_stop=true`, `claims_critic=true`, floor S1):
`OCTO-CONVERGED` after cycle 4 Half A CLEAN. Scratch:
`/tmp/critic-octo-repark-2026-08-22/OCTO-REPORT.md`.

**Critic-overload Wave 1:** in-scope S1s remediated (ledger this section; finalize
list-identity pin; STATUS inventory wording). Window RANGE/ROWS/desc findings
W1-L-001..003 WITHDRAWN — pre-existing on the nested body; LRS forbids changing
values. Join/MIA leak S2s ACCEPTED_FLAGGED (pre-existing). Waves 3–5 not run:
`OVERLOAD-PARTIAL` (same as V3-1). Scratch:
`/tmp/critic-overload-repark-2026-08-22/`.

**COMPLETE.** `make preflight` exit 0 — facade **3646 passed, 70 skipped**.
