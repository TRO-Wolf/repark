# F-3 — public docstring backfill (facade)

## Purpose

Bring `python/repark/src/repark/` to **100% public-docstring coverage outside
`spark/dataframe/core.py`**, so the facade a user reads in an editor tooltip explains itself.
Docstring-only: no signature, logic, import or `__all__` moved, no dependency added, no lockfile
touched, no file-size ceiling raised.

**Census method** (re-derived, not inherited): AST walk over every `*.py` under
`python/repark/src/repark/`; a node counts when it is a `FunctionDef` / `AsyncFunctionDef` /
`ClassDef` whose name does not start with `_`; it is *missing* when `ast.get_docstring` returns
`None`. The walk is `ast.walk`, so nested helper closures (`probe`, `verifier`, `rec`, …) count
too — they are public by that rule and they were the long tail.

## Census — before / after

Base `834b2df`. Every count below is that walk, not an estimate.

| File | Public defs/classes | Documented before | Documented after | Missing after |
|---|---|---|---|---|
| `spark/functions_udf.py` | 53 | 16 | 53 | 0 |
| `spark/polars.py` | 50 | 20 | 50 | 0 |
| `spark/dataframe/writer_readwriter.py` | 31 | 25 | 31 | 0 |
| `spark/merge.py` | 17 | 14 | 17 | 0 |
| `spark/functions.py` | 41 | 40 | 41 | 0 |
| `spark/row.py` | 5 | 4 | 5 | 0 |
| `spark/types.py` | 93 | 92 | 93 | 0 |
| `spark/session/_funcs.py` | 3 | 2 | 3 | 0 |
| `spark/session/session_core.py` | 40 | 39 | 40 | 0 |
| `spark/ml/feature/_transformers.py` | 40 | 39 | 40 | 0 |
| **`spark/dataframe/core.py`** *(deferred)* | 103 | 92 | 92 | **11** |
| **Package total** | **1210** | **1117** | **1199** | **11** |

82 docstrings added across 10 files; 101 inserted lines, 0 deleted (the diff is purely
additive — verified with `git diff -U0`).

## Why `core.py` is deferred, not skipped

`spark/dataframe/core.py` measures **8199 lines against its 8200-line ceiling** in
`scripts/check_lib_py.py`. Its 11 remaining names are all nested rendering/binding closures
(`head`, `fmt_row`, `hline`, `row_line`, `is_numeric_cell`, `replace_idents`, `replacer`). Eleven
docstrings cannot fit in one line of headroom, and the sanctioned outs are *split the module* or
*raise the ceiling* — F-3 is a docs unit and does neither. They land with the next extract that
frees headroom; ceilings ratchet down only.

## Content rules applied

- First line is an imperative summary; parameters/returns spelled out only where the signature is
  not self-evident. Style follows each file's documented neighbours (`functions_expr.py`, at
  201/201, is the house style for the function modules).
- **No PySpark text was copied.** Every line is original, and each divergence is stated rather
  than smoothed over.
- **No invented examples.** The house style in the touched files uses prose and `::` literal
  blocks, not doctests (nothing in the repo collects doctests), so no worked output values were
  written. Every *behavioural* claim below was executed against the module built in this tree
  rather than assumed from Spark or from polars.

### `functions_udf.py` — the UDF execution model, stated honestly

37 of the 82 are the composition-refuse stubs on `PandasUDFColumn` and `PythonUDFColumn`. These
are **not** plan `Column`s: a UDF result is a projection-rewrite bridge node, so `udf_col + 1`,
`udf_col > 0` in a filter, or nesting under `coalesce` raises
`UnsupportedOperationException` instead of silently composing. Each stub now names the surface it
refuses and points at the fix (materialize via `select`/`withColumn`, then compose). `over` is
refused on the classic scalar `udf` marker while `PandasUDFColumn.over` is real (unbounded
whole-partition `GROUPED_AGG`) — the docstrings keep that asymmetry visible.

### `polars.py` — the interop contract, measured not assumed

`repark.polars` is polars-*style* naming over repark `Column` machinery, not real polars. Four
`.str` / `.dt` behaviours differ from the library whose spelling they borrow, so each carries an
explicit **Divergence** note, and each was executed before it was written:

| Name | Measured on the built engine | Real polars |
|---|---|---|
| `str.replace("X", "-")` on `aXbXc` | `a-b-c` — every match | first match only |
| `str.replace_all` | `a-b-c` — identical lowering | (same) |
| `str.zfill(6)` on `-42` | `000-42` — plain `lpad`, sign not hoisted | `-00042` |
| `dt.weekday()` on a Monday | `0` (Spark 0=Mon..6=Sun) | `1` (1=Mon..7=Sun) |
| `dt.truncate("month")` | Spark `date_trunc` granularity name | polars duration (`"1mo"`) |

These are polars-only differences. Per the divergence registry's own admission rule, a
polars-or-fork-only difference cannot become a registry row without a pin, so they stay
docstring-local and contradict nothing in
[../docs/spark-sql-iceberg-parity.md](../../../../docs/spark-sql-iceberg-parity.md).

The loud-unsupported members (`str.split`, `dt.hour` / `minute` / `second`, `dt.offset_by`) say
they refuse today and why, instead of reading as if they work.

### `writer_readwriter.py` / `merge.py`

The five delegating `DataFrameStatFunctions` methods point at their `DataFrame` twin rather than
restating semantics — one truth, no drift. `freqItems` states that it refuses. `merge.py`'s
`WhenNotMatchedBySource` terminals now read like their `WhenMatched` siblings.

## Verification

- `make ci` → exit 0.
- `make py-test-facade` → exit 0 (built native module; the divergence table above came from it).
- `scripts/check_lib_py.sh` → 65 files clean; no ceiling raised, `core.py` byte-untouched.
- Post-change census re-run: 1199/1210, the 11 remaining all in `core.py`.

## Follow-on — CLOSED (F-4 increment 2, 2026-08-17)

The 11 landed as a docstring-only rider on the F-4 guides PR, on the headroom the SE-1 PR-B T0b
extract freed. **Census re-run with the same walk: 1211 / 1211, 0 missing.**

The count moved from 1210 to 1211 because the extract split the tail block out of `core.py`
(`plan_collapse.py` is now its own module), so seven of the eleven had *moved* with their bodies:

| File | Public defs/classes | Missing before | Missing after |
|---|---|---|---|
| `spark/dataframe/core.py` | 97 | 4 | 0 |
| `spark/dataframe/plan_collapse.py` | 7 | 7 | 0 |
| **Package total** | **1211** | **11** | **0** |

(`plan_collapse.py`'s module-level helpers are all underscore-private, so every name the walk
counts public in that file *is* one of the nested closures.)

**The honest caveat.** "100%" here is 100% *of this ledger's declared census rule*, which is
`ast.walk` — so it counts nested closures as public. It does not mean eleven user-facing API names
were undocumented. Of the eleven:

- **9 are nested rendering closures** — `fmt_row` ×2, `hline` ×2, `row_line` ×2, `is_numeric_cell`
  (all inside the `plan_collapse.py` formatters), plus `replace_idents` / `replacer` inside
  `core.py`'s SQL-identifier rewriter. None is reachable from outside its enclosing function.
- **2 are `@overload` typing stubs** for `DataFrame.head`, whose real implementation was already
  documented. Their bodies were `...`; each now carries a one-line docstring instead.

So the *user-facing* facade surface was already at 100% when F-3 shipped, and this rider closes the
census metric rather than a documentation gap. Recorded that way on purpose: the number is only
worth having if what it measures is stated.

**Cost:** 11 docstrings across two files. `git diff --numstat`: `core.py` +7 / −2 (the two
`@overload` `...` stub bodies became their docstrings), `plan_collapse.py` +7 / −0. No signature,
logic, import or `__all__` moved; no ceiling raised — `core.py` 7253 of 7350,
`plan_collapse.py` 1103 of 2500. `make ci` and `make py-test-facade` green.
