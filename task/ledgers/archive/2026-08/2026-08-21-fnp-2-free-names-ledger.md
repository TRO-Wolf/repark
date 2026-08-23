# Unit ledger — FNP-2 · the free names

**Unit:** FNP-2 · **Date:** 2026-08-20 · **Executor:** Claude (Opus 5) ·
**Branch:** `feat/spark-function-parity` · **Base:** `4b80ea6` (FNP-1) ·
**Charter:** [fnp-0-charter-ledger.md](../../staging/fnp-0-charter-ledger.md) clauses **C-007**, **C-009** ·
**Design:** [../docs/design/spark-function-parity.md §7](../../../../docs/design/spark-function-parity.md).
**SEPMO:** STANDARD. The unit was scoped LIGHT ("no Rust, aliases only") and **re-routed to
STANDARD in flight** when the first pin went red: two of the seven names could not be added
without fixing an engine-facing defect, which fails LIGHT criteria 1 and 5.

**Writable:** `python/repark/src/repark/spark/{column.py,window.py,functions.py,functions_expr.py,
functions_session.py,dataframe/core.py,dataframe/plan_collapse.py}`, the facade tests, this ledger,
`task/map.md`, the touched `map.md` files.

## Scope correction, taken at the top of the unit

The design listed seven names. Two are not free and were re-routed rather than forced:

| Name | Where it went | Why |
|---|---|---|
| `sha` | **FNP-3** | It aliases `sha1`, and `sha1` is currently an `UnsupportedOperationException` stub. Aliasing a stub exports a second spelling of the same refusal, which grows the surface without growing the capability. `datafusion-spark` ships `function/hash/sha1.rs`, so FNP-3 de-stubs `sha1` and lands `sha` beside it. |
| `typeof` | **FNP-12** | The design called it "facade-only — read the analyzed schema and emit a literal". A facade `Column` is standalone and has **no schema** at build time, which is the whole reason `function_dispatch` embeds UDFs by hand. There is no `typeof` kernel in `datafusion-spark` or DataFusion core, so this is a real kernel, not an alias. |

Delivered here: **`asc_nulls_last`, `desc_nulls_first`, `column`, `negate`, `session_user`** — and
the ordering defect the first two exposed. `__all__` moves 333 → 338.

## The defect the new names exposed

`asc_nulls_last` and `desc_nulls_first` are the two corners of the null-ordering 2×2 that RePark
did not export. Adding them and pinning the resulting **row order** — rather than which method
they delegate to — showed that the marker was being thrown away at three sites, each of which
independently derived null placement from the sort direction:

| Site | What it did |
|---|---|
| `dataframe/core.py` `_sort_specs` | `nulls_first_flags = list(ascending_flags)` — the column's `_sort_nulls_first` never read |
| `window.py` `_order_specs` | `nulls_first = list(ascending)` — same derivation |
| `dataframe/plan_collapse.py` `_window_spec_structural_key` | omitted null placement from the key entirely, so two window specs differing only in it compared **equal** and were merged |

The derivation was correct for as long as only `asc`/`asc_nulls_first` and `desc`/`desc_nulls_last`
were reachable, because each agreed with it. It becomes wrong the moment the other two corners
exist — and wrong **silently**: rows come back in a plausible order, and the merged window
produces a defensible-looking number.

All three now resolve through one helper, `column.sort_nulls_first_for(column, is_ascending)`:
an explicit marker wins, an unmarked column follows Spark's direction-derived default.

`Column` also gained the four PySpark method spellings. `asc` and `desc` were 25-line
near-identical constructor calls; four more copies would have been a hundred lines of duplication
across which the `sql_expr` / `generator` / origin-threading attributes must stay in lockstep, so
they collapse to one `_with_sort_order` whose docstring records why each carried attribute is
load-bearing.

## Findings

| ID | Sev | Finding | Disposition |
|---|---|---|---|
| F-FNP2-1 | S1 | `DataFrame.orderBy` discards an explicit null-placement marker | `REMEDIATED` — `test_all_four_null_ordering_corners`, red on the two new corners before the fix |
| F-FNP2-2 | S1 | `Window.orderBy` discards it likewise | `REMEDIATED` — `test_window_order_honours_explicit_null_placement`, proven red by reverting the derivation |
| F-FNP2-3 | S1 | Two window specs differing only in null placement are merged by plan-collapse | `REMEDIATED` — `test_window_specs_differing_only_in_null_placement_do_not_merge`, proven red the same way |
| F-FNP2-4 | S3 | The design's "seven free names" was wrong on two counts (`sha`, `typeof`) | `REMEDIATED` — re-routed above; the design table is corrected in place, dated |
| F-FNP2-5 | S2 | Routing null placement through the column marker silently changed `orderBy(col.asc(), ascending=False)`: the marker would have kept nulls first while sorting descending. PySpark's `ascending=` re-marks the columns wholesale, so it must supersede the marker on **both** halves. | `REMEDIATED` — the override branch restores the pre-unit behaviour; pinned by `test_ascending_keyword_supersedes_a_per_column_null_marker`, which nothing else covered |
| F-FNP2-6 | S3 | `_PRE_SPLIT_ALL` surface pin moved 333 → 338 | `REMEDIATED` — deliberate pin move per the FN-C/D/E/F/W/GT1/GT2 precedent, names declared in the file's own note and in the PR body |

## Regression proof (R5)

```
PRE-FIX  (orderBy derivation)   2 failed  — asc_nulls_last, desc_nulls_first
PRE-FIX  (window + plan-collapse restored to derivation)   2 failed
POST-FIX                        11 passed
```

## Gates


| Gate | Result |
|---|---|
| facade pytest (full) | first run **1 failed, 3,453 passed, 70 skipped** (see the vigilance note); after the pin move **3,455 passed, 70 skipped, 0 failed** |
| `make ci` | exit **0**. Two reds on the way, both mechanical: `RUF022` (`__all__` wants isort order, not `sorted()`) and one over-long docstring. Applying the isort order then required regenerating `_PRE_SPLIT_ALL` in that same order, since the pin asserts the tuple exactly. |
| `cargo test --workspace` | not re-run: FNP-2 changed no Rust |

## Vigilance note — I diagnosed the red wrong before reading it

One pre-existing test went red at ~37% of the suite. Before the run named it I had already
reasoned my way to a culprit — the `ascending=` override interaction — and applied a fix for it.

The actual failure was `test_functions_split_identity::test_functions_all_matches_pre_split_inventory`,
the `__all__` surface pin, which is simply the deliberate 333 → 338 move this unit was always
going to make. Nothing to do with ordering.

The speculative fix turned out to be **correct and necessary anyway** — F-FNP2-5 above is a real
regression my own helper introduced, and it would have shipped silently because no test covered
`ascending=` combined with a marked column. But it was right by luck, not by method: I applied it
before I had the evidence, which is exactly the failure mode AGENTS.md's "fixes stay narrow" and
SEPMO's D1 exist to prevent. The lesson is not "the fix was wrong"; it is that a fix applied ahead
of its evidence is an assumption wearing a diff, and the only reason this one is defensible is
that it now carries a pin that fails without it.

## Vigilance note 2 — the same defect class, twice, in opposite directions

FNP-1 recorded that a `head` on a verification pipeline turns an incomplete run into a passing
report. This unit's final facade run then reported **exit 1 on a suite that passed** — the chain
ended in `grep -c '^FAILED'`, and `grep -c` exits non-zero when it matches nothing, so "no
failures" became the failure.

Same root cause both times: **the exit status of a compound command is the last command's**, so
appending any summarising filter to a gate destroys the verdict — silently green if the filter
truncates, spuriously red if it counts zero. Writing the FNP-1 note did not stop me repeating it,
which is the useful part of the record: the countermeasure has to be structural, not a reminder.

The rule this unit adopts: **a gate runs alone and its own `$?` is captured immediately.**
Summarising and reading happen afterwards, from the log file, and never in the same chain.
Every gate result in this ledger was re-read from the log rather than trusted from a chained
exit code.
