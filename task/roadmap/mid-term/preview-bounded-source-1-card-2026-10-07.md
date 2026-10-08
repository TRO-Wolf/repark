# Card PREVIEW-BOUNDED-SOURCE-1: the styled preview over a source where a full count is a full read

**Date:** 2026-10-07. **Filed by:** Claude (Haiku 5.5), docs lane, from the orchestrator's brief. **Source:** the `q1_premise` field of the C-2d fold 1 re-verify verdict (PR #991), and the orchestrator's Q1 ruling in the C-2d fold 1 rulings (2026-10-07).

**Status:** filed, not scheduled. **The ask needs an owner decision before any code.**

## Why

**What was measured.** On a lazy parquet frame of 200000 rows, `show(3)` under the default `polars` style and under the `duckdb` style each calls `DataFrame.count()` once and `_preview_tail_rows` once. Spark style calls neither. The verdict says the behaviour predates C-2 and is not Postgres-specific. `_preview_tail_rows` has been in `python/repark` since 2026-08-08, and the lazy display path since 2026-09-10.

**Over a mounted Postgres source.** A full count there would be a full remote read. Not measured: the verdict did not run the styled preview over a mounted Postgres source.

**PERF-EAGER-PREVIEW-1, as the verifier reads it.** The verifier's reading, not a ruling: the row names `repr`, `_repr_html_` and vertical `show`. It holds for all three in Spark style (0 counts each), and for lazy `repr` and `_repr_html_` in every style. Under the `polars` and `duckdb` styles, `show(3, vertical=True)` and `repr` with `spark.sql.repl.eagerEval.enabled=true` each count once and read the tail once. The row says "styled totals are unchanged". The verifier reads that as scoped to the plain doors, where it still holds. If the row is meant to cover the styled doors, the verifier's reading is that it does not hold there.

**The Q1 ruling.** Lean RATIFIED. The Spark-style pin and the declaration on `CONNECT-DECL-pg-numeric-special` stand. The styled head-and-tail preview is facade behaviour that predates C-2 and applies to every lazy source. A bounded or refusal-tolerant preview over a mounted source is this card, outside C-2.

## The ask

Decide what the styled preview should do over a source where a full count is a full remote read. Two shapes are open: a bounded preview, or a preview that tolerates a refusal past the rows it shows.

Also decide whether PERF-EAGER-PREVIEW-1 is meant to cover the styled doors.

**Owner decision needed before any code.**

## Gates

Not measured.

## Pointers

- The C-2d fold 1 re-verify verdict (PR #991), field `q1_premise`.
- The orchestrator's C-2d fold 1 rulings (2026-10-07), Q1.
- The declaration row `CONNECT-DECL-pg-numeric-special` (PR #991 branch; not on `main` yet).
- The registry row PERF-EAGER-PREVIEW-1, as the verdict names it.
