# map — task/ledgers/completed/

## Purpose
Ledgers of finished units, moved here by the unit's last commit
(`python3 scripts/ledger_lifecycle.py move task/ledgers/staging/<unit>-ledger.md completed`) and
frozen: `make check-ledgers` allows a link repair or a dated errata note at the top, nothing
else. The next pickup's `make ledger-archive` files everything here under
[../archive/](../archive/map.md) by the merge date.

## Contents
- [ballista-audit-0-ledger.md](ballista-audit-0-ledger.md) —
  **BALLISTA-AUDIT-0 steps 1–3 (2026-09-08), in flight:** Half A facts plus Half B
  judgement for the Ballista audit at upstream tag `54.1.0` (`f4e66525`) —
  clauses C-001…C-012 PROVEN on document evidence under the grammar gate's
  reading-unit rule (R-10; LEDGER-READING-1 step 2, 2026-09-09), §26 A–H +
  §27 + §28 gate filled, appendix A6 closes the python/tooling gap; the ADR
  disposition and the brief's Milestone 0 row landed in #426.
  Branch `docs/ballista-audit-0`.
- [df-explain-1-ledger.md](df-explain-1-ledger.md) —
  **DF-EXPLAIN-1 (2026-09-08), in flight:** `DataFrame.explain()` prints plan text, not
  `Row(...)` reprs. Step 1 done: the seven red-first pins in
  `python/repark/tests/test_df_explain_1.py` run RED on base `f00ed9ea` (run recorded in the
  ledger); D-4 measured — `EXPLAIN FORMAT TREE` passes the Spark door unchanged. Step 2 open:
  implement D-1..D-3 in `core.py` (`_explain_text` + the method body) plus the guide
  paragraph. `risk_tier: standard`. Branch `feat/df-explain-1`.
  pins: df-explain-1/C-001, C-002, C-003, C-004, C-005, C-006
- [dfcore-1-ledger.md](dfcore-1-ledger.md) —
  **DFCORE-1 (2026-09-07), in flight:** leaf helpers out of `core.py` — Arrow cell
  conversion to `rows_export.py`, export-error mapping to new `export_errors.py`,
  mapInArrow schema checks to new `udf_schema.py`, grouped-UDF assembly to new
  `grouped_udf.py`. Move-only: `core.py` 6302 → 5954, `joins_columns.py` 1239 → 1238,
  export surfaces pinned identical, 6093 pre-existing collected IDs unchanged, 6 added. `risk_tier: standard`.
  Branch `refactor/dfcore-1`.
  pins: dfcore-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008
- [dfcore-2-ledger.md](dfcore-2-ledger.md) — Unit ledger — DFCORE-2 · the four UDF projection rewrites out of `core.py`
- [dfcore-3-ledger.md](dfcore-3-ledger.md) — Unit ledger — DFCORE-3 · the statistics family out of `core.py`
- [dfcore-4a-ledger.md](dfcore-4a-ledger.md) — Unit ledger — DFCORE-4a · sampling out of `core.py`
- [dfcore-4b-ledger.md](dfcore-4b-ledger.md) — Unit ledger — DFCORE-4b · display out of `core.py`
- [dfcore-5-ledger.md](dfcore-5-ledger.md) — Charter ledger — DFCORE-5 · `approxQuantile` in one collect per frame
- [dfcore-6-ledger.md](dfcore-6-ledger.md) — Charter ledger — DFCORE-6 · eager previews fetch N+1, never `count()`
- [fnp-8-review-ledger.md](fnp-8-review-ledger.md) —
  **FNP-8-REVIEW (2026-09-07), in flight:** remediation round 1 for FNP-8 (PR #412,
  merged unreviewed) — the round-1 critic's F1–F7, each red-first against live
  PySpark 4.1.2, plus the no-regression held set. `risk_tier: standard`. Branch
  `review/fnp-8-review`.
- [ledger-reading-1-ledger.md](ledger-reading-1-ledger.md) —
  **LEDGER-READING-1 step 1 (2026-09-09), in flight:**   reading units may prove clauses on
  document evidence (R-10). A staging ledger whose first 40 lines carry the READING value of
  the `Path` header field is exempt from the grammar gate's rule B; rules A and C stay armed,
  the reading evidence shape is `docs: <path>#<heading-anchor>` (a convention, not a gate
  check), and `EXCEPTIONS` is untouched. Step 2 (2026-09-09) flipped BALLISTA-AUDIT-0's
  twelve clauses to `PROVEN` on `docs:` cells. Branch `feat/ledger-reading-1`.
  pins: ledger-reading-1/C-001, C-002, C-003, C-004
- [sql-describe-1-ledger.md](sql-describe-1-ledger.md) — Unit ledger — SQL-DESCRIBE-1 · `DESCRIBE [TABLE] [EXTENDED|FORMATTED]` on Iceberg tables (step 1: measurement)

## Pointers
- Up: [../map.md](../map.md)
- Policy: [../../../AGENTS.md](../../../AGENTS.md) "Markdown document lifecycle"
